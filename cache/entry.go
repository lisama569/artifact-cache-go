package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// FormatVersion 为当前条目格式版本。
//
// 版本兼容规则（确定性）：
//   - v3（当前）：magic + version + 过期时间(8) + 长度(8) + SHA-256 + 负载；
//   - v2（旧版）：magic + version=2 + 长度 + SHA-256 + 负载，无过期时间。
//     校验和通过后可无损迁移到 v3（过期时间置 0 = 永不过期）；
//   - v1（旧版）：magic + version=1 + 裸负载，无校验和。可无损迁移到 v3；
//   - 其他（无 magic、版本高于当前、或版本无法识别）：明确拒绝并移除，
//     绝不静默当作当前版本使用。
const FormatVersion = 3

var entryMagic = []byte("ACACHE")

// 条目读取失败的可区分原因。
var (
	// ErrCorrupted 表示条目截断或内容损坏（按未命中处理）。
	ErrCorrupted = errors.New("cache: corrupted entry")
	// ErrUnsupportedVersion 表示条目版本不被支持且不可迁移。
	ErrUnsupportedVersion = errors.New("cache: unsupported entry version")
)

// 当前格式头长：magic + version(2) + expiresAt(8) + length(8) + sha256(32)。
var entryHeaderLen = len(entryMagic) + 2 + 8 + 8 + sha256.Size

// v2 旧格式头长：magic + version(2) + length(8) + sha256(32)。
var entryHeaderLenV2 = len(entryMagic) + 2 + 8 + sha256.Size

// encodeEntry 以当前格式（v3）编码条目。
// expiresAt 为零值表示永不过期，持久化为 0。
func encodeEntry(payload []byte, expiresAt time.Time) []byte {
	var nano int64
	if !expiresAt.IsZero() {
		nano = expiresAt.UnixNano()
	}
	buf := make([]byte, 0, entryHeaderLen+len(payload))
	buf = append(buf, entryMagic...)
	buf = binary.BigEndian.AppendUint16(buf, FormatVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(nano))
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(payload)))
	sum := sha256.Sum256(payload)
	buf = append(buf, sum[:]...)
	buf = append(buf, payload...)
	return buf
}

// writeEntryAtomic 将产物原子写入 path：先写同目录临时文件、fsync、再 rename，
// 使并发读者只能看到完整旧条目或完整新条目，绝不看到部分写入。
func writeEntryAtomic(path string, payload []byte, expiresAt time.Time) error {
	data := encodeEntry(payload, expiresAt)
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

// readEntry 读取并校验当前版本条目，返回负载与过期时间（零值 = 永不过期）。
// 截断、长度不符或校验和不匹配返回 ErrCorrupted；
// 旧版本或无法识别的条目返回 ErrUnsupportedVersion（由 Open 统一迁移或拒绝）。
func readEntry(path string) ([]byte, time.Time, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, time.Time{}, err
	}
	return decodeEntry(data)
}

// decodeEntry 解析当前版本（v3）条目字节流。
func decodeEntry(data []byte) ([]byte, time.Time, error) {
	if len(data) < len(entryMagic)+2 || !bytes.Equal(data[:len(entryMagic)], entryMagic) {
		return nil, time.Time{}, ErrUnsupportedVersion
	}
	version := binary.BigEndian.Uint16(data[len(entryMagic):])
	switch version {
	case 1, 2:
		return nil, time.Time{}, ErrUnsupportedVersion // 由 migrateEntry 处理
	case FormatVersion:
		// 继续解析
	default:
		return nil, time.Time{}, fmt.Errorf("%w: v%d", ErrUnsupportedVersion, version)
	}
	if len(data) < entryHeaderLen {
		return nil, time.Time{}, fmt.Errorf("%w: truncated header", ErrCorrupted)
	}
	body := data[len(entryMagic)+2:]
	nano := int64(binary.BigEndian.Uint64(body[:8]))
	length := binary.BigEndian.Uint64(body[8:16])
	wantSum := body[16 : 16+sha256.Size]
	payload := body[16+sha256.Size:]
	if uint64(len(payload)) != length {
		return nil, time.Time{}, fmt.Errorf("%w: length mismatch (header %d, actual %d)", ErrCorrupted, length, len(payload))
	}
	sum := sha256.Sum256(payload)
	if !bytes.Equal(sum[:], wantSum) {
		return nil, time.Time{}, fmt.Errorf("%w: checksum mismatch", ErrCorrupted)
	}
	var expiresAt time.Time
	if nano != 0 {
		expiresAt = time.Unix(0, nano)
	}
	return payload, expiresAt, nil
}

// migrateEntry 尝试将旧版本条目迁移到当前格式（v3）。
// 确定且可解释的规则：旧格式不含有效期信息，迁移后一律视为永不过期。
//   - v1（裸负载）：直接取出负载重写为 v3；
//   - v2（带长度与校验和）：校验通过后取出负载重写为 v3，校验失败拒绝迁移；
//   - 其余：返回 ErrUnsupportedVersion。
func migrateEntry(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) < len(entryMagic)+2 || !bytes.Equal(data[:len(entryMagic)], entryMagic) {
		return ErrUnsupportedVersion
	}
	version := binary.BigEndian.Uint16(data[len(entryMagic):])
	switch version {
	case 1:
		payload := data[len(entryMagic)+2:]
		return writeEntryAtomic(path, payload, time.Time{})
	case 2:
		if len(data) < entryHeaderLenV2 {
			return fmt.Errorf("%w: truncated v2 header", ErrCorrupted)
		}
		body := data[len(entryMagic)+2:]
		length := binary.BigEndian.Uint64(body[:8])
		wantSum := body[8 : 8+sha256.Size]
		payload := body[8+sha256.Size:]
		sum := sha256.Sum256(payload)
		if uint64(len(payload)) != length || !bytes.Equal(sum[:], wantSum) {
			return fmt.Errorf("%w: v2 checksum mismatch", ErrCorrupted)
		}
		return writeEntryAtomic(path, payload, time.Time{})
	default:
		return ErrUnsupportedVersion
	}
}

// fileSize 返回文件大小，不存在时返回 0。
func fileSize(path string) int64 {
	if fi, err := os.Stat(path); err == nil {
		return fi.Size()
	}
	return 0
}
