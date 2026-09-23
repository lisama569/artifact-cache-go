package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FormatVersion 为当前条目格式版本。
//
// 版本兼容规则（确定性）：
//   - v2（当前）：magic + version + 长度 + SHA-256 + 负载；
//   - v1（旧版）：magic + version=1 + 裸负载，无校验和。可无损迁移到 v2；
//   - 其他（无 magic、版本高于当前、或版本无法识别）：明确拒绝并移除，
//     绝不静默当作当前版本使用。
const FormatVersion = 2

var entryMagic = []byte("ACACHE")

// 条目读取失败的可区分原因。
var (
	// ErrCorrupted 表示条目截断或内容损坏（按未命中处理）。
	ErrCorrupted = errors.New("cache: corrupted entry")
	// ErrUnsupportedVersion 表示条目版本不被支持且不可迁移。
	ErrUnsupportedVersion = errors.New("cache: unsupported entry version")
)

var entryHeaderLen = len(entryMagic) + 2 + 8 + sha256.Size

// encodeEntryV2 以当前格式编码条目。
func encodeEntryV2(payload []byte) []byte {
	buf := make([]byte, 0, entryHeaderLen+len(payload))
	buf = append(buf, entryMagic...)
	buf = binary.BigEndian.AppendUint16(buf, FormatVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(payload)))
	sum := sha256.Sum256(payload)
	buf = append(buf, sum[:]...)
	buf = append(buf, payload...)
	return buf
}

// writeEntryAtomic 将产物原子写入 path：先写同目录临时文件、fsync、再 rename，
// 使并发读者只能看到完整旧条目或完整新条目，绝不看到部分写入。
func writeEntryAtomic(path string, payload []byte) error {
	data := encodeEntryV2(payload)
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

// readEntry 读取并校验当前版本条目。
// 截断、长度不符或校验和不匹配返回 ErrCorrupted；
// 旧版本或无法识别的条目返回 ErrUnsupportedVersion（由 Open 统一迁移或拒绝）。
func readEntry(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return decodeEntry(data)
}

// decodeEntry 解析条目字节流。
func decodeEntry(data []byte) ([]byte, error) {
	if len(data) < len(entryMagic)+2 || !bytes.Equal(data[:len(entryMagic)], entryMagic) {
		return nil, ErrUnsupportedVersion
	}
	version := binary.BigEndian.Uint16(data[len(entryMagic):])
	switch version {
	case 1:
		return nil, ErrUnsupportedVersion // 由 migrateEntry 处理
	case FormatVersion:
		// 继续解析
	default:
		return nil, fmt.Errorf("%w: v%d", ErrUnsupportedVersion, version)
	}
	if len(data) < entryHeaderLen {
		return nil, fmt.Errorf("%w: truncated header", ErrCorrupted)
	}
	body := data[len(entryMagic)+2:]
	length := binary.BigEndian.Uint64(body[:8])
	wantSum := body[8 : 8+sha256.Size]
	payload := body[8+sha256.Size:]
	if uint64(len(payload)) != length {
		return nil, fmt.Errorf("%w: length mismatch (header %d, actual %d)", ErrCorrupted, length, len(payload))
	}
	sum := sha256.Sum256(payload)
	if !bytes.Equal(sum[:], wantSum) {
		return nil, fmt.Errorf("%w: checksum mismatch", ErrCorrupted)
	}
	return payload, nil
}

// migrateEntry 尝试将旧版本条目迁移到当前格式。
// v1（裸负载）可无损迁移；其余返回 ErrUnsupportedVersion。
func migrateEntry(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	magicBuf := make([]byte, len(entryMagic)+2)
	if _, err := io.ReadFull(f, magicBuf); err != nil {
		return ErrUnsupportedVersion
	}
	if !bytes.Equal(magicBuf[:len(entryMagic)], entryMagic) {
		return ErrUnsupportedVersion
	}
	if binary.BigEndian.Uint16(magicBuf[len(entryMagic):]) != 1 {
		return ErrUnsupportedVersion
	}
	payload, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	return writeEntryAtomic(path, payload)
}

// fileSize 返回文件大小，不存在时返回 0。
func fileSize(path string) int64 {
	if fi, err := os.Stat(path); err == nil {
		return fi.Size()
	}
	return 0
}
