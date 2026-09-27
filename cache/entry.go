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
	"time"
)

// v2 头部长度（升级前的上一代格式）：magic(6) + version(2) + length(8) + sha256(32)。
var entryHeaderLenV2 = len(entryMagic) + 2 + 8 + sha256.Size

// FormatVersion 为当前条目格式版本。
//
// 版本兼容规则（确定性）：
//   - v3（当前）：magic + version + 长度 + SHA-256 + 创建时间 + TTL + 负载，
//     有效期随条目持久化；
//   - v2（旧版）：magic + version=2 + 长度 + SHA-256 + 负载，无时间字段。
//     Open 时按确定规则升级到 v3：创建时间取文件 mtime，TTL 取实例默认值；
//   - v1（旧版）：magic + version=1 + 裸负载，无校验和与时间字段。升级规则同 v2；
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

// v3 头部长度：magic(6) + version(2) + length(8) + sha256(32)
// + created_unix_nano(8) + ttl_nanos(8)。
var entryHeaderLen = len(entryMagic) + 2 + 8 + sha256.Size + 8 + 8

// parsedEntry 为 v3 条目解析结果。
type parsedEntry struct {
	payload []byte
	created time.Time // 条目写入（构建完成）时刻
	ttl     time.Duration
}

// expiresAt 返回条目绝对过期时刻；ttl<=0 表示永不过期，ok 为 false。
func (p parsedEntry) expiresAt() (time.Time, bool) {
	if p.ttl <= 0 {
		return time.Time{}, false
	}
	return p.created.Add(p.ttl), true
}

// expiredAt 判断条目在 now 时刻是否已过期：now >= created+ttl 即过期。
// ttl<=0 的条目永不过期。
func (p parsedEntry) expiredAt(now time.Time) bool {
	at, ok := p.expiresAt()
	return ok && !now.Before(at)
}

// encodeEntryV2 以上一代 v2 格式编码条目。仅用于测试构造旧格式条目
// （校验升级与拒绝行为）。v2：magic + version=2 + 长度 + SHA-256 + 负载。
func encodeEntryV2(payload []byte) []byte {
	buf := make([]byte, 0, entryHeaderLenV2+len(payload))
	buf = append(buf, entryMagic...)
	buf = binary.BigEndian.AppendUint16(buf, 2)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(payload)))
	sum := sha256.Sum256(payload)
	buf = append(buf, sum[:]...)
	buf = append(buf, payload...)
	return buf
}

// fileSize 返回文件大小，不存在时返回 0。
func fileSize(path string) int64 {
	if fi, err := os.Stat(path); err == nil {
		return fi.Size()
	}
	return 0
}

// encodeEntryV3 以当前（v3）格式编码条目。
// created 为条目写入时刻；ttl<=0 表示该条目永不过期（TTL 字段存 0）。
func encodeEntryV3(payload []byte, created time.Time, ttl time.Duration) []byte {
	ttlNanos := int64(0)
	if ttl > 0 {
		ttlNanos = int64(ttl)
	}
	buf := make([]byte, 0, entryHeaderLen+len(payload))
	buf = append(buf, entryMagic...)
	buf = binary.BigEndian.AppendUint16(buf, FormatVersion)
	buf = binary.BigEndian.AppendUint64(buf, uint64(len(payload)))
	sum := sha256.Sum256(payload)
	buf = append(buf, sum[:]...)
	buf = binary.BigEndian.AppendUint64(buf, uint64(created.UnixNano()))
	buf = binary.BigEndian.AppendUint64(buf, uint64(ttlNanos))
	buf = append(buf, payload...)
	return buf
}

// writeEntryAtomicV3 将 v3 条目原子写入 path：先写同目录临时文件、fsync、
// 再 rename，使并发读者只能看到完整旧条目或完整新条目。
func writeEntryAtomicV3(path string, payload []byte, created time.Time, ttl time.Duration) error {
	data := encodeEntryV3(payload, created, ttl)
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

// readEntryV3 读取并校验 v3 条目。截断、长度不符或校验和不匹配返回
// ErrCorrupted；v1/v2 旧版本或无法识别条目返回 ErrUnsupportedVersion
// （由 Open 统一迁移或拒绝）。
func readEntryV3(path string) (parsedEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return parsedEntry{}, err
	}
	return decodeEntryV3(data)
}

// decodeEntryV3 解析 v3 条目字节流。
func decodeEntryV3(data []byte) (parsedEntry, error) {
	if len(data) < len(entryMagic)+2 || !bytes.Equal(data[:len(entryMagic)], entryMagic) {
		return parsedEntry{}, ErrUnsupportedVersion
	}
	version := binary.BigEndian.Uint16(data[len(entryMagic):])
	switch version {
	case 1, 2:
		return parsedEntry{}, ErrUnsupportedVersion // 由 migrateEntryV3 处理
	case FormatVersion:
		// 继续解析
	default:
		return parsedEntry{}, fmt.Errorf("%w: v%d", ErrUnsupportedVersion, version)
	}
	if len(data) < entryHeaderLen {
		return parsedEntry{}, fmt.Errorf("%w: truncated header", ErrCorrupted)
	}
	body := data[len(entryMagic)+2:]
	length := binary.BigEndian.Uint64(body[:8])
	wantSum := body[8 : 8+sha256.Size]
	created := time.Unix(0, int64(binary.BigEndian.Uint64(body[8+sha256.Size:8+sha256.Size+8])))
	ttl := time.Duration(binary.BigEndian.Uint64(body[8+sha256.Size+8 : 8+sha256.Size+16]))
	payload := body[8+sha256.Size+16:]
	if uint64(len(payload)) != length {
		return parsedEntry{}, fmt.Errorf("%w: length mismatch (header %d, actual %d)", ErrCorrupted, length, len(payload))
	}
	sum := sha256.Sum256(payload)
	if !bytes.Equal(sum[:], wantSum) {
		return parsedEntry{}, fmt.Errorf("%w: checksum mismatch", ErrCorrupted)
	}
	return parsedEntry{payload: payload, created: created, ttl: ttl}, nil
}

// migrateEntryV3 将旧版本条目升级为 v3。
//
// 确定规则（结果与是否在升级前被读取无关，重复执行结果一致）：
//   - v1（裸负载，无校验和）：头部之后的全部字节即负载；
//   - v2（长度 + SHA-256 + 负载）：按长度与校验和校验后取出负载；
//   - 创建时间：旧格式不带时间，统一取文件 mtime（升级时刻即落盘时刻，
//     重启结果确定且可解释）；
//   - TTL：旧格式不带 TTL，统一套用实例默认 TTL defaultTTL（<=0 为永不过期）。
//
// 无 magic、未知版本、v2 校验失败等情况返回 ErrUnsupportedVersion，
// 由调用方明确拒绝并移除。
func migrateEntryV3(path string, defaultTTL time.Duration) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	head := make([]byte, len(entryMagic)+2)
	if _, err := io.ReadFull(f, head); err != nil {
		f.Close()
		return ErrUnsupportedVersion
	}
	if !bytes.Equal(head[:len(entryMagic)], entryMagic) {
		f.Close()
		return ErrUnsupportedVersion
	}
	version := binary.BigEndian.Uint16(head[len(entryMagic):])

	var payload []byte
	switch version {
	case 1:
		payload, err = io.ReadAll(f)
		f.Close()
		if err != nil {
			return err
		}
	case 2:
		rest, rerr := io.ReadAll(f)
		f.Close()
		if rerr != nil {
			return rerr
		}
		if len(rest) < 8+sha256.Size {
			return ErrUnsupportedVersion
		}
		length := binary.BigEndian.Uint64(rest[:8])
		wantSum := rest[8 : 8+sha256.Size]
		payload = rest[8+sha256.Size:]
		if uint64(len(payload)) != length {
			return ErrUnsupportedVersion
		}
		sum := sha256.Sum256(payload)
		if !bytes.Equal(sum[:], wantSum) {
			return ErrUnsupportedVersion
		}
	default:
		f.Close()
		return fmt.Errorf("%w: v%d", ErrUnsupportedVersion, version)
	}

	created := fi.ModTime()
	return writeEntryAtomicV3(path, payload, created, defaultTTL)
}
