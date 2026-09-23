package cache

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// CurrentVersion 是当前条目格式版本。
const CurrentVersion uint32 = 2

// supportedReadVersions 是当前代码可读取的版本集合。
// v2 为当前格式；v1（旧版，无校验和）不做静默迁移，按未命中明确拒绝。
var supportedReadVersions = map[uint32]bool{CurrentVersion: true}

// 磁盘格式（全部大端）：
//
//	magic    = "ACHE"        (4B)
//	version  uint32          (4B)
//	keyLen   uint16          (2B)
//	keyHex   bytes[keyLen]
//	artLen   uint64          (8B)
//	artifact bytes[artLen]
//	checksum sha256(version..artifact 的全部字节) (32B)
var magic = [4]byte{'A', 'C', 'H', 'E'}

const headerFix = 4 + 4 + 2 + 8 // magic+version+keyLen+artLen；不含变长字段与校验和

// Entry 是缓存条目在内存中的表示。
type Entry struct {
	Version  uint32
	KeyHex   string
	Artifact []byte
}

// Size 返回条目在磁盘上占用的字节数（与实际文件大小口径一致）。
func (e *Entry) Size() int64 {
	return int64(headerFix + len(e.KeyHex) + len(e.Artifact) + sha256.Size)
}

var (
	errTruncated   = errors.New("entry truncated")
	errMagic       = errors.New("entry magic mismatch")
	errChecksum    = errors.New("entry checksum mismatch")
	errKeyMismatch = errors.New("entry key does not match index key")
)

// CorruptError 表示条目被截断或损坏。
type CorruptError struct {
	Op  string
	Err error
}

func (e *CorruptError) Error() string { return fmt.Sprintf("corrupt entry (%s): %v", e.Op, e.Err) }
func (e *CorruptError) Unwrap() error { return e.Err }

// VersionError 表示条目版本不受支持（过旧或过新），必须明确拒绝。
type VersionError struct{ Found uint32 }

func (e *VersionError) Error() string {
	return fmt.Sprintf("unsupported entry version: found=%d current=%d", e.Found, CurrentVersion)
}

// IsOld 报告是否为旧版本条目。
func (e *VersionError) IsOld() bool { return e.Found < CurrentVersion }

// encode 将条目序列化为带校验和的磁盘格式。
func (e *Entry) encode(w io.Writer) error {
	key := []byte(e.KeyHex)
	if len(key) > 0xffff {
		return fmt.Errorf("key too long: %d", len(key))
	}
	h := sha256.New()
	// magic 不参与校验和；h 覆盖 version..artifact，与 decodeEntry 一致。
	if _, err := w.Write(magic[:]); err != nil {
		return err
	}
	mw := io.MultiWriter(w, h)

	var buf [8]byte
	binary.BigEndian.PutUint32(buf[:4], e.Version)
	if _, err := mw.Write(buf[:4]); err != nil {
		return err
	}
	binary.BigEndian.PutUint16(buf[:2], uint16(len(key)))
	if _, err := mw.Write(buf[:2]); err != nil {
		return err
	}
	if _, err := mw.Write(key); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(buf[:8], uint64(len(e.Artifact)))
	if _, err := mw.Write(buf[:8]); err != nil {
		return err
	}
	if _, err := mw.Write(e.Artifact); err != nil {
		return err
	}
	if _, err := w.Write(h.Sum(nil)); err != nil {
		return err
	}
	return nil
}

// decodeEntry 从 r 解析条目；截断、魔数/校验失败或版本不兼容时返回错误。
// 校验范围为 version 起到 artifact 止的全部字节（不含 magic 与末尾校验和）。
func decodeEntry(r io.Reader) (*Entry, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(data) < headerFix+sha256.Size {
		return nil, &CorruptError{Op: "length", Err: fmt.Errorf("%w: got %d bytes", errTruncated, len(data))}
	}
	if !bytes.Equal(data[:4], magic[:]) {
		return nil, &CorruptError{Op: "magic", Err: errMagic}
	}
	off := 4
	version := binary.BigEndian.Uint32(data[off : off+4])
	if !supportedReadVersions[version] {
		return nil, &VersionError{Found: version}
	}
	off += 4
	keyLen := int(binary.BigEndian.Uint16(data[off : off+2]))
	off += 2
	bodyEnd := len(data) - sha256.Size // checksum 起点
	if off+keyLen+8 > bodyEnd {
		return nil, &CorruptError{Op: "header", Err: errTruncated}
	}
	keyHex := string(data[off : off+keyLen])
	off += keyLen
	artLen := int(binary.BigEndian.Uint64(data[off : off+8]))
	off += 8
	if off+artLen != bodyEnd {
		return nil, &CorruptError{Op: "length", Err: fmt.Errorf("%w: declared artifact=%d remaining=%d", errTruncated, artLen, bodyEnd-off)}
	}
	artifact := data[off : off+artLen]

	mac := sha256.New()
	mac.Write(data[4:bodyEnd])
	if !bytes.Equal(mac.Sum(nil), data[bodyEnd:]) {
		return nil, &CorruptError{Op: "checksum", Err: errChecksum}
	}
	art := make([]byte, artLen)
	copy(art, artifact)
	return &Entry{Version: version, KeyHex: keyHex, Artifact: art}, nil
}

func entryPath(dir, keyHex string) string {
	return filepath.Join(dir, "v2", keyHex+".entry")
}

func tempEntryPath(dir, keyHex string) string {
	return filepath.Join(dir, "v2", "."+keyHex+".tmp")
}

// readEntryFile 读取并校验条目文件；wantKeyHex 非空时还会核对条目内嵌键。
func readEntryFile(path, wantKeyHex string) (*Entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	e, err := decodeEntry(f)
	if err != nil {
		return nil, err
	}
	if wantKeyHex != "" && e.KeyHex != wantKeyHex {
		return nil, &CorruptError{Op: "key", Err: fmt.Errorf("%w: file=%s want=%s", errKeyMismatch, e.KeyHex, wantKeyHex)}
	}
	return e, nil
}

// writeEntryFile 以“临时文件 + fsync + rename”原子写入条目；
// 读者要么看到完整旧文件，要么看到完整新文件，不会看到部分写入。
func writeEntryFile(path string, e *Entry) (err error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".write-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()
	if err = e.encode(tmp); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpName, path); err != nil {
		return err
	}
	if d, derr := os.Open(dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// validHexKey 仅用于扫描时过滤临时文件与非条目文件。
func validHexKey(name string) (string, bool) {
	if len(name) != 64+len(".entry") {
		return "", false
	}
	hexPart := name[:64]
	if name[64:] != ".entry" {
		return "", false
	}
	if _, err := hex.DecodeString(hexPart); err != nil {
		return "", false
	}
	return hexPart, true
}
