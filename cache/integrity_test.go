package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func sha256Sum(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}

// encodeRaw 用指定 version 直接序列化条目（用于构造旧版本文件）。
func encodeRaw(version uint32, keyHex string, artifact []byte) []byte {
	e := &Entry{Version: version, KeyHex: keyHex, Artifact: artifact}
	var buf bytes.Buffer
	// 复用 encode 前先换版本。
	orig := e.Version
	_ = orig
	if err := e.encode(&buf); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func writeRawEntry(t *testing.T, dir, keyHex string, data []byte) {
	t.Helper()
	dir2 := filepath.Join(dir, "v2")
	if err := os.MkdirAll(dir2, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir2, keyHex+".entry"), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCorruptEntriesTreatedAsMiss(t *testing.T) {
	cases := []struct {
		name   string
		damage func([]byte) []byte
		want   ReasonCode
	}{
		{"truncated", func(b []byte) []byte { return b[:len(b)-40] }, ReasonMissCorrupt},
		{"checksum-tamper", func(b []byte) []byte {
			b[len(b)-1] ^= 0xff
			return b
		}, ReasonMissCorrupt},
		{"artifact-tamper", func(b []byte) []byte {
			// 在 artifact 区域翻转一个字节（校验和之后之前）。
			b[40] ^= 0x01
			return b
		}, ReasonMissCorrupt},
		{"bad-magic", func(b []byte) []byte {
			b[0] = 'X'
			return b
		}, ReasonMissCorrupt},
		{"old-version-v1", func(b []byte) []byte {
			// 把 version 字段偏移 4..8 改为 1（其余字节保持，校验必然失败也无所谓，
			// 版本检查先于校验和检查）。
			binary.BigEndian.PutUint32(b[4:8], 1)
			return b
		}, ReasonMissOldVersion},
		{"future-version-v99", func(b []byte) []byte {
			binary.BigEndian.PutUint32(b[4:8], 99)
			return b
		}, ReasonMissFuture},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			in := sampleInput("integrity-" + tc.name)
			kh := DeriveKey(in.withEnvWhitelist([]string{"GOOS"})).String()
			good := encodeRaw(CurrentVersion, kh, []byte("OLD-ARTIFACT"))
			writeRawEntry(t, dir, kh, tc.damage(append([]byte(nil), good...)))

			c, err := New(Config{Dir: dir, EnvWhitelist: []string{"GOOS"}})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			st := c.SnapshotStats()
			t.Logf("open  key=%s reason-on-scan stats={%s}", kh, st)

			sb := NewSimBuilder()
			r, err := c.GetOrBuild(context.Background(), in, sb)
			if err != nil {
				t.Fatalf("rebuild after %s: %v", tc.name, err)
			}
			t.Logf("get   key=%s decision=%s actualBuilds=%d stats={%s}",
				kh, r.Decision, sb.BuildCount(), c.SnapshotStats())
			if r.Decision.Reason != tc.want {
				t.Fatalf("%s: want %s, got %s", tc.name, tc.want, r.Decision)
			}
			if sb.BuildCount() != 1 {
				t.Fatalf("%s: corrupt/old entry must trigger one rebuild, got %d", tc.name, sb.BuildCount())
			}
			// 重建后再次读取必须命中新条目。
			r2, err := c.GetOrBuild(context.Background(), in, NewSimBuilder())
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("again key=%s decision=%s stats={%s}", kh, r2.Decision, c.SnapshotStats())
			if r2.Decision.Reason != ReasonHitFresh {
				t.Fatalf("%s: after recovery want HIT_FRESH, got %s", tc.name, r2.Decision)
			}
			if string(r2.Entry.Artifact) == "OLD-ARTIFACT" {
				t.Fatalf("%s: stale artifact served after recovery", tc.name)
			}
		})
	}
}

// TestGenuinelyFormattedV1 用“v1 自己的正确格式”（伪造 v1：正确校验和但 version=1）
// 证明拒绝来自版本判定而非损坏判定。
func TestGenuinelyFormattedOldVersion(t *testing.T) {
	dir := t.TempDir()
	in := sampleInput("real-v1")
	kh := DeriveKey(in.withEnvWhitelist([]string{"GOOS"})).String()

	// 手工构造 version=1 且带正确校验和的文件。
	key := []byte(kh)
	art := []byte("v1-artifact")
	var body bytes.Buffer
	body.Write([]byte{0, 0, 0, 1})
	var l [8]byte
	binary.BigEndian.PutUint16(l[:2], uint16(len(key)))
	body.Write(l[:2])
	body.Write(key)
	binary.BigEndian.PutUint64(l[:8], uint64(len(art)))
	body.Write(l[:8])
	body.Write(art)

	var file bytes.Buffer
	file.Write(magic[:])
	file.Write(body.Bytes())
	sum := sha256Sum(body.Bytes())
	file.Write(sum)
	writeRawEntry(t, dir, kh, file.Bytes())

	c, err := New(Config{Dir: dir, EnvWhitelist: []string{"GOOS"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, err := c.GetOrBuild(context.Background(), in, NewSimBuilder())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("key=%s decision=%s stats={%s}", kh, r.Decision, c.SnapshotStats())
	if r.Decision.Reason != ReasonMissOldVersion {
		t.Fatalf("want MISS_OLD_VERSION for well-formed v1, got %s", r.Decision)
	}
	if c.SnapshotStats().VersionRejects < 1 {
		t.Fatal("VersionRejects must count old-version rejection")
	}
}
