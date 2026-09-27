package cache

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// writeV1 在磁盘构造 v1 旧版条目：magic + version=1 + 裸负载。
func writeV1(t *testing.T, dir string, key Key, payload []byte) {
	t.Helper()
	var data []byte
	data = append(data, entryMagic...)
	data = binary.BigEndian.AppendUint16(data, 1)
	data = append(data, payload...)
	if err := os.WriteFile(filepath.Join(dir, string(key)), data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// writeV2 在磁盘构造 v2 旧版条目：magic + version=2 + 长度 + SHA-256 + 负载。
func writeV2(t *testing.T, dir string, key Key, payload []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, string(key)), encodeEntryV2(payload), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestV1V2EntriesUpgradeToV3 用含 v1/v2 旧条目的目录启动：
// 二者都按确定规则升级为 v3（mtime 作为创建时间、默认 TTL 套用），
// 升级后立即可命中，磁盘上的文件已是 v3 格式，计数准确。
func TestV1V2EntriesUpgradeToV3(t *testing.T) {
	dir := t.TempDir()
	v1Key, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"legacy-v1"}})
	v2Key, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"legacy-v2"}})
	v1Payload, v2Payload := []byte("v1 artifact"), []byte("v2 artifact")
	writeV1(t, dir, v1Key, v1Payload)
	writeV2(t, dir, v2Key, v2Payload)

	// 迁移条目的创建时间取文件 mtime（真实时间），可控时钟对齐到当前，
	// 使“刚迁移”的条目在默认 TTL 内不会过期。
	clk := NewFakeClock(time.Now())
	c, err := OpenWithOptions(dir, 1<<20,
		OpenOptions{DefaultTTL: time.Hour, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	s := c.Stats()
	logStats(t, c)
	if s.Migrated != 2 || s.Rejected != 0 {
		t.Fatalf("migrated=%d rejected=%d want 2/0", s.Migrated, s.Rejected)
	}

	// 升级后的条目可直接命中，内容无损。
	for _, tc := range []struct {
		key     Key
		payload []byte
	}{{v1Key, v1Payload}, {v2Key, v2Payload}} {
		r, err := c.GetOrBuild(context.Background(), tc.key,
			func() ([]byte, error) { return nil, errors.New("should not build") })
		if err != nil {
			t.Fatal(err)
		}
		logResult(t, r)
		if !r.Hit || string(r.Payload) != string(tc.payload) {
			t.Fatalf("upgraded entry not usable: %+v payload=%q", r, r.Payload)
		}
	}

	// 磁盘文件必须已是 v3 格式（绝不保留旧格式被当新格式使用）。
	for _, k := range []Key{v1Key, v2Key} {
		data, err := os.ReadFile(filepath.Join(dir, string(k)))
		if err != nil {
			t.Fatal(err)
		}
		if v := binary.BigEndian.Uint16(data[len(entryMagic):]); v != FormatVersion {
			t.Fatalf("key %s on-disk version=%d want v3", k[:8], v)
		}
		if _, err := decodeEntryV3(data); err != nil {
			t.Fatalf("upgraded file not decodable as v3: %v", err)
		}
	}

	// 升级结果确定性：再次用同一目录打开，不应再有迁移/拒绝，
	// 且条目继续可用（迁移是一次性的、幂等的）。
	c2, err := OpenWithOptions(dir, 1<<20,
		OpenOptions{DefaultTTL: time.Hour, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	if s := c2.Stats(); s.Migrated != 0 || s.Rejected != 0 {
		t.Fatalf("second open: migrated=%d rejected=%d want 0/0", s.Migrated, s.Rejected)
	}
	r, _ := c2.GetOrBuild(context.Background(), v2Key,
		func() ([]byte, error) { return nil, errors.New("should not build") })
	logResult(t, r)
	if !r.Hit {
		t.Fatalf("entry should remain usable on second open: %+v", r)
	}
	if d := diskUsage(t, dir); d != c2.Stats().Bytes {
		t.Fatalf("disk=%d stats=%d", d, c2.Stats().Bytes)
	}
}

// TestUpgradedTTLComesFromInstanceDefault 旧条目升级时 TTL 取实例默认值：
// 带默认 TTL 打开时，旧条目升级后按 mtime+默认TTL 到期；
// 不带默认 TTL 打开时，升级条目永久有效。
func TestUpgradedTTLComesFromInstanceDefault(t *testing.T) {
	dir := t.TempDir()
	key, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"legacy-ttl"}})
	writeV2(t, dir, key, []byte("old"))

	// 不带默认 TTL：升级后永久有效，虚拟时间任意推进仍命中。
	clk := NewFakeClock(time.Date(2026, 9, 27, 17, 0, 0, 0, time.UTC))
	c, _ := OpenWithOptions(dir, 1<<20, OpenOptions{Clock: clk})
	if s := c.Stats(); s.Migrated != 1 {
		t.Fatalf("migrated=%d want 1", s.Migrated)
	}
	clk.Advance(10 * 365 * 24 * time.Hour)
	r, _ := c.GetOrBuild(context.Background(), key,
		func() ([]byte, error) { return []byte("rebuilt"), nil })
	logResult(t, r)
	if !r.Hit {
		t.Fatalf("upgraded entry with no default TTL must never expire: %+v", r)
	}

	// 全新目录：带上默认 TTL 打开，旧条目升级后按该 TTL 判定。
	// 创建时间取 mtime（近似现在），虚拟时钟从现在开始推进超过默认 TTL 即过期。
	dir2 := t.TempDir()
	key2, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"legacy-ttl2"}})
	writeV2(t, dir2, key2, []byte("old2"))
	c2, _ := OpenWithOptions(dir2, 1<<20,
		OpenOptions{DefaultTTL: time.Minute, Clock: NewFakeClock(time.Now())})
	r, _ = c2.GetOrBuild(context.Background(), key2,
		func() ([]byte, error) { return []byte("ok"), nil })
	logResult(t, r)
	if !r.Hit {
		t.Fatalf("freshly upgraded entry should hit before TTL: %+v", r)
	}
	// 用推进后的可控时钟重新打开并访问：超过默认 TTL 必须判为过期重建。
	clk2 := NewFakeClock(time.Now().Add(2 * time.Minute))
	c3, _ := OpenWithOptions(dir2, 1<<20,
		OpenOptions{DefaultTTL: time.Minute, Clock: clk2})
	r, _ = c3.GetOrBuild(context.Background(), key2,
		func() ([]byte, error) { return []byte("rebuilt"), nil })
	logResult(t, r)
	if r.Hit || r.Reason != ReasonMissExpired {
		t.Fatalf("upgraded entry should expire at mtime+defaultTTL, got %+v", r)
	}
}

// TestUnsupportedAndCorruptedLegacyEntriesRejected 无法识别/损坏的旧条目
// 必须被明确拒绝并计数，绝不被静默当成新格式；拒绝后访问按未命中重建。
func TestUnsupportedAndCorruptedLegacyEntriesRejected(t *testing.T) {
	dir := t.TempDir()
	junkKey, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"junk"}})
	futureKey, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"future"}})
	badV2Key, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"bad-v2"}})
	badV3Key, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"bad-v3"}})

	// 无 magic：拒绝。
	if err := os.WriteFile(filepath.Join(dir, string(junkKey)), []byte("random bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	// 高于当前的版本号（v99）：拒绝，绝不静默使用。
	var future []byte
	future = append(future, entryMagic...)
	future = binary.BigEndian.AppendUint16(future, 99)
	future = append(future, []byte("future payload")...)
	if err := os.WriteFile(filepath.Join(dir, string(futureKey)), future, 0o644); err != nil {
		t.Fatal(err)
	}
	// 校验和不匹配的 v2：迁移失败，拒绝。
	badV2 := encodeEntryV2([]byte("x"))
	badV2[len(badV2)-1] ^= 0xff
	if err := os.WriteFile(filepath.Join(dir, string(badV2Key)), badV2, 0o644); err != nil {
		t.Fatal(err)
	}
	// 截断的当前版本 v3：拒绝。
	badV3 := encodeEntryV3([]byte("z"), time.Now(), 0)
	badV3 = badV3[:len(badV3)-1]
	if err := os.WriteFile(filepath.Join(dir, string(badV3Key)), badV3, 0o644); err != nil {
		t.Fatal(err)
	}

	c, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	s := c.Stats()
	logStats(t, c)
	if s.Rejected != 4 {
		t.Fatalf("rejected=%d want 4", s.Rejected)
	}
	if s.Entries != 0 {
		t.Fatalf("entries=%d want 0", s.Entries)
	}

	// 被拒绝条目在磁盘上必须已不存在。
	for _, k := range []Key{junkKey, futureKey, badV2Key, badV3Key} {
		if _, err := os.Stat(filepath.Join(dir, string(k))); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rejected entry %s still on disk: %v", k[:8], err)
		}
	}

	// 被拒绝的条目按未命中处理，各自重建一次。
	var builds atomic.Int64
	b := func() ([]byte, error) { builds.Add(1); return []byte("rebuilt"), nil }
	for _, k := range []Key{junkKey, futureKey, badV2Key, badV3Key} {
		r, _ := c.GetOrBuild(context.Background(), k, b)
		logResult(t, r)
		if r.Hit {
			t.Fatalf("rejected key %s must not hit", k[:8])
		}
	}
	if builds.Load() != 4 {
		t.Fatalf("builds=%d want 4", builds.Load())
	}
	if d := diskUsage(t, dir); d != c.Stats().Bytes {
		t.Fatalf("disk=%d stats=%d", d, c.Stats().Bytes)
	}
}
