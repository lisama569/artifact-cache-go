package cache

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// manualClock 为可控时钟：测试里把逻辑时间往前推，无需真实等待。
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func newManualClock() *manualClock {
	return &manualClock{t: time.Unix(1_700_000_000, 0)}
}

func (c *manualClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func mustKey(t *testing.T, args ...string) Key {
	t.Helper()
	k, err := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: args})
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// TestExpiredEntryNotReused 验证：过期条目即使内容完好也不得复用，
// 且 "已过期" 与 "不存在"、"内容损坏" 三类原因可区分。
func TestExpiredEntryNotReused(t *testing.T) {
	dir := t.TempDir()
	clock := newManualClock()
	c, err := Open(dir, 1<<20, WithMaxAge(time.Hour), WithClock(clock.now))
	if err != nil {
		t.Fatal(err)
	}
	key := mustKey(t, "ttl")
	var builds atomic.Int64
	b := func() ([]byte, error) { builds.Add(1); return []byte("v1"), nil }

	r1, _ := c.GetOrBuild(context.Background(), key, b)
	logResult(t, r1)
	if r1.Hit || r1.Reason != "miss: not found" {
		t.Fatalf("first access: %+v", r1)
	}

	// 有效期内：命中，不重建。
	clock.advance(30 * time.Minute)
	r2, _ := c.GetOrBuild(context.Background(), key, b)
	logResult(t, r2)
	if !r2.Hit || r2.Reason != "hit" {
		t.Fatalf("within TTL should hit: %+v", r2)
	}

	// 超过有效期：不得复用，按未命中重建，原因可区分。
	clock.advance(31 * time.Minute) // 累计 61 分钟 > 1 小时
	r3, _ := c.GetOrBuild(context.Background(), key, b)
	logResult(t, r3)
	if r3.Hit || r3.Reason != "miss: expired entry" {
		t.Fatalf("expired entry must not be reused: %+v", r3)
	}
	if builds.Load() != 2 {
		t.Fatalf("builds=%d, want 2 (rebuild after expiry)", builds.Load())
	}

	// 三类未命中原因必须可区分。
	reasons := map[string]bool{r1.Reason: true, r3.Reason: true}
	path := filepath.Join(dir, string(key))
	data, _ := os.ReadFile(path)
	os.WriteFile(path, data[:len(data)/2], 0o644)
	r4, _ := c.GetOrBuild(context.Background(), key, b)
	logResult(t, r4)
	reasons[r4.Reason] = true
	if !strings.HasPrefix(r4.Reason, "miss: corrupted entry") {
		t.Fatalf("corrupted reason: %+v", r4)
	}
	if len(reasons) != 3 {
		t.Fatalf("reasons not distinguishable: %v", reasons)
	}
	if s := c.Stats(); s.Expired == 0 {
		t.Fatalf("expired counter not bumped: %+v", s)
	}
	logStats(t, c)
}

// TestPerWriteTTLOverride 验证单次写入可覆盖实例默认有效期。
func TestPerWriteTTLOverride(t *testing.T) {
	dir := t.TempDir()
	clock := newManualClock()
	c, err := Open(dir, 1<<20, WithMaxAge(time.Hour), WithClock(clock.now))
	if err != nil {
		t.Fatal(err)
	}
	shortKey := mustKey(t, "short")
	foreverKey := mustKey(t, "forever")
	b := func() ([]byte, error) { return []byte("x"), nil }

	// 单次指定更短的有效期。
	if _, err := c.GetOrBuildWithTTL(context.Background(), shortKey, time.Minute, b); err != nil {
		t.Fatal(err)
	}
	// 单次指定永不过期（覆盖默认 1 小时）。
	if _, err := c.GetOrBuildWithTTL(context.Background(), foreverKey, -1, b); err != nil {
		t.Fatal(err)
	}

	clock.advance(2 * time.Minute) // 超过单次指定的 1 分钟，但未超默认 1 小时
	r, _ := c.GetOrBuild(context.Background(), shortKey, b)
	logResult(t, r)
	if r.Hit || r.Reason != "miss: expired entry" {
		t.Fatalf("per-write TTL not applied: %+v", r)
	}
	r, _ = c.GetOrBuild(context.Background(), foreverKey, b)
	logResult(t, r)
	if !r.Hit {
		t.Fatalf("negative TTL should never expire: %+v", r)
	}

	clock.advance(2 * time.Hour) // 远超默认有效期
	r, _ = c.GetOrBuild(context.Background(), foreverKey, b)
	logResult(t, r)
	if !r.Hit {
		t.Fatalf("negative TTL should survive default maxAge: %+v", r)
	}
	logStats(t, c)
}

// TestExpiredAtOpenRemoved 验证：用既有目录启动时，已过期条目被移除并计数。
func TestExpiredAtOpenRemoved(t *testing.T) {
	dir := t.TempDir()
	clock := newManualClock()
	c, err := Open(dir, 1<<20, WithMaxAge(time.Hour), WithClock(clock.now))
	if err != nil {
		t.Fatal(err)
	}
	key := mustKey(t, "open-expired")
	if _, err := c.GetOrBuild(context.Background(), key,
		func() ([]byte, error) { return []byte("x"), nil }); err != nil {
		t.Fatal(err)
	}

	clock.advance(2 * time.Hour)
	c2, err := Open(dir, 1<<20, WithMaxAge(time.Hour), WithClock(clock.now))
	if err != nil {
		t.Fatal(err)
	}
	s := c2.Stats()
	logStats(t, c2)
	if s.Expired != 1 || s.Entries != 0 {
		t.Fatalf("expired entry should be dropped at open: %+v", s)
	}
	var builds atomic.Int64
	r, _ := c2.GetOrBuild(context.Background(), key,
		func() ([]byte, error) { builds.Add(1); return []byte("y"), nil })
	logResult(t, r)
	if r.Hit || builds.Load() != 1 {
		t.Fatalf("expired entry must be rebuilt after reopen: %+v", r)
	}
}

// TestConcurrentCorruptedEntryAccounting 验证：多个请求同时撞上同一个坏条目时，
// 只构建一次，且占用统计与磁盘始终一致（不重复扣减）。
func TestConcurrentCorruptedEntryAccounting(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	key := mustKey(t, "bad-entry")
	good := func() ([]byte, error) { return []byte("good"), nil }
	if _, err := c.GetOrBuild(context.Background(), key, good); err != nil {
		t.Fatal(err)
	}

	// 破坏条目内容。
	path := filepath.Join(dir, string(key))
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	const n = 32
	var builds atomic.Int64
	release := make(chan struct{})
	b := func() ([]byte, error) {
		builds.Add(1)
		<-release
		return []byte("good"), nil
	}
	var wg sync.WaitGroup
	results := make([]Result, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], _ = c.GetOrBuild(context.Background(), key, b)
		}(i)
	}
	for c.Stats().Misses < n {
	}
	close(release)
	wg.Wait()

	for i, r := range results {
		logResult(t, r)
		if string(r.Payload) != "good" {
			t.Fatalf("request %d payload=%q", i, r.Payload)
		}
	}
	if builds.Load() != 1 {
		t.Fatalf("builds=%d, want 1 (singleflight on corrupted entry)", builds.Load())
	}
	s := c.Stats()
	logStats(t, c)
	if d := diskUsage(t, dir); d != s.Bytes {
		t.Fatalf("stats bytes=%d != disk usage=%d after concurrent corrupted access", s.Bytes, d)
	}
	t.Logf("disk usage=%d matches stats bytes=%d", diskUsage(t, dir), s.Bytes)
}

// TestSweepCleansExpiredCorruptedOrphan 验证巡检的分类清理与计数，
// 且报告占用与磁盘完全一致。
func TestSweepCleansExpiredCorruptedOrphan(t *testing.T) {
	dir := t.TempDir()
	clock := newManualClock()
	c, err := Open(dir, 1<<20, WithMaxAge(time.Hour), WithClock(clock.now))
	if err != nil {
		t.Fatal(err)
	}
	b := func() ([]byte, error) { return []byte("payload"), nil }
	expKey := mustKey(t, "expire-me")
	okKey := mustKey(t, "keep-me")
	badKey := mustKey(t, "corrupt-me")
	for _, k := range []Key{expKey, okKey, badKey} {
		if _, err := c.GetOrBuild(context.Background(), k, b); err != nil {
			t.Fatal(err)
		}
	}

	// 制造三种待清理对象：过期、损坏、孤儿文件。
	clock.advance(2 * time.Hour) // 全部过期
	// 重建 okKey/badKey（不过期的新条目），留下 expKey 过期。
	if _, err := c.GetOrBuildWithTTL(context.Background(), okKey, -1, b); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetOrBuildWithTTL(context.Background(), badKey, -1, b); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, string(badKey))
	data, _ := os.ReadFile(path)
	os.WriteFile(path, data[:len(data)/2], 0o644) // 损坏
	orphan := filepath.Join(dir, "orphan-file")
	os.WriteFile(orphan, []byte("orphan"), 0o644) // 索引没有的孤儿

	rep := c.Sweep()
	t.Logf("sweep report: %+v", rep)
	if rep.Expired != 1 || rep.Corrupted != 1 || rep.Orphaned != 1 {
		t.Fatalf("sweep counts: %+v, want expired=1 corrupted=1 orphaned=1", rep)
	}
	if rep.Entries != 1 {
		t.Fatalf("entries after sweep=%d, want 1 (only okKey)", rep.Entries)
	}
	if d := diskUsage(t, dir); d != rep.Bytes {
		t.Fatalf("report bytes=%d != disk usage=%d", rep.Bytes, d)
	}
	if s := c.Stats(); s.Bytes != rep.Bytes || s.Entries != rep.Entries {
		t.Fatalf("stats %+v inconsistent with report %+v", s, rep)
	}
	t.Logf("disk usage=%d matches report bytes=%d", diskUsage(t, dir), rep.Bytes)

	// 被清理的条目可正常重建。
	var builds atomic.Int64
	rb := func() ([]byte, error) { builds.Add(1); return []byte("rebuilt"), nil }
	for _, k := range []Key{expKey, badKey} {
		r, err := c.GetOrBuildWithTTL(context.Background(), k, -1, rb)
		if err != nil {
			t.Fatal(err)
		}
		logResult(t, r)
	}
	if builds.Load() != 2 {
		t.Fatalf("builds=%d, want 2", builds.Load())
	}
	logStats(t, c)
}

// TestSweepConsistencyUnderConcurrency 验证：巡检与并发读写任意交错，
// 结束后报告的占用与磁盘实际占用完全一致。
func TestSweepConsistencyUnderConcurrency(t *testing.T) {
	dir := t.TempDir()
	clock := newManualClock()
	payload := []byte("0123456789")
	entrySize := int64(entryHeaderLen + len(payload))
	maxBytes := entrySize*8 + entrySize/2
	c, err := Open(dir, maxBytes, WithMaxAge(30*time.Minute), WithClock(clock.now))
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	// 并发读写：一部分键会随时间过期，触发过期重建路径。
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := mustKey(t, fmt.Sprintf("worker-%d", i%4)) // 4 个键复用，制造同键并发
			for {
				select {
				case <-stop:
					return
				default:
				}
				r, err := c.GetOrBuild(context.Background(), key,
					func() ([]byte, error) { return payload, nil })
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				if string(r.Payload) != string(payload) {
					t.Errorf("key=%s wrong payload %q", r.Key, r.Payload)
					return
				}
			}
		}(i)
	}
	// 并发写入大量不同键，制造淘汰压力。
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := 0; n < 60; n++ {
				key := mustKey(t, fmt.Sprintf("churn-%d-%d", i, n))
				if _, err := c.GetOrBuild(context.Background(), key,
					func() ([]byte, error) { return payload, nil }); err != nil {
					t.Errorf("churn get: %v", err)
					return
				}
			}
		}(i)
	}
	// 并发巡检 + 推进时间制造过期。
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				clock.advance(10 * time.Minute)
				rep := c.Sweep()
				_ = rep // 巡检中途条目可能被并发读取钉住，占用可瞬时超限；最终一致性在结束后校验
			}
		}(i)
	}
	// 等待写入者完成，再停读写与巡检。
	for {
		if c.Stats().Builds >= 4*60 {
			break
		}
	}
	close(stop)
	wg.Wait()

	// 所有操作结束后：最终巡检报告、Stats、磁盘三者必须完全一致。
	rep := c.Sweep()
	s := c.Stats()
	d := diskUsage(t, dir)
	t.Logf("final sweep report: %+v", rep)
	logStats(t, c)
	t.Logf("disk usage=%d", d)
	if rep.Bytes != d {
		t.Fatalf("sweep report bytes=%d != disk usage=%d", rep.Bytes, d)
	}
	if s.Bytes != d {
		t.Fatalf("stats bytes=%d != disk usage=%d", s.Bytes, d)
	}
	if s.Bytes > maxBytes {
		t.Fatalf("bytes=%d exceeds max=%d after sweeps", s.Bytes, maxBytes)
	}

	// 后续继续写入：淘汰仍能把真实占用压回上限以内。
	for i := 0; i < 50; i++ {
		key := mustKey(t, fmt.Sprintf("post-sweep-%d", i))
		if _, err := c.GetOrBuildWithTTL(context.Background(), key, -1,
			func() ([]byte, error) { return payload, nil }); err != nil {
			t.Fatal(err)
		}
	}
	s = c.Stats()
	d = diskUsage(t, dir)
	logStats(t, c)
	if s.Bytes != d || s.Bytes > maxBytes {
		t.Fatalf("after post-sweep writes: stats bytes=%d disk=%d max=%d", s.Bytes, d, maxBytes)
	}
	if s.Evictions == 0 {
		t.Fatal("expected evictions after sweep under capacity pressure")
	}
	t.Logf("disk usage=%d matches stats bytes=%d (<= max %d)", d, s.Bytes, maxBytes)
}

// TestLegacyV2EntriesMigratedOrRejected 验证 v2 旧格式（无有效期）的
// 确定性处理：校验通过的无损升级为 v3 并继续可用；校验失败的明确拒绝。
func TestLegacyV2EntriesMigratedOrRejected(t *testing.T) {
	dir := t.TempDir()

	// 合法 v2 条目：magic + version=2 + 长度 + SHA-256 + 负载。
	v2Key := mustKey(t, "legacy-v2")
	v2Payload := []byte("v2 artifact")
	var v2data []byte
	v2data = append(v2data, entryMagic...)
	v2data = binary.BigEndian.AppendUint16(v2data, 2)
	v2data = binary.BigEndian.AppendUint64(v2data, uint64(len(v2Payload)))
	sum := sha256.Sum256(v2Payload)
	v2data = append(v2data, sum[:]...)
	v2data = append(v2data, v2Payload...)
	if err := os.WriteFile(filepath.Join(dir, string(v2Key)), v2data, 0o644); err != nil {
		t.Fatal(err)
	}

	// 校验和损坏的 v2 条目：必须被拒绝，不得静默误用。
	badV2Key := mustKey(t, "bad-v2")
	badV2 := make([]byte, len(v2data))
	copy(badV2, v2data)
	badV2[len(badV2)-1] ^= 0xff
	if err := os.WriteFile(filepath.Join(dir, string(badV2Key)), badV2, 0o644); err != nil {
		t.Fatal(err)
	}

	// 更高版本（v99）：无法识别，必须拒绝。
	futureKey := mustKey(t, "future")
	var fdata []byte
	fdata = append(fdata, entryMagic...)
	fdata = binary.BigEndian.AppendUint16(fdata, 99)
	fdata = append(fdata, []byte("future")...)
	if err := os.WriteFile(filepath.Join(dir, string(futureKey)), fdata, 0o644); err != nil {
		t.Fatal(err)
	}

	c, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	s := c.Stats()
	logStats(t, c)
	if s.Migrated != 1 || s.Rejected != 2 {
		t.Fatalf("migrated=%d rejected=%d, want 1/2", s.Migrated, s.Rejected)
	}

	// 迁移后的 v2 条目可正常命中；旧格式无有效期信息，迁移后永不过期。
	r, err := c.GetOrBuild(context.Background(), v2Key,
		func() ([]byte, error) { return nil, errors.New("should not build") })
	if err != nil {
		t.Fatal(err)
	}
	logResult(t, r)
	if !r.Hit || string(r.Payload) != string(v2Payload) {
		t.Fatalf("migrated v2 entry not usable: %+v", r)
	}

	// 被拒绝的条目按未命中重建。
	var builds atomic.Int64
	b := func() ([]byte, error) { builds.Add(1); return []byte("rebuilt"), nil }
	for _, k := range []Key{badV2Key, futureKey} {
		r, _ := c.GetOrBuild(context.Background(), k, b)
		logResult(t, r)
		if r.Hit {
			t.Fatalf("rejected entry must not be reused: %+v", r)
		}
	}
	if builds.Load() != 2 {
		t.Fatalf("builds=%d, want 2", builds.Load())
	}
	if d := diskUsage(t, dir); d != c.Stats().Bytes {
		t.Fatalf("stats bytes=%d != disk usage=%d", c.Stats().Bytes, d)
	}
}

// TestSweepThenEvictionEnforcesCapacity 验证巡检之后超限淘汰仍然生效。
func TestSweepThenEvictionEnforcesCapacity(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("0123456789")
	entrySize := int64(entryHeaderLen + len(payload))
	maxBytes := entrySize*3 + entrySize/2
	c, err := Open(dir, maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	b := func() ([]byte, error) { return payload, nil }
	for i := 0; i < 3; i++ {
		if _, err := c.GetOrBuild(context.Background(), mustKey(t, fmt.Sprintf("k%d", i)), b); err != nil {
			t.Fatal(err)
		}
	}
	rep := c.Sweep()
	t.Logf("sweep report: %+v", rep)
	if rep.Entries != 3 {
		t.Fatalf("entries=%d, want 3", rep.Entries)
	}

	// 巡检后继续写入：淘汰必须把真实占用压回上限以内。
	for i := 3; i < 10; i++ {
		if _, err := c.GetOrBuild(context.Background(), mustKey(t, fmt.Sprintf("k%d", i)), b); err != nil {
			t.Fatal(err)
		}
	}
	s := c.Stats()
	logStats(t, c)
	d := diskUsage(t, dir)
	if s.Bytes > maxBytes {
		t.Fatalf("bytes=%d exceeds max=%d", s.Bytes, maxBytes)
	}
	if s.Bytes != d {
		t.Fatalf("stats bytes=%d != disk usage=%d", s.Bytes, d)
	}
	if s.Evictions == 0 {
		t.Fatal("expected evictions after sweep")
	}
	t.Logf("disk usage=%d matches stats bytes=%d (<= max %d)", d, s.Bytes, maxBytes)
}
