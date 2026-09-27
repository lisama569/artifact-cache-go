package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// diskFileCount 统计缓存目录中的最终文件数（排除 .tmp- 临时文件）。
func diskFileCount(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && !isTmpName(e.Name()) {
			n++
		}
	}
	return n
}

func isTmpName(name string) bool {
	return len(name) >= 5 && name[:5] == ".tmp-"
}

func assertDiskConsistent(t *testing.T, c *Cache, dir, where string) {
	t.Helper()
	s := c.Stats()
	d := diskUsage(t, dir)
	n := diskFileCount(t, dir)
	if s.Bytes != d {
		t.Fatalf("[%s] stats bytes=%d != disk usage=%d", where, s.Bytes, d)
	}
	if s.Entries != n {
		t.Fatalf("[%s] stats entries=%d != disk files=%d", where, s.Entries, n)
	}
}

// TestSweepCleansExpiredCorruptedMismatchedOrphans 覆盖巡检基本能力：
// 过期、损坏、索引与磁盘不符、孤儿文件分别处理，报告与磁盘完全一致，
// 被清理条目之后访问按未命中重建。
func TestSweepCleansExpiredCorruptedMismatchedOrphans(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)
	c, _ := OpenWithOptions(dir, 1<<20, OpenOptions{Clock: clk})
	ctx := context.Background()

	goodKey, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"good"}})
	expiredKey, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"expired"}})
	corruptKey, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"corrupt"}})
	missingKey, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"missing"}})
	junkKey, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"junk"}})
	orphanKey, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"orphan"}})

	get := func(key Key, v string, opts ...WriteOption) {
		t.Helper()
		r, err := c.GetOrBuild(ctx, key, func() ([]byte, error) { return []byte(v), nil }, opts...)
		if err != nil {
			t.Fatal(err)
		}
		logResult(t, r)
	}
	get(goodKey, "good", WithTTL(time.Hour))
	get(expiredKey, "expired", WithTTL(time.Nanosecond))
	get(corruptKey, "corrupt", WithTTL(0))
	get(missingKey, "missing", WithTTL(0))

	clk.Advance(2 * time.Nanosecond)

	// 损坏条目：篡改最终字节，校验和不符。
	p := filepath.Join(dir, string(corruptKey))
	data, _ := os.ReadFile(p)
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	// 索引有、磁盘无：直接删掉文件模拟记录对不上。
	if err := os.Remove(filepath.Join(dir, string(missingKey))); err != nil {
		t.Fatal(err)
	}
	// 磁盘有、索引无：无法识别的残留文件。
	if err := os.WriteFile(filepath.Join(dir, string(junkKey)), []byte("random bytes"), 0o644); err != nil {
		t.Fatal(err)
	}
	// 磁盘有、索引无：内容完好的 v3 文件（外部放入/上次崩溃残留）应被收养。
	orphan := encodeEntryV3([]byte("adopted"), start, time.Hour)
	if err := os.WriteFile(filepath.Join(dir, string(orphanKey)), orphan, 0o644); err != nil {
		t.Fatal(err)
	}

	rep := c.Sweep()
	t.Logf("sweep report: %+v", rep)
	if rep.Expired != 1 {
		t.Fatalf("expired=%d want 1", rep.Expired)
	}
	if rep.Corrupted != 1 {
		t.Fatalf("corrupted=%d want 1", rep.Corrupted)
	}
	if rep.Mismatched < 1 {
		t.Fatalf("mismatched=%d want >=1", rep.Mismatched)
	}
	if rep.Orphans != 1 {
		t.Fatalf("orphans=%d want 1 (junk only)", rep.Orphans)
	}
	if rep.Adopted != 1 {
		t.Fatalf("adopted=%d want 1", rep.Adopted)
	}
	// 存活：good + adopted，共 2 个；报告必须与磁盘真实占用完全一致。
	if rep.Entries != 2 {
		t.Fatalf("entries=%d want 2", rep.Entries)
	}
	if d := diskUsage(t, dir); d != rep.Bytes {
		t.Fatalf("report bytes=%d != disk=%d", rep.Bytes, d)
	}
	assertDiskConsistent(t, c, dir, "after sweep")
	logStats(t, c)

	// 被巡检清理的条目：磁盘与索引中的痕迹都已消失，后续访问统一按
	// “不存在”重建（“已过期”仅在懒过期、条目尚在时区分）。
	var builds atomic.Int64
	reb := func() ([]byte, error) { builds.Add(1); return []byte("rebuilt"), nil }
	r, _ := c.GetOrBuild(ctx, expiredKey, reb)
	logResult(t, r)
	if r.Reason != ReasonMissNotFound || !r.Built {
		t.Fatalf("sweep-removed expired key: %+v", r)
	}
	r, _ = c.GetOrBuild(ctx, corruptKey, reb)
	logResult(t, r)
	if r.Hit || r.Reason == ReasonHit {
		t.Fatalf("corrupt key should miss: %+v", r)
	}
	r, _ = c.GetOrBuild(ctx, missingKey, reb)
	logResult(t, r)
	if r.Reason != ReasonMissNotFound {
		t.Fatalf("missing key: %+v", r)
	}
	if builds.Load() != 3 {
		t.Fatalf("builds=%d want 3", builds.Load())
	}
	// 收养的孤儿可直接命中。
	r, _ = c.GetOrBuild(ctx, orphanKey,
		func() ([]byte, error) { return nil, fmt.Errorf("should not build") })
	logResult(t, r)
	if !r.Hit || string(r.Payload) != "adopted" {
		t.Fatalf("adopted orphan not usable: %+v", r)
	}
	// 好条目未被巡检波及。
	r, _ = c.GetOrBuild(ctx, goodKey,
		func() ([]byte, error) { return nil, fmt.Errorf("should not build") })
	logResult(t, r)
	if !r.Hit {
		t.Fatalf("good entry should survive sweep: %+v", r)
	}
	assertDiskConsistent(t, c, dir, "after rebuild")
}

// TestSweepConcurrentWithReadWrite 是核心并发交错测试：
// 多个请求持续读/写/制造新键，多个巡检并发进行；任何时刻巡检结束时
// 报告都必须与磁盘一致，整体结束后占用与统计仍然一致，且不超容量。
func TestSweepConcurrentWithReadWrite(t *testing.T) {
	dir := t.TempDir()
	clk := NewFakeClock(time.Date(2026, 9, 27, 13, 0, 0, 0, time.UTC))
	payload := []byte("0123456789")
	entrySize := int64(entryHeaderLen + len(payload))
	c, _ := OpenWithOptions(dir, entrySize*4+entrySize/2,
		OpenOptions{DefaultTTL: 0, Clock: clk})
	ctx := context.Background()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var inconsistencies atomic.Int64

	// 巡检结束瞬间的精确校验钩子：此刻持锁、无进行中构建、磁盘状态冻结，
	// 报告必须与磁盘真实条目数与占用完全一致。
	c.testSweepHook = func(rep *SweepReport) {
		d, n := measureDisk(t, dir)
		if rep.Bytes != d || rep.Entries != n {
			inconsistencies.Add(1)
			t.Errorf("sweep report inconsistent: report={entries=%d bytes=%d} disk={files=%d bytes=%d}",
				rep.Entries, rep.Bytes, n, d)
		}
		if rep.Bytes > c.maxBytes {
			inconsistencies.Add(1)
			t.Errorf("sweep leaves bytes=%d over max=%d", rep.Bytes, c.maxBytes)
		}
	}

	// 持续写入大量不同键，制造淘汰压力；偶发推进虚拟时间（TTL=0 不影响）。
	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				key, _ := DeriveKey(BuildInput{
					Toolchain: "go1.25.0",
					Args:      []string{fmt.Sprintf("w=%d i=%d", w, i)},
				})
				r, err := c.GetOrBuild(ctx, key, func() ([]byte, error) {
					// 极短随机阻塞，放大与巡检交错的窗口。
					if i%37 == 0 {
						time.Sleep(time.Microsecond)
					}
					return payload, nil
				})
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				if string(r.Payload) != string(payload) {
					t.Errorf("partial/wrong payload key=%s", r.Key)
					return
				}
			}
		}(w)
	}

	// 多个并发巡检：报告由 testSweepHook 在持锁瞬间精确校验。
	for s := 0; s < 4; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				c.Sweep()
			}
		}()
	}

	// 主 goroutine 周期性校验结构性不变量（持锁快照 + 磁盘扫描）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			checkInvariant(t, c, dir, &inconsistencies)
			if inconsistencies.Load() != 0 {
				return
			}
		}
	}()

	time.Sleep(150 * time.Millisecond)
	close(stop)
	wg.Wait()

	rep := c.Sweep()
	if rep.Bytes != diskUsage(t, dir) || rep.Entries != diskFileCount(t, dir) {
		t.Fatalf("final sweep inconsistent: %+v disk=%d files=%d",
			rep, diskUsage(t, dir), diskFileCount(t, dir))
	}
	assertDiskConsistent(t, c, dir, "final")
	s := c.Stats()
	if s.Bytes > s.MaxBytes {
		t.Fatalf("final bytes=%d over max=%d", s.Bytes, s.MaxBytes)
	}
	t.Logf("final: %+v", s)
	logStats(t, c)
	if inconsistencies.Load() != 0 {
		t.Fatalf("found %d inconsistencies", inconsistencies.Load())
	}
}

// TestSweepWithExpiryAndConcurrentHits 多个请求同时撞上同一批
// 即将过期/损坏的条目并与巡检交错：清理计数正确、重建只发生一次/键，
// 结束后占用与磁盘一致。
func TestSweepWithExpiryAndConcurrentHits(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 27, 14, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)
	c, _ := OpenWithOptions(dir, 1<<20, OpenOptions{Clock: clk})
	ctx := context.Background()

	const keysN = 8
	keys := make([]Key, keysN)
	for i := range keys {
		k, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{fmt.Sprintf("k=%d", i)}})
		keys[i] = k
		r, _ := c.GetOrBuild(ctx, k,
			func() ([]byte, error) { return []byte("v1"), nil }, WithTTL(time.Nanosecond))
		logResult(t, r)
	}
	clk.Advance(2 * time.Nanosecond) // 全部过期

	var wg sync.WaitGroup
	// 并发读取过期键：每个键的重建应被单飞合并。
	var buildsTotal atomic.Int64
	var hookFaults atomic.Int64
	c.testSweepHook = func(rep *SweepReport) {
		d, n := measureDisk(t, dir)
		if rep.Bytes != d || rep.Entries != n {
			hookFaults.Add(1)
			t.Errorf("sweep report inconsistent during concurrent hits: %+v disk=%d/%d", rep, d, n)
		}
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, k := range keys {
				r, err := c.GetOrBuild(ctx, k, func() ([]byte, error) {
					buildsTotal.Add(1)
					return []byte("v2"), nil
				})
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				if string(r.Payload) != "v2" {
					t.Errorf("payload=%q", r.Payload)
				}
			}
		}()
	}
	// 同时巡检若干轮（报告一致性由钩子校验）。
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Sweep()
		}()
	}
	wg.Wait()
	if hookFaults.Load() != 0 {
		t.Fatalf("sweep hook found %d faults", hookFaults.Load())
	}

	// 每个键重建恰好一次。
	if got := buildsTotal.Load(); got != keysN {
		t.Fatalf("builds=%d want %d (one per key)", got, keysN)
	}
	rep := c.Sweep()
	if rep.Expired != 0 {
		t.Fatalf("second sweep should find no expired, got %+v", rep)
	}
	assertDiskConsistent(t, c, dir, "after concurrent expiry")
	logStats(t, c)
}

// TestEvictionStillWorksAfterSweep 巡检收养孤儿导致超上限时，巡检自身
// 会回收至上限；之后继续写入，淘汰仍能把真实占用压回上限以内。
func TestEvictionStillWorksAfterSweep(t *testing.T) {
	dir := t.TempDir()
	clk := NewFakeClock(time.Date(2026, 9, 27, 15, 0, 0, 0, time.UTC))
	payload := []byte("0123456789")
	entrySize := int64(entryHeaderLen + len(payload))
	maxBytes := entrySize*3 + entrySize/2
	c, _ := OpenWithOptions(dir, maxBytes, OpenOptions{Clock: clk})
	ctx := context.Background()

	for i := 0; i < 20; i++ {
		k, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{fmt.Sprintf("a=%d", i)}})
		if _, err := c.GetOrBuild(ctx, k, func() ([]byte, error) { return payload, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if s := c.Stats(); s.Bytes > s.MaxBytes {
		t.Fatalf("precondition: bytes=%d over max", s.Bytes)
	}

	// 直接在磁盘放入一批完好但未索引的孤儿文件，巡检收养后将超上限。
	for i := 0; i < 10; i++ {
		k, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{fmt.Sprintf("orphan=%d", i)}})
		data := encodeEntryV3(payload, clk.Now(), 0)
		if err := os.WriteFile(filepath.Join(dir, string(k)), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	rep := c.Sweep()
	t.Logf("sweep after orphan dump: %+v", rep)
	if rep.Adopted != 10 {
		t.Fatalf("adopted=%d want 10", rep.Adopted)
	}
	if rep.Bytes > maxBytes {
		t.Fatalf("sweep did not evict: bytes=%d max=%d", rep.Bytes, maxBytes)
	}
	assertDiskConsistent(t, c, dir, "after adopting sweep")

	// 巡检之后继续写入：淘汰必须继续生效。
	for i := 0; i < 40; i++ {
		k, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{fmt.Sprintf("b=%d", i)}})
		if _, err := c.GetOrBuild(ctx, k, func() ([]byte, error) { return payload, nil }); err != nil {
			t.Fatal(err)
		}
	}
	assertDiskConsistent(t, c, dir, "after post-sweep writes")
	s := c.Stats()
	if s.Bytes > s.MaxBytes {
		t.Fatalf("post-sweep eviction broken: bytes=%d max=%d", s.Bytes, s.MaxBytes)
	}
	logStats(t, c)
}
