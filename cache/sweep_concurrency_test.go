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

// waitFor 轮询 cond 直至为真或超时，避免用 sleep 猜时序。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// TestSweepAdoptsLandingBuilds 确定性复现"巡检扫描期间、返回之前，
// 进行中的构建把产物落盘"的交错，并验证：
//
//  1. 巡检返回的瞬间，报告 Entries/Bytes 与目录磁盘真实占用逐文件一致；
//  2. 落盘产物被认领（Adopted），既没被当孤儿删掉也没漏统；
//  3. 构建方提交后继续写入，LRU 淘汰仍把真实占用压回上限以内；
//  4. 巡检返回后再读 Stats，报告、统计、磁盘三者一致。
//
// 不依赖网络/外部服务；-race 下可重复执行。
func TestSweepAdoptsLandingBuilds(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("0123456789")
	entrySize := int64(entryHeaderLen + len(payload))
	maxBytes := entrySize*3 + entrySize/2
	c, err := Open(dir, maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	b := func() ([]byte, error) { return payload, nil }

	// 先占住容量（3 个普通条目，均为可淘汰的旧条目）。
	var baseKeys []Key
	for i := 0; i < 3; i++ {
		k := mustKey(t, fmt.Sprintf("base-%d", i))
		baseKeys = append(baseKeys, k)
		if _, err := c.GetOrBuild(context.Background(), k, b); err != nil {
			t.Fatal(err)
		}
	}

	const n = 4
	landingKeys := make([]Key, n)
	for i := range landingKeys {
		landingKeys[i] = mustKey(t, fmt.Sprintf("landing-%d", i))
	}

	buildStarted := make(chan struct{}, n) // 构建函数被调用（尚未开始发布）
	committed := make(chan struct{}, n)    // 构建方 rename 落盘后停在此处
	releaseCommit := make(chan struct{})   // 放行构建方入索引
	scanReached := make(chan struct{})     // 巡检第一遍扫描完成
	releaseScan := make(chan struct{})     // 放行巡检进入收尾
	var hooksActive atomic.Bool
	hooksActive.Store(true)

	c.afterBuildCommitted = func(Key) {
		if !hooksActive.Load() {
			return
		}
		committed <- struct{}{}
		<-releaseCommit
	}
	c.afterSweepScanned = func() {
		if !hooksActive.Load() {
			return
		}
		close(scanReached)
		<-releaseScan
	}

	var built atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := c.GetOrBuild(context.Background(), landingKeys[i], func() ([]byte, error) {
				built.Add(1)
				buildStarted <- struct{}{}
				return b()
			})
			if err != nil {
				t.Errorf("landing build %d: %v", i, err)
				return
			}
			if string(r.Payload) != string(payload) {
				t.Errorf("landing build %d wrong payload %q", i, r.Payload)
			}
		}(i)
	}

	// 等全部构建都已进入（已注册 inflight），确保它们的发布与巡检收尾交错。
	waitFor(t, 5*time.Second, func() bool {
		c.mu.Lock()
		nf := len(c.inflight)
		c.mu.Unlock()
		return nf == n
	}, "all builds registered as inflight")
	// 同时确保构建函数本体也已在运行（即将发布）。
	for i := 0; i < n; i++ {
		<-buildStarted
	}

	// 巡检开始：第一遍扫描时这些键还没落盘；钩子把巡检停在扫描完成处。
	sweepDone := make(chan SweepReport, 1)
	go func() { sweepDone <- c.Sweep() }()
	<-scanReached

	// 在巡检返回之前，让全部构建把产物落盘，然后停在入索引之前。
	for i := 0; i < n; i++ {
		<-committed
	}
	// 此刻磁盘上 3 个旧条目 + 4 个落盘产物，且没有写了一半的临时文件。
	diskBefore := diskUsage(t, dir)
	if diskBefore != entrySize*(3+n) {
		t.Fatalf("disk before sweep finalize=%d, want %d", diskBefore, entrySize*(3+n))
	}

	// 放行巡检收尾；收尾的发布写锁会等所有 rename 静止，再把 4 个产物认纳入索引。
	// 同时关闭钩子，避免后续普通写入被拦截。
	hooksActive.Store(false)
	close(releaseScan)
	var rep SweepReport
	select {
	case rep = <-sweepDone:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep did not return")
	}
	t.Logf("sweep report: %+v", rep)

	// 巡检返回的瞬间：报告必须与磁盘真实占用逐文件一致。
	diskAtReturn := diskUsage(t, dir)
	if rep.Bytes != diskAtReturn {
		t.Fatalf("sweep report bytes=%d != disk usage at return=%d", rep.Bytes, diskAtReturn)
	}
	if int64(rep.Entries)*entrySize != diskAtReturn {
		t.Fatalf("sweep report entries=%d do not match disk usage=%d", rep.Entries, diskAtReturn)
	}
	if rep.Adopted != n {
		t.Fatalf("adopted=%d, want %d (landing artifacts must not be missed)", rep.Adopted, n)
	}
	// 被认领的落盘产物绝不能被当成孤儿删掉：磁盘文件必须都在。
	for _, k := range landingKeys {
		if _, err := os.Stat(filepath.Join(dir, string(k))); err != nil {
			t.Fatalf("landing artifact %s removed as garbage: %v", k, err)
		}
	}
	// 所有构建仍停在入索引之前，因此再读 Stats 也必须与报告、磁盘一致。
	if s := c.Stats(); s.Bytes != rep.Bytes || s.Entries != rep.Entries {
		t.Fatalf("stats %+v inconsistent with sweep report %+v", s, rep)
	}

	// 放行构建方提交；各自提交后补做淘汰，真实占用回落。
	close(releaseCommit)
	wg.Wait()

	s := c.Stats()
	d := diskUsage(t, dir)
	logStats(t, c)
	t.Logf("after builds committed: disk usage=%d", d)
	if s.Bytes != d {
		t.Fatalf("stats bytes=%d != disk usage=%d after builds commit", s.Bytes, d)
	}
	if s.Bytes > maxBytes {
		t.Fatalf("bytes=%d still exceeds max=%d after builds commit", s.Bytes, maxBytes)
	}
	if s.Evictions == 0 {
		t.Fatal("expected evictions once landing builds committed")
	}

	// 巡检结束后继续写入：淘汰仍要把真实占用压回上限以内。
	for i := 0; i < 30; i++ {
		k := mustKey(t, fmt.Sprintf("post-sweep-%d", i))
		if _, err := c.GetOrBuildWithTTL(context.Background(), k, -1, b); err != nil {
			t.Fatal(err)
		}
	}
	s = c.Stats()
	d = diskUsage(t, dir)
	logStats(t, c)
	if s.Bytes != d || s.Bytes > maxBytes {
		t.Fatalf("post-sweep writes: stats bytes=%d disk=%d max=%d", s.Bytes, d, maxBytes)
	}

	// 再做一次安静巡检：无新增变化时报告、Stats、磁盘三者一致，计数稳定。
	rep2 := c.Sweep()
	s = c.Stats()
	d = diskUsage(t, dir)
	t.Logf("quiet sweep report: %+v", rep2)
	if rep2.Bytes != d || rep2.Bytes != s.Bytes || rep2.Entries != s.Entries {
		t.Fatalf("quiet sweep report=%+v stats=%+v disk=%d", rep2, s, d)
	}
}

// TestSweepRepeatedAndRetriesStable 验证：反复巡检、多个巡检并发、
// 同键构建失败后重试等路径交错时，过期/损坏/缺失/孤儿分类计数不重复
// 扣减、不漏计，且每次巡检返回的报告与当时磁盘真实占用一致。
func TestSweepRepeatedAndRetriesStable(t *testing.T) {
	dir := t.TempDir()
	clock := newManualClock()
	c, err := Open(dir, 1<<20, WithMaxAge(time.Hour), WithClock(clock.now))
	if err != nil {
		t.Fatal(err)
	}
	b := func() ([]byte, error) { return []byte("payload"), nil }

	expKey := mustKey(t, "exp")
	badKey := mustKey(t, "bad")
	okKey := mustKey(t, "ok")
	for _, k := range []Key{expKey, badKey, okKey} {
		if _, err := c.GetOrBuild(context.Background(), k, b); err != nil {
			t.Fatal(err)
		}
	}

	clock.advance(2 * time.Hour) // 三个条目均已过期
	// okKey/badKey 重写为长期有效，只留 expKey 过期；再把 badKey 破坏掉。
	if _, err := c.GetOrBuildWithTTL(context.Background(), okKey, -1, b); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetOrBuildWithTTL(context.Background(), badKey, -1, b); err != nil {
		t.Fatal(err)
	}
	bp := filepath.Join(dir, string(badKey))
	data, _ := os.ReadFile(bp)
	if err := os.WriteFile(bp, data[:len(data)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	// 真孤儿文件。
	if err := os.WriteFile(filepath.Join(dir, "orphan"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	// 第一次巡检：四类计数各一，且报告与磁盘一致。
	rep := c.Sweep()
	t.Logf("first sweep: %+v", rep)
	if rep.Expired != 1 || rep.Corrupted != 1 || rep.Orphaned != 1 {
		t.Fatalf("first sweep counts: %+v", rep)
	}
	if d := diskUsage(t, dir); d != rep.Bytes {
		t.Fatalf("first sweep bytes=%d != disk=%d", rep.Bytes, d)
	}

	// 并发再来多个巡检：系统已安静，计数必须不再变化，报告始终等于磁盘。
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := c.Sweep()
			if d := diskUsage(t, dir); d != r.Bytes {
				t.Errorf("concurrent sweep bytes=%d != disk=%d", r.Bytes, d)
			}
			if r.Expired != 0 || r.Corrupted != 0 || r.Orphaned != 0 ||
				r.Missing != 0 || r.Adopted != 0 {
				t.Errorf("quiet sweep reported nonzero actions: %+v", r)
			}
		}()
	}
	wg.Wait()

	// 被清理条目可重建；同键先失败再重试也不污染统计。
	failKey := mustKey(t, "retry")
	var attempts atomic.Int64
	build := func() ([]byte, error) {
		if attempts.Add(1) == 1 {
			return nil, fmt.Errorf("boom")
		}
		return []byte("recovered"), nil
	}
	if _, err := c.GetOrBuild(context.Background(), failKey, build); err == nil {
		t.Fatal("first attempt must fail")
	}
	if _, err := c.GetOrBuild(context.Background(), failKey, build); err != nil {
		t.Fatalf("retry must succeed: %v", err)
	}
	if _, err := c.GetOrBuild(context.Background(), expKey, b); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetOrBuild(context.Background(), badKey, b); err != nil {
		t.Fatal(err)
	}

	// 最终巡检 + Stats + 磁盘三者一致。
	rep = c.Sweep()
	s := c.Stats()
	d := diskUsage(t, dir)
	t.Logf("final sweep: %+v", rep)
	logStats(t, c)
	if rep.Bytes != d || s.Bytes != d || rep.Entries != s.Entries {
		t.Fatalf("final: report=%+v stats=%+v disk=%d", rep, s, d)
	}
}
