package cache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSweepAdoptsInflightLandedBuilds 稳定复现"巡检与并发构建落盘交错"：
// 并发构建先停在构建过程中，巡检开始扫描；扫描中途放行构建，让产物在
// 巡检返回前落盘（rename 完成、正阻塞等待入索引）；巡检返回的瞬间，
// 报告里的条目数与字节数必须和目录真实占用完全一致，且再读一次
// Stats 三者一致。随后覆盖：巡检结束后继续写入仍触发淘汰并压回上限。
func TestSweepAdoptsInflightLandedBuilds(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("0123456789")
	entrySize := int64(entryHeaderLen + len(payload))
	maxBytes := entrySize*4 + entrySize/2 // 至多容纳 4 个条目
	c, err := Open(dir, maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	b := func() ([]byte, error) { return payload, nil }

	// 预置 2 个条目。
	for _, k := range []Key{mustKey(t, "pre-0"), mustKey(t, "pre-1")} {
		if _, err := c.GetOrBuild(context.Background(), k, b); err != nil {
			t.Fatal(err)
		}
	}

	// 启动 2 个并发构建，先停在构建过程中。
	keys := []Key{mustKey(t, "inflight-0"), mustKey(t, "inflight-1")}
	release := make(chan struct{})
	var entered atomic.Int64
	var wg sync.WaitGroup
	for _, k := range keys {
		wg.Add(1)
		go func(k Key) {
			defer wg.Done()
			if _, err := c.GetOrBuild(context.Background(), k, func() ([]byte, error) {
				entered.Add(1)
				<-release // 停在构建过程中，等巡检开始扫描后放行
				return payload, nil
			}); err != nil {
				t.Errorf("inflight build: %v", err)
			}
		}(k)
	}
	// 再启动 1 个会失败的构建，覆盖"落盘前失败不污染统计"的交错。
	failKey := mustKey(t, "inflight-fail")
	wantErr := errors.New("boom")
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := c.GetOrBuild(context.Background(), failKey, func() ([]byte, error) {
			entered.Add(1)
			<-release
			return nil, wantErr
		}); !errors.Is(err, wantErr) {
			t.Errorf("failing build err=%v", err)
		}
	}()
	for entered.Load() < 3 {
		time.Sleep(time.Millisecond)
	}

	// 注入扫描中途钩子：放行构建，并等两个产物在巡检返回前落盘。
	var hookErr error
	c.sweepMidHook = func() {
		c.sweepMidHook = nil // 一次性钩子，后续巡检不再触发
		close(release)
		deadline := time.Now().Add(10 * time.Second)
		for {
			landed := 0
			for _, k := range keys {
				if _, err := os.Stat(filepath.Join(dir, string(k))); err == nil {
					landed++
				}
			}
			if landed == len(keys) {
				return
			}
			if time.Now().After(deadline) {
				hookErr = errors.New("inflight builds did not land before sweep returns")
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	rep := c.Sweep()
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	t.Logf("sweep report: %+v", rep)

	// 巡检返回的瞬间：报告与目录真实占用完全一致（构建尚未入索引，
	// 但产物已落盘，必须被巡检认领统计）。
	d := diskUsage(t, dir)
	wantEntries := 2 + len(keys)
	if rep.Adopted != len(keys) {
		t.Fatalf("adopted=%d, want %d (landed inflight builds)", rep.Adopted, len(keys))
	}
	if rep.Entries != wantEntries {
		t.Fatalf("report entries=%d, want %d", rep.Entries, wantEntries)
	}
	if rep.Bytes != d {
		t.Fatalf("report bytes=%d != disk usage=%d at sweep return", rep.Bytes, d)
	}
	// 再读一次缓存统计：报告、Stats、磁盘三者一致。
	s := c.Stats()
	logStats(t, c)
	if s.Entries != rep.Entries || s.Bytes != rep.Bytes || s.Bytes != d {
		t.Fatalf("inconsistent: report=%+v stats entries=%d bytes=%d disk=%d",
			rep, s.Entries, s.Bytes, d)
	}
	t.Logf("at sweep return: report entries=%d bytes=%d == disk %d", rep.Entries, rep.Bytes, d)

	wg.Wait() // 构建完成入索引（同键替换，幂等）

	// 被认领的产物可正常命中，不触发重建。
	var builds atomic.Int64
	noBuild := func() ([]byte, error) { builds.Add(1); return nil, errors.New("should not build") }
	for _, k := range keys {
		r, err := c.GetOrBuild(context.Background(), k, noBuild)
		if err != nil {
			t.Fatal(err)
		}
		logResult(t, r)
		if !r.Hit {
			t.Fatalf("adopted entry should hit: %+v", r)
		}
	}
	// 失败的构建不污染缓存，重试可成功。
	r, err := c.GetOrBuild(context.Background(), failKey, b)
	if err != nil || !r.Built {
		t.Fatalf("retry after failure: r=%+v err=%v", r, err)
	}
	logResult(t, r)

	// 再巡检一次：分类计数不重复扣减或漏计，结果稳定。
	rep2 := c.Sweep()
	t.Logf("second sweep report: %+v", rep2)
	if rep2.Expired != 0 || rep2.Corrupted != 0 || rep2.Orphaned != 0 ||
		rep2.Missing != 0 || rep2.Adopted != 0 {
		t.Fatalf("second sweep not stable: %+v", rep2)
	}
	if d2 := diskUsage(t, dir); rep2.Bytes != d2 || rep2.Entries != len(c.entries) {
		t.Fatalf("second sweep: report=%+v disk=%d", rep2, d2)
	}

	// 巡检结束后继续写入：淘汰仍要把真实占用压回配置的上限以内。
	for i := 0; i < 8; i++ {
		if _, err := c.GetOrBuildWithTTL(context.Background(),
			mustKey(t, fmt.Sprintf("post-sweep-%d", i)), -1, b); err != nil {
			t.Fatal(err)
		}
	}
	s = c.Stats()
	d = diskUsage(t, dir)
	logStats(t, c)
	if s.Bytes > maxBytes {
		t.Fatalf("bytes=%d exceeds max=%d after post-sweep writes", s.Bytes, maxBytes)
	}
	if s.Bytes != d {
		t.Fatalf("stats bytes=%d != disk usage=%d after post-sweep writes", s.Bytes, d)
	}
	if s.Evictions == 0 {
		t.Fatal("expected evictions after sweep under capacity pressure")
	}
	t.Logf("disk usage=%d matches stats bytes=%d (<= max %d)", d, s.Bytes, maxBytes)
}
