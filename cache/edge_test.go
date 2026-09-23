package cache

import (
	"context"
	"os"
	"sync"
	"testing"
)

func TestOversizedArtifactNotCached(t *testing.T) {
	c, dir := newTestCache(t, 150, "GOOS") // 磁盘条目头约 114 字节；小容量
	in := sampleInput("oversized")
	big := BuilderFunc(func(ctx context.Context, _ BuildInput) ([]byte, error) {
		return make([]byte, 1000), nil // 编码后必然超过 150
	})
	r, err := c.GetOrBuild(context.Background(), in, big)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("key=%s decision=%s stats={%s}", r.Key, r.Decision, c.SnapshotStats())
	if r.Decision.Reason != ReasonMissNotFound {
		t.Fatalf("want MISS_NOT_FOUND, got %s", r.Decision)
	}
	st := c.SnapshotStats()
	if st.Entries != 0 || st.CurrentBytes != 0 {
		t.Fatalf("oversized artifact must not be stored, got entries=%d bytes=%d", st.Entries, st.CurrentBytes)
	}
	if n, _ := diskBytes(t, dir); n != 0 {
		t.Fatalf("oversized artifact left %d files on disk", n)
	}
	// 再来一次仍然未命中且重新构建。
	var builds int
	var mu sync.Mutex
	counting := BuilderFunc(func(ctx context.Context, _ BuildInput) ([]byte, error) {
		mu.Lock()
		builds++
		mu.Unlock()
		return make([]byte, 1000), nil
	})
	r2, _ := c.GetOrBuild(context.Background(), in, counting)
	t.Logf("second: %s stats={%s}", r2.Decision, c.SnapshotStats())
	if builds != 1 || r2.Decision.Reason != ReasonMissNotFound {
		t.Fatalf("oversized artifact must rebuild every time, builds=%d reason=%s", builds, r2.Decision)
	}
}

// 打开缓存时扫描即发现损坏条目（归类删除、不进索引），随后 N 个并发请求
// 必须合并为恰好一次重建，且重建请求携带 MISS_CORRUPT 的可解释原因。
func TestCorruptEntryFoundAtOpenThenConcurrentRebuildOnce(t *testing.T) {
	dir := t.TempDir()
	in := sampleInput("corrupt-inflight")
	kh := DeriveKey(in.withEnvWhitelist([]string{"GOOS"})).String()

	// 先写一个好条目。
	c0, err := New(Config{Dir: dir, EnvWhitelist: []string{"GOOS"}})
	if err != nil {
		t.Fatal(err)
	}
	r0, err := c0.GetOrBuild(context.Background(), in, NewSimBuilder())
	if err != nil {
		t.Fatal(err)
	}
	c0.Close()

	// 直接损坏该文件（截断校验和）。
	p := entryPath(dir, kh)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, raw[:len(raw)-5], 0o644); err != nil {
		t.Fatal(err)
	}

	c, err := New(Config{Dir: dir, EnvWhitelist: []string{"GOOS"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	t.Logf("open stats={%s}", c.SnapshotStats()) // 损坏文件若在扫描期已被删，索引为空

	const N = 16
	var wg sync.WaitGroup
	results := make([]Result, N)
	errs := make([]error, N)
	start := make(chan struct{})
	sb := NewSimBuilder()
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = c.GetOrBuild(context.Background(), in, sb)
		}(i)
	}
	close(start)
	// 扫描打开时损坏文件已被归类删除，索引为空，所有请求正常合并为一次构建。
	wg.Wait()

	if sb.BuildCount() != 1 {
		t.Fatalf("want exactly 1 rebuild after corruption, got %d", sb.BuildCount())
	}
	var corruptReasons int
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("req %d: %v", i, errs[i])
		}
		if results[i].Decision.Reason == ReasonMissCorrupt {
			corruptReasons++
		}
	}
	t.Logf("key=%s leaders/waiters corruptReasonCount=%d actualBuilds=%d stats={%s}",
		r0.Key, corruptReasons, sb.BuildCount(), c.SnapshotStats())
	if corruptReasons == 0 {
		t.Fatal("at least the rebuilding request should report MISS_CORRUPT lookup reason")
	}
}
