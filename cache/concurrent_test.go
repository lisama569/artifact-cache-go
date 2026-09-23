package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// blockingBuilder 进入构建后阻塞，直到 release 关闭，用于稳定制造并发窗口。
type blockingBuilder struct {
	started chan struct{}
	once    sync.Once
	release chan struct{}
	delay   time.Duration
	count   int
	mu      sync.Mutex
	fail    bool
}

func newBlockingBuilder(fail bool) *blockingBuilder {
	return &blockingBuilder{started: make(chan struct{}), release: make(chan struct{}), fail: fail}
}

func (b *blockingBuilder) Build(ctx context.Context, in BuildInput) ([]byte, error) {
	b.mu.Lock()
	b.count++
	n := b.count
	b.mu.Unlock()
	b.once.Do(func() { close(b.started) })
	select {
	case <-b.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if b.fail {
		return nil, errors.New("boom")
	}
	key := DeriveKey(in)
	return []byte(fmt.Sprintf("artifact:%s#%d", key.String(), n)), nil
}

func (b *blockingBuilder) BuildCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
}

func TestConcurrentSameKeyBuildsOnce(t *testing.T) {
	c, _ := newTestCache(t, 0, "GOOS")
	in := sampleInput("concurrent")
	const N = 32

	builder := newBlockingBuilder(false)
	var wg sync.WaitGroup
	results := make([]Result, N)
	errs := make([]error, N)
	start := make(chan struct{})
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = c.GetOrBuild(context.Background(), in, builder)
		}(i)
	}
	close(start)
	<-builder.started // 唯一一次构建已开始
	// 给等待者足够时间登记为 flight waiter。
	time.Sleep(50 * time.Millisecond)
	if got := c.SnapshotStats().InFlight; got != 1 {
		t.Fatalf("want exactly 1 in-flight build, got %d", got)
	}
	close(builder.release)
	wg.Wait()

	if builder.BuildCount() != 1 {
		t.Fatalf("want 1 actual build, got %d", builder.BuildCount())
	}
	var leaders, shared, hits int
	artifact := results[0].Entry.Artifact
	for i, r := range results {
		if errs[i] != nil {
			t.Fatalf("request %d unexpected err: %v", i, errs[i])
		}
		if string(r.Entry.Artifact) != string(artifact) {
			t.Fatalf("request %d artifact differs", i)
		}
		switch {
		case r.Decision.Reason == ReasonHitFresh:
			hits++
		case r.Shared && r.Decision.Reason == ReasonMissShared:
			shared++
		case r.Decision.Reason == ReasonMissNotFound:
			leaders++
		default:
			t.Fatalf("request %d unexpected decision: %s", i, r.Decision)
		}
	}
	t.Logf("key=%s leaders=%d shared=%d hits=%d actualBuilds=%d stats={%s}",
		results[0].Key, leaders, shared, hits, builder.BuildCount(), c.SnapshotStats())
	if leaders != 1 || shared != N-1 {
		t.Fatalf("want 1 leader and %d waiters, got leaders=%d shared=%d", N-1, leaders, shared)
	}

	// 全部完成后再来一轮：必须全部命中，不再构建。
	var wg2 sync.WaitGroup
	wg2.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg2.Done()
			r, err := c.GetOrBuild(context.Background(), in, NewSimBuilder())
			if err != nil || r.Decision.Reason != ReasonHitFresh {
				t.Errorf("post-build lookup want hit, got %s err=%v", r.Decision, err)
			}
		}()
	}
	wg2.Wait()
	t.Logf("after completion: %s", c.SnapshotStats())
}

func TestConcurrentSameKeyFailureShared(t *testing.T) {
	c, _ := newTestCache(t, 0, "GOOS")
	in := sampleInput("concurrent-fail")
	const N = 8
	builder := newBlockingBuilder(true)

	var wg sync.WaitGroup
	start := make(chan struct{})
	decisions := make([]Decision, N)
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(idx int) {
			defer wg.Done()
			<-start
			r, err := c.GetOrBuild(context.Background(), in, builder)
			if err == nil {
				t.Errorf("request %d want error", idx)
			}
			decisions[idx] = r.Decision
		}(i)
	}
	close(start)
	<-builder.started
	time.Sleep(50 * time.Millisecond)
	close(builder.release)
	wg.Wait()

	if builder.BuildCount() != 1 {
		t.Fatalf("want 1 build attempt, got %d", builder.BuildCount())
	}
	for i, d := range decisions {
		if d.Reason != ReasonMissBuildFailed {
			t.Fatalf("request %d want MISS_BUILD_FAILED, got %s", i, d)
		}
	}
	st := c.SnapshotStats()
	t.Logf("key=%s all waiters failed consistently; actualBuilds=%d stats={%s}",
		DeriveKey(in), builder.BuildCount(), st)
	if st.Entries != 0 || st.CurrentBytes != 0 {
		t.Fatalf("failed build must leave empty cache, got entries=%d bytes=%d", st.Entries, st.CurrentBytes)
	}
}
