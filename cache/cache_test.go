package cache

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func newTestCache(t *testing.T, maxBytes int64, whitelist ...string) (*Cache, string) {
	t.Helper()
	dir := t.TempDir()
	c, err := New(Config{Dir: dir, MaxBytes: maxBytes, EnvWhitelist: whitelist})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c, dir
}

func sampleInput(tag string) BuildInput {
	return BuildInput{
		Content:    map[string]string{"main.go": "package main // " + tag},
		Toolchain:  "go:1.26.0",
		Params:     map[string]string{"mode": "release"},
		ManagedEnv: map[string]string{"GOOS": "linux"},
	}
}

func logResult(t *testing.T, c *Cache, r Result) {
	t.Helper()
	t.Logf("key=%s decision=%s shared=%v stats={%s}", r.Key, r.Decision, r.Shared, c.SnapshotStats())
}

func TestBasicMissThenHit(t *testing.T) {
	c, _ := newTestCache(t, 0, "GOOS")
	sb := NewSimBuilder()
	in := sampleInput("a")

	r1, err := c.GetOrBuild(context.Background(), in, sb)
	if err != nil {
		t.Fatalf("first build: %v", err)
	}
	logResult(t, c, r1)
	if r1.Decision.Kind != KindMiss || r1.Decision.Reason != ReasonMissNotFound {
		t.Fatalf("first lookup want MISS_NOT_FOUND, got %s", r1.Decision)
	}
	r2, err := c.GetOrBuild(context.Background(), in, sb)
	if err != nil {
		t.Fatalf("second lookup: %v", err)
	}
	logResult(t, c, r2)
	if r2.Decision.Kind != KindHit || r2.Decision.Reason != ReasonHitFresh {
		t.Fatalf("second lookup want HIT_FRESH, got %s", r2.Decision)
	}
	if sb.BuildCount() != 1 {
		t.Fatalf("want exactly 1 build, got %d", sb.BuildCount())
	}
	if string(r1.Entry.Artifact) != string(r2.Entry.Artifact) {
		t.Fatalf("cached artifact mismatch:\n%q\n%q", r1.Entry.Artifact, r2.Entry.Artifact)
	}
}

func TestBuildFailureNotCached(t *testing.T) {
	c, _ := newTestCache(t, 0, "GOOS")
	in := sampleInput("fail")
	kh := DeriveKey(in.withEnvWhitelist([]string{"GOOS"})).String()
	sb := NewSimBuilder(kh) // 该键强制失败

	r1, err := c.GetOrBuild(context.Background(), in, sb)
	if err == nil {
		t.Fatal("want build error, got nil")
	}
	t.Logf("attempt1 key=%s decision=%s err=%v stats={%s}", r1.Key, r1.Decision, err, c.SnapshotStats())
	if r1.Decision.Reason != ReasonMissBuildFailed {
		t.Fatalf("want MISS_BUILD_FAILED, got %s", r1.Decision)
	}
	// 磁盘上不得留下任何条目文件（含临时文件）。
	matches, _ := filepath.Glob(filepath.Join(c.cfg.Dir, "v2", "*"))
	for _, m := range matches {
		if !strings.HasSuffix(m, ".tmp") && strings.Contains(m, kh) {
			t.Fatalf("failed build left cache file: %s", m)
		}
		if strings.HasSuffix(m, ".tmp") {
			t.Fatalf("failed build left temp file: %s", m)
		}
	}

	// 第二次换用成功构建器：应仍然是未命中（NOT_FOUND），证明失败结果未被缓存。
	sb2 := NewSimBuilder()
	r2, err := c.GetOrBuild(context.Background(), in, sb2)
	if err != nil {
		t.Fatalf("retry build: %v", err)
	}
	logResult(t, c, r2)
	if r2.Decision.Reason != ReasonMissNotFound {
		t.Fatalf("retry after failure want MISS_NOT_FOUND, got %s", r2.Decision)
	}
	if sb2.BuildCount() != 1 {
		t.Fatalf("retry must actually build once, got %d", sb2.BuildCount())
	}
}

func TestPersistAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	c1, err := New(Config{Dir: dir, EnvWhitelist: []string{"GOOS"}})
	if err != nil {
		t.Fatal(err)
	}
	sb := NewSimBuilder()
	in := sampleInput("persist")
	r1, err := c1.GetOrBuild(context.Background(), in, sb)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("before close: %s stats={%s}", r1.Decision, c1.SnapshotStats())
	c1.Close()

	c2, err := New(Config{Dir: dir, EnvWhitelist: []string{"GOOS"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	sb2 := NewSimBuilder()
	r2, err := c2.GetOrBuild(context.Background(), in, sb2)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after reopen: %s stats={%s}", r2.Decision, c2.SnapshotStats())
	if r2.Decision.Reason != ReasonHitFresh {
		t.Fatalf("want HIT_FRESH after reopen, got %s", r2.Decision)
	}
	if sb2.BuildCount() != 0 {
		t.Fatalf("reopened cache must not rebuild, builds=%d", sb2.BuildCount())
	}
}
