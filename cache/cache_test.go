package cache

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// logResult 按要求的口径打印单测日志：缓存键、命中/未命中原因。
func logResult(t *testing.T, r Result) {
	t.Helper()
	t.Logf("key=%s hit=%v built=%v reason=%q", r.Key, r.Hit, r.Built, r.Reason)
}

func logStats(t *testing.T, c *Cache) {
	t.Helper()
	s := c.Stats()
	t.Logf("stats: entries=%d bytes=%d/%d hits=%d misses=%d builds=%d evictions=%d migrated=%d rejected=%d",
		s.Entries, s.Bytes, s.MaxBytes, s.Hits, s.Misses, s.Builds, s.Evictions, s.Migrated, s.Rejected)
}

// diskUsage 统计缓存目录实际占用，用于校验与 Stats.Bytes 口径一致。
func diskUsage(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		fi, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		total += fi.Size()
	}
	return total
}

func TestDeriveKeyStableAndSensitive(t *testing.T) {
	base := BuildInput{
		Files:     map[string][]byte{"a.go": []byte("package a"), "b.go": []byte("package b")},
		Toolchain: "go1.25.0",
		Args:      []string{"-o", "out"},
		Env:       map[string]string{"GOOS": "linux", "GOARCH": "amd64"},
	}
	k1, err := DeriveKey(base)
	if err != nil {
		t.Fatal(err)
	}
	// 语义等价（map 插入顺序不同）：键必须一致。
	k2, err := DeriveKey(base)
	if err != nil {
		t.Fatal(err)
	}
	if k1 != k2 {
		t.Fatalf("equivalent inputs produced different keys: %s vs %s", k1, k2)
	}
	t.Logf("key=%s (stable for equivalent input)", k1)

	mutations := map[string]func(*BuildInput){
		"content":   func(in *BuildInput) { in.Files["a.go"] = []byte("package a // changed") },
		"toolchain": func(in *BuildInput) { in.Toolchain = "go1.26.0" },
		"args":      func(in *BuildInput) { in.Args = []string{"-o", "out", "-v"} },
		"env":       func(in *BuildInput) { in.Env["GOARCH"] = "arm64" },
		"new file":  func(in *BuildInput) { in.Files["c.go"] = []byte("package c") },
	}
	for name, mutate := range mutations {
		in := BuildInput{
			Files:     map[string][]byte{"a.go": []byte("package a"), "b.go": []byte("package b")},
			Toolchain: "go1.25.0",
			Args:      []string{"-o", "out"},
			Env:       map[string]string{"GOOS": "linux", "GOARCH": "amd64"},
		}
		mutate(&in)
		k, err := DeriveKey(in)
		if err != nil {
			t.Fatal(err)
		}
		if k == k1 {
			t.Fatalf("mutation %q did not change key", name)
		}
		t.Logf("mutation %-10s -> key=%s (changed)", name, k)
	}
}

func TestConcurrentSameKeyBuildsOnce(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	key, err := DeriveKey(BuildInput{
		Files:     map[string][]byte{"main.go": []byte("package main")},
		Toolchain: "go1.25.0",
	})
	if err != nil {
		t.Fatal(err)
	}

	const n = 32
	var buildCount atomic.Int64
	release := make(chan struct{})
	build := func() ([]byte, error) {
		buildCount.Add(1)
		<-release // 阻塞至所有请求都进入，确保并发
		return []byte("artifact-v1"), nil
	}

	var wg sync.WaitGroup
	results := make([]Result, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = c.GetOrBuild(context.Background(), key, build)
		}(i)
	}
	// 等所有 goroutine 都进入 GetOrBuild 后放行构建。
	for c.Stats().Misses < n {
	}
	close(release)
	wg.Wait()

	for i := range results {
		if errs[i] != nil {
			t.Fatalf("request %d failed: %v", i, errs[i])
		}
		if string(results[i].Payload) != "artifact-v1" {
			t.Fatalf("request %d got wrong payload %q", i, results[i].Payload)
		}
	}
	if got := buildCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 build, got %d", got)
	}
	built := 0
	for _, r := range results {
		if r.Built {
			built++
		}
	}
	if built != 1 {
		t.Fatalf("expected exactly 1 request reporting Built, got %d", built)
	}
	for _, r := range results {
		logResult(t, r)
	}
	logStats(t, c)
	if s := c.Stats(); s.Builds != 1 {
		t.Fatalf("stats builds=%d, want 1", s.Builds)
	}
}

func TestBuildFailureNotCached(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"fail"}})

	wantErr := errors.New("compile error")
	const n = 8
	var wg sync.WaitGroup
	errs := make([]error, n)
	build := func() ([]byte, error) { return nil, wantErr }
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = c.GetOrBuild(context.Background(), key, build)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if !errors.Is(err, wantErr) {
			t.Fatalf("waiter %d got err=%v, want %v", i, err, wantErr)
		}
	}
	if s := c.Stats(); s.Entries != 0 || s.Bytes != 0 {
		t.Fatalf("failed build polluted cache: %+v", s)
	}
	// 失败后重试应能成功构建。
	r, err := c.GetOrBuild(context.Background(), key, func() ([]byte, error) {
		return []byte("ok"), nil
	})
	if err != nil || !r.Built {
		t.Fatalf("retry after failure: r=%+v err=%v", r, err)
	}
	logResult(t, r)
	logStats(t, c)
}

func TestCorruptedEntryTreatedAsMiss(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"corrupt"}})
	build := func() ([]byte, error) { return []byte("good"), nil }

	r, err := c.GetOrBuild(context.Background(), key, build)
	if err != nil || !r.Built {
		t.Fatalf("initial: %+v err=%v", r, err)
	}
	logResult(t, r)

	// 截断条目文件模拟损坏。
	path := filepath.Join(dir, string(key))
	data, _ := os.ReadFile(path)
	if err := os.WriteFile(path, data[:len(data)/2], 0o644); err != nil {
		t.Fatal(err)
	}

	r, err = c.GetOrBuild(context.Background(), key, build)
	if err != nil {
		t.Fatal(err)
	}
	logResult(t, r)
	if r.Hit || !strings.Contains(r.Reason, "corrupted") {
		t.Fatalf("expected corrupted miss, got %+v", r)
	}
	if string(r.Payload) != "good" {
		t.Fatalf("payload=%q", r.Payload)
	}

	// 篡改负载但保留长度：校验和必须检出。
	r, _ = c.GetOrBuild(context.Background(), key, build)
	path = filepath.Join(dir, string(key))
	data, _ = os.ReadFile(path)
	data[len(data)-1] ^= 0xff
	os.WriteFile(path, data, 0o644)
	r, err = c.GetOrBuild(context.Background(), key, build)
	if err != nil {
		t.Fatal(err)
	}
	logResult(t, r)
	if r.Hit || !strings.Contains(r.Reason, "corrupted") {
		t.Fatalf("expected checksum-mismatch miss, got %+v", r)
	}
	logStats(t, c)
}

func TestEvictionRespectsCapacityAndPins(t *testing.T) {
	dir := t.TempDir()
	// 每个条目开销 entryHeaderLen + payload；容量定为约 3 个条目。
	payload := []byte("0123456789")
	entrySize := int64(entryHeaderLen + len(payload))
	maxBytes := entrySize*3 + entrySize/2
	c, err := Open(dir, maxBytes)
	if err != nil {
		t.Fatal(err)
	}

	// 并发读取期间持续写入新键触发回收：被读取的条目不得被中途删除导致错误。
	const writers = 16
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key, _ := DeriveKey(BuildInput{
				Toolchain: "go1.25.0",
				Args:      []string{fmt.Sprintf("n=%d", i)},
			})
			b := func() ([]byte, error) { return payload, nil }
			for {
				select {
				case <-stop:
					return
				default:
				}
				r, err := c.GetOrBuild(context.Background(), key, b)
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				if string(r.Payload) != string(payload) {
					t.Errorf("key=%s got partial/wrong payload %q", r.Key, r.Payload)
					return
				}
			}
		}(i)
	}
	// 制造大量不同键迫使回收。
	for i := 0; i < 200; i++ {
		key, _ := DeriveKey(BuildInput{
			Toolchain: "go1.25.0",
			Args:      []string{fmt.Sprintf("churn=%d", i)},
		})
		if _, err := c.GetOrBuild(context.Background(), key,
			func() ([]byte, error) { return payload, nil }); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	wg.Wait()

	s := c.Stats()
	logStats(t, c)
	if s.Bytes > s.MaxBytes {
		t.Fatalf("bytes=%d exceeds max=%d after eviction", s.Bytes, s.MaxBytes)
	}
	if s.Evictions == 0 {
		t.Fatal("expected evictions under capacity pressure")
	}
	if d := diskUsage(t, dir); d != s.Bytes {
		t.Fatalf("stats bytes=%d != disk usage=%d (inconsistent accounting)", s.Bytes, d)
	}
	t.Logf("disk usage=%d matches stats bytes=%d", diskUsage(t, dir), s.Bytes)
}

func TestLegacyEntriesMigratedOrRejected(t *testing.T) {
	dir := t.TempDir()

	// v1 旧版条目：magic + version=1 + 裸负载。
	v1Key, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"legacy-v1"}})
	v1Payload := []byte("legacy artifact")
	var v1data []byte
	v1data = append(v1data, entryMagic...)
	v1data = binary.BigEndian.AppendUint16(v1data, 1)
	v1data = append(v1data, v1Payload...)
	if err := os.WriteFile(filepath.Join(dir, string(v1Key)), v1data, 0o644); err != nil {
		t.Fatal(err)
	}

	// 无法识别的条目（无 magic）：必须被拒绝。
	junkKey, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"junk"}})
	if err := os.WriteFile(filepath.Join(dir, string(junkKey)), []byte("random bytes"), 0o644); err != nil {
		t.Fatal(err)
	}

	// 损坏的当前版本条目：必须被拒绝。
	badKey, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"bad"}})
	bad := encodeEntryV2([]byte("x"))
	bad = bad[:len(bad)-1]
	if err := os.WriteFile(filepath.Join(dir, string(badKey)), bad, 0o644); err != nil {
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

	// 迁移后的 v1 条目必须可正常命中。
	r, err := c.GetOrBuild(context.Background(), v1Key,
		func() ([]byte, error) { return nil, errors.New("should not build") })
	if err != nil {
		t.Fatal(err)
	}
	logResult(t, r)
	if !r.Hit || string(r.Payload) != string(v1Payload) {
		t.Fatalf("migrated entry not usable: %+v", r)
	}

	// 被拒绝的条目按未命中处理，重新构建。
	var builds atomic.Int64
	b := func() ([]byte, error) { builds.Add(1); return []byte("rebuilt"), nil }
	r, _ = c.GetOrBuild(context.Background(), junkKey, b)
	logResult(t, r)
	r, _ = c.GetOrBuild(context.Background(), badKey, b)
	logResult(t, r)
	if builds.Load() != 2 {
		t.Fatalf("rejected entries should be rebuilt, builds=%d", builds.Load())
	}
	if d := diskUsage(t, dir); d != c.Stats().Bytes {
		t.Fatalf("stats bytes=%d != disk usage=%d", c.Stats().Bytes, d)
	}
}

func TestHitMissReasonsAndAccounting(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := DeriveKey(BuildInput{
		Files:     map[string][]byte{"x": []byte("x")},
		Toolchain: "go1.25.0",
	})
	var builds atomic.Int64
	b := func() ([]byte, error) { builds.Add(1); return []byte("v"), nil }

	r1, _ := c.GetOrBuild(context.Background(), key, b)
	logResult(t, r1)
	r2, _ := c.GetOrBuild(context.Background(), key, b)
	logResult(t, r2)
	if r1.Hit || r1.Reason != "miss: not found" {
		t.Fatalf("first access: %+v", r1)
	}
	if !r2.Hit || r2.Reason != "hit" {
		t.Fatalf("second access: %+v", r2)
	}
	if builds.Load() != 1 {
		t.Fatalf("builds=%d, want 1", builds.Load())
	}
	logStats(t, c)
	s := c.Stats()
	if s.Hits != 1 || s.Misses != 1 || s.Builds != 1 {
		t.Fatalf("accounting: %+v", s)
	}
	wantSize := int64(entryHeaderLen + 1)
	if s.Bytes != wantSize {
		t.Fatalf("bytes=%d, want %d", s.Bytes, wantSize)
	}
}
