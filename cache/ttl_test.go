package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestEntryTTLExpiresAndRebuilds 覆盖有效期的核心判定：
// 未到期复用、到期后即使内容完好也必须重建、单条 TTL 覆盖实例默认值、
// 单条指定永不过期。
func TestEntryTTLExpiresAndRebuilds(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 27, 8, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)
	c, err := OpenWithOptions(dir, 1<<20, OpenOptions{DefaultTTL: time.Hour, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}

	key, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"ttl"}})
	var builds atomic.Int64
	build := func() ([]byte, error) {
		n := builds.Add(1)
		return []byte(fmt.Sprintf("artifact-%d", n)), nil
	}

	// 首次：未命中并构建，TTL 取实例默认 1h。
	r, err := c.GetOrBuild(context.Background(), key, build)
	if err != nil || !r.Built {
		t.Fatalf("initial build: %+v err=%v", r, err)
	}
	logResult(t, r)
	if r.Reason != ReasonMissNotFound {
		t.Fatalf("reason=%q want %q", r.Reason, ReasonMissNotFound)
	}

	// 推进到 TTL 前一刻：仍应命中且复用，不重新构建。
	clk.Advance(59 * time.Minute)
	r, err = c.GetOrBuild(context.Background(), key, build)
	if err != nil {
		t.Fatal(err)
	}
	logResult(t, r)
	if !r.Hit || string(r.Payload) != "artifact-1" {
		t.Fatalf("before TTL should reuse, got %+v", r)
	}

	// 推进到恰好 TTL：now >= created+ttl，判定过期，必须重建。
	clk.Advance(time.Minute)
	r, err = c.GetOrBuild(context.Background(), key, build)
	if err != nil {
		t.Fatal(err)
	}
	logResult(t, r)
	if r.Hit || r.Reason != ReasonMissExpired || !r.Built {
		t.Fatalf("at TTL boundary want expired miss+rebuild, got %+v", r)
	}
	if string(r.Payload) != "artifact-2" {
		t.Fatalf("payload=%q want artifact-2", r.Payload)
	}
	if s := c.Stats(); s.Expired != 1 {
		t.Fatalf("stats expired=%d want 1", s.Expired)
	}

	// 继续向前推远超 1h：上一条是刚重建的，仍未到期。
	clk.Advance(30 * time.Minute)
	r, _ = c.GetOrBuild(context.Background(), key, build)
	logResult(t, r)
	if !r.Hit {
		t.Fatalf("freshly rebuilt entry should hit, got %+v", r)
	}
	logStats(t, c)
	if builds.Load() != 2 {
		t.Fatalf("builds=%d want 2", builds.Load())
	}

	// 单条写入指定更短 TTL（1 秒），覆盖实例默认 1h。
	shortKey, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"short-ttl"}})
	r, _ = c.GetOrBuild(context.Background(), shortKey, build, WithTTL(time.Second))
	logResult(t, r)
	clk.Advance(time.Second)
	r, _ = c.GetOrBuild(context.Background(), shortKey, build)
	logResult(t, r)
	if r.Hit || !strings.HasPrefix(r.Reason, ReasonMissExpired) {
		t.Fatalf("per-entry TTL should expire, got %+v", r)
	}

	// 单条写入显式永不过期（WithTTL(0)）：时间再怎么推也复用。
	foreverKey, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"forever"}})
	r, _ = c.GetOrBuild(context.Background(), foreverKey, build, WithTTL(0))
	logResult(t, r)
	clk.Advance(100 * 24 * time.Hour)
	r, _ = c.GetOrBuild(context.Background(), foreverKey, build)
	logResult(t, r)
	if !r.Hit || r.Reason != ReasonHit {
		t.Fatalf("WithTTL(0) entry must never expire, got %+v", r)
	}
	logStats(t, c)

	if d := diskUsage(t, dir); d != c.Stats().Bytes {
		t.Fatalf("disk=%d != stats bytes=%d", d, c.Stats().Bytes)
	}
}

// TestDefaultTTLAppliesWithoutPerWriteOption 确认未显式指定 WithTTL 时
// 套用实例默认 TTL；不配置默认 TTL 时条目永久有效。
func TestDefaultTTLAppliesWithoutPerWriteOption(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 27, 9, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)
	c, err := OpenWithOptions(dir, 1<<20, OpenOptions{DefaultTTL: 10 * time.Minute, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	key, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"default-ttl"}})
	var builds atomic.Int64
	build := func() ([]byte, error) { builds.Add(1); return []byte("x"), nil }
	r, _ := c.GetOrBuild(context.Background(), key, build)
	logResult(t, r)
	clk.Advance(10*time.Minute + time.Nanosecond)
	r, _ = c.GetOrBuild(context.Background(), key, build)
	logResult(t, r)
	if r.Hit || r.Reason != ReasonMissExpired {
		t.Fatalf("default TTL not applied: %+v", r)
	}

	// 无默认 TTL 的实例：时间任意推进都不命中过期。
	dir2 := t.TempDir()
	clk2 := NewFakeClock(start)
	c2, _ := OpenWithOptions(dir2, 1<<20, OpenOptions{Clock: clk2})
	k2, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"no-default"}})
	c2.GetOrBuild(context.Background(), k2, func() ([]byte, error) { return []byte("y"), nil })
	clk2.Advance(10 * 365 * 24 * time.Hour)
	r, _ = c2.GetOrBuild(context.Background(), k2, func() ([]byte, error) { return []byte("y"), nil })
	logResult(t, r)
	if !r.Hit {
		t.Fatalf("no default TTL means never expire, got %+v", r)
	}
}

// TestTTLPersistedAcrossReopen 确认有效期随条目落盘：用旧目录重新打开后，
// 条目仍按持久化的创建时间+TTL 判定过期。
func TestTTLPersistedAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)
	c, _ := OpenWithOptions(dir, 1<<20, OpenOptions{Clock: clk})
	key, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"persist-ttl"}})
	r, _ := c.GetOrBuild(context.Background(), key,
		func() ([]byte, error) { return []byte("persisted"), nil }, WithTTL(time.Hour))
	logResult(t, r)

	// 重新打开：虚拟时间推进 61 分钟，持久化条目必须已过期。
	clk.Advance(61 * time.Minute)
	c2, err := OpenWithOptions(dir, 1<<20, OpenOptions{Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	var builds atomic.Int64
	r, err = c2.GetOrBuild(context.Background(), key, func() ([]byte, error) {
		builds.Add(1)
		return []byte("rebuilt"), nil
	})
	logResult(t, r)
	if r.Hit || r.Reason != ReasonMissExpired {
		t.Fatalf("persisted TTL should expire after reopen, got %+v", r)
	}
	if builds.Load() != 1 || string(r.Payload) != "rebuilt" {
		t.Fatalf("rebuild mismatch: builds=%d payload=%q", builds.Load(), r.Payload)
	}
	logStats(t, c2)
}

// TestMissReasonsDistinguishNotFoundExpiredCorrupted 确认三类未命中原因
// （不存在 / 已过期 / 内容有问题）可明确区分。
func TestMissReasonsDistinguishNotFoundExpiredCorrupted(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 27, 11, 0, 0, 0, time.UTC)
	clk := NewFakeClock(start)
	c, _ := OpenWithOptions(dir, 1<<20, OpenOptions{DefaultTTL: time.Minute, Clock: clk})

	missing, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"missing"}})
	expired, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"expired"}})
	corrupt, _ := DeriveKey(BuildInput{Toolchain: "go1.25.0", Args: []string{"corrupt"}})
	build := func(v string) func() ([]byte, error) {
		return func() ([]byte, error) { return []byte(v), nil }
	}

	r, _ := c.GetOrBuild(context.Background(), missing, build("m"))
	logResult(t, r)
	if r.Reason != ReasonMissNotFound {
		t.Fatalf("want not found, got %q", r.Reason)
	}

	r, _ = c.GetOrBuild(context.Background(), expired, build("e"), WithTTL(time.Second))
	logResult(t, r)
	clk.Advance(time.Second)
	r, _ = c.GetOrBuild(context.Background(), expired, build("e2"))
	logResult(t, r)
	if r.Reason != ReasonMissExpired {
		t.Fatalf("want expired, got %q", r.Reason)
	}

	r, _ = c.GetOrBuild(context.Background(), corrupt, build("c"))
	logResult(t, r)
	path := filepath.Join(dir, string(corrupt))
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	r, _ = c.GetOrBuild(context.Background(), corrupt, build("c2"))
	logResult(t, r)
	if r.Hit || !strings.HasPrefix(r.Reason, ReasonMissCorrupted) {
		t.Fatalf("want corrupted, got %q", r.Reason)
	}
	logStats(t, c)
}
