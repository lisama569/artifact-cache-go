package cache

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func diskBytes(t *testing.T, dir string) (int, int64) {
	t.Helper()
	var n int
	var total int64
	err := filepath.Walk(filepath.Join(dir, "v2"), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		n++
		total += info.Size()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return n, total
}

func distinctInputs(prefix string, n int) []BuildInput {
	out := make([]BuildInput, n)
	for i := 0; i < n; i++ {
		out[i] = BuildInput{
			Content:    map[string]string{"f.go": prefix + ":" + string(rune('a'+i))},
			Toolchain:  "go:1.26.0",
			Params:     map[string]string{"i": string(rune('a' + i))},
			ManagedEnv: map[string]string{"GOOS": "linux"},
		}
	}
	return out
}

func TestEvictionLRUDeterministicAndBounded(t *testing.T) {
	c, dir := newTestCache(t, 400, "GOOS") // 每条磁盘约 188~191 字节 => 最多保留 2 条
	inputs := distinctInputs("lru", 5)
	var keys []string
	for i, in := range inputs {
		sb := NewSimBuilder()
		r, err := c.GetOrBuild(context.Background(), in, sb)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, r.Key.String())
		st := c.SnapshotStats()
		diskN, diskTotal := diskBytes(t, dir)
		t.Logf("insert#%d key=%s decision=%s stats={%s} diskFiles=%d diskBytes=%d",
			i, r.Key, r.Decision.Reason, st, diskN, diskTotal)
		if st.CurrentBytes > st.LimitBytes {
			t.Fatalf("after insert#%d bytes %d exceeded limit %d", i, st.CurrentBytes, st.LimitBytes)
		}
		if diskTotal != st.CurrentBytes || diskN != st.Entries {
			t.Fatalf("disk/accounting mismatch: files=%d/%d bytes=%d/%d",
				diskN, st.Entries, diskTotal, st.CurrentBytes)
		}
		if sb.BuildCount() != 1 {
			t.Fatalf("each new key must build once")
		}
	}
	st := c.SnapshotStats()
	if st.Entries != 2 {
		t.Fatalf("want 2 surviving entries, got %d (%s)", st.Entries, st)
	}
	// 确定性：被淘汰的必须是最早插入的 3 个键；最后 2 个存活。
	survivors := map[string]bool{keys[3]: true, keys[4]: true}
	for _, kh := range keys[:3] {
		if _, err := os.Stat(filepath.Join(dir, "v2", kh+".entry")); !os.IsNotExist(err) {
			t.Fatalf("evicted key %s still on disk", kh)
		}
	}
	for kh := range survivors {
		if _, err := os.Stat(filepath.Join(dir, "v2", kh+".entry")); err != nil {
			t.Fatalf("surviving key %s missing: %v", kh, err)
		}
	}
	// 重新打开：索引与磁盘仍一致。
	c.Close()
	c2, err := New(Config{Dir: dir, MaxBytes: 400, EnvWhitelist: []string{"GOOS"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	st2 := c2.SnapshotStats()
	diskN, diskTotal := diskBytes(t, dir)
	t.Logf("after reopen: stats={%s} diskFiles=%d diskBytes=%d", st2, diskN, diskTotal)
	if st2.Entries != diskN || st2.CurrentBytes != diskTotal {
		t.Fatal("accounting inconsistent after reopen")
	}
}

func TestEvictionTieBreakDeterministic(t *testing.T) {
	// 容量仅够 2 条；5 个键的 atime 各不相同，LRU 应严格按访问顺序。
	// 本用例在 -count=N 下重复执行，结果必须一致（用排序后的存活键集合比对）。
	c, _ := newTestCache(t, 400, "GOOS")
	inputs := distinctInputs("tie", 5)
	var last2 []string
	for i, in := range inputs {
		r, err := c.GetOrBuild(context.Background(), in, NewSimBuilder())
		if err != nil {
			t.Fatal(err)
		}
		if i >= 3 {
			last2 = append(last2, r.Key.String())
		}
	}
	sort.Strings(last2)
	var got []string
	for kh := range c.items {
		got = append(got, kh)
	}
	sort.Strings(got)
	if len(got) != 2 || got[0] != last2[0] || got[1] != last2[1] {
		t.Fatalf("deterministic LRU survivors mismatch: got=%v want=%v stats={%s}", got, last2, c.SnapshotStats())
	}
	t.Logf("survivors=%v stats={%s}", got, c.SnapshotStats())
}

// slowReadBuilder 让我们在“读取磁盘”窗口内插入新条目触发回收：
// 条目读取发生在锁外，期间另一个键写入并超限时，正在读取的键因 refs>0 必须存活。
func TestEntryProtectedWhileBeingRead(t *testing.T) {
	dir := t.TempDir()
	c, err := New(Config{Dir: dir, MaxBytes: 400, EnvWhitelist: []string{"GOOS"}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// 先放入两个键 kA、kB（占满 ~400 字节以内）。
	inA := distinctInputs("prot", 3)[0]
	inB := distinctInputs("prot", 3)[1]
	inC := distinctInputs("prot", 3)[2]
	rA, _ := c.GetOrBuild(context.Background(), inA, NewSimBuilder())
	c.GetOrBuild(context.Background(), inB, NewSimBuilder())

	// 人为给 kA 加读引用，模拟“正在读取”。
	c.mu.Lock()
	c.items[rA.Key.String()].refs++
	guard := c.items[rA.Key.String()].atime
	c.mu.Unlock()

	// 插入 kC 触发回收：kB（较旧且未在用）应被淘汰，kA 因 refs>0 不得淘汰。
	rC, err := c.GetOrBuild(context.Background(), inC, NewSimBuilder())
	if err != nil {
		t.Fatal(err)
	}
	_ = guard
	t.Logf("inserted during read-protection: %s stats={%s}", rC.Decision, c.SnapshotStats())
	st := c.SnapshotStats()
	if st.CurrentBytes > st.LimitBytes {
		t.Fatalf("bytes %d over limit %d", st.CurrentBytes, st.LimitBytes)
	}
	if _, ok := c.items[rA.Key.String()]; !ok {
		t.Fatal("protected (being-read) entry was evicted")
	}
	if _, err := os.Stat(entryPath(dir, rA.Key.String())); err != nil {
		t.Fatalf("protected entry missing on disk: %v", err)
	}
	kb := DeriveKey(inB.withEnvWhitelist([]string{"GOOS"})).String()
	if _, ok := c.items[kb]; ok {
		t.Fatal("old unprotected entry kB should have been evicted")
	}

	// 释放读引用后，下一轮回收可以淘汰 kA。
	c.mu.Lock()
	c.items[rA.Key.String()].refs--
	c.mu.Unlock()
	inD := func() BuildInput {
		xs := distinctInputs("prot2", 5)
		return xs[4]
	}()
	rD, err := c.GetOrBuild(context.Background(), inD, NewSimBuilder())
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after release: %s stats={%s}", rD.Decision, c.SnapshotStats())
	if _, ok := c.items[rA.Key.String()]; ok {
		t.Fatal("kA must be evictable after read reference released")
	}
}

// TestEvictionDuringActiveReadRealInterleave 使用测试钩子制造真实交错：
// 读取协程持读引用等待时，写入协程插入新键触发回收，被读条目不得被淘汰。
func TestEvictionDuringActiveReadRealInterleave(t *testing.T) {
	dir := t.TempDir()
	readEntered := make(chan struct{})
	releaseRead := make(chan struct{})
	ins := distinctInputs("interleave", 4)
	kA := DeriveKey(ins[0].withEnvWhitelist([]string{"GOOS"})).String()

	cfg := Config{
		Dir:          dir,
		MaxBytes:     400,
		EnvWhitelist: []string{"GOOS"},
		BeforeReadEntry: func(keyHex string, refs int) {
			if keyHex != kA {
				return // 只在读取 kA 时制造交错窗口
			}
			select {
			case <-readEntered:
			default:
				close(readEntered)
			}
			<-releaseRead
		},
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// 预置 kA、kB 两条（约 384 字节）。
	c.GetOrBuild(context.Background(), ins[0], NewSimBuilder())
	c.GetOrBuild(context.Background(), ins[1], NewSimBuilder())

	// 读取 kA：命中读取路径，钩子内阻塞，持 refs=1。
	readDone := make(chan Result, 1)
	go func() {
		r, err := c.GetOrBuild(context.Background(), ins[0], NewSimBuilder())
		if err != nil {
			t.Errorf("read kA: %v", err)
		}
		readDone <- r
	}()
	<-readEntered

	// 此刻 kA 正在被读取。写入 kC（新键）触发 LRU 回收：
	// kB 最旧且无引用 -> 淘汰；kA refs>0 -> 必须存活，哪怕它更旧。
	rC, err := c.GetOrBuild(context.Background(), ins[2], NewSimBuilder())
	if err != nil {
		t.Fatal(err)
	}
	st1 := c.SnapshotStats()
	t.Logf("during read: inserted kC %s stats={%s}", rC.Decision.Reason, st1)
	if st1.CurrentBytes > st1.LimitBytes {
		t.Fatalf("over limit during active read: %d > %d", st1.CurrentBytes, st1.LimitBytes)
	}
	c.mu.Lock()
	if _, ok := c.items[kA]; !ok {
		c.mu.Unlock()
		t.Fatal("entry being read was evicted (protected by refs)")
	}
	kB := DeriveKey(ins[1].withEnvWhitelist([]string{"GOOS"})).String()
	if _, ok := c.items[kB]; ok {
		c.mu.Unlock()
		t.Fatal("oldest unprotected kB should be evicted")
	}
	c.mu.Unlock()
	if _, err := os.Stat(entryPath(dir, kA)); err != nil {
		t.Fatalf("protected file missing on disk: %v", err)
	}

	// 释放读取窗口：读者应正常拿到完整产物并命中。
	close(releaseRead)
	r := <-readDone
	if r.Decision.Reason != ReasonHitFresh {
		t.Fatalf("blocked reader want HIT_FRESH, got %s", r.Decision)
	}
	t.Logf("reader resumed: %s stats={%s}", r.Decision, c.SnapshotStats())
}
