package cache

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Builder 由使用方提供，模拟真实构建：给定输入产出产物字节。
// 失败时返回 error，失败产物不得写入缓存。
type Builder interface {
	Build(ctx context.Context, in BuildInput) ([]byte, error)
}

// BuilderFunc 让普通函数满足 Builder。
type BuilderFunc func(ctx context.Context, in BuildInput) ([]byte, error)

func (f BuilderFunc) Build(ctx context.Context, in BuildInput) ([]byte, error) {
	return f(ctx, in)
}

// Result 是 GetOrBuild 的返回。
type Result struct {
	Key      Key
	Entry    *Entry
	Decision Decision
	Shared   bool // 是否复用了同键并发构建的结果（本请求没有执行构建）
}

// Config 配置本地缓存。
type Config struct {
	// Dir 缓存根目录。
	Dir string
	// MaxBytes 容量上限；<=0 表示不限。
	MaxBytes int64
	// EnvWhitelist 参与键派生的受管环境变量白名单；其余环境变量被忽略。
	EnvWhitelist []string
	// BeforeReadEntry 为测试钩子：在索引命中、读引用已登记之后、
	// 释放锁去读磁盘之前回调（持锁调用）。生产代码留空即可。
	BeforeReadEntry func(keyHex string, refs int)
}

// Cache 是本地构建产物缓存的调度器。磁盘为权威存储，内存仅保存索引。
type Cache struct {
	cfg     Config
	mu      sync.Mutex
	items   map[string]*item
	bad     map[string]Decision // 扫描期发现的损坏/旧版本条目，供解释
	flights map[string]*flight
	clock   int64
	closed  bool
	stats   Stats
}

type item struct {
	keyHex string
	size   int64
	refs   int   // 正在读取或正在等待该键构建的引用数；>0 时禁止淘汰
	atime  int64 // LRU 访问逻辑时钟（越大越近）
}

type flight struct {
	done   chan struct{}
	entry  *Entry
	err    error
	hit    bool // 结果来自既有完整条目（leader 仅读盘），等待者应计为命中
	waiter int  // 等待者数量（不含 leader），用于保护条目不被淘汰
}

// New 创建并打开缓存：确保目录存在，扫描磁盘重建索引。
// 损坏条目与不兼容版本条目不进索引；旧版本被明确拒绝而非静默误用。
func New(cfg Config) (*Cache, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("cache: Config.Dir is required")
	}
	if err := os.MkdirAll(filepath.Join(cfg.Dir, "v2"), 0o755); err != nil {
		return nil, fmt.Errorf("cache: create dir: %w", err)
	}
	c := &Cache{
		cfg:     cfg,
		items:   map[string]*item{},
		bad:     map[string]Decision{},
		flights: map[string]*flight{},
	}
	c.stats.LimitBytes = cfg.MaxBytes
	if err := c.scanLocked(); err != nil {
		return nil, err
	}
	return c, nil
}

// scanLocked 扫描 v2 目录：有效条目入索引，其余归类为损坏/版本拒绝并清理，
// 临时文件予以删除（上次写入崩溃的残留）。
func (c *Cache) scanLocked() error {
	dir := filepath.Join(c.cfg.Dir, "v2")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("cache: scan dir: %w", err)
	}
	type pending struct{ keyHex string }
	var load []pending
	for _, de := range entries {
		name := de.Name()
		if de.IsDir() {
			continue
		}
		if len(name) > 0 && name[0] == '.' {
			// 临时文件（原子写残留），删除。
			_ = os.Remove(filepath.Join(dir, name))
			continue
		}
		keyHex, ok := validHexKey(name)
		if !ok {
			// 无法识别为合法条目名：按损坏统计并删除。
			c.stats.CorruptRejects++
			c.stats.Misses++
			_ = os.Remove(filepath.Join(dir, name))
			continue
		}
		load = append(load, pending{keyHex})
	}
	// 文件名排序保证大键集下重建顺序确定。
	sort.Slice(load, func(i, j int) bool { return load[i].keyHex < load[j].keyHex })
	var total int64
	for _, p := range load {
		path := filepath.Join(dir, p.keyHex+".entry")
		e, err := readEntryFile(path, p.keyHex)
		if err != nil {
			c.rejectFileLocked(p.keyHex, path, err)
			continue
		}
		fi, err := os.Stat(path)
		if err != nil {
			c.rejectFileLocked(p.keyHex, path, err)
			continue
		}
		if fi.Size() != e.Size() {
			c.rejectFileLocked(p.keyHex, path, &CorruptError{
				Op:  "size",
				Err: fmt.Errorf("file size %d != encoded size %d", fi.Size(), e.Size()),
			})
			continue
		}
		c.clock++
		c.items[p.keyHex] = &item{keyHex: p.keyHex, size: fi.Size(), atime: c.clock}
		total += fi.Size()
	}
	c.stats.Entries = len(c.items)
	c.stats.CurrentBytes = total
	return nil
}

// rejectFileLocked 把文件归类为可解释的未命中原因，更新统计并删除该文件。
func (c *Cache) rejectFileLocked(keyHex, path string, err error) Decision {
	var d Decision
	var ve *VersionError
	switch {
	case asVersionError(err, &ve):
		if ve.IsOld() {
			d = Decision{Kind: KindMiss, Reason: ReasonMissOldVersion,
				Detail: fmt.Sprintf("key=%s found=%d < current=%d; rejected, not migrated silently", keyHex, ve.Found, CurrentVersion)}
		} else {
			d = Decision{Kind: KindMiss, Reason: ReasonMissFuture,
				Detail: fmt.Sprintf("key=%s found=%d > current=%d; rejected", keyHex, ve.Found, CurrentVersion)}
		}
		c.stats.VersionRejects++
	default:
		d = Decision{Kind: KindMiss, Reason: ReasonMissCorrupt,
			Detail: fmt.Sprintf("key=%s treated as miss: %v", keyHex, err)}
		c.stats.CorruptRejects++
	}
	c.stats.Misses++
	c.bad[keyHex] = d
	_ = os.Remove(path)
	return d
}

func asVersionError(err error, target **VersionError) bool {
	for err != nil {
		if ve, ok := err.(*VersionError); ok {
			*target = ve
			return true
		}
		if u, ok := err.(interface{ Unwrap() error }); ok {
			err = u.Unwrap()
			continue
		}
		break
	}
	return false
}

// Close 关闭缓存（当前无后台资源，保留接口以便扩展）。
func (c *Cache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

// GetOrBuild 返回缓存产物：
//   - 完整且版本当前的条目 => HIT_FRESH；
//   - 同键并发构建进行中 => 等待并复用，不发生第二次构建（MISS_SHARED_BUILD）；
//   - 缺失/损坏/旧版本 => 执行一次构建；失败不写缓存（MISS_BUILD_FAILED）。
func (c *Cache) GetOrBuild(ctx context.Context, in BuildInput, b Builder) (Result, error) {
	if b == nil {
		return Result{}, fmt.Errorf("cache: nil builder")
	}
	filtered := in.withEnvWhitelist(c.cfg.EnvWhitelist)
	key := DeriveKey(filtered)
	kh := key.String()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return Result{}, fmt.Errorf("cache: closed")
	}
	// 并发合并：已有同键构建则成为等待者。
	if f, ok := c.flights[kh]; ok {
		f.waiter++
		c.mu.Unlock()
		return c.awaitFlight(ctx, key, filtered, f, true)
	}

	// 尝试读取现有条目：先在索引中登记引用，防止读取期间被回收。
	var miss Decision
	var path string
	var it *item
	if existing, ok := c.items[kh]; ok {
		it = existing
		it.refs++
		path = entryPath(c.cfg.Dir, kh)
	} else if d, ok := c.bad[kh]; ok {
		miss = d
		delete(c.bad, kh)
	} else {
		miss = Decision{Kind: KindMiss, Reason: ReasonMissNotFound,
			Detail: fmt.Sprintf("key=%s no entry on disk", kh)}
	}

	// 成为 leader：登记 flight，等待者会在释放锁后看到它。
	f := &flight{done: make(chan struct{})}
	c.flights[kh] = f
	c.stats.InFlight++
	c.mu.Unlock()

	// 锁外读取磁盘，避免长 I/O 阻塞其他键。
	if it != nil {
		if c.cfg.BeforeReadEntry != nil {
			c.cfg.BeforeReadEntry(kh, it.refs)
		}
		e, err := readEntryFile(path, kh)
		c.mu.Lock()
		it.refs--
		if err == nil {
			c.clock++
			it.atime = c.clock
			c.stats.Hits++
			// 读盘期间可能已有同键等待者挂在该 flight 上：
			// 必须先把结果交给 flight 再关闭 done，等待者复用同一产物。
			f.entry = e
			f.hit = true
			delete(c.flights, kh)
			c.stats.InFlight--
			c.mu.Unlock()
			close(f.done)
			return Result{Key: key, Entry: e, Decision: Decision{
				Kind: KindHit, Reason: ReasonHitFresh,
				Detail: fmt.Sprintf("key=%s bytes=%d", kh, len(e.Artifact)),
			}}, nil
		}
		// 读取时发现损坏/版本问题：摘除索引并归类，随后转入构建。
		miss = c.dropItemLocked(kh, err)
		c.mu.Unlock()
	}

	return c.runBuild(ctx, key, filtered, b, f, miss)
}

// awaitFlight 等待同键构建完成。等待期间该键条目受引用保护，不会被淘汰。
func (c *Cache) awaitFlight(ctx context.Context, key Key, in BuildInput, f *flight, waiter bool) (Result, error) {
	kh := key.String()
	select {
	case <-f.done:
	case <-ctx.Done():
		c.mu.Lock()
		f.waiter--
		c.mu.Unlock()
		return Result{}, ctx.Err()
	}
	c.mu.Lock()
	f.waiter--
	if f.err == nil && f.hit {
		c.stats.Hits++
	}
	c.mu.Unlock()
	if f.err != nil {
		return Result{Key: key, Decision: Decision{
			Kind: KindMiss, Reason: ReasonMissBuildFailed,
			Detail: fmt.Sprintf("key=%s shared build failed: %v", kh, f.err),
		}}, f.err
	}
	if f.hit {
		return Result{Key: key, Entry: f.entry, Shared: true, Decision: Decision{
			Kind: KindHit, Reason: ReasonHitFresh,
			Detail: fmt.Sprintf("key=%s joined in-flight read of existing entry, bytes=%d", kh, len(f.entry.Artifact)),
		}}, nil
	}
	return Result{Key: key, Entry: f.entry, Shared: true, Decision: Decision{
		Kind: KindMiss, Reason: ReasonMissShared,
		Detail: fmt.Sprintf("key=%s reused in-flight build, bytes=%d", kh, len(f.entry.Artifact)),
	}}, nil
}

// dropItemLocked 从索引摘除一个读取失败的条目，归类原因并删除文件。
// 调用方仍是同键 flight 的 leader，flight 本身保留到构建完成，
// 以保证损坏重建期间新到请求继续合并，不会发起第二次构建。
func (c *Cache) dropItemLocked(kh string, err error) Decision {
	if it, ok := c.items[kh]; ok {
		c.stats.Entries--
		c.stats.CurrentBytes -= it.size
		delete(c.items, kh)
	}
	path := entryPath(c.cfg.Dir, kh)
	return c.rejectFileLocked(kh, path, err)
}

// runBuild 由 leader 实际执行构建、原子写入、触发回收并唤醒等待者。
func (c *Cache) runBuild(ctx context.Context, key Key, in BuildInput, b Builder, f *flight, miss Decision) (Result, error) {
	kh := key.String()
	artifact, berr := b.Build(ctx, in)

	c.mu.Lock()
	if berr != nil {
		// 构建失败：不写缓存，等待者得到同一错误，不会拿到部分产物。
		f.err = berr
		delete(c.flights, kh)
		c.stats.InFlight--
		c.stats.Misses++
		c.mu.Unlock()
		close(f.done)
		return Result{Key: key, Decision: Decision{
			Kind: KindMiss, Reason: ReasonMissBuildFailed,
			Detail: fmt.Sprintf("key=%s build failed, nothing cached: %v (prior: %s)", kh, berr, miss.Reason),
		}}, berr
	}

	e := &Entry{Version: CurrentVersion, KeyHex: kh, Artifact: artifact}
	size := e.Size()
	stored := true
	var storeNote string
	if c.cfg.MaxBytes > 0 && size > c.cfg.MaxBytes {
		// 单条产物超过上限：不写入，保证回收后占用绝不超过上限。
		stored = false
		storeNote = fmt.Sprintf("; artifact size=%d exceeds limit=%d, not cached", size, c.cfg.MaxBytes)
	} else {
		path := entryPath(c.cfg.Dir, kh)
		if werr := writeEntryFile(path, e); werr != nil {
			// 写入失败按构建失败口径处理：不对外暴露半成品。
			f.err = werr
			delete(c.flights, kh)
			c.stats.InFlight--
			c.stats.Misses++
			c.mu.Unlock()
			close(f.done)
			return Result{Key: key, Decision: Decision{
				Kind: KindMiss, Reason: ReasonMissBuildFailed,
				Detail: fmt.Sprintf("key=%s atomic write failed: %v", kh, werr),
			}}, werr
		}
		// 替换同名旧索引项（理论上前面已摘除），保持口径一致。
		if old, ok := c.items[kh]; ok {
			c.stats.Entries--
			c.stats.CurrentBytes -= old.size
			delete(c.items, kh)
		}
		c.clock++
		c.items[kh] = &item{keyHex: kh, size: size, atime: c.clock}
		c.stats.Entries++
		c.stats.CurrentBytes += size
		c.evictLocked(kh)
	}
	c.stats.Misses++
	c.stats.Builds++
	f.entry = e
	delete(c.flights, kh)
	c.stats.InFlight--
	decision := Decision{Kind: KindMiss, Reason: miss.Reason,
		Detail: fmt.Sprintf("key=%s built once, bytes=%d%s (lookup: %s)", kh, len(artifact), storeNote, miss.Reason)}
	_ = stored
	c.mu.Unlock()

	close(f.done)
	return Result{Key: key, Entry: e, Decision: decision}, nil
}

// evictLocked 在超限时按确定的 LRU 策略淘汰：
// 按 atime 升序（同值按键十六进制升序）选择；refs>0（读取中/等待中）跳过，
// 直到占用不超过上限。受保护条目若导致无法收敛，则保留（永不淘汰在用条目）。
func (c *Cache) evictLocked(protectKey string) {
	if c.cfg.MaxBytes <= 0 {
		return
	}
	for c.stats.CurrentBytes > c.cfg.MaxBytes {
		var victim *item
		candidates := make([]*item, 0, len(c.items))
		for _, it := range c.items {
			candidates = append(candidates, it)
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].atime != candidates[j].atime {
				return candidates[i].atime < candidates[j].atime
			}
			return candidates[i].keyHex < candidates[j].keyHex
		})
		for _, it := range candidates {
			if it.keyHex == protectKey {
				continue
			}
			if it.refs > 0 {
				continue
			}
			victim = it
			break
		}
		if victim == nil {
			// 全部在用或仅剩保护键：不能淘汰，退出（新条目本身 <= 上限）。
			return
		}
		path := entryPath(c.cfg.Dir, victim.keyHex)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return // 磁盘删除失败时保持索引与文件一致，宁可不淘汰
		}
		delete(c.items, victim.keyHex)
		c.stats.Entries--
		c.stats.CurrentBytes -= victim.size
		c.stats.Evictions++
	}
}

// SnapshotStats 返回统计快照（拷贝，调用方可长期持有）。
func (c *Cache) SnapshotStats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.stats
	s.Entries = len(c.items)
	s.InFlight = len(c.flights)
	return s
}
