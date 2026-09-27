package cache

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Result 描述一次缓存访问的结果，命中与未命中均给出可解释原因。
type Result struct {
	Key     Key
	Payload []byte
	Hit     bool
	// Reason 解释结果来源：
	//   "hit"                       直接命中；
	//   "miss: not found"           无此条目后由本请求构建；
	//   "miss: expired entry"       条目已超过有效期，按未命中处理后重建；
	//   "miss: corrupted entry: …"  条目损坏，按未命中处理后重建；
	//   "shared build result"       合并到并发构建，复用其结果。
	Reason string
	// Built 表示本次请求触发了实际构建。
	Built bool
}

// Stats 为缓存统计。口径一致：Bytes 等于索引中各条目大小之和，
// 与磁盘实际占用一致；Hits+Misses 等于 GetOrBuild 调用次数。
type Stats struct {
	Entries   int
	Bytes     int64
	MaxBytes  int64
	Hits      int64
	Misses    int64
	Builds    int64 // 实际执行的构建次数（含并发合并后仅一次）
	Evictions int64
	Expired   int64 // 因超过有效期被移除的条目数（Open/访问/巡检累计）
	Migrated  int64 // Open 时迁移到当前格式的旧版本条目数
	Rejected  int64 // Open 时拒绝并移除的不可识别条目数
}

// Option 配置缓存实例的可选行为。
type Option func(*Cache)

// WithMaxAge 设置缓存实例的默认最大存活时长。
// <=0 表示条目默认永不过期；单次写入可用 GetOrBuildWithTTL 覆盖。
func WithMaxAge(d time.Duration) Option {
	return func(c *Cache) { c.maxAge = d }
}

// WithClock 注入时间源，用于有效期判定。
// 测试中可注入可控时钟，把逻辑时间往前推而无需真实等待。
func WithClock(now func() time.Time) Option {
	return func(c *Cache) {
		if now != nil {
			c.now = now
		}
	}
}

// entryMeta 为索引中的条目元数据。
type entryMeta struct {
	key       Key
	size      int64
	expiresAt time.Time // 零值 = 永不过期
	refs      int       // 正在读取的引用计数；>0 时不得被淘汰
	// publishing 表示该条目由巡检"认领"（并发构建已 rename 落盘但构建方
	// 尚未把它纳入索引）。构建方提交前不得淘汰，否则会删掉构建方即将
	// 声明的产物，造成索引/磁盘错位。
	publishing bool
	elem       *list.Element
}

// call 表示一次进行中的构建（单飞）。
type call struct {
	done      chan struct{}
	payload   []byte
	err       error
	expiresAt time.Time // 构建方为本次写入计算的过期时间（零值 = 永不过期）
}

// Cache 是基于本地文件系统的构建产物缓存。
type Cache struct {
	dir      string
	maxBytes int64
	maxAge   time.Duration
	now      func() time.Time

	mu       sync.Mutex
	entries  map[Key]*entryMeta
	lru      *list.List // 前端为最近使用
	bytes    int64
	inflight map[Key]*call

	// pubMu 为"发布锁"，只协调两件事：
	//   - 构建方把临时文件 rename 为最终条目文件（持有读锁）；
	//   - 巡检收尾做目录重扫与对账（持有写锁）。
	// 它不覆盖耗时的构建与临时文件写入，因此不会拖慢正常读写；
	// 收尾取写锁后，不会有任何 rename 与收尾交错，收尾看到的目录
	// 与索引可以做到逐文件一一对应。锁序固定为：mu → pubMu
	// （收尾是先放 mu 再取 pubMu、再取 mu，二者不嵌套），无死锁。
	pubMu sync.RWMutex

	// 测试钩子（仅测试注入；nil 表示不触发）。
	// afterBuildCommitted：构建方 rename 落盘、释放发布锁之后、
	//   再取 mu 入索引之前触发（不持任何锁）。
	// afterSweepScanned：巡检完成第一遍索引核对之后、释放 mu 等待
	//   发布静止之前触发（持有 mu）。
	afterBuildCommitted func(key Key)
	afterSweepScanned   func()

	hits, misses, builds, evictions int64
	expired, migrated, rejected     int64
}

// Open 打开（必要时创建）位于 dir 的缓存，容量上限为 maxBytes。
// 打开时扫描既有条目：当前版本条目纳入索引；v1/v2 旧版条目按确定规则
// 迁移到当前格式；已过期、损坏或无法识别的条目明确移除并计数，
// 绝不静默误用。
func Open(dir string, maxBytes int64, opts ...Option) (*Cache, error) {
	if maxBytes <= 0 {
		return nil, errors.New("cache: maxBytes must be positive")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	c := &Cache{
		dir:      dir,
		maxBytes: maxBytes,
		now:      time.Now,
		entries:  make(map[Key]*entryMeta),
		lru:      list.New(),
		inflight: make(map[Key]*call),
	}
	for _, opt := range opts {
		opt(c)
	}
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.IsDir() || strings.HasPrefix(f.Name(), ".tmp-") {
			os.Remove(filepath.Join(dir, f.Name())) // 清理残留临时文件
			continue
		}
		key := Key(f.Name())
		path := c.path(key)
		_, expiresAt, err := readEntry(path)
		if err != nil {
			if errors.Is(err, ErrUnsupportedVersion) {
				if merr := migrateEntry(path); merr == nil {
					c.migrated++
					// 迁移后即为当前格式；旧格式无有效期信息，视为永不过期。
					c.indexLocked(key, fileSize(path), time.Time{}, false)
					continue
				}
				// 迁移失败：明确拒绝
				os.Remove(path)
				c.rejected++
				continue
			}
			// 损坏条目：明确拒绝
			os.Remove(path)
			c.rejected++
			continue
		}
		if !expiresAt.IsZero() && !c.now().Before(expiresAt) {
			// 打开时已过期：不得复用，移除并计数。
			os.Remove(path)
			c.expired++
			continue
		}
		c.indexLocked(key, fileSize(path), expiresAt, false)
	}
	c.evictLocked()
	return c, nil
}

func (c *Cache) path(key Key) string {
	return filepath.Join(c.dir, string(key))
}

// expiredLocked 判定条目是否已过期；调用者须持有锁。
func (c *Cache) expiredLocked(m *entryMeta) bool {
	return !m.expiresAt.IsZero() && !c.now().Before(m.expiresAt)
}

// indexLocked 将条目加入索引与 LRU 前端；调用者须持有锁。
// publishing=true 表示该条目由巡检认领（构建方尚未提交），
// 构建方提交前不得淘汰。
func (c *Cache) indexLocked(key Key, size int64, expiresAt time.Time, publishing bool) {
	if m, ok := c.entries[key]; ok {
		c.bytes -= m.size
		c.lru.Remove(m.elem)
	}
	m := &entryMeta{key: key, size: size, expiresAt: expiresAt, publishing: publishing}
	m.elem = c.lru.PushFront(key)
	c.entries[key] = m
	c.bytes += size
}

// evictLocked 按 LRU（最久未使用优先）淘汰，直至占用不超过上限。
// refs>0（正在被读取）或 publishing（被巡检认领、构建方尚未提交）
// 的条目跳过不淘汰。调用者须持有锁。
func (c *Cache) evictLocked() {
	for c.bytes > c.maxBytes {
		victim := c.evictableLocked()
		if victim == nil {
			return // 其余条目均被钉住，待引用释放/构建提交后再回收
		}
		c.removeLocked(victim)
		c.evictions++
	}
}

// evictableLocked 从最久未使用端找第一个未钉住的条目。
func (c *Cache) evictableLocked() *entryMeta {
	for e := c.lru.Back(); e != nil; e = e.Prev() {
		m := c.entries[e.Value.(Key)]
		if m.refs == 0 && !m.publishing {
			return m
		}
	}
	return nil
}

// removeLocked 从索引与磁盘移除条目；调用者须持有锁。
// 幂等：条目已被移除时直接返回，绝不重复扣减占用统计——
// 多个请求同时撞上同一个坏条目时，占用统计仍与磁盘一致。
func (c *Cache) removeLocked(m *entryMeta) {
	if c.entries[m.key] != m {
		return
	}
	os.Remove(c.path(m.key))
	c.lru.Remove(m.elem)
	delete(c.entries, m.key)
	c.bytes -= m.size
}

// GetOrBuild 查询缓存；未命中时执行 build，条目使用实例默认有效期。
// 同一键的并发请求只触发一次实际构建，其余请求等待并复用结果；
// 构建失败不写入缓存，所有等待者收到相同错误。
func (c *Cache) GetOrBuild(ctx context.Context, key Key, build func() ([]byte, error)) (Result, error) {
	return c.GetOrBuildWithTTL(ctx, key, 0, build)
}

// GetOrBuildWithTTL 同 GetOrBuild，但允许单次写入单独指定有效期：
//
//	ttl > 0：本次写入使用该有效期；
//	ttl = 0：使用实例默认有效期（WithMaxAge）；
//	ttl < 0：本次写入永不过期。
func (c *Cache) GetOrBuildWithTTL(ctx context.Context, key Key, ttl time.Duration, build func() ([]byte, error)) (Result, error) {
	// 1. 尝试命中。
	c.mu.Lock()
	if m, ok := c.entries[key]; ok {
		if c.expiredLocked(m) {
			// 已过期：即使内容完好也不得复用，移除后按未命中重建。
			c.removeLocked(m)
			c.expired++
			c.misses++
			c.mu.Unlock()
			return c.buildLocked(ctx, key, c.expiryOf(ttl), build, "miss: expired entry")
		}
		m.refs++
		c.lru.MoveToFront(m.elem)
		c.mu.Unlock()
		payload, _, err := readEntry(c.path(key))
		c.mu.Lock()
		m.refs--
		if m.refs == 0 {
			c.evictLocked() // 释放钉住后补做回收
		}
		if err == nil {
			c.hits++
			c.mu.Unlock()
			return Result{Key: key, Payload: payload, Hit: true, Reason: "hit"}, nil
		}
		// 条目损坏或版本不符：按未命中处理，移除坏条目。
		c.removeLocked(m)
		c.misses++
		c.mu.Unlock()
		return c.buildLocked(ctx, key, c.expiryOf(ttl), build,
			fmt.Sprintf("miss: corrupted entry: %v", err))
	}
	c.misses++
	c.mu.Unlock()
	return c.buildLocked(ctx, key, c.expiryOf(ttl), build, "miss: not found")
}

// expiryOf 计算一次写入的过期时间；零值表示永不过期。
func (c *Cache) expiryOf(ttl time.Duration) time.Time {
	if ttl == 0 {
		ttl = c.maxAge
	}
	if ttl <= 0 {
		return time.Time{}
	}
	return c.now().Add(ttl)
}

// buildLocked 处理未命中：合并并发构建，仅一个请求执行实际构建。
func (c *Cache) buildLocked(ctx context.Context, key Key, expiresAt time.Time, build func() ([]byte, error), missReason string) (Result, error) {
	c.mu.Lock()
	if cl, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		select {
		case <-cl.done:
			if cl.err != nil {
				return Result{Key: key, Reason: missReason}, cl.err
			}
			return Result{Key: key, Payload: cl.payload, Hit: true,
				Reason: missReason + "; shared build result"}, nil
		case <-ctx.Done():
			return Result{Key: key, Reason: missReason}, ctx.Err()
		}
	}
	cl := &call{done: make(chan struct{}), expiresAt: expiresAt}
	c.inflight[key] = cl
	c.mu.Unlock()

	payload, err := build()
	if err != nil {
		cl.err = err
		c.mu.Lock()
		delete(c.inflight, key)
		c.mu.Unlock()
		close(cl.done)
		return Result{Key: key, Reason: missReason}, err
	}

	// 原子发布：暂存完整条目（含 fsync）后 rename。全程持有发布读锁，
	// 与巡检收尾（发布写锁）互斥——巡检要么在 rename 前看到目录，
	// 要么在 rename 后看到，绝不可能在中间态对账。
	// 临时文件同样处于读锁保护下：巡检收尾时不会有写到一半的 .tmp 文件。
	path := c.path(key)
	c.pubMu.RLock()
	tmpName, serr := stageEntry(filepath.Dir(path), payload, expiresAt)
	if serr == nil {
		serr = commitEntry(tmpName, path)
	}
	c.pubMu.RUnlock()
	if serr != nil {
		cl.err = serr
		c.mu.Lock()
		delete(c.inflight, key)
		c.mu.Unlock()
		close(cl.done)
		return Result{Key: key, Reason: missReason}, serr
	}

	// 测试钩子（如有）：已落盘、发布锁已释放，但尚未入索引。
	if c.afterBuildCommitted != nil {
		c.afterBuildCommitted(key)
	}

	cl.payload = payload
	c.mu.Lock()
	c.builds++
	// 若巡检在钩子期间把该产物"认领"进索引（publishing），此处用同一
	// key 正常替换为构建方自己的元数据（publishing 解除），占用只计一次。
	c.indexLocked(key, fileSize(path), expiresAt, false)
	c.evictLocked()
	delete(c.inflight, key)
	c.mu.Unlock()
	close(cl.done)
	return Result{Key: key, Payload: payload, Hit: false, Built: true, Reason: missReason}, nil
}

// Stats 返回当前缓存统计。
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Entries:   len(c.entries),
		Bytes:     c.bytes,
		MaxBytes:  c.maxBytes,
		Hits:      c.hits,
		Misses:    c.misses,
		Builds:    c.builds,
		Evictions: c.evictions,
		Expired:   c.expired,
		Migrated:  c.migrated,
		Rejected:  c.rejected,
	}
}
