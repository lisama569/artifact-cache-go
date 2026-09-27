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
	elem      *list.Element
}

// call 表示一次进行中的构建（单飞）。
type call struct {
	done    chan struct{}
	payload []byte
	err     error
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
					c.indexLocked(key, fileSize(path), time.Time{})
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
		c.indexLocked(key, fileSize(path), expiresAt)
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
func (c *Cache) indexLocked(key Key, size int64, expiresAt time.Time) {
	if m, ok := c.entries[key]; ok {
		c.bytes -= m.size
		c.lru.Remove(m.elem)
	}
	m := &entryMeta{key: key, size: size, expiresAt: expiresAt}
	m.elem = c.lru.PushFront(key)
	c.entries[key] = m
	c.bytes += size
}

// evictLocked 按 LRU（最久未使用优先）淘汰，直至占用不超过上限。
// refs>0（正在被读取）的条目跳过不淘汰。调用者须持有锁。
func (c *Cache) evictLocked() {
	for c.bytes > c.maxBytes {
		victim := c.evictableLocked()
		if victim == nil {
			return // 其余条目均被钉住，待引用释放后再回收
		}
		c.removeLocked(victim)
		c.evictions++
	}
}

// evictableLocked 从最久未使用端找第一个未钉住的条目。
func (c *Cache) evictableLocked() *entryMeta {
	for e := c.lru.Back(); e != nil; e = e.Prev() {
		m := c.entries[e.Value.(Key)]
		if m.refs == 0 {
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
	cl := &call{done: make(chan struct{})}
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

	// 原子写入后再纳入索引；写入失败不污染缓存。
	if werr := writeEntryAtomic(c.path(key), payload, expiresAt); werr != nil {
		cl.err = werr
		c.mu.Lock()
		delete(c.inflight, key)
		c.mu.Unlock()
		close(cl.done)
		return Result{Key: key, Reason: missReason}, werr
	}
	cl.payload = payload
	c.mu.Lock()
	c.builds++
	c.indexLocked(key, fileSize(c.path(key)), expiresAt)
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
