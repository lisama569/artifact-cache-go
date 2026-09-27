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

// 访问结果原因（Result.Reason 的可区分取值，均为前缀，损坏类会追加具体错误）。
const (
	// ReasonHit 表示直接命中。
	ReasonHit = "hit"
	// ReasonMissNotFound 表示无此条目。
	ReasonMissNotFound = "miss: not found"
	// ReasonMissExpired 表示条目已超过有效期，即使内容完好也不复用。
	ReasonMissExpired = "miss: expired"
	// ReasonMissCorrupted 表示条目截断/校验和不符（Reason 为其加具体错误的后缀）。
	ReasonMissCorrupted = "miss: corrupted entry"
)

// Result 描述一次缓存访问的结果，命中与未命中均给出可解释原因。
type Result struct {
	Key     Key
	Payload []byte
	Hit     bool
	// Reason 解释结果来源，取值为 ReasonXxx 常量（损坏类追加具体错误），
	// 合并到并发构建时追加 "; shared build result"。
	Reason string
	// Built 表示本次请求触发了实际构建。
	Built bool
}

// Stats 为缓存统计。口径一致：Bytes 等于索引中各条目大小之和，
// 与磁盘实际占用一致；Hits+Misses 等于 GetOrBuild 调用次数。
type Stats struct {
	Entries      int
	Bytes        int64
	MaxBytes     int64
	Hits         int64
	Misses       int64
	Builds       int64 // 实际执行的构建次数（含并发合并后仅一次）
	Evictions    int64
	Expired      int64 // 访问时判定过期并移除的条目数（懒过期）
	Migrated     int64 // Open 时迁移到当前格式的旧版本条目数
	Rejected     int64 // Open 时拒绝并移除的不可识别条目数
	Sweeps       int64 // 已执行的巡检次数
	SweepRemoved int64 // 历次巡检清理的条目/文件总数
}

// SweepReport 为一次巡检/自愈的报告。巡检结束后，
// Entries 与 Bytes 与磁盘上的真实条目数与占用完全一致。
type SweepReport struct {
	Expired    int   // 因超过有效期被移除的条目数
	Corrupted  int   // 因损坏（截断/校验和不符）被移除的条目数
	Mismatched int   // 索引与磁盘对不上而被修正的条目数（索引有磁盘无、大小漂移）
	Orphans    int   // 索引没有的残留文件被删除的数量
	Adopted    int   // 索引没有但内容完好的 v3 文件被收养进索引的数量
	Entries    int   // 巡检结束时的条目数（与磁盘一致）
	Bytes      int64 // 巡检结束时的总占用（与磁盘一致）
}

// entryMeta 为索引中的条目元数据。
type entryMeta struct {
	key       Key
	size      int64
	hasExpiry bool      // 是否有有效期
	expiry    time.Time // 绝对过期时刻（hasExpiry 为 true 时有效）
	elem      *list.Element
}

// call 表示一次进行中的构建（单飞）。
type call struct {
	done    chan struct{}
	payload []byte
	err     error
}

// Cache 是基于本地文件系统的构建产物缓存。
//
// 一致性设计：所有索引变更、条目读取、rename 后的索引登记都在同一把
// 互斥锁内完成，因此“索引记录”与“磁盘最终文件”不存在中间态；
// 巡检持锁扫描并对账，无论与读写如何交错，报告都与磁盘实际占用一致。
type Cache struct {
	dir        string
	maxBytes   int64
	defaultTTL time.Duration
	clock      Clock

	mu       sync.Mutex
	cond     *sync.Cond // 用于构建完成/巡检结束的等待唤醒
	entries  map[Key]*entryMeta
	lru      *list.List // 前端为最近使用
	bytes    int64
	inflight map[Key]*call
	sweeping bool // 巡检进行中：新的构建等待，保证对账期间无 rename 窗口

	hits, misses, builds, evictions int64
	expired                         int64
	migrated, rejected              int64
	sweeps, sweepRemoved            int64

	// testSweepHook 若非 nil，在 Sweep 结束、仍持有锁（磁盘状态已冻结：
	// 无进行中构建、新构建被阻塞）时调用。仅供测试精确校验报告。
	testSweepHook func(rep *SweepReport)
}

// OpenOptions 为缓存实例级配置。
type OpenOptions struct {
	// DefaultTTL 为条目默认最大存活时长；<=0 表示默认永不过期。
	// 单次写入可用 WithTTL 覆盖。
	DefaultTTL time.Duration
	// Clock 为时间来源；nil 时使用系统墙钟。
	// 测试可注入 FakeClock 把时间可控地往前推，不依赖真实时间流逝。
	Clock Clock
}

// Open 打开（必要时创建）位于 dir 的缓存，容量上限为 maxBytes，
// 使用默认配置（无默认 TTL、系统墙钟）。等价于 OpenWithOptions(dir, maxBytes, OpenOptions{})。
func Open(dir string, maxBytes int64) (*Cache, error) {
	return OpenWithOptions(dir, maxBytes, OpenOptions{})
}

// OpenWithOptions 打开（必要时创建）位于 dir 的缓存。
// 打开时扫描既有条目：当前版本（v3）条目纳入索引；v1/v2 旧版条目按确定
// 规则迁移到 v3（创建时间取文件 mtime，TTL 取实例默认值）；损坏或无法
// 识别的条目明确拒绝（移除并计数），绝不静默误用。
func OpenWithOptions(dir string, maxBytes int64, opts OpenOptions) (*Cache, error) {
	if maxBytes <= 0 {
		return nil, errors.New("cache: maxBytes must be positive")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	clock := opts.Clock
	if clock == nil {
		clock = wallClock{}
	}
	c := &Cache{
		dir:        dir,
		maxBytes:   maxBytes,
		defaultTTL: opts.DefaultTTL,
		clock:      clock,
		entries:    make(map[Key]*entryMeta),
		lru:        list.New(),
		inflight:   make(map[Key]*call),
	}
	c.cond = sync.NewCond(&c.mu)
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
		pe, err := readEntryV3(path)
		if err != nil {
			if errors.Is(err, ErrUnsupportedVersion) {
				if merr := migrateEntryV3(path, opts.DefaultTTL); merr == nil {
					c.migrated++
					pe, err = readEntryV3(path)
					if err != nil {
						os.Remove(path)
						c.rejected++
						continue
					}
				} else {
					os.Remove(path)
					c.rejected++
					continue
				}
			} else {
				// 损坏条目：明确拒绝
				os.Remove(path)
				c.rejected++
				continue
			}
		}
		c.indexLocked(key, fileSize(path), pe.created, pe.ttl)
	}
	c.evictLocked()
	return c, nil
}

func (c *Cache) path(key Key) string {
	return filepath.Join(c.dir, string(key))
}

// indexLocked 将条目加入索引与 LRU 前端；调用者须持有锁。
func (c *Cache) indexLocked(key Key, size int64, created time.Time, ttl time.Duration) {
	if m, ok := c.entries[key]; ok {
		c.bytes -= m.size
		c.lru.Remove(m.elem)
	}
	m := &entryMeta{key: key, size: size}
	if ttl > 0 {
		m.hasExpiry = true
		m.expiry = created.Add(ttl)
	}
	m.elem = c.lru.PushFront(key)
	c.entries[key] = m
	c.bytes += size
}

// evictLocked 按 LRU（最久未使用优先）淘汰，直至占用不超过上限。
// 调用者须持有锁。
func (c *Cache) evictLocked() {
	for c.bytes > c.maxBytes {
		e := c.lru.Back()
		if e == nil {
			return
		}
		m := c.entries[e.Value.(Key)]
		c.removeLocked(m)
		c.evictions++
	}
}

// removeLocked 从索引与磁盘移除条目；调用者须持有锁。
func (c *Cache) removeLocked(m *entryMeta) {
	os.Remove(c.path(m.key))
	c.lru.Remove(m.elem)
	delete(c.entries, m.key)
	c.bytes -= m.size
}

// WriteOption 调整单次写入行为。
type WriteOption func(*writeOptions)

type writeOptions struct {
	ttl *time.Duration
}

// WithTTL 为本次写入单独指定最大存活时长：d>0 覆盖实例默认 TTL；
// d<=0 表示该条目永不过期。未指定时使用实例默认 TTL。
func WithTTL(d time.Duration) WriteOption {
	return func(o *writeOptions) { o.ttl = &d }
}

// GetOrBuild 查询缓存；未命中（不存在、已过期或已损坏）时执行 build。
// 同一键的并发请求只触发一次实际构建，其余请求等待并复用结果；
// 构建失败不写入缓存，所有等待者收到相同错误。
// 有效期判定：条目在 写入时刻+TTL 及之后视为过期，即使内容完好也不复用，
// 按未命中重建；ttl<=0 的条目永不过期。
func (c *Cache) GetOrBuild(ctx context.Context, key Key, build func() ([]byte, error), opts ...WriteOption) (Result, error) {
	var wo writeOptions
	for _, o := range opts {
		o(&wo)
	}
	ttl := c.defaultTTL
	if wo.ttl != nil {
		ttl = *wo.ttl
	}

	c.mu.Lock()
	// 巡检对账期间暂停新的构建请求，保证巡检结束时报告与磁盘完全一致。
	for c.sweeping {
		c.cond.Wait()
	}
	if payload, missReason, found := c.serveFromIndexLocked(key); found && missReason == "" {
		c.hits++
		c.mu.Unlock()
		return Result{Key: key, Payload: payload, Hit: true, Reason: ReasonHit}, nil
	} else {
		if missReason == ReasonMissExpired {
			c.expired++
		}
		c.misses++
		c.mu.Unlock()
		return c.buildLocked(ctx, key, build, missReason, ttl)
	}
}

// serveFromIndexLocked 在锁内尝试服务一个键。返回值约定：
//   - found=true 且 missReason==""：命中，payload 可用（元数据已刷新）；
//   - found=true 且 missReason!=""：条目已过期/损坏，元数据已移除，需构建；
//   - found=false：索引中没有该键，missReason 为 ReasonMissNotFound。
//
// 本函数不改动 hits/misses/expired 计数，由调用方按结果统一计数。
// 调用者须持有锁。
func (c *Cache) serveFromIndexLocked(key Key) (payload []byte, missReason string, found bool) {
	m, ok := c.entries[key]
	if !ok {
		return nil, ReasonMissNotFound, false
	}
	now := c.clock.Now()
	if m.hasExpiry && !now.Before(m.expiry) {
		// 已过期：即使内容完好也不得复用，移除后由调用方重建。
		c.removeLocked(m)
		return nil, ReasonMissExpired, true
	}
	// 读取在锁内完成：读到的一定是索引中记录的完整条目，
	// 与巡检/淘汰任意交错都不会读到半个文件。
	pe, err := readEntryV3(c.path(key))
	if err != nil {
		// 条目损坏或版本不符：移除坏条目，由调用方重建。
		c.removeLocked(m)
		return nil, fmt.Sprintf("%s: %v", ReasonMissCorrupted, err), true
	}
	// 以磁盘为准刷新元数据（防御外部篡改导致的大小漂移）。
	if fi, serr := os.Stat(c.path(key)); serr == nil && fi.Size() != m.size {
		c.bytes += fi.Size() - m.size
		m.size = fi.Size()
	}
	if pe.ttl > 0 {
		m.hasExpiry = true
		m.expiry = pe.created.Add(pe.ttl)
	} else {
		m.hasExpiry = false
	}
	c.lru.MoveToFront(m.elem)
	return pe.payload, "", true
}

// buildLocked 处理未命中：合并并发构建，仅一个请求执行实际构建。
func (c *Cache) buildLocked(ctx context.Context, key Key, build func() ([]byte, error), missReason string, ttl time.Duration) (Result, error) {
	c.mu.Lock()
	// 巡检对账期间暂停新构建，保证巡检结束时报告与磁盘完全一致。
	for c.sweeping {
		c.cond.Wait()
	}
	// 等待期间该键可能已被别的构建完成并入索引：回查，绝不重复构建。
	if payload, missReason, found := c.serveFromIndexLocked(key); found {
		if missReason == "" {
			c.hits++
			c.mu.Unlock()
			return Result{Key: key, Payload: payload, Hit: true,
				Reason: ReasonHit}, nil
		}
		c.misses++
		if missReason == ReasonMissExpired {
			c.expired++
		}
		c.mu.Unlock()
		return c.buildLocked(ctx, key, build, missReason, ttl)
	}
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
		c.cond.Broadcast()
		c.mu.Unlock()
		close(cl.done)
		return Result{Key: key, Reason: missReason}, err
	}

	// 原子写入：rename 在加锁索引之前完成，保证任何时刻被巡检看到的
	// 最终文件名必然已（或将随本次构建完成而）被索引，无中间窗口。
	created := c.clock.Now()
	if werr := writeEntryAtomicV3(c.path(key), payload, created, ttl); werr != nil {
		cl.err = werr
		c.mu.Lock()
		delete(c.inflight, key)
		c.cond.Broadcast()
		c.mu.Unlock()
		close(cl.done)
		return Result{Key: key, Reason: missReason}, werr
	}
	cl.payload = payload
	c.mu.Lock()
	c.builds++
	c.indexLocked(key, fileSize(c.path(key)), created, ttl)
	c.evictLocked()
	delete(c.inflight, key)
	c.cond.Broadcast()
	c.mu.Unlock()
	close(cl.done)
	return Result{Key: key, Payload: payload, Hit: false, Built: true, Reason: missReason}, nil
}

// Sweep 巡检并自愈：扫描整个缓存目录，
//   - 移除已过期的条目（即使内容完好）；
//   - 移除损坏（截断/校验和不符）的条目；
//   - 修正索引与磁盘对不上的条目（索引有磁盘无的删除索引，大小漂移的以磁盘对账）；
//   - 删除索引没有的残留文件；内容完好的 v3 孤儿文件收养进索引；
//   - 最后按容量上限回收。
//
// 被移除的条目后续访问时按未命中重建。巡检全程持锁并等待进行中的构建
// 完成，结束时报告的 Entries/Bytes 与磁盘真实占用完全一致；无论巡检与
// 并发读写如何交错，之后继续写入时淘汰仍能把真实占用压回上限以内。
func (c *Cache) Sweep() SweepReport {
	c.mu.Lock()
	c.sweeping = true // 阻止新构建，关闭 rename 窗口
	var rep SweepReport
	now := c.clock.Now()

	// 1. 校验索引内条目：以磁盘文件内容为准。
	for key, m := range c.entries {
		path := c.path(key)
		pe, err := readEntryV3(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// 索引有、磁盘无：记录对不上，删除索引。
				c.lru.Remove(m.elem)
				delete(c.entries, key)
				c.bytes -= m.size
				rep.Mismatched++
			} else {
				// 损坏条目：移除。
				c.removeLocked(m)
				rep.Corrupted++
			}
			continue
		}
		if pe.expiredAt(now) {
			c.removeLocked(m)
			rep.Expired++
			continue
		}
		// 大小漂移：以磁盘真实大小对账，内容完好则保留条目。
		if fi, serr := os.Stat(path); serr == nil && fi.Size() != m.size {
			c.bytes += fi.Size() - m.size
			m.size = fi.Size()
			rep.Mismatched++
		}
		// 以文件内容为准刷新有效期元数据。
		if pe.ttl > 0 {
			m.hasExpiry = true
			m.expiry = pe.created.Add(pe.ttl)
		} else {
			m.hasExpiry = false
		}
	}

	// 2. 扫描磁盘：处理索引没有的最终文件（孤儿）。
	//    跳过 inflight 键（构建完成时会自行索引）与 .tmp- 临时文件
	//   （进行中的原子写，由写方负责清理）。
	if files, err := os.ReadDir(c.dir); err == nil {
		for _, f := range files {
			if f.IsDir() || strings.HasPrefix(f.Name(), ".tmp-") {
				continue
			}
			key := Key(f.Name())
			if _, ok := c.entries[key]; ok {
				continue
			}
			if _, ok := c.inflight[key]; ok {
				continue
			}
			path := c.path(key)
			if pe, rerr := readEntryV3(path); rerr == nil && !pe.expiredAt(now) {
				// 内容完好且未过期的 v3 孤儿：收养进索引。
				c.indexLocked(key, fileSize(path), pe.created, pe.ttl)
				rep.Adopted++
			} else {
				os.Remove(path)
				rep.Orphans++
			}
		}
	}

	// 3. 等待进行中的构建完成（它们完成时自行索引并触发回收）。
	for len(c.inflight) > 0 {
		c.cond.Wait()
	}

	// 4. 回收至容量上限，最终对账：报告与磁盘实际占用完全一致。
	c.evictLocked()
	c.reconcileLocked(&rep)
	rep.Entries = len(c.entries)
	rep.Bytes = c.bytes

	c.sweeps++
	c.sweepRemoved += int64(rep.Expired + rep.Corrupted + rep.Mismatched + rep.Orphans)
	if c.testSweepHook != nil {
		c.testSweepHook(&rep)
	}
	c.sweeping = false
	c.cond.Broadcast()
	c.mu.Unlock()
	return rep
}

// reconcileLocked 最终对账：以磁盘真实状态修正索引与占用，
// 保证 Sweep 报告与磁盘完全一致。调用者须持有锁且处于巡检期间
// （无进行中的构建，无 rename 窗口）。
func (c *Cache) reconcileLocked(rep *SweepReport) {
	files, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	onDisk := make(map[Key]int64, len(files))
	for _, f := range files {
		if f.IsDir() || strings.HasPrefix(f.Name(), ".tmp-") {
			continue
		}
		fi, serr := f.Info()
		if serr != nil {
			continue
		}
		onDisk[Key(f.Name())] = fi.Size()
	}
	// 磁盘有而索引无：删除残留文件。
	for key := range onDisk {
		if _, ok := c.entries[key]; !ok {
			os.Remove(c.path(key))
			rep.Orphans++
		}
	}
	// 索引有而磁盘无：删除索引；大小不一致：以磁盘为准。
	for key, m := range c.entries {
		size, ok := onDisk[key]
		if !ok {
			c.lru.Remove(m.elem)
			delete(c.entries, key)
			c.bytes -= m.size
			rep.Mismatched++
			continue
		}
		if size != m.size {
			c.bytes += size - m.size
			m.size = size
			rep.Mismatched++
		}
	}
}

// Stats 返回当前缓存统计。
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{
		Entries:      len(c.entries),
		Bytes:        c.bytes,
		MaxBytes:     c.maxBytes,
		Hits:         c.hits,
		Misses:       c.misses,
		Builds:       c.builds,
		Evictions:    c.evictions,
		Expired:      c.expired,
		Migrated:     c.migrated,
		Rejected:     c.rejected,
		Sweeps:       c.sweeps,
		SweepRemoved: c.sweepRemoved,
	}
}
