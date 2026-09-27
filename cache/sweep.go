package cache

import (
	"os"
	"strings"
)

// SweepReport 为一次巡检的结果报告。巡检返回的那一刻，Entries/Bytes
// 即与磁盘上的真实占用完全一致，无需等待并发构建全部结束。
type SweepReport struct {
	Expired   int   // 因过期被移除的条目数
	Corrupted int   // 因损坏/校验失败被移除的条目数
	Orphaned  int   // 磁盘上存在但索引没有的孤儿文件数（已清除）
	Missing   int   // 索引中存在但磁盘缺失、被剔除的条目数
	Repaired  int   // 索引大小与磁盘不符、已按磁盘修正的条目数
	Adopted   int   // 巡检期间已落盘、尚未入索引的并发构建产物，已纳入索引
	Entries   int   // 巡检结束后的条目数
	Bytes     int64 // 巡检结束后的占用字节数（与磁盘实际占用一致）
}

// Sweep 扫描缓存目录并自愈：
//   - 过期条目：移除并计数（Expired）；
//   - 损坏或校验失败的条目：移除并计数（Corrupted）；
//   - 索引有、磁盘没有的条目：从索引剔除（Missing）；
//   - 磁盘有、索引没有的孤儿文件：清除（Orphaned），
//     正在进行中的并发构建（inflight）写入的文件除外；
//   - 索引大小与磁盘实际大小不符的条目：以磁盘为准修正（Repaired）；
//   - 巡检期间已 rename 落盘、正等待入索引的并发构建产物：按磁盘内容
//     校验并直接纳入索引（Adopted），钉住防淘汰；构建随后入索引时
//     按同键替换，幂等不重复计数。
//
// 全程持有缓存互斥锁，与并发读写、淘汰安全交错。构建的产物落盘
// （临时文件 rename 为正式文件）不依赖该锁，因此巡检扫描期间磁盘上
// 可能多出尚未入索引的完整产物：认领步骤保证巡检返回的那一刻，
// 报告中的条目数与占用字节数即与目录真实占用完全一致，
// 后续 Stats.Bytes 也与磁盘一致，继续写入触发淘汰仍能把真实占用
// 压回上限以内。
func (c *Cache) Sweep() SweepReport {
	c.mu.Lock()
	defer c.mu.Unlock()
	var rep SweepReport

	// 1. 核对索引中的每一条目与磁盘内容。
	for _, m := range c.entries {
		path := c.path(m.key)
		fi, err := os.Stat(path)
		if err != nil {
			c.removeLocked(m)
			rep.Missing++
			continue
		}
		_, diskExpiry, err := readEntry(path)
		if err != nil {
			c.removeLocked(m)
			rep.Corrupted++
			continue
		}
		if diskExpiry != m.expiresAt {
			m.expiresAt = diskExpiry // 以磁盘为准修正有效期记录
			rep.Repaired++
		}
		if c.expiredLocked(m) {
			c.removeLocked(m)
			c.expired++
			rep.Expired++
			continue
		}
		if fi.Size() != m.size {
			c.bytes += fi.Size() - m.size
			m.size = fi.Size()
			rep.Repaired++
		}
	}

	// 测试钩子：扫描中途回调，用于稳定复现并发交错时序。
	if c.sweepMidHook != nil {
		c.sweepMidHook()
	}

	// 2. 认领巡检期间已落盘、尚未入索引的并发构建产物。
	// 构建先 rename 落盘再取锁入索引；巡检持锁期间，inflight 键的
	// 正式文件若已出现在磁盘上，说明对应构建已完成原子写入、正阻塞
	// 等待入索引。此处按磁盘内容校验后直接纳入索引（refs=1 钉住，
	// 防止巡检末尾的淘汰把它清掉）；构建随后入索引时同键替换，幂等。
	for key := range c.inflight {
		if _, ok := c.entries[key]; ok {
			continue
		}
		path := c.path(key)
		fi, err := os.Stat(path)
		if err != nil {
			continue // 构建仍在临时文件阶段，尚未落盘
		}
		_, expiresAt, err := readEntry(path)
		if err != nil {
			// 原子写入不会产生坏文件；万一出现按损坏清理。
			os.Remove(path)
			rep.Corrupted++
			continue
		}
		if !expiresAt.IsZero() && !c.now().Before(expiresAt) {
			os.Remove(path)
			c.expired++
			rep.Expired++
			continue
		}
		m := &entryMeta{key: key, size: fi.Size(), expiresAt: expiresAt, refs: 1}
		m.elem = c.lru.PushFront(key)
		c.entries[key] = m
		c.bytes += m.size
		rep.Adopted++
	}

	// 3. 清除磁盘上的孤儿文件（索引没有的条目文件与残留临时文件）。
	files, err := os.ReadDir(c.dir)
	if err == nil {
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			name := f.Name()
			if strings.HasPrefix(name, ".tmp-") {
				// 仅在没有进行中的构建时清理残留临时文件，
				// 避免误删并发构建正在写入的临时文件。
				if len(c.inflight) == 0 {
					os.Remove(c.path(Key(name)))
					rep.Orphaned++
				}
				continue
			}
			key := Key(name)
			if _, ok := c.entries[key]; ok {
				continue
			}
			if _, ok := c.inflight[key]; ok {
				continue // 并发构建刚 rename 完成、尚未入索引
			}
			os.Remove(c.path(key))
			rep.Orphaned++
		}
	}

	// 4. 以索引为准重算占用（此时索引与磁盘上的正式条目一一对应）。
	c.bytes = 0
	for _, m := range c.entries {
		c.bytes += m.size
	}
	// 巡检后若仍超限（例如认领产物或修正大小后），立即按 LRU 回收。
	c.evictLocked()

	// 5. 以返回前最终的目录状态出报告，保证报告与磁盘真实占用一致
	// （含仍在写入中的临时文件字节；条目数只计正式条目）。
	rep.Entries = len(c.entries)
	rep.Bytes = c.bytes
	if files, err := os.ReadDir(c.dir); err == nil {
		var disk int64
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			if fi, err := f.Info(); err == nil {
				disk += fi.Size()
			}
		}
		rep.Bytes = disk
	}
	return rep
}
