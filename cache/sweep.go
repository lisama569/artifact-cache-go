package cache

import (
	"os"
	"strings"
)

// SweepReport 为一次巡检的结果报告。巡检结束后 Entries/Bytes
// 与磁盘上的真实占用完全一致。
type SweepReport struct {
	Expired   int   // 因过期被移除的条目数
	Corrupted int   // 因损坏/校验失败被移除的条目数
	Orphaned  int   // 磁盘上存在但索引没有的孤儿文件数（已清除）
	Missing   int   // 索引中存在但磁盘缺失、被剔除的条目数
	Repaired  int   // 索引大小与磁盘不符、已按磁盘修正的条目数
	Entries   int   // 巡检结束后的条目数
	Bytes     int64 // 巡检结束后的占用字节数（与磁盘实际占用一致）
}

// Sweep 扫描缓存目录并自愈：
//   - 过期条目：移除并计数（Expired）；
//   - 损坏或校验失败的条目：移除并计数（Corrupted）；
//   - 索引有、磁盘没有的条目：从索引剔除（Missing）；
//   - 磁盘有、索引没有的孤儿文件：清除（Orphaned），
//     正在进行中的并发构建（inflight）写入的文件除外；
//   - 索引大小与磁盘实际大小不符的条目：以磁盘为准修正（Repaired）。
//
// 全程持有缓存互斥锁，与并发读写、淘汰安全交错：无论操作如何交错，
// 巡检报告中的占用与后续 Stats.Bytes 都和磁盘实际占用一致，
// 后续写入触发淘汰仍能把真实占用压回上限以内。
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

	// 2. 清除磁盘上的孤儿文件（索引没有的条目文件与残留临时文件）。
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

	// 3. 以索引为准重算占用（此时索引与磁盘一一对应）。
	c.bytes = 0
	for _, m := range c.entries {
		c.bytes += m.size
	}
	// 巡检后若仍超限（例如修正大小后），立即按 LRU 回收。
	c.evictLocked()

	rep.Entries = len(c.entries)
	rep.Bytes = c.bytes
	return rep
}
