package cache

import (
	"os"
	"strings"
)

// SweepReport 为一次巡检的结果报告。
// Entries/Bytes 对应巡检"收尾点"（等待所有进行中的发布静止后）
// 目录在磁盘上的真实条目与占用，逐文件完全一致。
type SweepReport struct {
	Expired   int   // 因过期被移除的条目数
	Corrupted int   // 因损坏/校验失败被移除的条目数
	Orphaned  int   // 磁盘上存在但索引没有的孤儿文件数（已清除）
	Missing   int   // 索引中存在但磁盘缺失、被剔除的条目数
	Repaired  int   // 索引大小/有效期与磁盘不符、已按磁盘修正的条目数
	Adopted   int   // 并发构建已落盘、被本次巡检认领进索引的条目数
	Entries   int   // 巡检结束后的条目数
	Bytes     int64 // 巡检结束后的占用字节数（与磁盘实际占用一致）
}

// Sweep 扫描缓存目录并自愈，分两个阶段，与并发构建安全交错：
//
// 第一遍（只持有缓存锁 mu，构建方仍可正常 rename 落盘）：
//   - 过期条目：移除并计数（Expired）；
//   - 损坏或校验失败的条目：移除并计数（Corrupted）；
//   - 索引有、磁盘没有的条目：从索引剔除（Missing）；
//   - 索引大小/有效期与磁盘不符：以磁盘为准修正（Repaired）。
//
// 收尾（释放 mu 后取得发布写锁 pubMu，再重新取得 mu）：
//   - 等待所有进行中的发布（rename）静止——收尾期间没有任何新文件能
//     落盘、也没有写了一半的临时文件；
//   - 重新列目录：把进行中构建（inflight）已落盘的合法产物"认领"进
//     索引（Adopted，publishing 标记，构建方提交前不得淘汰），既不当
//     垃圾删掉也不漏统；真正的孤儿文件清除（Orphaned）；
//   - 重算占用并按 LRU 补做淘汰（被认领条目除外）。
//
// 因此报告返回时，Entries/Bytes 与此刻目录磁盘真实占用逐文件一致；
// 认领条目随各自构建提交后补做淘汰，后续写入的容量判断不失真，
// 真实占用不会长期超过上限。
func (c *Cache) Sweep() SweepReport {
	var rep SweepReport

	// —— 第一遍：在缓存锁下核对索引条目与磁盘内容。 ——
	c.mu.Lock()
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
		if !diskExpiry.Equal(m.expiresAt) {
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
	// 测试钩子（如有）：第一遍完成，仍持有 mu。
	if c.afterSweepScanned != nil {
		c.afterSweepScanned()
	}
	c.mu.Unlock()

	// —— 收尾：取得发布写锁，等待所有 rename 静止。 ——
	// 锁序 mu -> pubMu，这里先放 mu 再取 pubMu，二者不嵌套，无死锁；
	// 构建方的发布只短暂持有 pubMu 读锁（rename），不会在此长期阻塞。
	c.pubMu.Lock()
	defer c.pubMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()

	// 收尾期间不可能再有 rename，也不可能有写了一半的临时文件。
	files, err := os.ReadDir(c.dir)
	if err == nil {
		for _, f := range files {
			if f.IsDir() {
				continue
			}
			name := f.Name()
			if strings.HasPrefix(name, ".tmp-") {
				// 发布静止后仍存在的 .tmp 必为崩溃残留（此刻没有构建
				// 正在写临时文件），可安全清理。
				if os.Remove(c.path(Key(name))) == nil {
					rep.Orphaned++
				}
				continue
			}
			key := Key(name)
			if _, ok := c.entries[key]; ok {
				continue
			}
			cl, inflight := c.inflight[key]
			if !inflight {
				// 磁盘有、索引没有、也不属于任何进行中构建：真孤儿。
				if os.Remove(c.path(key)) == nil {
					rep.Orphaned++
				}
				continue
			}
			// 进行中构建已落盘的产物：认纳入索引。rename 是原子的，
			// 文件必为构建方刚发布的完整条目；以构建方声明的有效期与
			// 磁盘大小为准，并打 publishing 标记防止被提前淘汰。
			if size := fileSize(c.path(key)); size > 0 {
				c.indexLocked(key, size, cl.expiresAt, true)
				rep.Adopted++
			}
		}
	}

	// 以索引为准重算占用（收尾点上索引与磁盘一一对应）。
	c.bytes = 0
	for _, m := range c.entries {
		c.bytes += m.size
	}
	// 若认领/修正后超限，立即按 LRU 回收；publishing 条目跳过，
	// 待其构建方提交时补做淘汰。
	c.evictLocked()

	rep.Entries = len(c.entries)
	rep.Bytes = c.bytes
	return rep
}
