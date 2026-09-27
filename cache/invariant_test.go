package cache

import (
	"os"
	"sync/atomic"
	"testing"
)

// measureDisk 返回缓存目录的真实占用与最终文件数（排除 .tmp- 临时文件）。
func measureDisk(t *testing.T, dir string) (int64, int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Errorf("read dir: %v", err)
		return 0, 0
	}
	var total int64
	n := 0
	for _, e := range entries {
		if e.IsDir() || isTmpName(e.Name()) {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue // 扫描期间被删除：跳过
		}
		total += fi.Size()
		n++
	}
	return total, n
}

// checkInvariant 在持有缓存锁期间扫描磁盘，校验结构性不变量：
//   - 每个索引条目在磁盘上存在且大小一致（索引登记总在 rename 之后，
//     索引与删除都在锁内，因此持锁期间必然成立）；
//   - 磁盘上每个最终文件属于索引或进行中的构建（不存在游离文件）；
//   - 账面占用不超过容量上限。
func checkInvariant(t *testing.T, c *Cache, dir string, counter *atomic.Int64) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Errorf("read dir: %v", err)
		counter.Add(1)
		return
	}
	onDisk := make(map[Key]int64, len(entries))
	for _, e := range entries {
		if e.IsDir() || isTmpName(e.Name()) {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			continue
		}
		onDisk[Key(e.Name())] = fi.Size()
	}
	for k, m := range c.entries {
		size, ok := onDisk[k]
		if !ok {
			t.Errorf("indexed key %s missing on disk (index/disk drift)", k[:8])
			counter.Add(1)
			continue
		}
		if size != m.size {
			t.Errorf("key %s size drift: index=%d disk=%d", k[:8], m.size, size)
			counter.Add(1)
		}
	}
	for k := range onDisk {
		_, indexed := c.entries[k]
		_, building := c.inflight[k]
		if !indexed && !building {
			t.Errorf("final file %s on disk is neither indexed nor inflight", k[:8])
			counter.Add(1)
		}
	}
	if c.bytes > c.maxBytes {
		t.Errorf("accounted bytes=%d over max=%d", c.bytes, c.maxBytes)
		counter.Add(1)
	}
}
