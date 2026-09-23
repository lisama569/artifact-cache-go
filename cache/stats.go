package cache

import "fmt"

// Stats 是缓存统计快照，占用口径与磁盘当前版本条目实际占用一致。
type Stats struct {
	Entries        int   // 当前内存索引中的可用条目数
	CurrentBytes   int64 // 当前条目占用字节合计（与磁盘文件大小一致）
	LimitBytes     int64 // 容量上限；<=0 表示不限
	Hits           int64 // HIT_FRESH 次数
	Misses         int64 // 各类未命中次数之和
	Builds         int64 // 实际触发构建的次数
	CorruptRejects int64 // 因损坏按未命中处理的次数
	VersionRejects int64 // 因版本不兼容明确拒绝的次数
	Evictions      int64 // 淘汰条目次数
	InFlight       int   // 正在进行（含被等待）的同键合并构建数
}

// Utilization 返回占用百分比字符串，便于日志解释容量口径。
func (s Stats) String() string {
	limit := s.LimitBytes
	if limit <= 0 {
		return fmt.Sprintf(
			"entries=%d bytes=%d limit=unlimited hits=%d misses=%d builds=%d corrupt=%d versionReject=%d evictions=%d inFlight=%d",
			s.Entries, s.CurrentBytes, s.Hits, s.Misses, s.Builds,
			s.CorruptRejects, s.VersionRejects, s.Evictions, s.InFlight,
		)
	}
	return fmt.Sprintf(
		"entries=%d bytes=%d/%d (%.1f%%) hits=%d misses=%d builds=%d corrupt=%d versionReject=%d evictions=%d inFlight=%d",
		s.Entries, s.CurrentBytes, limit, float64(s.CurrentBytes)*100/float64(limit),
		s.Hits, s.Misses, s.Builds, s.CorruptRejects, s.VersionRejects, s.Evictions, s.InFlight,
	)
}
