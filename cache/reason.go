package cache

import "fmt"

// HitKind 区分命中与未命中。
type HitKind int

const (
	// KindMiss 未命中。
	KindMiss HitKind = iota
	// KindHit 命中。
	KindHit
)

func (k HitKind) String() string {
	if k == KindHit {
		return "HIT"
	}
	return "MISS"
}

// ReasonCode 是可区分、可解释的命中/未命中原因码。
type ReasonCode string

const (
	// ReasonHitFresh 命中且条目完整、版本当前。
	ReasonHitFresh ReasonCode = "HIT_FRESH"

	// ReasonMissNotFound    键不存在对应条目。
	ReasonMissNotFound ReasonCode = "MISS_NOT_FOUND"
	// ReasonMissCorrupt     条目被截断或校验失败，按未命中处理。
	ReasonMissCorrupt ReasonCode = "MISS_CORRUPT"
	// ReasonMissOldVersion  条目属于不受支持的旧版本，明确拒绝。
	ReasonMissOldVersion ReasonCode = "MISS_OLD_VERSION"
	// ReasonMissFuture      条目版本高于当前版本，明确拒绝。
	ReasonMissFuture ReasonCode = "MISS_FUTURE_VERSION"
	// ReasonMissShared      复用了同键并发构建的结果（本请求未执行构建）。
	ReasonMissShared ReasonCode = "MISS_SHARED_BUILD"
	// ReasonMissBuildFailed 构建失败，未写入缓存。
	ReasonMissBuildFailed ReasonCode = "MISS_BUILD_FAILED"
)

// Decision 描述一次 GetOrBuild 的结论：命中与否、原因、解释文本。
type Decision struct {
	Kind   HitKind
	Reason ReasonCode
	Detail string
}

func (d Decision) String() string {
	return fmt.Sprintf("%s/%s: %s", d.Kind, d.Reason, d.Detail)
}
