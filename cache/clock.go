package cache

import "time"

// Clock 抽象时间来源，使有效期判定不依赖真实时间流逝：
// 生产环境使用系统墙钟，测试可注入可控时钟把时间任意往前推。
type Clock interface {
	Now() time.Time
}

// wallClock 为默认系统墙钟。
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// FakeClock 是可控时钟，主要用于测试：可反复 Advance 把时间往前推，
// 而无需 sleep，且测试结果完全确定、可重复。
type FakeClock struct {
	t time.Time
}

// NewFakeClock 以起点 start 创建可控时钟。
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{t: start}
}

// Now 返回当前虚拟时间。
func (c *FakeClock) Now() time.Time { return c.t }

// Set 直接设定虚拟时间。
func (c *FakeClock) Set(t time.Time) { c.t = t }

// Advance 把虚拟时间向前推进 d。
func (c *FakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
