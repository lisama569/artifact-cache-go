package cache

import (
	"context"
	"fmt"
	"sync"
)

// SimBuilder 用纯计算模拟构建执行，不触碰网络与外部服务：
// 产物由缓存键确定性生成，因此同键产物字节一致、可重复；
// 内置实际构建次数计数，便于测试断言“并发同键只构建一次”。
type SimBuilder struct {
	mu       sync.Mutex
	builds   int
	failKeys map[string]bool
}

// NewSimBuilder 创建模拟构建器；failHexes 中的键将返回构建失败。
func NewSimBuilder(failHexes ...string) *SimBuilder {
	s := &SimBuilder{failKeys: map[string]bool{}}
	for _, k := range failHexes {
		s.failKeys[k] = true
	}
	return s
}

// BuildCount 返回实际执行的构建次数。
func (s *SimBuilder) BuildCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.builds
}

// Build 实现 Builder。产物格式：artifact:v2:<key>:<n>（n 为该构建器第几次构建），
// 这样测试既能验证确定性复用，也能区分产物来自哪一次实际构建。
func (s *SimBuilder) Build(ctx context.Context, in BuildInput) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key := DeriveKey(in)
	s.mu.Lock()
	s.builds++
	n := s.builds
	fail := s.failKeys[key.String()]
	s.mu.Unlock()
	if fail {
		return nil, fmt.Errorf("simulated build failure for key %s", key.String())
	}
	return []byte(fmt.Sprintf("artifact:v2:%s#%d", key.String(), n)), nil
}
