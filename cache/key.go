// Package cache 提供本地构建产物缓存：稳定键派生、并发构建合并、
// 原子写入与完整性校验、容量回收与版本兼容。
package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
)

// Key 是派生自构建输入的稳定缓存键（十六进制编码的摘要）。
type Key string

// BuildInput 描述一次构建的声明式输入。
type BuildInput struct {
	// Files 为参与构建的文件内容，键为相对路径。
	Files map[string][]byte
	// Toolchain 标识工具链（如 "go1.25.0"）。
	Toolchain string
	// Args 为构建参数。
	Args []string
	// Env 为受管环境变量（仅这些变量参与键派生）。
	Env map[string]string
}

// DeriveKey 从构建输入派生稳定缓存键。
//
// 规范化规则（保证语义等价输入得到相同键）：
//   - 文件按路径排序后逐一写入 "file <path> <len>\n<content>"；
//   - 环境变量按名字排序后写入 "env <name>=<value>\n"；
//   - 参数按给定顺序写入 "arg <value>\n"（参数顺序有语义，不参与排序）；
//   - 工具链写入 "toolchain <id>\n"。
//
// 任一组成部分（内容、工具链、参数、受管环境变量）变化都会改变键。
func DeriveKey(in BuildInput) (Key, error) {
	if in.Toolchain == "" {
		return "", errors.New("cache: toolchain identifier is required")
	}
	h := sha256.New()
	fmt.Fprintf(h, "toolchain %s\n", in.Toolchain)

	paths := make([]string, 0, len(in.Files))
	for p := range in.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		content := in.Files[p]
		fmt.Fprintf(h, "file %s %d\n", p, len(content))
		h.Write(content)
		h.Write([]byte{'\n'})
	}

	for _, a := range in.Args {
		fmt.Fprintf(h, "arg %s\n", a)
	}

	names := make([]string, 0, len(in.Env))
	for n := range in.Env {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Fprintf(h, "env %s=%s\n", n, in.Env[n])
	}

	return Key(hex.EncodeToString(h.Sum(nil))), nil
}
