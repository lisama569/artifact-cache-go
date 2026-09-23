package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sort"
	"strconv"
)

// keyDomain 是键派生的域分隔前缀；条目格式版本升级且输入语义变化时可调整。
const keyDomain = "artifact-cache/key/v2"

// BuildInput 是声明式构建输入。语义等价的输入必须派生相同缓存键。
type BuildInput struct {
	// Content 为构建源内容的 逻辑名 -> 内容 映射（顺序无关）。
	Content map[string]string
	// Toolchain 标识工具链，例如 "go:1.26.0"；变化必然导致键变化。
	Toolchain string
	// Params 为构建参数（顺序无关，键与值均敏感）。
	Params map[string]string
	// ManagedEnv 为受管环境变量（顺序无关）。
	// 调用方应只放入受白名单管理的变量；Cache 会按配置白名单过滤。
	ManagedEnv map[string]string
}

// Key 是由 BuildInput 派生的稳定缓存键。
type Key struct {
	hex string
}

// String 返回键的十六进制文本（64 个字符，文件名安全）。
func (k Key) String() string { return k.hex }

// IsZero 报告键是否为零值（未派生）。
func (k Key) IsZero() bool { return k.hex == "" }

// writeSection 以“段标记 + 长度前缀键值对”写入一个顺序无关映射，
// 长度前缀消除空值/分隔符歧义，排序保证确定性。
func writeSection(w io.Writer, tag byte, m map[string]string) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := m[k]
		io.WriteString(w, string(tag))
		io.WriteString(w, " ")
		io.WriteString(w, strconv.Itoa(len(k)))
		io.WriteString(w, " ")
		io.WriteString(w, k)
		io.WriteString(w, " ")
		io.WriteString(w, strconv.Itoa(len(v)))
		io.WriteString(w, " ")
		io.WriteString(w, v)
		io.WriteString(w, "\n")
	}
	io.WriteString(w, string(tag))
	io.WriteString(w, " END\n")
}

// DeriveKey 从声明式输入派生稳定、确定的缓存键。
// 规则：对四类输入（内容、工具链、参数、受管环境变量）做规范化序列化后
// 计算 SHA-256。映射按键排序、字段长度前缀编码，因此：
//   - 映射声明顺序、nil 与空映射不影响结果（语义等价 => 键相同）；
//   - 内容/工具链/参数/受管环境变量中任意字节变化 => 键变化。
func DeriveKey(in BuildInput) Key {
	h := sha256.New()
	io.WriteString(h, keyDomain)
	io.WriteString(h, "\n")
	io.WriteString(h, "T ")
	io.WriteString(h, strconv.Itoa(len(in.Toolchain)))
	io.WriteString(h, " ")
	io.WriteString(h, in.Toolchain)
	io.WriteString(h, "\n")
	writeSection(h, 'C', in.Content)
	writeSection(h, 'P', in.Params)
	writeSection(h, 'E', in.ManagedEnv)
	return Key{hex: hex.EncodeToString(h.Sum(nil))}
}

// withEnvWhitelist 返回只保留白名单内受管环境变量的输入副本（顺序无关）。
func (in BuildInput) withEnvWhitelist(whitelist []string) BuildInput {
	if len(whitelist) == 0 || len(in.ManagedEnv) == 0 {
		in.Content = cloneMap(in.Content)
		in.Params = cloneMap(in.Params)
		in.ManagedEnv = cloneMap(in.ManagedEnv)
		return in
	}
	allowed := make(map[string]bool, len(whitelist))
	for _, name := range whitelist {
		allowed[name] = true
	}
	filtered := make(map[string]string, len(in.ManagedEnv))
	for k, v := range in.ManagedEnv {
		if allowed[k] {
			filtered[k] = v
		}
	}
	out := in
	out.Content = cloneMap(in.Content)
	out.Params = cloneMap(in.Params)
	out.ManagedEnv = filtered
	return out
}

func cloneMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
