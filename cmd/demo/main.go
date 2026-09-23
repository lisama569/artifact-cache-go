// Command demo 在本地临时目录上演示构建产物缓存的调度与复用：
// 稳定键派生、命中/未命中原因、并发同键合并与容量回收统计。
// 不访问网络或任何外部服务。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/lisama569/artifact-cache-go/cache"
)

func main() {
	dir := flag.String("dir", filepath.Join(os.TempDir(), "artifact-cache-demo"), "cache directory")
	maxBytes := flag.Int64("max-bytes", 600, "capacity limit in bytes (<=0 unlimited)")
	flag.Parse()

	c, err := cache.New(cache.Config{
		Dir:          *dir,
		MaxBytes:     *maxBytes,
		EnvWhitelist: []string{"GOOS", "GOARCH"},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "open cache:", err)
		os.Exit(1)
	}
	defer c.Close()

	in := cache.BuildInput{
		Content:    map[string]string{"main.go": "package main\nfunc main() {}"},
		Toolchain:  "go:1.26.0",
		Params:     map[string]string{"-tags": "netgo"},
		ManagedEnv: map[string]string{"GOOS": "linux", "GOARCH": "amd64", "SECRET": "ignored"},
	}
	ctx := context.Background()

	// 1) 首次：未命中 -> 构建
	r, err := c.GetOrBuild(ctx, in, cache.NewSimBuilder())
	if err != nil {
		fmt.Fprintln(os.Stderr, "get:", err)
		os.Exit(1)
	}
	fmt.Printf("[1] %s\n    %s\n", r.Key, r.Decision)
	fmt.Printf("    stats: %s\n", c.SnapshotStats())

	// 2) 并发 8 个同键请求：全部命中（产物已在缓存中）
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = c.GetOrBuild(ctx, in, cache.NewSimBuilder())
		}()
	}
	wg.Wait()
	fmt.Printf("[2] after 8 concurrent same-key lookups:\n    stats: %s\n", c.SnapshotStats())

	// 3) 插入若干新键，演示 LRU 回收与容量口径
	for i := 0; i < 4; i++ {
		in2 := in
		in2.Params = map[string]string{"variant": string(rune('a' + i))}
		rr, berr := c.GetOrBuild(ctx, in2, cache.NewSimBuilder())
		if berr != nil {
			fmt.Fprintln(os.Stderr, "get variant:", berr)
			os.Exit(1)
		}
		fmt.Printf("[3.%d] key=%s reason=%s\n    stats: %s\n", i, rr.Key, rr.Decision.Reason, c.SnapshotStats())
	}
}
