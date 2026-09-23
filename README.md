# artifact-cache-go

本地构建产物缓存的调度与复用机制（纯 Go 标准库实现，不依赖网络、外部服务或凭据）。
以本地文件系统同时模拟“构建执行”与“缓存后端”，适用于并发请求、条目损坏与
容量回收场景下结论一致、可解释的本地验证。

## 适用范围与设计边界

- 单进程内并发合并：同一缓存键的并发请求只执行一次构建（singleflight）。
- 单主机本地 FS 后端：磁盘为权威存储，进程内仅保存索引；重启后扫描重建索引。
- 当前格式版本 **v2**；兼容范围仅限 v2。v1 及以下、v99 及以上明确拒绝，不静默误用。
- 不提供跨主机/跨进程分布式锁；多进程同时写同一目录不在保证范围内。
- 构建执行由调用方提供 (`cache.Builder`)；`cache.SimBuilder` 为本地确定性模拟器。

## 1. 缓存键的派生规则

`cache.DeriveKey(BuildInput)` 对声明式输入做规范化序列化后计算 SHA-256，
输出 64 位十六进制字符串（文件名安全）：

1. 固定域前缀 `artifact-cache/key/v2`（格式换代且输入语义变化时调整）。
2. 工具链标识 `Toolchain`（如 `go:1.26.0`），长度前缀编码。
3. 三个 map（`Content` 源内容、`Params` 构建参数、`ManagedEnv` 受管环境变量）：
   - **按键排序后遍历**，因此声明顺序、迭代顺序不影响键；
   - 每个键、值均以十进制长度作长度前缀，消除分隔符与空值歧义；
   - `nil` 映射与空映射语义等价，产生相同键。
4. `Config.EnvWhitelist` 配置受管环境变量白名单，派生前过滤：
   白名单外的变量（如 `SECRET_TOKEN`）一律不参与键；白名单内变量值变化必然改键。

因此：

| 变化点 | 键是否变化 |
|---|---|
| 源内容 (`Content`) 任意字节 | 变 |
| 工具链标识 (`Toolchain`) | 变 |
| 构建参数 (`Params`) 键或值 | 变 |
| 白名单内受管环境变量 | 变 |
| map 声明顺序、nil/空差异 | 不变 |
| 白名单外环境变量 | 不变 |

## 2. 命中与未命中的可解释原因

`GetOrBuild` 返回 `Result.Decision`（不是布尔值），区分 `HIT` / `MISS` 与具体原因：

| 原因码 | 含义 |
|---|---|
| `HIT_FRESH` | 命中完整、版本当前的条目 |
| `MISS_NOT_FOUND` | 磁盘上无此键的可用条目，本次实际构建 |
| `MISS_SHARED_BUILD` | 同键构建进行中，本请求复用其结果（未执行构建） |
| `MISS_CORRUPT` | 条目截断 / 魔数错误 / 长度不符 / SHA-256 校验失败，按未命中处理并重建 |
| `MISS_OLD_VERSION` | 条目版本低于 v2（如 v1），明确拒绝并重建，不静默迁移 |
| `MISS_FUTURE_VERSION` | 条目版本高于当前版本，明确拒绝 |
| `MISS_BUILD_FAILED` | 构建或原子写失败，未写入缓存；等待者得到同一错误 |

## 3. 并发与完整性保证

### 3.1 同键并发合并

- 首个请求成为 leader 并登记 in-flight；同键后到请求成为等待者，仅阻塞等待。
- leader 命中既有条目（仅锁外读盘）时，读盘期间到达的请求也挂同一 flight，
  读完共享同一产物并计为命中，不会触发第二次磁盘读以外的构建。
- 构建成功后一次性发布结果并 `close(done)`；等待者绝不可能观察到未完成状态。
- 构建失败时错误与空产物一起发布：**失败不写缓存**，等待者得到同一错误。

### 3.2 原子写入与损坏识别

磁盘布局（所有整数大端）：

```
"ACHE" | version uint32 | keyLen uint16 | keyHex | artLen uint64 | artifact | sha256
```

校验和覆盖 `version..artifact`（不含 magic 与末尾 32 字节摘要本身）。

- 写入：同目录临时文件 → `Sync` → `rename` → 目录 fsync。读者只能看到完整旧文件
  或完整新文件，看不到部分写入；崩溃残留的 `.*.tmp` 在打开扫描时清理。
- 读取：校验魔数 → **版本号（先于校验和）** → 长度边界与键名一致性 → SHA-256。
  截断、翻转、长度声明不符分别可解释（错误中带 `truncated/checksum/magic/key` 操作位）。

### 3.3 引用保护

索引项维护引用计数 `refs`：

- 读取磁盘期间 `refs++`，完成后 `--`；
- 同键 flight 的每个等待者同样占一个引用；
- 回收器跳过任何 `refs > 0` 的条目——正在被读取或被等待的条目不会被淘汰。

## 4. 回收策略与版本兼容

### 4.1 容量回收（确定的 LRU）

- 每次成功写入并登记索引后检查 `CurrentBytes > MaxBytes`。
- 候选条目按 `atime` 升序（逻辑时钟，命中/写入都会刷新；同值再按键十六进制升序）
  选择牺牲者，因此回收结果确定、可重复。
- `refs > 0`（读进行中 / 等待中）与本轮新写入的保护键跳过。
- 每删一个磁盘文件同步扣减索引字节数与条目数，统计 `Evictions++`；
  若磁盘删除失败则保留索引，维持“索引与磁盘一致、占用不超过上限”。
- 单条产物编码后超过上限时**不写入缓存**，从规则上保证回收后绝不超上限。
- 统计口径：`Stats.CurrentBytes = Σ 磁盘条目文件实际大小`，测试逐项与 `os.Stat` 对账。
- `MaxBytes <= 0` 表示不限制。

### 4.2 版本兼容范围

- 仅 `version == 2` 进入索引并可服务；`supportedReadVersions = {2}`。
- 打开缓存扫描时，对非临时文件逐个解码：
  - 旧版本（< 2）→ `MISS_OLD_VERSION`，计 `VersionRejects`，删除后下次请求重建；
  - 未来版本（> 2）→ `MISS_FUTURE_VERSION`，同上；
  - 其他损坏 → `MISS_CORRUPT`，计 `CorruptRejects`。
- 版本检查先于校验和：即使旧条目字节“恰好自洽”，也只能因版本被拒绝，
  不会被静默当作当前版本读取。

## 5. 目录结构

```
<Dir>/
  v2/
    <64-hex-key>.entry   # 当前版本条目
    .<...>.tmp           # 原子写临时文件（崩溃残留，扫描即清）
```

## 6. 本地验证方法

仅依赖 Go 工具链与标准库，全程离线、无外部服务：

```sh
# 全部单测，竞态检测，重复 10 次验证可重复性
go test ./... -race -count=10 -v

# 只跑某一类场景
go test ./cache/ -run TestConcurrent -race -v   # 并发同键合并 / 失败共享
go test ./cache/ -run TestCorrupt -v            # 截断/翻转/魔数/版本
go test ./cache/ -run TestGenuinely -v          # 格式正确的 v1 被拒
go test ./cache/ -run TestEviction -race -v     # LRU 回收、上限、重启对账
go test ./cache/ -run TestEntryProtected -race -v
go test ./cache/ -run 'RealInterleave' -race -v # 读取期间触发回收的真实交错

# 可运行演示：打印键、原因、实际构建次数与占用统计
go run ./cmd/demo --max-bytes 600
```

`-v` 日志中每条用例都会打印：缓存键、`HIT/MISS` 及原因码、
`SimBuilder.BuildCount()` 实际构建次数，以及
`entries/bytes/limit/hits/misses/builds/corrupt/versionReject/evictions/inFlight` 统计。

失败原因可区分：不同异常有不同 `ReasonCode`，且 `Decision.Detail` 带键与具体错误。

## 7. 包 API 速览

```go
c, _ := cache.New(cache.Config{Dir: dir, MaxBytes: 4096, EnvWhitelist: []string{"GOOS"}})
defer c.Close()

res, err := c.GetOrBuild(ctx, cache.BuildInput{
    Content:    map[string]string{"main.go": src},
    Toolchain:  "go:1.26.0",
    Params:     map[string]string{"-tags": "netgo"},
    ManagedEnv: map[string]string{"GOOS": "linux"},
}, cache.NewSimBuilder())

fmt.Println(res.Key, res.Decision, res.Shared, c.SnapshotStats())
```
