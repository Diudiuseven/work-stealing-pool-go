# work-stealing-pool-go

一个可自适应负载均衡、不丢任务的本地任务执行器（work-stealing executor）。
位于 `pool` 包，纯标准库实现。

## 调度语义

- **多生产者并发提交**：`Submit` / `SubmitTask` 可从任意 goroutine 并发调用。
  提交按轮询（round-robin）放入某个 worker 的**本地队列**，尽量让任务留在本地执行，
  减少全局争抢。
- **工作窃取（work stealing）**：每个 worker 优先从自己本地队列**尾部**取任务
  （LIFO，缓存友好）；本地为空时从其他 worker 队列**头部**窃取最老的任务
  （FIFO，降低与属主的冲突），并可按 `StealBatchSize` 批量窃取摊薄成本。
  任务耗时波动剧烈时，空闲 worker 会自动从繁忙 worker 处分担积压，抑制长尾。
- **恰好一次执行**：每个任务的状态机为 `pending → running → done`，或
  `pending → cancelled`，状态迁移使用 CAS；终止回调经 `sync.Once` 触发，
  在负载倾斜、并发取消与窃取下不会重复执行也不会丢失。
- **取消**：`Task.Cancel()` 在任务未开始时保证其永不执行（返回 `true`）；
  已在运行时则取消任务的 `context.Context`（返回 `false`），由任务函数自行响应。
- **优雅关闭与排水**：`Shutdown()` 后立即拒绝新任务（返回 `ErrClosed`）；
  已入队未开始的任务会被 worker 继续执行完毕（明确处理，不丢弃）；
  正在执行的任务运行到完成。`Drain()` 阻塞直到所有已接受任务到达终态、
  所有 worker 退出。
- **异常隔离**：任务 panic 被 recover 隔离，记录到 `Task.PanicValue()`、
  计入 `Stats.Panicked` 并回调 `PanicHandler`；worker 与其他任务不受影响。
- **优先级与公平**：每 worker 维护高/中/低三条本地队列，默认高优先级先调度；
  `StarveEvery = N` 时，连续执行 N 个高优先级任务后强制服务一个低优先级任务，
  避免低优先级被无限饿死。

## 配置项（Config）

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `Workers` | 4 | worker goroutine 数（并发度） |
| `QueueCapacity` | 256 | 每个 worker、每个优先级的本地队列容量（有界，不静默丢弃） |
| `Policy` | `OverflowBlock` | 所有队列满时的溢出策略，见下 |
| `PanicHandler` | nil | 任务 panic 时的回调 `(taskID, panicValue)` |
| `StarveEvery` | 0（不限制） | 连续 N 个高优先级任务后让出一次给低优先级 |
| `StealBatchSize` | 1 | 单次窃取的最大任务数 |

溢出策略（`OverflowPolicy`）——提交方永不被无条件阻塞、任务永不静默丢失：

- `OverflowBlock`：等待空位（随提交 ctx 可取消），超时/取消返回 ctx 错误；
- `OverflowReject`：立即返回 `ErrQueueFull`，由调用方决定重试或降级；
- `OverflowRunInline`：在调用方 goroutine 上同步执行该任务（天然背压）。

## 可观测性

`Pool.Stats()` 返回实时原子计数：`Submitted / Completed / Panicked /
Rejected / Stolen / Cancelled / Inlined`。`Task.Done()`、`Task.Err()`、
`Task.PanicValue()` 用于单任务级别的结果与异常上报。

## 使用示例

```go
p := pool.New(pool.Config{Workers: 8, QueueCapacity: 128,
    Policy: pool.OverflowBlock, StarveEvery: 8})
t, _ := p.SubmitTask(ctx, pool.PriorityHigh, func(ctx context.Context) error {
    // do work
    return nil
})
<-t.Done()          // 等待终态
p.Shutdown()        // 停止接收新任务
p.Drain()           // 等待排水完成
```

## 验证方法

```sh
go vet ./...
go test ./pool/ -race -count=3 -timeout 300s
```

测试覆盖（`pool/pool_test.go`）：

- `TestExactlyOnceHighConcurrency`：16 生产者 × 500 任务，恰好一次、不丢；
- `TestWorkStealingUnderSkew`：人为把 200 个任务压到单个 worker，验证窃取分担（`Stolen > 0`）；
- `TestCancellationConcurrent`：执行/窃取与并发取消竞争下，每任务恰好进入一次终态；
- `TestGracefulShutdown`：关闭后拒绝新任务（`ErrClosed`），排队任务被排水执行完；
- `TestPanicIsolation`：panic 被隔离上报，其余 100 个任务全部完成；
- `TestOverflowReject` / `TestOverflowRunInline`：两种溢出策略的明确行为；
- `TestPriorityFairness`：高优先级洪泛下低优先级任务在 `StarveEvery+1` 内被服务。
