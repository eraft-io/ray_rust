# Local Demo — 单机运行时原理

> 运行命令：`cargo run -p ray-runtime --example local_demo`

## 概述

本示例展示了 **Ray Rust 单机模式** 的完整工作流程：在单个进程内完成
任务注册 → 提交 → 调度 → 执行 → 结果存储 → 获取，全程不启动任何网络服务器。

## 架构组成

`LocalRuntime` 将 6 个核心组件组装在一个进程里，通过 `Arc` 共享所有权实现零网络开销的本地协作：

```
┌─────────────────────────────────────────────────────────┐
│                     LocalRuntime                        │
│                                                         │
│   ┌─────────────┐   ┌────────────────┐                  │
│   │  InMemoryGcs│   │ ResourceManager│                  │
│   │  Store      │   │ (CPU 配额池)    │                  │
│   │  (节点/Actor│   │                │                  │
│   │   /Job 元数据)│   └───────┬────────┘                  │
│   └─────────────┘           │                           │
│                             │ 资源分配/释放              │
│                             ▼                           │
│   ┌─────────────┐   ┌────────────────┐                  │
│   │  WorkerPool │◄──│ LocalScheduler │── mpsc channel ──│◄─ 用户调用
│   │  (Worker    │   │ (事件循环)      │                  │   submit_fn()
│   │   进程池)    │   └───────┬────────┘                  │
│   └──────┬──────┘           │                           │
│          │ 获取/归还 worker   │ 调用执行器                 │
│          │                  ▼                           │
│          │          ┌────────────────┐                  │
│          └─────────►│ FunctionRegistry│                  │
│                     │ Executor       │                  │
│                     │ (按名称查函数)  │                  │
│                     └───────┬────────┘                  │
│                             │ 写入结果                   │
│                             ▼                           │
│                     ┌────────────────┐                  │
│                     │ InMemoryObject │                  │
│                     │ Store          │                  │
│                     │ (对象存储)      │                  │
│                     └────────────────┘                  │
└─────────────────────────────────────────────────────────┘
```

## 各组件职责

| 组件 | 来源 crate | 职责 |
|---|---|---|
| **InMemoryGcsStore** | `ray-gcs` | 集群元数据（节点、Actor、Job、资源用量） |
| **ResourceManager** | `ray-raylet` | 维护 CPU 配额池，`try_allocate` / `release` 操作 |
| **LocalScheduler** | `ray-raylet` | 事件循环接收命令，资源感知调度，分发任务给执行器 |
| **FunctionRegistryExecutor** | `ray-raylet` | 按函数名查找已注册的 Rust 闭包并执行 |
| **WorkerPool** | `ray-raylet` | 管理 worker 生命周期（空闲/忙碌/死亡），控制并发数 |
| **InMemoryObjectStore** | `ray-object-store` | 内存中的 key-value 对象存储，支持引用计数和磁盘溢写 |

## Demo 执行流程详解

### 步骤 1 — 创建运行时

```rust
let runtime = LocalRuntime::new(RuntimeConfig {
    num_cpus: 4,
    max_workers: 8,
    object_store_memory: 0,
})?;
```

`LocalRuntime::new()` 依次：
1. 生成随机 `NodeId`
2. 创建 `ResourceManager`（4 CPU 配额）
3. 创建 `InMemoryObjectStore`（不限制内存）
4. 创建 `FunctionRegistryExecutor`（空函数注册表）
5. 创建 `WorkerPool`（最多 8 个并发 worker）
6. 创建 `InMemoryGcsStore`
7. 启动 `LocalScheduler` 的后台 tokio 事件循环

### 步骤 2 — 注册函数

```rust
runtime.register_function("double_bytes", Arc::new(|payload| {
    Ok(payload.iter().map(|b| b.wrapping_mul(2)).collect())
})).await;
```

函数被注册到 `FunctionRegistryExecutor` 的内部 `HashMap<String, TaskFn>` 中。
后续任务通过 `function_name = "double_bytes"` 触发此闭包。

### 步骤 3 — Put / Get 对象

```rust
let obj_id = runtime.put_object(vec![10, 20, 30]).await?;
let data = runtime.get_object(&obj_id, 1000).await?;
```

直接操作 `InMemoryObjectStore`：
- `put` → 生成随机 `ObjectId`，数据存入 `HashMap<ObjectId, ObjectEntry>`，初始 `ref_count = 1`
- `get` → 轮询查找（最多等 `timeout_ms` 毫秒），返回数据副本

### 步骤 4 — 提交任务并获取结果

```rust
let result_id = runtime.submit_fn("double_bytes", vec![1, 2, 3]).await?;
let result = runtime.get_object(&result_id, 5000).await?;
// result == [2, 4, 6]
```

完整链路如下：

```
submit_fn("double_bytes", [1,2,3])
  │
  ▼
  构造 TaskSpec { function_name, function_payload, return_ids, required_resources }
  │
  ▼
  scheduler.submit_task(spec)  ──mpsc──►  事件循环接收
  │                                        │
  │  return return_id                      │  ResourceManager.try_allocate(CPU=1.0) ✓
  │                                        │
  │                                        ▼
  │                                    dispatch_task() — tokio::spawn
  │                                        │
  │                                        ├─ WorkerPool.get_worker() → 获取一个空闲 worker
  │                                        │
  │                                        ├─ Executor.execute(spec)
  │                                        │    └─ 查表 "double_bytes" → 调用闭包
  │                                        │    └─ spawn_blocking 执行，返回 [2, 4, 6]
  │                                        │
  │                                        ├─ ObjectStore.put(return_id, [2, 4, 6])
  │                                        │
  │                                        ├─ WorkerPool.return_worker() → worker 标记空闲
  │                                        │
  │                                        └─ ResourceManager.release(CPU=1.0)
  │
  ▼
  get_object(return_id, 5000ms)
  └─ ObjectStore.get(return_id) → 轮询找到 → 返回 [2, 4, 6]
```

### 步骤 5 — 并行任务

```rust
for i in 0u8..4 {
    let rid = runtime.submit_fn("double_bytes", vec![i, i+1, i+2]).await?;
    result_ids.push(rid);
}
```

4 个任务通过 `submit_fn` 快速提交，调度器事件循环依次处理：
- 每个任务消耗 1 CPU → 4 CPU 配额足够 → 全部并发执行
- 每个任务独立 `tokio::spawn`，互不阻塞
- `get_object` 按顺序阻塞等待结果（最多 5 秒）

### 步骤 6 — 多函数注册

注册第二个函数 `sum_bytes`，演示运行时支持动态注册多个函数，
任务通过 `function_name` 路由到对应的闭包。

### 步骤 7 — Shutdown

`runtime.shutdown()` 消费 `self`，drop 所有 `Arc` 引用。
当所有引用归零时，各组件自然释放，事件循环随 channel 关闭而退出。

## 关键设计

### 异步模型

所有组件基于 `tokio` 构建：
- **Scheduler** — 独立的 mpsc 事件循环，避免锁竞争
- **Executor** — `spawn_blocking` 执行 CPU 密集型闭包，不阻塞 async runtime
- **ObjectStore** — `get()` 使用轮询 + sleep，支持跨任务的异步等待

### 资源管理

`ResourceManager` 采用简单的计数器模型：
- `try_allocate(required)` — 原子检查并扣减可用资源
- `release(resources)` — 归还资源到可用池
- 每次命令处理后自动尝试调度 pending queue 中的任务

### 对象生命周期

```
put(obj)  →  ref_count = 1
add_ref() →  ref_count += 1
remove_ref() →  ref_count -= 1
               if ref_count == 0 → 自动驱逐
```

对象可被溢写到磁盘（`spill_to_disk`），再从磁盘恢复（`restore_from_disk`）。

### 与集群模式的区别

| 特性 | 单机模式 (LocalRuntime) | 集群模式 (Phase 4) |
|---|---|---|
| 进程数 | 1 | N (Head + Worker 节点) |
| 通信方式 | Arc 直接引用 | gRPC (tonic) |
| GCS | InMemory | 可替换为 etcd/Redis |
| 对象定位 | 全部本地 | 跨节点查询 + 传输 |
| 调度 | 本地调度器 | 全局调度器 + 策略选择 |

## 运行与调试

```bash
# 正常运行
cargo run -p ray-runtime --example local_demo

# 带 debug 日志
RUST_LOG=debug cargo run -p ray-runtime --example local_demo

# 运行集成测试
cargo test -p ray-runtime --test integration_local_mode
```
