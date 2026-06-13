# Ray Rust — Ray 分布式计算框架的 Rust 实现

用 Rust 重写 [Ray](https://github.com/ray-project/ray) 分布式计算框架的核心运行时（Ray Core），通过 PyO3 提供兼容的 Python API。上层 AI Libraries（Data / Train / Tune / Serve / RLlib）保持 Python 不变。

## 架构概览

```
┌─────────────────────────────────────────────────────────────────┐
│                       Python (ray_rust)                         │
│                        PyO3 bindings                            │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
┌─────────────────────────────────▼───────────────────────────────┐
│                          Ray Core (Rust)                        │
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────────┐  ┌────────────┐  │
│  │   GCS    │  │ Raylet   │  │ Object Store │  │ Scheduler  │  │
│  │ (tonic)  │  │ (tonic)  │  │   (tonic)    │  │  (global)  │  │
│  └──────────┘  └──────────┘  └──────────────┘  └────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  ray-core: ID types / Resources / Serialize / Traits      │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
                                  │
                     gRPC (tonic + prost)
                     Proto definitions (protox)
```

## 项目结构

```
ray_rust/
├── Cargo.toml                     # Workspace 根配置
├── proto/                         # gRPC 协议定义
│   ├── common.proto               # 公共类型 (TaskId/ActorId/ObjectId/Resources 等)
│   ├── gcs.proto                  # GCS 服务 (节点/Actor/Job/资源管理)
│   ├── raylet.proto               # Raylet 服务 (任务提交/Worker 管理/对象定位)
│   └── object_store.proto         # ObjectStore 服务 (Put/Get/Delete/Spill/引用计数)
│
└── crates/
    ├── ray-core/                  # 公共基础层 — 所有 crate 的共享依赖
    │   └── src/
    │       ├── error.rs           #   RayError 枚举 + RayResult 类型别名
    │       ├── id.rs              #   TaskId/ActorId/ObjectId/NodeId/JobId (宏生成)
    │       ├── resource.rs        #   Resources 资源管理 (can_satisfy/subtract/add)
    │       ├── serialize.rs       #   bincode/JSON 序列化 + ZeroCopyRead/Write traits
    │       └── traits.rs          #   ObjectStore/Scheduler/GcsStore 异步 trait 接口
    │
    ├── ray-gcs/                   # Global Control Store — 集群元数据中心
    │   ├── build.rs               #   protox + tonic-build (无需安装 protoc)
    │   └── src/
    │       ├── store.rs           #   InMemoryGcsStore (HashMap + RwLock)
    │       └── server.rs          #   GcsServer (tonic gRPC 服务实现)
    │
    ├── ray-raylet/                # Raylet — 每节点守护进程
    │   ├── build.rs
    │   └── src/
    │       ├── resource_manager.rs  #  ResourceManager (资源分配/释放/利用率统计)
    │       ├── scheduler.rs         #  LocalScheduler (事件循环 + mpsc 消息调度)
    │       └── server.rs            #  RayletServer (tonic gRPC 服务实现)
    │
    ├── ray-object-store/          # Object Store — 分布式对象存储 (Plasma 替代)
    │   ├── build.rs
    │   └── src/
    │       ├── store.rs           #   InMemoryObjectStore (内存预算/GC/wait)
    │       ├── shared_memory.rs   #   ShmRegion/ShmPool (mmap 跨进程零拷贝)
    │       └── server.rs          #   ObjectStoreServer (tonic gRPC 服务实现)
    │
    ├── ray-scheduler/             # Global Scheduler — 全局调度器
    │   └── src/
    │       ├── global.rs          #   GlobalScheduler (集群节点视图 + 调度决策)
    │       └── policy.rs          #   SchedulingPolicy trait + Spread/Pack 策略
    │
    ├── ray-runtime/               # Local Runtime — 单机本地运行时
    │   ├── Cargo.toml
    │   └── src/
    │       └── lib.rs             #   LocalRuntime (GCS+Scheduler+Store+Worker 一体化)
    │
    └── ray-py/                    # Python Bindings — PyO3 绑定入口
        └── src/
            ├── lib.rs             #   模块入口 (re-export pymodule)
            └── runtime.rs         #   PyO3 API: init/shutdown/put/get/wait + ObjectRef
```

## 技术栈

| 领域 | 技术选型 | 说明 |
|---|---|---|
| 异步运行时 | [tokio](https://tokio.rs) 1.x (full) | Work-stealing 多线程调度器 |
| gRPC | [tonic](https://github.com/hyperium/tonic) 0.12 | 基于 hyper 的纯 Rust gRPC 实现 |
| Protobuf 解析 | [protox](https://github.com/andrewhickman/protox) 0.7 | 纯 Rust proto 解析器，**无需安装 protoc** |
| 序列化 | [serde](https://serde.rs) + [bincode](https://github.com/bincode-org/bincode) | 高性能二进制序列化，支持零拷贝 |
| 共享内存 | [shared_memory](https://github.com/elast0ny/shared_memory-rs) 0.12 | 跨进程 mmap 零拷贝数据交换 |
| Python 绑定 | [PyO3](https://pyo3.rs) 0.23 | Rust ↔ Python FFI，业界标准 |
| 错误处理 | [thiserror](https://github.com/dtolnay/thiserror) 2 + [anyhow](https://github.com/dtolnay/anyhow) | 类型安全的错误定义与传播 |
| 日志/追踪 | [tracing](https://github.com/tokio-rs/tracing) 0.1 | 结构化异步日志 |

## 快速开始

### 前置要求

- **Rust** ≥ 1.70 (stable)
- **Python** ≥ 3.8 + `maturin` (仅构建 `ray-py` 时需要)

> 无需安装 `protoc` — proto 文件由 `protox` (纯 Rust) 解析。

### 编译

```bash
# 编译所有核心 crate (不含 ray-py)
cargo build

# 编译指定 crate
cargo build -p ray-core
cargo build -p ray-gcs
```

### 运行测试

```bash
cargo test
```

### 构建 Python 绑定

```bash
pip install maturin

# 构建并安装到当前 Python 环境
maturin develop -m crates/ray-py/Cargo.toml
```

```python
import ray_rust

ray_rust.init(address="auto", num_cpus=4)

# Put / Get
obj_ref = ray_rust.put(b"hello world")
data = ray_rust.get(obj_ref)

# Wait
ready, not_ready = ray_rust.wait([obj_ref], num_returns=1, timeout_ms=5000)

ray_rust.shutdown()
```

## 核心 Crate 说明

### `ray-core` — 公共基础层

所有 crate 的共享依赖，提供：

- **ID 类型**：`TaskId`(16B)、`ActorId`(16B)、`ObjectId`(28B)、`NodeId`(16B)、`JobId`(16B)
- **资源管理**：`Resources` 支持自定义资源标签，提供 `can_satisfy` / `subtract` / `add` 操作
- **序列化**：基于 `bincode` 的二进制序列化 + `ZeroCopyRead` / `ZeroCopyWrite` traits
- **异步 trait**：`ObjectStore`、`Scheduler`、`GcsStore` 可替换的后端接口

### `ray-gcs` — Global Control Store

集群元数据中心，管理：

- 节点注册、心跳、存活状态
- Actor 注册、查询、生命周期
- Job 注册与状态追踪
- 全局资源使用量汇总

### `ray-raylet` — 每节点守护进程

每个集群节点运行一个 Raylet，负责：

- **ResourceManager**：节点级资源池管理（CPU/GPU/内存/自定义资源）
- **LocalScheduler**：基于 mpsc 事件循环的本地调度器，资源感知的任务分配
- gRPC 服务：任务提交/取消、Worker 注册、对象定位、跨节点对象传输

### `ray-object-store` — 分布式对象存储

替代 C++ Plasma 的 Rust 实现：

- **InMemoryObjectStore**：进程内对象存储，支持内存预算与零引用 GC
- **ShmRegion / ShmPool**：基于 mmap 的共享内存，实现跨进程零拷贝读取
- 对象溢写（Spill）与恢复接口

### `ray-scheduler` — 全局调度器

跨节点任务放置决策：

- **GlobalScheduler**：维护集群节点视图，根据策略选择执行节点
- **SchedulingPolicy**：可插拔策略 trait，内置 `SpreadPolicy`（负载均衡）和 `PackPolicy`（资源紧凑）

### `ray-runtime` — 单机本地运行时

将所有组件组装为单进程运行时，无需启动任何 gRPC 服务器：

- **LocalRuntime**：一键初始化 GCS + Scheduler + ObjectStore + WorkerPool
- 支持注册自定义函数 (`register_function`)、提交任务 (`submit_fn`)、获取结果 (`get_object`)
- 提供 `put_object` / `cancel_task` / `get_task_status` 等完整 API

### `ray-py` — Python 绑定

通过 PyO3 暴露 Rust Core 给 Python：

- `ray_rust.init()` / `shutdown()` — 运行时生命周期
- `ray_rust.put()` / `get()` / `wait()` — 对象存储操作
- `ObjectRef` / `TaskResult` — Python 可见的类

## gRPC 协议

所有 `.proto` 文件位于 `proto/` 目录，由 `protox` 在 `build.rs` 中编译：

| 文件 | 包名 | 服务 |
|---|---|---|
| `common.proto` | `ray.common` | 公共消息类型定义 |
| `gcs.proto` | `ray.gcs` | `GcsService` (14 个 RPC) |
| `raylet.proto` | `ray.raylet` | `RayletService` (11 个 RPC) |
| `object_store.proto` | `ray.object_store` | `ObjectStoreService` (12 个 RPC) |

## 本地运行 Demo

### 运行内置示例

```bash
# 一键运行本地运行时 Demo
cargo run -p ray-runtime --example local_demo
```

### 代码示例

在 Rust 项目中直接使用 `ray-runtime` crate 运行本地任务：

```bash
# 在 Cargo.toml 中添加依赖
# ray-runtime = { path = "crates/ray-runtime" }
```

```rust
use ray_core::error::RayResult;
use ray_runtime::{LocalRuntime, RuntimeConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 创建本地运行时（4 CPU, 8 workers）
    let runtime = LocalRuntime::new(RuntimeConfig {
        num_cpus: 4,
        max_workers: 8,
        object_store_memory: 0, // 0 = 不限制内存
    })?;

    // 2. 注册一个函数：将 payload 中每个字节乘以 2
    runtime
        .register_function(
            "double_bytes",
            Arc::new(|payload: &[u8]| -> RayResult<Vec<u8>> {
                Ok(payload.iter().map(|b| b.wrapping_mul(2)).collect())
            }),
        )
        .await;

    // 3. 提交一个对象到对象存储
    let obj_id = runtime.put_object(vec![10, 20, 30]).await?;
    let data = runtime.get_object(&obj_id, 1000).await?;
    println!("Stored and retrieved: {:?}", data); // [10, 20, 30]

    // 4. 提交一个任务并等待结果
    let result_id = runtime.submit_fn("double_bytes", vec![1, 2, 3]).await?;
    let result = runtime.get_object(&result_id, 5000).await?;
    println!("Task result: {:?}", result); // [2, 4, 6]

    // 5. 也可以并行提交多个任务
    let mut result_ids = Vec::new();
    for i in 0u8..4 {
        let rid = runtime
            .submit_fn("double_bytes", vec![i, i + 1, i + 2])
            .await?;
        result_ids.push((rid, i));
    }

    // 收集所有结果
    for (rid, i) in result_ids {
        let result = runtime.get_object(&rid, 5000).await?;
        println!("Task {} result: {:?}", i, result);
    }

    // 6. 关闭运行时
    runtime.shutdown().await;
    Ok(())
}
```

运行集成测试：

```bash
# 运行所有测试（包括集成测试）
cargo test -p ray-runtime

# 只运行集成测试
cargo test -p ray-runtime --test integration_local_mode

# 带日志查看详细执行过程
RUST_LOG=debug cargo test -p ray-runtime --test integration_local_mode -- --nocapture
```

## 开发指南

```bash
# 格式化
cargo fmt

# Lint 检查
cargo clippy --all-targets -- -D warnings

# 运行单个 crate 的测试
cargo test -p ray-scheduler

# 带日志运行测试
RUST_LOG=debug cargo test -p ray-raylet -- --nocapture
```

## License

Apache-2.0
