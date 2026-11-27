# Qubit Magika

[![Rust CI](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-magika/coverage-badge.json)](https://qubit-ltd.github.io/rs-magika/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-magika.svg?color=blue)](https://crates.io/crates/qubit-magika)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-magika` 为 `qubit-mime` 提供基于 Magika 的
`qubit_mime::MimeDetector`，适用于文件名可能与实际内容不符的上传检测场景。它能在
不让 `qubit-mime` 直接依赖 Magika 和 ONNX Runtime 的前提下识别内容，并允许应用在启动时
创建一个 detector，供后续检测复用。

## 概述

本 crate 默认启用 `bundled-onnxruntime`，普通构建无需单独安装 ONNX Runtime 即可
链接和运行。应用也可以关闭默认 feature，自行提供兼容的 ONNX Runtime 共享库。使用
`ort` 动态加载器时，须先调用 `ort::init_from` 初始化共享库，再创建 detector。
Linux CI 的 [`dynamic-loader` job](https://github.com/qubit-ltd/rs-magika/blob/main/.github/workflows/ci.yml) 会用这套配置对
Python 内容实际执行一次推理；依赖和启动示例见[用户手册](doc/user_guide.zh_CN.md)。

## 安装

```toml
[dependencies]
qubit-mime = "0.22"
qubit-magika = "0.18"
qubit-spi = "0.14"
qubit-operation = "0.3.2"
```

上面的应用清单保留示例所需的直接依赖。`qubit-local-files ^0.6` 是 `qubit-mime` 引入的生产依赖，应用无需直接声明它。截至 2026-11-25，crates.io 最新非 yanked 版本为 `qubit-magika` 0.16.0、`qubit-mime` 0.21.1、`qubit-fs` 0.3.3、`qubit-operation` 0.3.1 和 `qubit-local-files` 0.5.4。所需的 Magika 0.18、MIME 0.22、FS 0.4 和 Operation 0.3.2 尚未发布，因此 `--registry-only` 无法解析此应用。请使用[依赖表](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/ci/local-path-dependencies.tsv)中的固定同级源码 checkout，并从仓库根运行 `python3 .infra/check-readme-examples.py`。checker 默认只用 default features 编译提取出的中英文程序；增加 `--run-example` 后，才会分别用 default 和 `inventory` 运行这两份程序。锁定的 `readme_consumer` fixture 是隔离动态加载应用，Magika 关闭了默认 features，因此它的依赖图不同于本仓库根清单启用 bundled runtime 的默认图。dynamic-loader CI 按 host 过滤 feature 图 metadata，先检查 `dynamic_loader` 的 feature 图，再编译其普通二进制，以验证常规应用依赖能否编译 `ort::init_from`；随后检查 `readme_consumer` 在 default、`inventory` 下的 feature 图，以及针对其他平台依赖的负例，再编译 default 配置，最后用两种配置运行该 fixture 并隔离加载 ONNX Runtime。

## 快速开始

```rust
use std::error::Error;
use std::sync::Arc;

use qubit_magika::{MagikaMimeDetectorProvider, ManagedInferenceRuntime};
use qubit_mime::{
    MimeConfig,
    MimeDetector,
    MimeDetectorRegistry,
    MimeDetectionPolicy,
};
use qubit_operation::{ManagerOptions, OperationManager};
use qubit_spi::ProviderSelection;

// 应用在整个生命周期内保留 Registry 和 runtime。
fn create_detector() -> Result<(
    MimeDetectorRegistry,
    Arc<ManagedInferenceRuntime>,
    Arc<dyn MimeDetector>,
), Box<dyn Error>> {
    let manager = OperationManager::new(ManagerOptions::default())?;
    let runtime = Arc::new(ManagedInferenceRuntime::new(
        manager,
        MimeConfig::default().max_buffer_size(),
    )?);
    let registry = MimeDetectorRegistry::default();
    registry.register(
        MagikaMimeDetectorProvider::new().managed_runtime(Arc::clone(&runtime)),
    )?;
    let selection = ProviderSelection::named("magika")?;
    let provider = registry.resolve_selected(&selection)?;
    let detector = provider.create_configured(&MimeConfig::default())?;
    Ok((registry, runtime, detector))
}

// 文件名有误时，按上传内容验证类型。
fn inspect_upload(detector: &dyn MimeDetector) -> Result<Option<String>, qubit_mime::MimeError> {
    detector.detect(
        b"#!/usr/bin/env python3\nprint('hello')\n",
        Some("upload.txt"),
        MimeDetectionPolicy::VerifyContent,
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    let (_registry, runtime, detector) = create_detector()?;
    assert_eq!(
        Some("text/x-python".to_owned()),
        inspect_upload(detector.as_ref())?,
    );
    let report = runtime.shutdown_blocking(
        qubit_operation::ShutdownMode::Drain,
        std::time::Duration::from_secs(5),
    );
    assert!(report.worker_stopped);
    Ok(())
}
```

直接调用 `MagikaMimeDetector::new()` 或 `from_mime_config(...)` 会新建并初始化 runtime，
初始化失败返回 `MimeError`。异步服务可调用
`MagikaMimeDetector::builder().build_async().await`，等待模型会话就绪时让出执行器线程。Provider 的 `create_configured(&MimeConfig)` 则返回
`ProviderFailure<MimeError>`，并保留原始错误来源。通过
`MagikaMimeDetectorBuilder::managed_runtime` 或
`MagikaMimeDetectorProvider::managed_runtime` 注入已有 runtime 时会直接复用，不会再初始化
Session；builder 会校验其字节预算不小于 detector 配置的 buffer limit。

## Provider 名称

- `magika`
- `magika-mime-detector`
- `magikamimedetector`

## 说明

`MagikaMimeDetector` 的文件名检测委托给
`qubit_mime::RepositoryMimeDetector`。内容、reader 和文件检测使用 Magika 推理，
再返回 Magika 映射出的 MIME type。

### 带 Operation scope 的异步 Provider 检测

需要对异步 Provider 采样进行取消和资源释放跟踪的应用，可向
`MagikaMimeDetector::detect_async_provider_path_managed` 或 `detect_bytes_managed` 传入
`OperationScope`。受管调用会将工作关联到该 scope，使其报告能独立跟踪资源释放和业务结果。
应用启动时创建 runtime 和 detector，每个受管请求传入 scope，并在应用关闭时 drain runtime。
Provider path 的 session 和旧范围读取路由由 worker 发出采样请求，并等待 operation body 执行读取；缓慢的
范围读取可能占用共享 worker 并延迟其他请求。`read_all` 回退则先读取完整内容，再把拥有的缓冲区提交给 worker。
Managed 调用由 scope manager 的执行器在 operation body 中轮询 Provider `open`、`stat` 和样本读取；旧 hook
使用 runtime 服务组执行器，调用方等待 operation outcome。Provider poll 必须非阻塞并兼容该执行器的 reactor。
session 或旧范围读取不得同步等待同一 runtime 中已受理且仍需该 worker 的嵌套检测；应解除依赖或使用独立 runtime。
`read_all` 的 I/O 在派发给 worker 之前完成。
丢弃请求会停止本地轮询；若无法观察 native 或远端 I/O 完成状态，对应的容量预留会继续保留。详见
[Provider 采样与 runtime 说明](doc/user_guide.zh_CN.md#进阶用法)。
检测 future 返回的是业务结果；scope observer 会单独报告资源结算。有界等待可能返回 `Pending(snapshot)`，
也可能返回终态 `Ready(report)`，但 `all_released()` 仍为 false；负面报告也可能来自 scope 中其他资源，不能据此推断 Magika 仍占用容量。
本次请求的 Provider I/O 完成状态未确认，或 worker 仍在访问请求资源时，Magika 会继续保留自己的容量。停止请求会析构本地 future、reader
和 driver，但这不能证明 native 或远端 I/O 已完成。内置清理没有接收已丢弃 Provider I/O 完成证据的入口，
按当前策略重试也不能释放这笔容量。`RecoveryHandle` 可检查状态、重试策略或转移责任。
`worker_stopped` 只报告 worker join，与结算和资源释放相互独立。

Inventory 只提交同步 `MimeDetectorSpec` factory，不会向 `qubit-mime` 独立的 async provider
registry 添加 Provider，也不会附加应用 scope。通过支持 scope 的 async `MimeRuntime` 路由
选择这个仅支持同步的 `magika` Provider 时，会返回 `AsyncUnsupported`，不会回退到同步推理。
若需 scope 跟踪，可直接调用 Magika 的 managed API。旧 content hook 通常等待内部操作结算；旧 Provider hook 通常等待业务结果。
runtime 取消时，两者都可能提前返回停止错误。受管 API 会单独提供资源报告：有界等待可能返回 `Pending`；
`Ready` 报告也可能指出释放尚未确认。
本地停止请求不会在 Provider I/O 或 worker 访问尚未确认结束时释放容量。关闭期限用于观察请求和
worker 进展；同步取消通知或 panic 载荷析构可能使整次调用超过期限。关闭不会强制中断 native
推理或 Provider I/O。`worker_stopped == false` 表示本次调用返回时尚未确认 worker 已 join；`true` 表示返回前已确认 join，但不代表资源报告中的所有资源都已释放。关闭可能在 deadline 后再做一次 join 检查，因此该字段不能确定 worker 在 deadline 前何时退出。
`worker_panicked` 只记录 join 时观察到的未捕获 panic。详见[受管生命周期说明](doc/user_guide.zh_CN.md#迁移指南)。

启用 inventory 时，`MimeDetectorRegistry::builtin()` 会发现未配置的同步 Provider；默认选择仍是
`repository`，因此需显式选择 `magika`。独立的 `MimeDetectorRegistry::default()` 初始为空，适合
像上例那样为应用单独注册 Provider。重复注册同名 Provider 会报错。依赖 inventory 时需通过
`use qubit_magika as _;` 引用 crate，避免链接器移除提交项。

在源码仓库中准备缺少的本地依赖 checkout 时，请使用
[准备脚本](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/bin/prepare-local-path-dependencies.sh)。
只读校验请从仓库根运行 [`check-locked-local-path-dependencies.sh`](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/lib/check-locked-local-path-dependencies.sh)
（`.infra/lib/check-locked-local-path-dependencies.sh`）。该命令检查每个兄弟 checkout 的固定提交和干净工作区，
缺失、提交不符或工作区有改动时会报错但不会修复；行为与 [CI 的固定依赖检查](https://github.com/qubit-ltd/rs-magika/blob/main/.github/workflows/ci.yml)一致。
准备操作不会改动现有 Git checkout，因此需先手动将漂移的 checkout 恢复到固定提交再检查。
常规和动态 fixture bootstrap 模式共用同一个准备 helper。发布新建的缺失 checkout 需要独占目标路径的重命名：
工具优先使用 GNU `mv -Tn`（某些系统上命令名为 `gmv`）；在 Darwin 上找不到支持 `-T` 的 `mv` 或 `gmv` 时，
默认调用系统 `python3` 的 `renamex_np`。可通过 `RS_INFRA_PYTHON` 指定 Python 解释器；其他平台仍需 GNU `mv -Tn` 或 `gmv`。
复用已存在的合法 worktree 和严格只读校验不需要发布工具。准备脚本会拒绝覆盖已占用的非 Git 路径。Cargo 包不
包含 `.infra`，因此脚本和[固定提交列表](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/ci/local-path-dependencies.tsv)
链接到源码仓库。准备好兄弟目录后，请从源码仓库根目录运行以下命令，检查各 manifest 的锁定依赖：

```bash
cargo metadata --locked --offline --format-version 1 --manifest-path Cargo.toml
cargo metadata --locked --offline --format-version 1 --manifest-path tests/fixtures/dynamic_loader/Cargo.toml
cargo metadata --locked --offline --format-version 1 --manifest-path tests/fixtures/readme_consumer/Cargo.toml
```

```bash
cargo +1.94.0 check --locked --manifest-path tests/fixtures/dynamic_loader/Cargo.toml --bin magika-dynamic-loader-fixture
```

最后一条命令从仓库根目录重现 CI 的普通二进制编译检查，验证常规应用依赖图能够编译 `ort::init_from`；以上 metadata 命令仍用于检查锁定清单。

独立应用的固定源码清单示例见[用户手册中的固定源码应用示例](doc/user_guide.zh_CN.md#概念模型)。

受管请求会在访问 Provider 之前预留 runtime 容量、逻辑上的 `max_bytes` 采样预算，
并提交 scope 子操作；runtime 已关闭、容量已满或请求已取消时，不会调用 Provider。
如果 Provider future 尚处于 Pending 就被丢弃，只能确认本地轮询停止。无法观察 native
或远端操作何时完成时，容量会继续保留；结算观察仍可能到达终态 `Ready(report)`，但
`all_released()` 为 false。详见
[受管生命周期说明](doc/user_guide.zh_CN.md#迁移指南)中的边界、worker 错误和关闭超时。

Magika 返回 `Unknown` 或 `Undefined` 时没有 MIME 候选，结果为 `Ok(None)`，由选定的
`MimeDetectionPolicy` 决定文件名回退。关闭超时不会强制中断 native 推理或 Provider I/O。
`worker_stopped == false` 表示本次调用返回时尚未确认 join，`true` 表示返回前已确认 join；两者都与资源结算报告相互独立。关闭可能在 deadline 后再做一次 join 检查，因此该字段不能确定 worker 在 deadline 前何时退出。
`worker_panicked` 记录 join 时观察到的未捕获 panic。推理期间捕获的 panic 会作为失败操作报告，并将 runtime 标为 faulted。
未确认 join 时，runtime 会保留句柄，后续关闭调用可继续观察并等待 worker 退出。

Provider 选择和 detector 配置是两个独立输入：`ProviderSelection` 决定允许哪个已注册
Provider 创建服务，`MimeConfig` 则控制 Provider 解析完成后创建的 detector 实例。
未启用 inventory 时，应用需在启动时显式注册 Provider。启用 `qubit-magika` 的
`inventory` feature 后，已链接的应用会在 `MimeDetectorRegistry::builtin()` 和
`global()` 中发现由 `MagikaMimeDetectorProvider::new()` 提交的 `magika` Provider。
默认选择仍是 `repository`；需要 Magika 时应显式选择。若需配置媒体流分类器，
仍须通过 `with_media_stream_classifier` 显式构造并注册 Provider。
应用还需引用该 crate（`use qubit_magika as _;`），确保链接器保留 inventory 提交项。
Inventory 只提交未配置的 Provider。若要共享应用管理的 runtime，应显式注册
`MagikaMimeDetectorProvider::new().managed_runtime(...)`，并在 detector 使用期间保留
runtime；[Provider runtime 示例](doc/user_guide.zh_CN.md#共享应用管理的推理-runtime)说明了
注册方式及共享预算和关闭行为。
同步构造器新建 runtime 时，会等待 worker 初始化内嵌的 Magika 模型和 ONNX Runtime
会话。`build_async()` 异步等待会话就绪。两种方式都可复用注入的 runtime，并校验其字节预算。
应用应在启动阶段构造
detector 一次并共享（例如使用 `Arc`）；共享 runtime 上的推理调用会在内部串行执行。

同步借用内容、reader 和本地文件入口在调用线程采样；需要模型处理时提交 worker 并同步等待。
匹配的仓库规则可以直接返回。借用同步内容不会仅因逻辑长度超过
`MimeConfig::max_buffer_size()` 而拒绝。`MimeDetectorBackend` 的
`guess_from_content_async` hook 在首次轮询时检查 detector 的单请求 owned payload 上限，再预留
共享 runtime 的请求容量和聚合 owned payload 字节预算，然后复制已受理的输入，由 worker 采样和推理。
该 future 异步等待结果，且没有 parent operation scope。`detect_bytes_managed` 在追踪显式 scope
时执行相同的单请求和 runtime 聚合预算。Provider 的 `max_bytes` 限制累计逻辑采样读取；`read_all` 回退会限制可接受载荷，并可能额外探测 1 字节，
该读取中发现超限时返回文件系统错误。这些逻辑预算、采样长度和复制的 payload 字节都不保证是总进程内存上限；Provider 缓冲区和临时分配
可能增加内存。关闭 runtime 后，规则分类的同步输入仍可直接返回而不使用已关闭 worker；需要模型推理
的请求则会经由已关闭 runtime 返回错误。

### Media refinement 支持范围

已配置的媒体流分类器只会细化符合条件的高层输入：

| 检测入口 | 分类器行为 |
| --- | --- |
| 完整内容（`detect_by_content` / `detect`） | 启用 precise detection 且 MIME 映射匹配时，可以分类完整内容 |
| 可 seek 的 reader | 满足相同配置条件时，可以分类完整 reader |
| 本地文件 | 满足相同配置条件时，可以分类文件 |
| Provider path（`detect_path` / `detect_async_path`） | 不调用分类器，包括随机读取会话、旧范围读取和 `read_all` 回退 |
| 直接调用 `MimeContentBackend` | 返回 MIME 候选；高层选择和细化不属于 backend 返回结果 |

在 builder 或 Provider 上设置分类器不会启用 provider-path media refinement。高层输入还须
匹配已配置的 MIME 映射和 precise-detection 设置，才会进行细化。

Provider `max_bytes` 会在任何 Provider I/O 前与 detector 单请求上限比较；即使文件很小，参数过大也会
得到 `BufferLimitExceeded { requested, limit }`，且不会调用 `open`、`stat` 或读取。该值限制累计逻辑采样量和
`read_all` 可接受的 payload；异步请求还会按该值占用 runtime 聚合在途 payload 预留。这些限制不等于进程总内存、
Provider 缓冲区或网络流量上限。

Provider path 检测会把 `max_bytes` 作为 Magika 字节采样的累计请求预算。文件系统支持随机
读取会话时，Magika 只打开一次，并通过调用方缓冲区从同一已打开对象读取各个样本。打开时
观察到的长度限定可读偏移，但不是内容快照。若打开返回 `UnsupportedOperation` 或
`UnsupportedCapability`，则使用既有按路径路由；其他打开错误或会话读取错误都会直接返回，
不按路径重新打开。旧路由只有在资源长度已知时才按路径逐段读取，并在支持条件读取时为每一段附带相同的 ETag
条件；没有条件读取时无法保证多段读取期间内容一致。若不支持范围读取，或长度未知、因 `stat`
不受支持而无法取得长度，即使声明支持范围读取也会使用 `qubit-fs::read_all`。该回退限制可接受载荷为
`max_bytes`，并可能额外探测 1 字节。只有不能使用范围读取时，才会在完整读取前因已知长度
超过预算而返回 `MimeError::BufferLimitExceeded`；使用范围采样的已知长度资源不会仅因长度超预算而提前拒绝。
在 `read_all` 中发现超限时仍返回 `MimeError::FileSystem`。Magika 返回
`Unknown` 或 `Undefined` 时结果为
`Ok(None)`，随后由选定的 `MimeDetectionPolicy` 统一处理文件名回退。
Provider 原生异步 I/O 由调用方 future 等待；若 Provider 一直不完成，future 也可能一直
Pending。对于 session 和旧范围读取路由，worker 会在采样期间请求样本，并由 operation body 在执行器上
异步服务每次读取；`read_all` 则先读取完整内容，再提交拥有的缓冲区。旧 hooks 不提供 parent-scope
生命周期观察；需要 scope 跟踪时使用 managed API。


## 延伸阅读

- [Core random-access session guide](https://github.com/qubit-ltd/rs-fs/blob/main/doc/user_guide.md#sampling-the-same-opened-object-at-several-offsets)
- [核心随机读取会话说明](https://github.com/qubit-ltd/rs-fs/blob/main/doc/user_guide.zh_CN.md#从同一已打开对象的多个位置采样)
- [English user guide](doc/user_guide.md)
- [中文用户手册](doc/user_guide.zh_CN.md)
- [API 文档](https://docs.rs/qubit-magika)
- [English README](README.md)
- [固定的本地依赖提交](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/ci/local-path-dependencies.tsv)

## 测试

构建和测试本源码 checkout 还需要固定的同级 `../rs-fs-local` checkout，提供
`qubit-fs-local ^0.12`；路径见[固定依赖表](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/ci/local-path-dependencies.tsv)。

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目检查（仅源码仓库提供）
./.infra/bin/project-ci-check.sh

# 检查代码覆盖率
./.infra/bin/coverage.sh
```

发布的 Cargo 包不包含 `.infra` 脚本，请到[源码仓库](https://github.com/qubit-ltd/rs-magika)查看。

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交 Pull Request 前运行源码仓库中的
[`align-ci.sh`](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/bin/align-ci.sh) 格式化代码，
再运行 [`project-ci-check.sh`](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/bin/project-ci-check.sh)。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-magika](https://github.com/qubit-ltd/rs-magika)
