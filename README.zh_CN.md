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

此示例所需的 crates.io 版本尚未全部发布（`qubit-magika` 0.18、`qubit-mime` 0.22、`qubit-fs` 0.4，以及 `qubit-operation` 0.3.2 或更高版本）。2026-11-24 的 registry 搜索分别返回 0.16.0、0.21.1、0.3.3 和 0.3.1；registry-only README 检查仍无法解析 `qubit-magika = "^0.18"`。发布完成前，请使用[依赖表](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/ci/local-path-dependencies.tsv)中的固定同级源码 checkout，并在本源码仓库运行 `python3 .infra/check-readme-examples.py`。`python3 .infra/check-readme-examples.py --registry-only` 是干净 registry 验收门禁；所需版本可解析后即可通过。给该命令增加 `--run-example` 会通过 registry 版本运行当前的 `examples/basic.rs`；此 opt-in 会下载并运行 bundled ONNX Runtime，要求当前原生环境受支持。

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
检测 future 返回的是业务结果；scope observer 会单独报告资源结算。`Pending(snapshot)` 只表示本次有界等待结束，
`Ready(report)` 也可能包含未释放资源的负面证据。Provider I/O 完成情况未知时，容量会继续保留；内置清理策略
不会在没有新完成证据时通过重试释放容量。shutdown report 中的 `worker_stopped` 只记录 worker join，与这两项观察相互独立。

Inventory 只提交同步 `MimeDetectorSpec` factory，不会向 `qubit-mime` 独立的 async provider
registry 添加 Provider，也不会附加应用 scope。通过支持 scope 的 async `MimeRuntime` 路由
选择这个仅支持同步的 `magika` Provider 时，会返回 `AsyncUnsupported`，不会回退到同步推理。
若需 scope 跟踪，可直接调用 Magika 的 managed API。旧版 content hook 在正常完成时等待内部操作结算；
runtime `Cancel` 可能先返回停止错误，worker 仍会保留输入和许可。旧版 provider-path hook 只观察子操作业务结果，
`Cancel` 可在 Provider I/O 尚未确认结束时返回停止结果。两个旧 hook 都没有 parent-scope 或 settlement observer。
区别见[受管生命周期说明](doc/user_guide.zh_CN.md#迁移指南)。

启用 inventory 时，`MimeDetectorRegistry::builtin()` 会发现未配置的同步 Provider；默认选择仍是
`repository`，因此需显式选择 `magika`。独立的 `MimeDetectorRegistry::default()` 初始为空，适合
像上例那样为应用单独注册 Provider。重复注册同名 Provider 会报错。依赖 inventory 时需通过
`use qubit_magika as _;` 引用 crate，避免链接器移除提交项。

在源码仓库中准备缺少的本地依赖 checkout 时，请使用
[准备脚本](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/bin/prepare-local-path-dependencies.sh)。
`--check-locked` 只检查现有兄弟仓库是否处于固定提交且工作区干净；发现缺失、提交不符或工作区有改动时会报错，
但不会修复。准备操作不会改动现有 Git checkout，因此需先手动将漂移的 checkout 恢复到固定提交再检查。
常规和动态 fixture bootstrap 模式共用同一个准备 helper。发布新建的缺失 checkout 需要独占目标路径的重命名：
工具优先使用 GNU `mv -Tn`（某些系统上命令名为 `gmv`）；在 Darwin 上找不到支持 `-T` 的 `mv` 或 `gmv` 时，
默认调用系统 `python3` 的 `renamex_np`。可通过 `RS_INFRA_PYTHON` 指定 Python 解释器；其他平台仍需 GNU `mv -Tn` 或 `gmv`。
复用已存在的合法 worktree 和严格的 `--check-locked` 校验不需要发布工具。它会拒绝覆盖已占用的非 Git 路径。Cargo 包不
包含 `.infra`，因此脚本和[固定提交列表](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/ci/local-path-dependencies.tsv)
链接到源码仓库。准备好兄弟目录后，可用以下命令检查各 manifest 的锁定依赖：

```bash
cargo metadata --locked --offline --format-version 1 --manifest-path Cargo.toml
cargo metadata --locked --offline --format-version 1 --manifest-path tests/fixtures/dynamic_loader/Cargo.toml
cargo metadata --locked --offline --format-version 1 --manifest-path tests/fixtures/readme_consumer/Cargo.toml
```

受管请求会在访问 Provider 之前预留 runtime 容量、逻辑上的 `max_bytes` 采样预算，
并提交 scope 子操作；runtime 已关闭、容量已满或请求已取消时，不会调用 Provider。
如果 Provider future 尚处于 Pending 就被丢弃，只能确认本地轮询停止。无法观察 native
或远端操作何时完成时，子操作释放和容量会继续保持 Pending，甚至可能一直如此。详见
[受管生命周期说明](doc/user_guide.zh_CN.md#迁移指南)中的边界、worker 错误和关闭超时。

Magika 返回 `Unknown` 或 `Undefined` 时没有 MIME 候选，结果为 `Ok(None)`，由选定的
`MimeDetectionPolicy` 决定文件名回退。关闭超时不会强制中断 native 推理或 Provider I/O；请检查报告中的
`worker_stopped` 和 `worker_panicked`。worker 尚未停止时，runtime 会保留句柄，后续关闭调用可继续观察并等待它退出。

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
构造 detector 时会同步等待 worker 初始化内嵌 Magika 模型和 ONNX Runtime Session。
应用应在启动阶段构造一次并共享（例如使用 `Arc`）；共享 runtime 上的推理调用会在内部串行执行。

同步借用内容、reader 和本地文件入口在调用线程采样；需要模型处理时提交 worker 并同步等待。
匹配的仓库规则可以直接返回。借用同步内容不会仅因逻辑长度超过
`MimeConfig::max_buffer_size()` 而拒绝。`MimeDetectorBackend` 的
`guess_from_content_async` hook 在首次轮询时检查 detector 的单请求 owned payload 上限，再预留
共享 runtime 的请求容量和聚合 owned payload 字节预算，然后复制已受理的输入，由 worker 采样和推理。
该 future 异步等待结果，且没有 parent operation scope。`detect_bytes_managed` 在追踪显式 scope
时执行相同的单请求和 runtime 聚合预算。Provider 的 `max_bytes` 限制累计逻辑采样和回退读取字节。
这些逻辑预算、采样长度和复制的 payload 字节都不保证是总进程内存上限；Provider 缓冲区和临时分配
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

Provider path 检测会把 `max_bytes` 作为 Magika 字节采样的累计请求预算。文件系统支持随机
读取会话时，Magika 只打开一次，并通过调用方缓冲区从同一已打开对象读取各个样本。打开时
观察到的长度限定可读偏移，但不是内容快照。若打开返回 `UnsupportedOperation` 或
`UnsupportedCapability`，则使用既有按路径路由；其他打开错误或会话读取错误都会直接返回，
不按路径重新打开。旧路由逐段按路径打开，并在支持条件读取时为每一段附带相同的 ETag
条件；没有条件读取时无法保证
多段读取期间内容一致。不支持范围读取时，`qubit-fs::read_all` 为判断是否超限，可能额外探测第
`max_bytes + 1` 个字节；已知长度超限时返回 `MimeError::BufferLimitExceeded`，
在 `read_all` 中发现超限时返回 `MimeError::FileSystem`。Magika 返回
`Unknown` 或 `Undefined` 时结果为
`Ok(None)`，随后由选定的 `MimeDetectionPolicy` 统一处理文件名回退。
Provider 原生异步 I/O 由调用方 future 等待；若 Provider 一直不完成，future 也可能一直
Pending。采样就绪后，Magika 采样与推理由 session-owning worker 执行。旧 hooks 不提供
parent-scope 生命周期观察；需要 scope 跟踪时使用 managed API。

## 延伸阅读

- [Core random-access session guide](https://github.com/qubit-ltd/rs-fs/blob/main/doc/user_guide.md#sampling-the-same-opened-object-at-several-offsets)
- [核心随机读取会话说明](https://github.com/qubit-ltd/rs-fs/blob/main/doc/user_guide.zh_CN.md#从同一已打开对象的多个位置采样)
- [English user guide](doc/user_guide.md)
- [中文用户手册](doc/user_guide.zh_CN.md)
- [API 文档](https://docs.rs/qubit-magika)
- [English README](README.md)
- [固定的本地依赖提交](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/ci/local-path-dependencies.tsv)

## 测试

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
