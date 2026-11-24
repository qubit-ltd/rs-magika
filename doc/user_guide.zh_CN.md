# qubit-magika 用户手册

[English user guide](user_guide.md)｜[README](../README.zh_CN.md)｜[API 文档](https://docs.rs/qubit-magika)

## 手册目标与读者

本手册面向使用 `qubit-mime`、需要通过 Magika 判断文件内容的 Rust 应用。内容对应
`qubit-magika` 0.18，最低 Rust 版本为 1.94。crate 提供
`MagikaMimeDetector` 及可选的 `MagikaMimeDetectorProvider`，但不会替代
`qubit-mime` 的 detector 选择机制和 MIME 策略。

## 概念模型

应用启动时需要分别完成两件事：

1. 保留一个应用级 `MimeDetectorRegistry`，注册
   `MagikaMimeDetectorProvider`，并选择 `magika` provider。
2. 使用 `MimeConfig` 创建 detector；该配置负责 `qubit-mime` 周边的选择和结果
   优化行为。

显式注册时，使用 `MimeDetectorRegistry::default()` 创建独立 registry，并将它保存在应用状态中。
启用 `inventory` 后，`MimeDetectorRegistry::builtin()` 也会发现链接进来的 `magika`；
`MimeDetectorRegistry::global()` 则是进程级 registry。两者的默认选择仍为 `repository`。
如果 inventory 已经注册了 Magika provider，不要再向同一个 registry 重复注册。

仅按文件名检测时使用仓库中的文件名规则；按内容、reader 或本地文件检测时使用
Magika 推理。创建 detector 会初始化内嵌的 Magika 模型和 ONNX Runtime session。
同一个 detector 内部会串行化推理调用，因此可以用 `Arc` 在应用中共享它。

需要链接时自动发现时，可启用 `qubit-magika = { version = "0.18", features =
["inventory"] }`。这会将 `MagikaMimeDetectorProvider::new()` 提交给 MIME detector
inventory；`MimeDetectorRegistry::builtin()` 随后会包含 `magika`，但默认选择仍为
`repository`。配置媒体流分类器的 Provider 仍需显式构造和注册。
应用中还应加入 `use qubit_magika as _;`，使 inventory 提交项参与链接。

MIME 异步 provider registry 与旧的 async hooks 是两种能力。inventory 提交的是同步工厂，
因此通过 `MimeDetectorRegistry::create_async_backend` 选择 `magika` 会得到
`MimeError::AsyncUnsupported`。旧的 `MimeDetector` async hooks 可以异步等待 worker 推理或
Provider 读取，但不接收 parent operation scope。应用若要把 Provider 采样挂到某个 scope，
需显式调用 managed API。

发布的 crate 不包含源码仓库里的依赖准备脚本。需要准备缺失的兄弟 checkout 时，可运行
[`prepare-local-path-dependencies.sh`](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/bin/prepare-local-path-dependencies.sh)
（不带参数）。准备脚本不会覆盖已有 Git checkout，即使其提交或工作区状态与锁定值不同。
只读核验请从仓库根运行
[`check-locked-local-path-dependencies.sh`](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/lib/check-locked-local-path-dependencies.sh)。
它会检查每个固定提交和工作区状态，报告缺失、脏状态或提交不符，但不会修复目录；这与
[CI 的固定依赖检查](https://github.com/qubit-ltd/rs-magika/blob/main/.github/workflows/ci.yml)一致。
请对照固定提交清单
[`local-path-dependencies.tsv`](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/ci/local-path-dependencies.tsv)
检查结果，并审慎处理漂移。
常规和动态 fixture bootstrap 模式共用同一个准备 helper。发布新建的缺失 checkout 时，目标路径必须通过独占重命名创建。
工具优先使用 GNU `mv -Tn`（某些系统上 GNU coreutils 命令名为 `gmv`）。在 Darwin 上找不到支持 `-T` 的 `mv` 或 `gmv` 时，
默认调用系统 `python3` 的 `renamex_np`；可用 `RS_INFRA_PYTHON` 指定解释器。其他平台发布缺失 checkout 仍需 GNU `mv -Tn` 或 `gmv`。
准备已存在的合法 worktree 和运行严格只读校验都不需要发布工具。

需要使用尚未发布的依赖版本时，可把应用 workspace 根放在固定 checkout
`rs-*` 的同级目录中，应用目录命名为 `app`。将以下清单保存为
`app/Cargo.toml`：

<!-- fixed-source-application-start -->
```toml
[workspace]

[package]
name = "my-app"
version = "0.1.0"
edition = "2024"

[dependencies]
qubit-magika = { version = "0.18", path = "../rs-magika" }
qubit-mime = { version = "0.22", path = "../rs-mime" }
qubit-fs = { version = "0.4", path = "../rs-fs", default-features = false, features = ["async"] }
qubit-operation = { version = "0.3.2", path = "../rs-operation" }
qubit-spi = "0.14"

[patch.crates-io]
qubit-operation = { path = "../rs-operation" }
```
<!-- fixed-source-application-end -->

成员依赖路径相对 package 目录解析；workspace patch 路径相对 workspace 根目录解析。
使用源码仓库的准备脚本准备固定 checkout，再运行
`check-locked-local-path-dependencies.sh` 核验，然后在独立应用中生成并检查自己的
lock 文件。本仓库源码 checker 使用相同的根清单结构。仅使用 registry 的消费者不写
path 或 patch，并使用自己的 lock 文件。

此根清单默认启用 `bundled-onnxruntime`。若要启用同步 Provider inventory，
将 Magika 项改为 `qubit-magika = { version = "0.18", path = "../rs-magika", features = ["inventory"] }`。
固定源码与 `--registry-only` 用于选择依赖来源。README checker 默认只用 default features 编译提取出的中英文程序；增加 `--run-example` 后，才会分别用 default 和 `inventory` 运行两份程序。
仓库锁定的[独立 `readme_consumer` fixture](../tests/fixtures/readme_consumer/Cargo.toml)用于隔离动态加载；它关闭了 Magika 默认 features，因此依赖图不同于本仓库根清单启用 bundled runtime 的默认图。动态加载 CI 先检查独立 [`dynamic_loader` fixture](../tests/fixtures/dynamic_loader/Cargo.toml)的 feature 图，以及 `readme_consumer` 在 default、`inventory` 下的两张 feature 图，再编译 `readme_consumer`，最后用这两种配置执行该 fixture。

## 贯穿场景

假设应用接收一个上传的 Python 脚本：它需要根据内容得到 MIME 类型，同时保留对普通
路径的文件名检测能力。成功标准是应用启动时只初始化一个 detector，对 Python 内容
返回 `text/x-python`，对 `.pdf` 文件名返回 `application/pdf`。

## 安装与最小配置

在依赖中加入当前版本：

截至 2026-11-24，crates.io 搜索返回 `qubit-magika` 0.16.0、`qubit-mime`
0.21.1、`qubit-fs` 0.3.3 和 `qubit-operation` 0.3.1；本手册需要的版本分别为
0.18、0.22、0.4 和 0.3.2。README 的 registry-only 检查仍无法解析
`qubit-magika = "^0.18"`。所需版本发布前，请按[概念模型](#概念模型)中的固定源码
checkout 步骤操作，并在源码仓库运行 `python3 .infra/check-managed-guide-examples.py`。

```toml
[dependencies]
qubit-mime = "0.22"
qubit-magika = "0.18"
qubit-spi = "0.14"
qubit-fs = "0.4" # 直接调用 detect_path 时需要
```

默认 feature `bundled-onnxruntime` 会下载并链接默认 Magika session 所需的 ONNX
Runtime binary。如果应用通过其他方式提供 ONNX Runtime，可以关闭 default features，
再在自己的依赖图中配置对应的链接方案。

## 核心工作流

在应用启动阶段注册 provider，并创建一个可共享的 detector：

```rust
use std::error::Error;
use std::sync::Arc;

use qubit_magika::MagikaMimeDetectorProvider;
use qubit_mime::{MimeConfig, MimeDetector, MimeDetectorRegistry};
use qubit_spi::ProviderSelection;

fn create_registry() -> Result<MimeDetectorRegistry, Box<dyn Error>> {
    let registry = MimeDetectorRegistry::default();
    registry.register(MagikaMimeDetectorProvider::new())?;
    Ok(registry)
}

fn create_detector(registry: &MimeDetectorRegistry) -> Result<Arc<dyn MimeDetector>, Box<dyn Error>> {
    let selection = ProviderSelection::named("magika")?;
    let provider = registry.resolve_selected(&selection)?;
    Ok(provider.create_configured(&MimeConfig::default())?)
}
```

应用启动时调用这两个函数，并将 registry 与共享 detector 一起保存在应用状态中。
后续组件可以从该 registry 解析 provider，无需依赖进程级全局状态。

使用这个 detector 分别检测内容和文件名：

```rust
use qubit_mime::MimeDetector;

fn classify(detector: &dyn MimeDetector) -> qubit_mime::MimeResult<()> {
    assert_eq!(
        Some("text/x-python".to_owned()),
        detector.detect_by_content(b"#!/usr/bin/env python3\nprint('hello')\n")?,
    );
    assert_eq!(
        Some("application/pdf".to_owned()),
        detector.detect_by_filename("document.pdf")?,
    );
    Ok(())
}
```

如果只需要默认配置，也可以直接调用 `MagikaMimeDetector::new()`。需要传入 MIME
配置或 media-stream classifier 时，使用 `MagikaMimeDetector::builder()` 或
`MagikaMimeDetector::from_mime_config`。

## Provider Path 示例

当数据位于 `qubit-fs` provider 后面，而不是本地 `std::fs::File` 时，使用
`detect_path`。文件系统和逻辑路径由应用自己持有；`max_bytes` 表示 Magika 范围读取
累计允许请求的最大字节数：

```rust
use qubit_fs::{FileSystem, Path};
use qubit_mime::{MimeDetectionPolicy, MimeDetector, MimeDetectorBackend};

fn detect_uploaded_path(
    detector: &dyn MimeDetector,
    file_system: &FileSystem,
    path: &Path,
) -> qubit_mime::MimeResult<Option<String>> {
    detector.detect_path(
        file_system,
        path,
        8 * 1024,
        MimeDetectionPolicy::VerifyContent,
    )
}
```

如果支持随机读取 session，Magika 会优先打开一次，并从同一个已打开对象的多个位置采样。
打开时报告的长度只限定可读取的偏移，并不代表内容快照。只有打开 session 时返回
`UnsupportedOperation` 或 `UnsupportedCapability` 才会回退到旧的按路径读取路由；其他打开错误
以及 session 读取错误都会直接传播，不会重新按路径打开。旧路由按路径逐段读取，并在支持时
对每一段使用相同的条件 ETag；没有条件读取时，无法保证多段读取期间内容一致。`max_bytes`
是所有采样读取共享的累计预算。不支持范围读取时，已知资源超过预算会在完整加载前被拒绝。
`qubit-fs::read_all` 为判断是否超限，可能额外探测第 `max_bytes + 1` 个字节；
已知长度超限时返回 `MimeError::BufferLimitExceeded`，在 `read_all` 中发现超限时
返回 `MimeError::FileSystem`。异步版本是在
`AsyncFileSystem` 上调用 `detect_async_path`。

## Media Classifier 示例

Media refinement 是可选的。例如，内置的 ffprobe classifier 可以区分仅音频、仅视频和
音视频内容。需要先安装 `ffprobe`，并确保它位于 `PATH` 中：

应用启动前设置 `QUBIT_MIME_ENABLE_PRECISE_DETECTION=true`（配置键为
`mime.enable.precise.detection`）以启用 precise detection；`MimeConfig::from_env()`
会读取该设置。默认的歧义映射覆盖 `webm`（`video/webm` 或 `audio/webm`）和
`ogg`（`video/ogg` 或 `audio/ogg`）。符合条件的输入在检测 MIME type 或文件名匹配这些映射时才会执行 refinement。

```rust
use std::sync::Arc;

use qubit_magika::MagikaMimeDetector;
use qubit_mime::{FfprobeCommandMediaStreamClassifier, MimeConfig};

fn create_media_aware_detector() -> qubit_mime::MimeResult<MagikaMimeDetector> {
    let config = MimeConfig::from_env()?;
    let classifier = Arc::new(FfprobeCommandMediaStreamClassifier::new());
    MagikaMimeDetector::builder()
        .mime_config(config)
        .media_stream_classifier(Some(classifier))
        .build()
}
```

如果应用使用 registry 构造 detector，应把同一个 classifier 传给
`MagikaMimeDetectorProvider::with_media_stream_classifier`，并将启用后的配置传给
`create_configured(&config)`。`MagikaMimeDetector::from_mime_config` 接收配置，但不会挂接
classifier；需要同时配置两者时请使用 builder。classifier 失败时返回 `MimeError`；只有 classifier
成功时才会进行 refinement。

### 共享应用管理的推理 runtime

共享 runtime 和后续带 Operation scope 的示例使用这些直接依赖。将示例复制到应用时，
请同时保留此依赖块：

<!-- managed-dependencies-start -->
```toml
[dependencies]
qubit-magika = "0.18"
qubit-mime = "0.22"
qubit-operation = "0.3.2"
qubit-fs = { version = "0.4", default-features = false, features = ["async"] }
```
<!-- managed-dependencies-end -->

若这些 registry 版本尚不可用，请先按[概念模型](#概念模型)中的固定源码 checkout 步骤准备依赖，再编译此代码块。

在应用启动时创建 runtime，并将已配置的 Provider 显式注册到独立 registry：

<!-- provider-runtime-example-start -->
```rust
use std::error::Error;
use std::sync::Arc;

use qubit_magika::{MagikaMimeDetectorProvider, ManagedInferenceRuntime};
use qubit_mime::{MimeConfig, MimeDetectorRegistry};
use qubit_operation::{ManagerOptions, OperationManager};

fn register_provider() -> Result<(MimeDetectorRegistry, Arc<ManagedInferenceRuntime>), Box<dyn Error>> {
    let manager = OperationManager::new(ManagerOptions::default())?;
    let runtime = Arc::new(ManagedInferenceRuntime::new(
        manager,
        MimeConfig::default().max_buffer_size(),
    )?);
    let registry = MimeDetectorRegistry::default();
    registry.register(
        MagikaMimeDetectorProvider::new().managed_runtime(Arc::clone(&runtime)),
    )?;
    Ok((registry, runtime))
}
```
<!-- provider-runtime-example-end -->

应用应在整个生命周期内保留返回的 registry 和 `Arc` runtime。随后从该 registry 调用
`resolve_selected` 选择 `magika`，再用 `create_configured(&MimeConfig)` 创建 detector；不要在注册后
丢弃 registry。detector 停止接收请求后再关闭 runtime。该 Provider 创建的 detector 共享同一 worker、
请求容量、字节预算和关闭域。`ManagedInferenceRuntime::new` 设置的
`max_in_flight_bytes` 是所有共享该 runtime 的 detector 共用的聚合上限；每个 detector 的
`MimeConfig::max_buffer_size` 则是单请求上限。聚合值应能容纳预期同时在途的 payload 总量，也应足以单独接纳
每个 detector 按配置上限发起的请求。Inventory 只提交不带 runtime 的 `MagikaMimeDetectorProvider::new()`；需要共享
应用 runtime 时，应在独立 registry 中显式注册配置后的 Provider。

## Media refinement 支持范围

已配置的媒体流分类器只用于符合条件的高层输入。必须启用 precise detection，且检测到的
MIME type 或文件名须匹配已配置的 ambiguous MIME mapping。

| 检测入口 | 分类器行为 |
| --- | --- |
| 完整内容（`detect_by_content` / `detect`） | 可以分类完整内容 |
| 可 seek 的 reader | 可以分类完整 reader，并恢复原游标 |
| 本地文件 | 可以分类文件 |
| Provider path（`detect_path` / `detect_async_path`） | 不调用分类器，包括 session、旧范围读取和 `read_all` 回退 |
| 直接调用 `MimeContentBackend` | 返回 MIME 候选；高层选择和细化彼此独立 |

在 `MagikaMimeDetectorBuilder` 或 `MagikaMimeDetectorProvider` 上配置分类器，不会使
provider-path 检测调用它。path backend 将候选 MIME type 返回给高层 detector 选择；当前
细化路由不会从 path backend 接收完整内容源。

## 自定义 ONNX Runtime 配置

如果应用自行提供 ONNX Runtime，可以关闭 bundled binary，并启用 `ort` 的动态加载器。
下面的声明可以直接复制，并与 `qubit-magika 0.18` 版本保持一致：

若 registry 前置版本尚未发布，请按[概念模型](#概念模型)中的固定源码 checkout 步骤操作；
受管依赖块和自定义 ONNX 依赖块共用该步骤。

<!-- dynamic-loader-application-start -->
```toml
[dependencies]
qubit-magika = { version = "0.18", path = "../rs-magika", default-features = false }
qubit-mime = { version = "0.22", path = "../rs-mime" }
qubit-operation = { version = "0.3.2", path = "../rs-operation" }
qubit-fs = { version = "0.4", path = "../rs-fs", default-features = false, features = ["async"] }
qubit-spi = "0.14"
ort = { version = ">=2.0.0-rc.12,<2.1", default-features = false, features = ["std", "ndarray", "load-dynamic", "api-24"] }

[patch.crates-io]
qubit-operation = { path = "../rs-operation" }
```
<!-- dynamic-loader-application-end -->

创建 detector 前先初始化 runtime。Windows 使用 `onnxruntime.dll`，macOS 使用
`libonnxruntime.dylib`，Linux 使用 `libonnxruntime.so`：

```rust,no_run
use std::error::Error;
use std::path::PathBuf;

use qubit_magika::MagikaMimeDetector;

fn create_detector() -> Result<MagikaMimeDetector, Box<dyn Error>> {
    let runtime = std::env::var_os("ORT_LIBRARY_PATH")
        .map(PathBuf::from)
        .ok_or("ORT_LIBRARY_PATH must name the ONNX Runtime shared library")?;
    if !ort::init_from(runtime)?.commit() {
        return Err("ONNX Runtime environment was already initialized".into());
    }
    Ok(MagikaMimeDetector::new()?)
}
```

将 `ORT_LIBRARY_PATH` 设置为完整的共享库路径，并确保 execution-provider 动态库能被
系统加载器找到。`ort::init_from` 必须早于第一个 detector（或其他 `ort` API）调用；
该环境是进程级全局状态，只能 commit 一次。

仓库的[`dynamic_loader` fixture](../tests/fixtures/dynamic_loader/Cargo.toml)
使用相同的 ONNX Runtime 版本和 feature 集合。Linux CI 的
[`dynamic-loader` job](https://github.com/qubit-ltd/rs-magika/blob/main/.github/workflows/ci.yml)
先检查该 fixture 的 feature 图，以及锁定的 `readme_consumer` 在 default 和 `inventory` 下的两张 feature 图，
再编译并运行 `readme_consumer` 的两种配置。运行时使用 ONNX Runtime 1.24.2：初始化共享库、创建
detector，并确认 Python 内容识别为 `text/x-python`。这些 feature 图检查会确认没有启用 bundled runtime
下载。`ORT_LIBRARY_PATH` 必须指向共享库文件本身，而不是所在目录。

## 进阶用法

provider 支持以下选择名称：`magika`、`magika-mime-detector` 和
`magikamimedetector`。推荐使用规范名称 `magika`。

`MagikaMimeDetector` 还实现了 `qubit-mime` 针对 seekable reader、本地文件以及同步或
异步 provider path 的后端操作。reader 检测结束后会恢复原来的位置；content backend
从当前 cursor 对应的剩余资源开始检测，而通用 detector API 仍保持整份资源语义。
Provider path 检测优先使用随机读取 session：只打开一次，并从同一个对象的多个位置采样。
打开时报告的长度只限定可读取偏移，不是内容快照。只有打开 session 返回
`UnsupportedOperation` 或 `UnsupportedCapability` 才回退到旧的按路径读取；其他打开错误和
session 读取错误直接传播，不会重新按路径打开。旧路由逐段按路径打开，并在支持时为每一段
附带相同的条件 ETag；没有该条件就不能保证多段期间内容一致。`max_bytes` 是所有采样读取的
累计预算。不支持范围读取时可能额外探测 1 字节以判断超限，并对超限资源返回错误。Magika 的
`Unknown` 和 `Undefined` 会转换为 `Ok(None)`，再由选定的 `MimeDetectionPolicy` 统一处理文件名回退。

本地文件媒体细化仅适用于普通文件。目录和符号链接会跳过细化；使用 `Freedesktop` 时，若存在文件名候选则可以选中，使用
`VerifyContent` 时则在没有内容的情况下返回 `None`。FIFO、Unix socket 等其他本地类型在内容检测的元数据检查阶段返回
`MimeError::Io`，其 `ErrorKind` 为 `Unsupported`。高层 `detect_file` 使用 `PreferFilename` 且文件名只有一个候选时，可能直接返回文件名结果，不检查路径或类型。
元数据检查与后续分类器打开是两个独立操作，因此无法防止路径在两者之间被替换。
一个 worker 独占 Magika Session，并串行执行 worker 上的采样和推理。异步 Provider path 采样时，worker
可能等待 Provider 读取；这会延迟共享同一 runtime 的其他请求，形成共同的等待和故障域。若应用需要隔离
延迟，可以为调用方使用独立 runtime，但每个 runtime 都会额外占用一个 Magika Session。同步借用内容、reader 和本地
文件入口在调用线程采样；需要模型处理时提交 worker 并同步等待，仓库规则命中时可以直接返回。
借用内容不会仅因逻辑长度超过 `MimeConfig::max_buffer_size()` 而拒绝。异步 Provider route
通过容量为 1 的范围读取 bridge 与 worker 协作：调用方 future 异步读取 Provider。旧
`guess_from_content_async` hook 在首次轮询时检查 detector 的单请求 owned payload 上限，先预留
runtime 请求容量和聚合 owned payload 字节容量，再复制输入，由 worker 采样和推理；future
异步等待结果，且没有 parent scope。`detect_bytes_managed` 在追踪显式 scope 时执行相同的单请求
和共享 runtime 预算。Provider `max_bytes` 是累计逻辑采样和回退读取预算。这些逻辑预算和复制的
payload 长度不构成总进程内存上限，Provider 缓冲区和临时分配也会占用内存。runtime 关闭后，规则
分类的同步输入仍可直接返回；需要模型推理的请求经由已关闭 runtime 失败。

async content hook 与同步 content hook 一样返回 backend 原始 Magika 候选，不执行高层 MIME
选择或媒体细化。输入受理后会按 owned payload 计入共享 runtime 聚合字节预算；即使仓库规则原本
可以直接决定结果，也会统一经过受理。因此 runtime 关闭、容量或预算不足、内容超限都可能产生异步
错误，而不是直接返回规则结果。该聚合 payload 记账和 Provider 采样记账都不是总进程内存上限。

## 迁移指南

### 带 Operation scope 的异步 Provider 请求

如果应用需要把异步 Provider 采样纳入 `qubit-operation` 的取消和释放观察，可使用
`detect_async_provider_path_managed`。启动时共享一个 supervisor 和 detector，再把每个
请求的 scope 显式传入：
启动时要向 `ManagerOptions` 注入应用的 `ClockSource`，并用同一个时钟生成观察截止时间。
默认的 `ManagerOptions` 不含时钟；若直接使用默认值，`wait_settlement_until` 会返回
`ClockUnavailable`。
调用方还要按该时钟文档规定的 tick 刻度，把预期等待时长换算为 `wait_ticks` 传入；tick 数不是秒数。

<!-- managed-guide-example-start -->
```rust,no_run
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use qubit_fs::{AsyncFileSystem, Path};
use qubit_magika::MagikaMimeDetector;
use qubit_magika::ManagedInferenceShutdownReport;
use qubit_operation::{GroupOptions, ManagerOptions, OperationManager, OperationOptions, OperationOutcome, ShutdownMode};
use qubit_operation::{AsyncOperationObserver, ClockSource, Deadline, MonotonicTime, SettlementWait, ShutdownHandle, ShutdownReport, ShutdownSnapshot, ShutdownWait};

fn prepare_application(clock: Arc<dyn ClockSource>) -> Result<(
    OperationManager,
    qubit_operation::OperationGroup,
    Arc<MagikaMimeDetector>,
    Arc<dyn ClockSource>,
), Box<dyn Error>> {
    let mut options = ManagerOptions::default();
    options.clock = Some(Arc::clone(&clock));
    let manager = OperationManager::new(options)?;
    let service = manager.group(GroupOptions { capacity: 4 })?;
    let detector = Arc::new(
        MagikaMimeDetector::builder()
            .operation_manager(manager.clone())
            .build()?,
    );
    Ok((manager, service, detector, clock))
}

async fn detect_request(
    service: &qubit_operation::OperationGroup,
    detector: &Arc<MagikaMimeDetector>,
    file_system: AsyncFileSystem,
    path: Path,
    clock: &dyn ClockSource,
    wait_ticks: u64,
) -> Result<(OperationOutcome<Option<String>, qubit_mime::MimeError>, AsyncOperationObserver, SettlementWait), Box<dyn Error>> {
    let request_detector = Arc::clone(detector);
    let mut request = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            request_detector
                .detect_async_provider_path_managed(&scope, &file_system, &path, 1024 * 1024)
                .await
        })?;
    let observer = request.observer();
    let outcome = request.wait_outcome().await?;
    match &outcome {
        OperationOutcome::Succeeded(mime) => println!("检测完成：{mime:?}"),
        OperationOutcome::Failed(error) => eprintln!("检测失败：{error}"),
        other => eprintln!("检测未正常完成：{other:?}"),
    }
    // 调用方已按此时钟的 tick 刻度换算好预期等待时长。
    let now = clock.now();
    let deadline = Deadline::at(MonotonicTime::new(now.domain(), now.ticks().saturating_add(wait_ticks)));
    let settlement = observer.wait_settlement_until(deadline).await?;
    match &settlement {
        SettlementWait::Ready(report) if report.all_released() => println!("请求资源已释放"),
        SettlementWait::Ready(report) => eprintln!("结算报告仍有未释放资源：{report:?}"),
        SettlementWait::Pending(snapshot) => eprintln!("截止时结算仍在进行：{snapshot:?}"),
    }
    Ok((outcome, observer, settlement))
}

enum ShutdownProgress {
    DetectorPending(ManagedInferenceShutdownReport),
    ManagerPending {
        detector: ManagedInferenceShutdownReport,
        handle: ShutdownHandle,
        snapshot: ShutdownSnapshot,
    },
    Complete { detector: ManagedInferenceShutdownReport, manager: ShutdownReport },
}

fn shutdown_application(
    manager: &OperationManager,
    detector: &MagikaMimeDetector,
) -> Result<ShutdownProgress, Box<dyn Error>> {
    let detector_report = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    if !detector_report.worker_stopped {
        return Ok(ShutdownProgress::DetectorPending(detector_report));
    }
    let handle = manager.request_shutdown(ShutdownMode::Drain);
    match handle.wait_for(Duration::from_secs(30))? {
        ShutdownWait::Completed(report) => Ok(ShutdownProgress::Complete { detector: detector_report, manager: report }),
        ShutdownWait::Pending(snapshot) => Ok(ShutdownProgress::ManagerPending { detector: detector_report, handle, snapshot }),
    }
}
```
<!-- managed-guide-example-end -->

`detect_request` 的精确返回类型是
`(OperationOutcome<Option<String>, MimeError>, AsyncOperationObserver, SettlementWait)`。
业务结果与资源清理分开处理：`Succeeded(Some(mime))` 表示得到分类，`Succeeded(None)` 表示无映射，
`Failed(error)` 保留 detector 错误，其他变体表示停止或执行失败。observer 使用的时钟就是
`ManagerOptions` 中注入的同一个 `ClockSource`；示例通过 `clock.now()` 生成同域 deadline。
应用应根据预期等待时长和该时钟的 tick 刻度换算后传入 `wait_ticks`，不能把 tick 当作秒。
`wait_settlement_until(deadline)` 返回
`Result<SettlementWait, ObservationError>`。`SettlementWait::Pending(snapshot)` 只表示
本次有界观察结束，不能证明资源已释放。截止时间必须来自 operation 使用的 clock domain。
示例显式匹配 Ready 和 Pending，并在 Ready 时检查 `all_released()`。`Ready(report)` 表示本次结算已终结，
但报告仍可能包含资源未释放等负面证据，也可能来自 scope 中其他资源；scope 中其他资源的负面报告不能推出 Magika 仍占用容量，应保留报告以便恢复。
`Pending(snapshot)` 只表示等待到达截止时间，调用方可继续观察。本次请求的 Provider I/O 完成状态未确认，
或 worker 仍在访问请求资源时，Magika 会继续保留自己的容量。停止请求仍会析构本地 future、reader 和 driver，但这不能证明 native 或远端 I/O
已经完成。内置清理无法接收已丢弃 Provider I/O 的新完成证据，因此重试不能释放这笔容量。`RecoveryHandle`
可查看状态、重试已登记的策略或转移责任；重新提交推理请求不会释放先前请求的资源。关闭函数返回
`DetectorPending` 时应先保留报告并重试 detector shutdown，不要继续关闭 manager；`ManagerPending`
保留 snapshot 和 handle，可再次 `wait_for`；`ShutdownProgress::Complete` 携带最终 manager report。`shutdown_application`
应放在同步应用关闭边界；仅支持 async 的宿主应在停止接收请求并
等待活动请求完成后，使用自身的阻塞执行机制调用它。

runtime 关闭后，可调用 `detector.runtime_snapshot()` 查看共享运行状态。
`ManagedInferenceSnapshot` 是非原子观测：字段可能来自略有差异的时刻，且统计
包含共享该 runtime 的所有 detector。`faulted` 表示推理 session 曾捕获 panic；
关闭报告的 `worker_panicked` 只表示 join 时观察到未捕获 panic。非零
`unconfirmed_provider_io` 表示某个 Pending Provider 操作被丢弃，完成状态无法确认。
快照不能证明资源已结算；需使用 operation observer 的有界 settlement 等待继续观察。

真实应用应让 service group、detector 和 supervisor 覆盖整个应用生命周期；停止接收新请求后再调用
关闭函数。`max_bytes` 既是单次采样的累计读取预算，也会占用 runtime 共享的在途字节预算。
它只约束逻辑采样和由请求持有的 payload 记账，不代表进程总内存或 Provider/网络流量上限。
runtime 会先预留容量和字节预算，再提交可选的 scope 子操作，然后才调用 Provider 的
`open`、`stat`、`read_all`、`length` 或范围读取。runtime 已关闭、容量已满或 scope
已取消时，会在 Provider 调用前拒绝请求。`read_all` 回退会把已读缓冲区交给 async dispatch 路径，
不会主动再克隆一份完整数据。该路径在将输入派发给 worker 前，把拥有数据的 `Vec` 转为 `Box<[u8]>`；
若需收缩未使用容量，`into_boxed_slice` 仍可能重新分配，因此不能据此承诺零分配。运行时构造时会创建 session-owning
worker 并等待 Magika Session 初始化完成，因此
当前没有额外的 `prepare()` API；builder 的 `build()` 就是准备阶段。采样工作在 worker
上运行，async operation future 负责服务范围读取，不会阻塞执行器线程。

旧版 `MimeDetector` hooks 仍可供不管理 operation scope 的调用方使用。当前上游 trait 不
传递 parent scope，因此旧 hooks 无法把请求关联到父 scope。旧 content hook 通常等待内部操作结算；
旧 Provider hook 通常等待业务结果。runtime 取消时，两者都可能提前返回停止错误。受管 API 会单独
提供结算观察：有界等待可能返回 `Pending`，终态 `Ready(report)` 中 `all_released()` 也可能为 false。
Provider I/O 或 worker 访问未确认结束时，容量继续保留。该行为由
[`test_unmanaged_content_waits_for_worker_settlement_after_business_result`](../src/managed_runtime/managed_inference_runtime/tests.rs)
和
[`test_legacy_async_hook_returns_cancel_result_while_provider_resources_remain_held`](../tests/provider_path_tests.rs)
覆盖。迁移时，需要按 scope 观察接纳和资源状态的 async provider 调用应改为显式调用 managed API。

如果丢弃请求时没有尚未完成的 Provider 范围读取，关闭采样 bridge 后即可结束 driver 所有权。
若最后一次读取已经返回 `Ready`，I/O 完成状态可确认；worker 也完成后就会释放容量。显式
managed 和 scope-less provider 入口都遵循此生命周期。

子操作的业务结果与资源释放是两件事。推理成功记录为 `Succeeded`；worker 返回错误时记录
`Failed`，调用方仍会收到原始错误。runtime 捕获推理 worker panic 后，也会将公开的 managed
子操作结果记为 `Failed(MimeError)`，并在 runtime snapshot 中设置 `faulted`。shutdown report
的 `worker_panicked` 则表示 join worker 时观察到未捕获 panic。即使调用方已经取走结果、资源也已
释放，终态结果仍可从子操作状态中读取。

worker panic 后，runtime 会关闭新请求准入，将排队任务以 backend 错误终结，并丢弃
session；Magika 与 ONNX Runtime 均未说明 unwind 后可安全恢复 session。该 runtime 不能再用于
模型请求；应用需要新建 runtime 并重新注册 Provider 才能恢复推理。此策略处理 worker 捕获到的
Rust panic，不涵盖 native 崩溃或 `panic = abort`。

`shutdown_blocking` 使用同一个 deadline 执行所选关闭模式并等待 worker 退出。`Drain` 会等待已接纳的服务操作结算。
`Cancel` 会等待本地 Provider 访问结束，随后关闭 worker sender 并尝试 join；排队操作仍可能留在资源组快照中，
直到其 executor 将其退休。超时时，`worker_stopped == false` 表示尚未确认 join，运行时会保留 worker handle；
`true` 表示 worker 已 join，但不代表每项操作资源都已释放，因此仍要单独读取资源报告。
`worker_panicked` 表示 join 时观察到未捕获 panic。shutdown 不会强行中断 native 推理或 Provider I/O。

从 0.14 以前的集成版本升级时，按以下顺序处理：

1. 将配套依赖升级到 `qubit-magika 0.18`、`qubit-mime 0.22`、`qubit-fs 0.4`、
   `qubit-spi 0.14` 和 `qubit-operation 0.3.2`，并使用 Rust 1.94。
2. 保持 provider 注册和 detector 配置分离：注册
   `MagikaMimeDetectorProvider`，选择 `magika`，然后调用
   `create_configured(&MimeConfig)`。
3. 将每次请求创建 `MagikaMimeDetector::new()` 改为启动时创建一次，并通过 `Arc`
   共享。
4. 如果使用 provider path，把 `max_bytes` 视为所有读取累计预算。支持范围读取的
   provider 可能执行多次有界读取；不支持范围读取的 provider 会拒绝已知的超大资源。
5. 如果应用自己管理 ONNX Runtime，关闭 default feature，并在创建 detector 前使用上面
   的自定义配置。

provider 名称 `magika`、`magika-mime-detector` 和 `magikamimedetector` 仍然兼容；新配置
推荐使用 `magika`。

## 错误与诊断

如果 Magika 或 ONNX Runtime 无法初始化，创建 detector 会返回 `MimeError`。检测过程
中的输入 I/O、内容不完整、detector 或 runtime 字节上限、受理拒绝及后端推理失败，也会以
`MimeError` 返回。async content hook 即使处理可由规则判定的内容也会使用共享 worker，因此可能
返回受理错误。

`Ok(None)` 表示 detector 没有找到 MIME 候选，不等同于初始化失败或 I/O 错误。应用应
在启动阶段处理初始化错误，在读取输入的边界处理每次检测的错误。

## 排障

- 如果第一次检测前创建 detector 就失败，检查 ONNX Runtime feature 选择以及
  `MimeError` 携带的 runtime/model 错误。
- 如果文件名检测和内容检测结果不同，请注意前者使用 `qubit-mime` 仓库规则，后者
  使用 Magika 推理；应根据输入选择合适的 `qubit-mime` 策略。
- 如果 provider 注册或选择失败，应先注册 provider，再解析 `ProviderSelection`，并
  使用规范名称 `magika`。
- 如果 reader 无法检测，请确认它支持 seek，并且可以恢复原始位置。

## 限制与最佳实践

- Provider 自动发现需要启用 `inventory` feature；默认构建仍使用显式注册。
  Inventory 仅提交未配置分类器的 Provider。
- 创建 detector 会初始化模型和 runtime；应创建一个 detector 并共享，不要为每个
  输入重复创建。
- 同一个 detector 上的推理会在内部串行执行。
- 旧 `MimeDetector` provider hooks 不提供 parent-scope 生命周期观察；managed provider API
  可以把接纳、子操作结果和资源释放状态关联到请求 scope，但仍受上文 Provider 完成观察限制。
- `qubit-magika` 不保证每个输入都有 MIME 结果；调用方必须处理 `Ok(None)` 和
  `MimeError`。
- 本 crate 暴露 Magika 映射后的 MIME 结果，不另行定义 MIME registry，也不替代
  `qubit-mime` 配置。

## 延伸阅读

- [README](../README.zh_CN.md)
- [English user guide](user_guide.md)
- [API 文档](https://docs.rs/qubit-magika)
- [示例](../examples/basic.rs)
