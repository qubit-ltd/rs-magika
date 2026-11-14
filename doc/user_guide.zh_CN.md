# qubit-magika 用户手册

[English user guide](user_guide.md)｜[README](../README.zh_CN.md)｜[API 文档](https://docs.rs/qubit-magika)

## 手册目标与读者

本手册面向使用 `qubit-mime`、需要通过 Magika 判断文件内容的 Rust 应用。内容对应
`qubit-magika` 0.18，最低 Rust 版本为 1.94。crate 提供
`MagikaMimeDetector` 及可选的 `MagikaMimeDetectorProvider`，但不会替代
`qubit-mime` 的 detector 选择机制和 MIME 策略。

## 概念模型

应用启动时需要分别完成两件事：

1. 把 `MagikaMimeDetectorProvider` 注册到全局 `MimeDetectorRegistry`，并选择
   `magika` provider。
2. 使用 `MimeConfig` 创建 detector；该配置负责 `qubit-mime` 周边的选择和结果
   优化行为。

仅按文件名检测时使用仓库中的文件名规则；按内容、reader 或本地文件检测时使用
Magika 推理。创建 detector 会初始化内嵌的 Magika 模型和 ONNX Runtime session。
同一个 detector 内部会串行化推理调用，因此可以用 `Arc` 在应用中共享它。

需要链接时自动发现时，可启用 `qubit-magika = { version = "0.18", features =
["inventory"] }`。这会将 `MagikaMimeDetectorProvider::new()` 提交给 MIME detector
inventory；`MimeDetectorRegistry::builtin()` 随后会包含 `magika`，但默认选择仍为
`repository`。配置媒体流分类器的 Provider 仍需显式构造和注册。
应用中还应加入 `use qubit_magika as _;`，使 inventory 提交项参与链接。

## 贯穿场景

假设应用接收一个上传的 Python 脚本：它需要根据内容得到 MIME 类型，同时保留对普通
路径的文件名检测能力。成功标准是应用启动时只初始化一个 detector，对 Python 内容
返回 `text/x-python`，对 `.pdf` 文件名返回 `application/pdf`。

## 安装与最小配置

在依赖中加入当前版本：

```toml
[dependencies]
qubit-mime = "0.21"
qubit-magika = "0.18"
qubit-spi = "0.14"
qubit-fs = "0.3" # 直接调用 detect_path 时需要
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

fn create_detector() -> Result<Arc<dyn MimeDetector>, Box<dyn Error>> {
    let registry = MimeDetectorRegistry::global();
    registry.register(MagikaMimeDetectorProvider::new())?;
    let selection = ProviderSelection::named("magika")?;
    registry
        .set_default_selection(selection.clone())
        .expect("default selection should be valid");
    let provider = registry.resolve_selected(&selection)?;
    Ok(provider.create_configured(&MimeConfig::default())?)
}
```

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

```rust
use std::sync::Arc;

use qubit_magika::{MagikaMimeDetector, ManagedInferenceRuntime};
use qubit_mime::FfprobeCommandMediaStreamClassifier;

fn create_media_aware_detector() -> qubit_mime::MimeResult<MagikaMimeDetector> {
    let classifier = Arc::new(FfprobeCommandMediaStreamClassifier::new());
    MagikaMimeDetector::builder()
        .media_stream_classifier(Some(classifier))
        .build()
}
```

如果应用使用 registry 构造 detector，也可以把同一个 classifier 传给
`MagikaMimeDetectorProvider::with_media_stream_classifier`。classifier 失败时返回
`MimeError`；只有 classifier 成功时才会进行 refinement。

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

```toml
[dependencies]
qubit-magika = { version = "0.18", default-features = false }
qubit-mime = "0.21"
qubit-spi = "0.14"
qubit-fs = "0.3"
ort = { version = ">=2.0.0-rc.12,<2.1", default-features = false, features = ["std", "ndarray", "load-dynamic", "api-24"] }
```

创建 detector 前先初始化 runtime。Windows 使用 `onnxruntime.dll`，macOS 使用
`libonnxruntime.dylib`，Linux 使用 `libonnxruntime.so`：

```rust,no_run
use std::error::Error;
use std::path::PathBuf;

use qubit_magika::MagikaMimeDetector;

fn create_detector() -> Result<MagikaMimeDetector, Box<dyn Error>> {
    let runtime = PathBuf::from(std::env::var("ORT_LIBRARY_PATH")?);
    if !ort::init_from(runtime)?.commit() {
        return Err("ONNX Runtime environment was already initialized".into());
    }
    Ok(MagikaMimeDetector::new()?)
}
```

将 `ORT_LIBRARY_PATH` 设置为完整的共享库路径，并确保 execution-provider 动态库能被
系统加载器找到。`ort::init_from` 必须早于第一个 detector（或其他 `ort` API）调用；
该环境是进程级全局状态，只能 commit 一次。

仓库 Linux CI 的[`dynamic-loader` job](../.github/workflows/ci.yml)使用 ONNX Runtime
1.24.2 验证这条动态加载路径：先初始化共享库，再创建 detector，并确认 Python 内容识别为
`text/x-python`。该 job 还检查 Cargo feature 图，确认未启用 bundled runtime 下载。
`ORT_LIBRARY_PATH` 必须指向共享库文件本身，而不是所在目录。

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
一个 worker 独占 Magika Session，并串行执行 worker 上的采样和推理。同步内容、reader 和本地
文件入口在调用线程采样；需要模型处理时提交 worker 并同步等待，仓库规则命中时可以直接返回。
异步 Provider route 通过容量为 1 的范围读取 bridge 与 worker 协作：调用方 future 异步读取
Provider。旧 `guess_from_content_async` hook 在首次轮询时检查 detector 上限，先预留 runtime
请求和字节容量，再复制输入，由 worker 采样和推理；future 异步等待结果，且没有 parent scope。
managed bytes 和 provider API 会把工作附加到显式 operation scope。每个 detector 执行自身的
请求上限；共用 runtime 的 detector 也共用聚合预算和关闭边界，任一 detector 调用 shutdown
都会关闭该共享 runtime。

async content hook 与同步 content hook 一样返回 backend 原始 Magika 候选，不执行高层 MIME
选择或媒体细化。输入受理后会计入共享 runtime 预算；即使仓库规则原本可以直接决定结果，也会
统一经过受理。因此 runtime 关闭、容量或预算不足、内容超限都可能产生异步错误，而不是直接
返回规则结果。

## 迁移指南

### 带 Operation scope 的异步 Provider 请求

如果应用需要把异步 Provider 采样纳入 `qubit-operation` 的取消和释放观察，可使用
`detect_async_provider_path_managed`。启动时共享一个 supervisor 和 detector，再把每个
请求的 scope 显式传入：

```rust,no_run
use std::sync::Arc;
use std::time::Duration;

use qubit_fs::{AsyncFileSystem, Path};
use qubit_magika::MagikaMimeDetector;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};

fn prepare_detector() -> qubit_mime::MimeResult<(OperationSupervisor, Arc<MagikaMimeDetector>)> {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let config = qubit_mime::MimeConfig::default();
    let runtime = Arc::new(ManagedInferenceRuntime::new(
        supervisor.clone(),
        config.max_buffer_size(),
    )?);
    let detector = Arc::new(
        MagikaMimeDetector::builder()
            .mime_config(config)
            .managed_runtime(runtime)
            .build()?,
    );
    Ok((supervisor, detector))
}

async fn detect_request(
    supervisor: &OperationSupervisor,
    detector: Arc<MagikaMimeDetector>,
    file_system: Arc<AsyncFileSystem>,
    path: Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let service = supervisor.service_group(4)?;
    let request_detector = Arc::clone(&detector);
    let mut request = service.start(ScopeOptions::default(), move |scope| async move {
        request_detector
            .detect_async_provider_path_managed(&scope, &file_system, &path, 1024 * 1024)
            .await
    })?;
    let _outcome = request.result_blocking()?;
    let _settlement = request.released_blocking()?;
    let _request_group_shutdown = service.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    let shutdown = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    assert!(shutdown.worker_stopped);
    let _supervisor_shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    Ok(())
}
```

真实应用应让 detector 和 supervisor 覆盖整个应用生命周期；停止接收新请求后再调用
shutdown。`max_bytes` 既是单次采样的累计读取预算，也会占用 runtime 共享的在途字节预算。
它只约束逻辑采样和由请求持有的 payload 记账，不代表进程总内存或 Provider/网络流量上限。
runtime 会先预留容量和字节预算，再提交可选的 scope 子操作，然后才调用 Provider 的
`open`、`stat`、`read_all`、`length` 或范围读取。runtime 已关闭、容量已满或 scope
已取消时，会在 Provider 调用前拒绝请求。`read_all` 回退会把已读缓冲区直接交给 worker，
不再复制一份完整数据。运行时构造时会创建 session-owning worker 并等待 Magika Session 初始化完成，因此
当前没有额外的 `prepare()` API；builder 的 `build()` 就是准备阶段。采样工作在 worker
上运行，async operation future 负责服务范围读取，不会阻塞执行器线程。

旧版 `MimeDetector` hooks 仍可供不管理 operation scope 的调用方使用。当前上游 trait 不
传递 parent scope，因此旧 hooks 无法把请求关联到父 scope。迁移时，需要按 scope 观察接纳和
资源状态的 async provider 调用应改为显式调用 managed API。

Provider future 的取消不等于底层 I/O 已停止。`rs-fs` 约定：丢弃仍处于 Pending 的
Provider future，只会停止本地轮询；Provider 启动的 native 或远端工作仍可能继续。Magika
没有观察这些工作何时完成的接口，也不会把 future `Drop` 当成 I/O 已结束的证据。因此，
如果请求在 Provider future 尚未完成时被丢弃，且无法观察底层完成时间，子操作的释放观察和
runtime/字节容量会保持 `Pending`，甚至可能一直如此。调用方可以尽快停止等待，但不能据此
断言 native 工作已中断。

如果丢弃请求时没有尚未完成的 Provider 范围读取，关闭采样 bridge 后即可结束 driver 所有权。
若最后一次读取已经返回 `Ready`，I/O 完成状态可确认；worker 也完成后就会释放容量。显式
managed 和 scope-less provider 入口都遵循此生命周期。

子操作的业务结果与资源释放是两件事。推理成功记录为 `Succeeded`；worker 返回错误时记录
`Failed`，调用方仍会收到原始错误；worker panic 则记录为 `Panicked`。即使调用方已经
取走结果、资源也已释放，终态结果仍可从子操作状态中读取。

`shutdown_blocking` 用同一个 deadline 等待服务接纳/结算和 worker 退出。超时返回时，
`worker_stopped == false` 表示 worker 尚未退出，运行时会保留其 handle；之后再次调用 shutdown
可以继续观察并回收 worker。shutdown 不会强行中断 native 推理或 Provider I/O。

从 0.14 以前的集成版本升级时，按以下顺序处理：

1. 将配套依赖升级到 `qubit-magika 0.18`、`qubit-mime 0.21`、`qubit-fs 0.3`、
   `qubit-spi 0.14`，并使用 Rust 1.94。
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
