# qubit-magika 用户手册

[English user guide](user_guide.md)｜[README](../README.zh_CN.md)｜[API 文档](https://docs.rs/qubit-magika)

## 手册目标与读者

本手册面向使用 `qubit-mime`、需要通过 Magika 判断文件内容的 Rust 应用。内容对应
`qubit-magika` 0.14，最低 Rust 版本为 1.94。crate 提供
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

## 贯穿场景

假设应用接收一个上传的 Python 脚本：它需要根据内容得到 MIME 类型，同时保留对普通
路径的文件名检测能力。成功标准是应用启动时只初始化一个 detector，对 Python 内容
返回 `text/x-python`，对 `.pdf` 文件名返回 `application/pdf`。

## 安装与最小配置

在依赖中加入当前版本：

```toml
[dependencies]
qubit-mime = "0.17"
qubit-magika = "0.14"
qubit-spi = "0.12"
qubit-fs = "0.2" # 直接调用 detect_path 时需要
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
`detect_path`。文件系统和逻辑路径由应用自己持有；`max_bytes` 表示 Magika 所有读取
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

如果文件系统声明支持范围读取，Magika 会请求受预算限制的窗口，并在可用时使用条件
ETag 读取；不支持范围读取时，已知资源超过预算会在完整加载前被拒绝。异步版本是在
`AsyncFileSystem` 上调用 `detect_async_path`。

## Media Classifier 示例

Media refinement 是可选的。例如，内置的 ffprobe classifier 可以区分仅音频、仅视频和
音视频内容。需要先安装 `ffprobe`，并确保它位于 `PATH` 中：

```rust
use std::sync::Arc;

use qubit_magika::MagikaMimeDetector;
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

## 自定义 ONNX Runtime 配置

如果应用自行提供 ONNX Runtime，可以关闭 bundled binary，并启用 `ort` 的动态加载器。
下面的声明可以直接复制，并与 `qubit-magika 0.14` 版本保持一致：

```toml
[dependencies]
qubit-magika = { version = "0.14", default-features = false }
qubit-mime = "0.17"
qubit-spi = "0.12"
ort = { version = "=2.0.0-rc.12", default-features = false, features = ["std", "ndarray", "load-dynamic", "api-24"] }
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

## 进阶用法

provider 支持以下选择名称：`magika`、`magika-mime-detector` 和
`magikamimedetector`。推荐使用规范名称 `magika`。

`MagikaMimeDetector` 还实现了 `qubit-mime` 针对 seekable reader、本地文件以及同步或
异步 provider path 的后端操作。reader 检测结束后会恢复原来的位置；content backend
从当前 cursor 对应的剩余资源开始检测，而通用 detector API 仍保持整份资源语义。
Provider path 检测会累计限制 `max_bytes`：支持范围读取时只请求受预算限制的窗口，并在
可用时使用条件 ETag 读取；不支持范围读取时执行一次有界读取，已知资源超过预算则返回
错误。Magika 的 `Unknown` 和 `Undefined` 会转换为 `Ok(None)`，再由选定的
`MimeDetectionPolicy` 统一处理文件名回退。
异步路径会先等待特征提取，再持有共享 Session 锁同步执行推理。推理和等待锁都可能占用
异步执行器线程；需要隔离时，应由应用把检测放到阻塞工作线程或服务边界中。

## 迁移指南

从 0.14 以前的集成版本升级时，按以下顺序处理：

1. 将配套依赖升级到 `qubit-mime 0.17`、`qubit-spi 0.12`，并使用 Rust 1.94。
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
中的输入 I/O、内容不完整、后端推理失败或共享 session 锁中毒，也会以 `MimeError`
返回。

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

- crate 不会自动注册 provider。
- 创建 detector 会初始化模型和 runtime；应创建一个 detector 并共享，不要为每个
  输入重复创建。
- 同一个 detector 上的推理会在内部串行执行。
- 异步 provider path 不会自动把模型推理转移到专用阻塞执行器；应用需要结合自己的
  runtime 和 worker 配置评估线程占用。
- `qubit-magika` 不保证每个输入都有 MIME 结果；调用方必须处理 `Ok(None)` 和
  `MimeError`。
- 本 crate 暴露 Magika 映射后的 MIME 结果，不另行定义 MIME registry，也不替代
  `qubit-mime` 配置。

## 延伸阅读

- [README](../README.zh_CN.md)
- [English user guide](user_guide.md)
- [API 文档](https://docs.rs/qubit-magika)
- [示例](../examples/basic.rs)
