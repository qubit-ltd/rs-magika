# Qubit Magika

[![Rust CI](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-magika/coverage-badge.json)](https://qubit-ltd.github.io/rs-magika/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-magika.svg?color=blue)](https://crates.io/crates/qubit-magika)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

`qubit-magika` 为 `qubit-mime` 提供基于 Magika 的
`qubit_mime::MimeDetector`。它适合需要结合文件内容识别 MIME 类型，并希望在应用
启动时创建一个 detector、在多次检测之间复用它的应用或库。

## 概述

本 crate 默认启用 `bundled-onnxruntime`，普通构建无需单独安装 ONNX Runtime 即可
链接和运行。如果应用通过其他方式提供 ONNX Runtime，可以关闭默认 feature。

## 安装

```toml
[dependencies]
qubit-mime = "0.17"
qubit-magika = "0.14"
qubit-spi = "0.12"
```

## 快速开始

```rust
use std::error::Error;
use std::sync::Arc;

use qubit_magika::MagikaMimeDetectorProvider;
use qubit_mime::{
    MimeConfig,
    MimeDetector,
    MimeDetectorRegistry,
};
use qubit_spi::ProviderSelection;

// 应用启动时注册 Provider，并且只创建一次开销较高的推理 Session。
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

// 下游库复用 Detector，不重复初始化模型。
fn library_x(detector: &dyn MimeDetector) -> Result<Option<String>, qubit_mime::MimeError> {
    detector.detect_by_content(
        b"#!/usr/bin/env python3\nprint('hello')\n",
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    let detector = create_detector()?;
    assert_eq!(
        Some("text/x-python".to_owned()),
        library_x(detector.as_ref())?,
    );
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

Provider 选择和 detector 配置是两个独立输入：`ProviderSelection` 决定允许哪个已注册
Provider 创建服务，`MimeConfig` 则控制 Provider 解析完成后创建的 detector 实例。
`qubit-magika` 不会自动注册自己；应用需要在启动时显式完成进程级注册并设置默认选择。
创建 detector 会初始化内嵌 Magika 模型和 ONNX Runtime Session。应用应只创建一次并
共享（例如使用 `Arc`）；同一 detector 上的推理调用会在内部串行执行。

## 延伸阅读

- [English user guide](doc/user_guide.md)
- [中文用户手册](doc/user_guide.zh_CN.md)
- [API 文档](https://docs.rs/qubit-magika)
- [English README](README.md)

## 测试

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./ci-check.sh

# 检查代码覆盖率
./coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交
Pull Request 前运行 `./align-ci.sh`格式化代码，运行`./ci-check.sh`对齐CI要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-magika](https://github.com/qubit-ltd/rs-magika)
