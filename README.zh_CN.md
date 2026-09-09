# Qubit Magika

[![Rust CI](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-magika/coverage-badge.json)](https://qubit-ltd.github.io/rs-magika/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-magika.svg?color=blue)](https://crates.io/crates/qubit-magika)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

面向 `qubit-mime` 的 Magika 后端 MIME detector 集成。

## 概述

Qubit Magika 提供 `MagikaMimeDetector`，它实现
`qubit_mime::MimeDetector`，底层使用 Google Magika 模型。这样可以把
Magika 和 ONNX Runtime 依赖隔离在 `qubit-magika` 中，而不是直接放进
`qubit-mime` 核心库。

本 crate 默认启用 `bundled-onnxruntime`，便于普通构建直接链接和运行。如果业务
程序自己提供 ONNX Runtime 链接方式，可以关闭默认 features。

## 安装

```toml
[dependencies]
qubit-mime = "0.14"
qubit-magika = "0.12"
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

// App 启动时注册 Provider，并且只创建一次昂贵的推理 Session。
fn create_detector() -> Result<Arc<dyn MimeDetector>, Box<dyn Error>> {
    let registry = MimeDetectorRegistry::global();
    registry.register(MagikaMimeDetectorProvider)?;
    let selection = ProviderSelection::named("magika")?;
    registry.set_default_selection(selection.clone());
    let provider = registry.resolve_selected(&selection)?;
    Ok(provider.create_configured(&MimeConfig::default())?)
}

// 下游库借用 Detector，不重复构建模型。
fn library_x(detector: &dyn MimeDetector) -> Option<String> {
    detector.detect_by_content(
        b"#!/usr/bin/env python3\nprint('hello')\n",
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    let detector = create_detector()?;
    assert_eq!(
        Some("text/x-python".to_owned()),
        library_x(detector.as_ref()),
    );
    Ok(())
}
```

## Provider 名称

- `magika`
- `magika-mime-detector`
- `MagikaMimeDetector`

## 说明

`MagikaMimeDetector` 的文件名检测委托给
`qubit_mime::RepositoryMimeDetector`。内容、reader 和文件检测使用 Magika 推理，
然后返回 Magika 映射出的 MIME type。

Provider 选择和 detector 配置是两个独立输入：`ProviderSelection` 决定允许哪个已注册
Provider 创建服务，`MimeConfig` 则控制 Provider 解析完成后创建的 detector 实例。
`qubit-magika` 不会自动注册自己；App 在启动时显式控制进程级注册和默认选择。
创建 detector 会初始化内嵌 Magika 模型和 ONNX Runtime Session。应用应只创建
一次并共享（例如使用 `Arc`）；同一 detector 上的推理调用会在内部串行执行。

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
