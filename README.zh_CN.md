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
qubit-mime = "0.10"
qubit-magika = "0.9"
qubit-spi = "0.8"
```

## 快速开始

```rust
use std::error::Error;

use qubit_magika::MagikaMimeDetectorProvider;
use qubit_mime::{
    MimeConfig,
    MimeDetector,
    MimeDetectorRegistry,
};
use qubit_spi::{ProviderSelection, ServiceProvider};

// App 启动代码负责全局 Provider 注册和默认选择策略。
fn configure_app() -> Result<(), Box<dyn Error>> {
    let registry = MimeDetectorRegistry::global();
    registry.register(MagikaMimeDetectorProvider)?;
    registry.set_default_selection(ProviderSelection::named("magika")?);
    Ok(())
}

// 库 X 可以分别接收 Provider 选择和 detector 配置。
fn library_x_with_explicit_requirements(
    selection: &ProviderSelection,
    config: &MimeConfig,
) -> Result<Option<String>, Box<dyn Error>> {
    let provider = MimeDetectorRegistry::global().resolve_selected(selection)?;
    let detector = provider.create_configured(config)?;
    Ok(detector.detect_by_content(
        b"#!/usr/bin/env python3\nprint('hello')\n",
    ))
}

// 库 X 也可以同时使用进程默认选择和默认配置。
fn library_x_with_defaults() -> Result<Option<String>, Box<dyn Error>> {
    let provider = MimeDetectorRegistry::global().resolve()?;
    let detector = provider.create()?;
    Ok(detector.detect_by_filename("document.pdf"))
}

fn main() -> Result<(), Box<dyn Error>> {
    configure_app()?;

    let selection = ProviderSelection::named("magika")?;
    let config = MimeConfig::default();
    assert_eq!(
        Some("text/x-python".to_owned()),
        library_x_with_explicit_requirements(&selection, &config)?,
    );
    assert_eq!(
        Some("application/pdf".to_owned()),
        library_x_with_defaults()?,
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

## 测试

```bash
# 使用默认的空 feature 集测试核心 API
cargo test --no-default-features

# 测试核心 API 和正则校验
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
