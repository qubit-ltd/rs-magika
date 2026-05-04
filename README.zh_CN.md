# Qubit Magika

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-magika.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-magika)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-magika/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-magika?branch=main)
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
qubit-config = "0.12"
qubit-mime = "0.2"
qubit-magika = "0.1"
```

## 快速开始

```rust
use qubit_magika::register_default_mime_detector;
use qubit_mime::{
    BoxMimeDetector,
    CONFIG_MIME_DETECTOR_DEFAULT,
    MimeConfig,
    MimeError,
};

fn main() -> Result<(), MimeError> {
    register_default_mime_detector()?;

    let mut raw_config = qubit_config::Config::new();
    raw_config.set(CONFIG_MIME_DETECTOR_DEFAULT, "magika")?;
    let config = MimeConfig::from_config(&raw_config)?;

    let detector = BoxMimeDetector::from_config(&config)?;
    let mime_type = detector.detect_by_content(b"#!/usr/bin/env python3\nprint('hello')\n");

    assert_eq!(Some("text/x-python".to_owned()), mime_type);
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
