# Qubit Magika

[![CircleCI](https://circleci.com/gh/qubit-ltd/rs-magika.svg?style=shield)](https://circleci.com/gh/qubit-ltd/rs-magika)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Magika-backed MIME detector integration for `qubit-mime`.

## Overview

Qubit Magika provides `MagikaMimeDetector`, a `qubit_mime::MimeDetector`
implementation backed by Google's Magika model. It keeps Magika and ONNX
Runtime dependencies outside `qubit-mime`, while still allowing applications to
register Magika as a detector provider.

The crate enables `bundled-onnxruntime` by default so ordinary builds can link
and run without a separately installed ONNX Runtime. Disable default features if
your application provides ONNX Runtime through another linking strategy.

## Installation

```toml
[dependencies]
qubit-config = "0.12"
qubit-mime = "0.2"
qubit-magika = "0.1"
```

## Quick Start

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

## Provider Names

- `magika`
- `magika-mime-detector`
- `MagikaMimeDetector`

## Notes

`MagikaMimeDetector` delegates filename-only detection to
`qubit_mime::RepositoryMimeDetector`. Content, reader, and file detection use
Magika inference, then return Magika's MIME type mapping.
