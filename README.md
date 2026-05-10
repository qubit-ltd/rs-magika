# Qubit Magika

[![Rust CI](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml)
[![Coverage Status](https://coveralls.io/repos/github/qubit-ltd/rs-magika/badge.svg?branch=main)](https://coveralls.io/github/qubit-ltd/rs-magika?branch=main)
[![Crates.io](https://img.shields.io/crates/v/qubit-magika.svg?color=blue)](https://crates.io/crates/qubit-magika)
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
qubit-mime = "0.3"
qubit-magika = "0.2"
```

## Quick Start

```rust
use qubit_magika::MagikaMimeDetectorProvider;
use qubit_mime::{
    CONFIG_MIME_DETECTOR_DEFAULT,
    MimeConfig,
    MimeDetectorRegistry,
    MimeError,
};

fn main() -> Result<(), MimeError> {
    MimeDetectorRegistry::register_default(MagikaMimeDetectorProvider)?;

    let mut raw_config = qubit_config::Config::new();
    raw_config.set(CONFIG_MIME_DETECTOR_DEFAULT, "magika")?;
    let config = MimeConfig::from_config(&raw_config)?;

    let detector = MimeDetectorRegistry::default_registry()?.create_default_box(&config)?;
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
