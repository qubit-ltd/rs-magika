# Qubit Magika

[![Rust CI](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-magika/coverage-badge.json)](https://qubit-ltd.github.io/rs-magika/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-magika.svg?color=blue)](https://crates.io/crates/qubit-magika)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-magika` adds a Magika-backed `qubit_mime::MimeDetector` for applications
that need content-aware MIME detection without putting Magika and ONNX Runtime
dependencies into `qubit-mime` itself. It is intended for applications and
libraries that can initialize one detector during startup and share it across
their detection calls.

## Overview

The crate enables `bundled-onnxruntime` by default so ordinary builds can link
and run without a separately installed ONNX Runtime. Disable default features if
your application provides a compatible ONNX Runtime shared library. With `ort`'s
dynamic loader, initialize that library with `ort::init_from` before creating a
detector. The Linux CI runs a real Python-content inference with this setup;
see the [user guide](doc/user_guide.md) for the dependency and startup example.

## Installation

```toml
[dependencies]
qubit-mime = "0.17"
qubit-magika = "0.14"
qubit-spi = "0.12"
```

## Quick Start

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

// App startup registers the provider and creates the expensive session once.
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

// Downstream libraries borrow the detector instead of rebuilding the model.
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

## Provider Names

- `magika`
- `magika-mime-detector`
- `magikamimedetector`

## Notes

`MagikaMimeDetector` delegates filename-only detection to
`qubit_mime::RepositoryMimeDetector`. Content, reader, and file detection use
Magika inference, then return Magika's MIME type mapping.

Provider selection and detector configuration are separate inputs. A
`ProviderSelection` decides which registered provider may create the service;
`MimeConfig` controls the detector instance after that provider is resolved.
`qubit-magika` does not register itself automatically: the App explicitly
controls process-wide registration and the default selection during startup.
Creating a detector initializes the embedded Magika model and ONNX Runtime
session. Create it once, share it (for example with `Arc`), and expect inference
calls on a shared detector to be serialized internally.

Provider-path detection uses `max_bytes` as the cumulative request budget for
Magika's range reads. Range-capable filesystems use bounded windows and an ETag
snapshot when conditional reads are available. Without range support,
`qubit-fs::read_all` may probe one byte beyond `max_bytes` to detect overflow;
known oversized resources return `MimeError::BufferLimitExceeded`, while an
overflow discovered during `read_all` returns `MimeError::FileSystem`. Unknown and
undefined Magika types return `Ok(None)`, allowing the selected
`MimeDetectionPolicy` to apply filename fallback consistently.
The asynchronous path awaits filesystem feature extraction first, then performs
Magika inference synchronously while holding the shared session lock. The
inference and lock wait can occupy the async executor thread; applications that
need isolation should move detection to a blocking worker or service boundary.

## Learn More

- [English user guide](doc/user_guide.md)
- [中文用户手册](doc/user_guide.zh_CN.md)
- [API documentation](https://docs.rs/qubit-magika)
- [中文 README](README.zh_CN.md)

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./ci-check.sh

# Check code coverage
./coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./align-ci.sh` to format code and
`./ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-magika](https://github.com/qubit-ltd/rs-magika)
