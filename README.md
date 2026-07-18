# Qubit Magika

[![Rust CI](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-magika/coverage-badge.json)](https://qubit-ltd.github.io/rs-magika/coverage/)
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
qubit-mime = "0.10"
qubit-magika = "0.9"
qubit-spi = "0.8"
```

## Quick Start

```rust
use std::error::Error;

use qubit_magika::MagikaMimeDetectorProvider;
use qubit_mime::{
    MimeConfig,
    MimeDetector,
    MimeDetectorRegistry,
};
use qubit_spi::{ProviderSelection, ServiceProvider};

// App startup owns process-wide provider registration and policy.
fn configure_app() -> Result<(), Box<dyn Error>> {
    let registry = MimeDetectorRegistry::global();
    registry.register(MagikaMimeDetectorProvider)?;
    registry.set_default_selection(ProviderSelection::named("magika")?);
    Ok(())
}

// Library X can keep provider selection and detector configuration independent.
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

// Library X can also use both process-default selection and default config.
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

## Provider Names

- `magika`
- `magika-mime-detector`
- `MagikaMimeDetector`

## Notes

`MagikaMimeDetector` delegates filename-only detection to
`qubit_mime::RepositoryMimeDetector`. Content, reader, and file detection use
Magika inference, then return Magika's MIME type mapping.

Provider selection and detector configuration are separate inputs. A
`ProviderSelection` decides which registered provider may create the service;
`MimeConfig` controls the detector instance after that provider is resolved.
`qubit-magika` does not register itself automatically: the App explicitly
controls process-wide registration and the default selection during startup.

## Testing

```bash
# Core API with the default empty feature set
cargo test --no-default-features

# Core API plus regex validation
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
