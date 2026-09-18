# qubit-magika User Guide

[中文用户手册](user_guide.zh_CN.md) | [README](../README.md) | [API documentation](https://docs.rs/qubit-magika)

## Purpose and Audience

This guide is for Rust applications that use `qubit-mime` and need Magika to
classify file content. It covers `qubit-magika` 0.14, which requires Rust 1.94.
The crate supplies the `MagikaMimeDetector` implementation and an optional
`MagikaMimeDetectorProvider`; it does not replace `qubit-mime`'s detector
selection or MIME policy.

## Conceptual Model

There are two separate startup decisions:

1. Register `MagikaMimeDetectorProvider` with the global
   `MimeDetectorRegistry` and select the provider name `magika`.
2. Create a detector with `MimeConfig`, which controls the surrounding
   `qubit-mime` selection and refinement behavior.

The detector uses repository filename rules for filename-only detection and
Magika inference for content, reader, and local-file detection. Creating a
detector initializes Magika's embedded model and the ONNX Runtime session.
Inference calls made through one detector are serialized internally, so one
initialized detector can be shared with `Arc`.

## Scenario

An application accepts an uploaded Python script and needs a content-derived
MIME type while also retaining filename detection for ordinary paths. The
success criteria are one detector initialized at startup, `text/x-python`
returned for Python content, and `application/pdf` returned for a `.pdf`
filename.

## Installation and Minimal Configuration

Add the crate versions used by this release:

```toml
[dependencies]
qubit-mime = "0.17"
qubit-magika = "0.14"
qubit-spi = "0.12"
```

The default feature, `bundled-onnxruntime`, downloads and links the ONNX Runtime
binary required by the default Magika session. If the application supplies a
different ONNX Runtime linking strategy, it can disable default features and
configure that strategy in its dependency graph.

## Core Workflow

Register the provider and create one shared detector during application
startup:

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

Use the resulting detector for content and filename checks:

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

For direct construction, `MagikaMimeDetector::new()` uses
`MimeConfig::default()`. `MagikaMimeDetector::builder()` and
`MagikaMimeDetector::from_mime_config` are available when the application needs
to supply MIME configuration or a media-stream classifier.

## Advanced Usage

The provider accepts these selection names: `magika`,
`magika-mime-detector`, and `magikamimedetector`. The canonical name is
`magika`.

`MagikaMimeDetector` also implements the `qubit-mime` backend operations for
seekable readers, local files, and synchronous or asynchronous provider paths.
Reader detection restores the original reader position. Provider-path
detection requires complete content and observes the configured content budget.

## Errors and Diagnostics

Detector construction returns `MimeError` when Magika or ONNX Runtime cannot
initialize. Detection returns `MimeError` for input I/O failures, incomplete
content, backend inference failures, or a poisoned shared session lock.

`Ok(None)` means that the selected detector found no MIME candidate; it is not
the same as an initialization or I/O error. Handle initialization errors during
startup and handle per-input errors at the boundary where the input is read.

## Troubleshooting

- If construction fails before the first detection, check the ONNX Runtime
  feature choice and the runtime/model error carried by `MimeError`.
- If filename detection and content detection disagree, remember that filename
  detection uses `qubit-mime` repository rules while content detection uses
  Magika inference. Choose the `qubit-mime` policy appropriate for the input.
- If provider registration or selection fails, register the provider before
  resolving `ProviderSelection`, and use the canonical name `magika`.
- If a reader cannot be classified, verify that it supports seeking and that
  its original position can be restored.

## Limitations and Best Practices

- The crate does not register its provider automatically.
- Detector construction initializes the model and runtime; create one detector
  and share it instead of constructing one per input.
- Inference on one detector is serialized internally.
- `qubit-magika` does not promise a MIME result for every input; callers must
  handle `Ok(None)` and `MimeError`.
- This crate exposes Magika's mapped MIME result; it does not define a separate
  MIME registry or replace `qubit-mime` configuration.

## Further Reading

- [README](../README.md)
- [中文用户手册](user_guide.zh_CN.md)
- [API documentation](https://docs.rs/qubit-magika)
- [Example](../examples/basic.rs)
