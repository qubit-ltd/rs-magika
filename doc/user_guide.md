# qubit-magika User Guide

[中文用户手册](user_guide.zh_CN.md) | [README](../README.md) | [API documentation](https://docs.rs/qubit-magika)

## Purpose and Audience

This guide is for Rust applications that use `qubit-mime` and need Magika to
classify file content. It covers `qubit-magika` 0.15, which requires Rust 1.94.
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

For link-time discovery, enable `qubit-magika = { version = "0.15", features =
["inventory"] }`. This submits `MagikaMimeDetectorProvider::new()` to the MIME
detector inventory; `MimeDetectorRegistry::builtin()` then includes `magika`
while retaining `repository` as its default. A provider configured with a
media-stream classifier must still be constructed and registered explicitly.
Reference the crate in the application with `use qubit_magika as _;` so its
inventory submission is linked.

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
qubit-mime = "0.18"
qubit-magika = "0.15"
qubit-spi = "0.13"
qubit-fs = "0.2" # needed when calling detect_path directly
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

## Provider-Path Example

Use `detect_path` when the bytes live behind a `qubit-fs` provider rather than a
local `std::fs::File`. The filesystem and logical path are owned by the
application; `max_bytes` is the maximum cumulative number of bytes Magika may
request through range reads:

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

For a filesystem that advertises range reads, Magika requests bounded windows
and can use a conditional ETag read. Without range support, a known resource
larger than the budget is rejected before the full content is loaded;
`qubit-fs::read_all` may probe one byte beyond `max_bytes` to detect an oversized
resource. Known oversized lengths return `MimeError::BufferLimitExceeded`;
overflow discovered during `read_all` returns `MimeError::FileSystem`. The
asynchronous equivalent is `detect_async_path` on `AsyncFileSystem`.

## Media Classifier Example

Media refinement is optional. For example, the built-in ffprobe classifier can
distinguish audio-only, video-only, and audio-video content. `ffprobe` must be
installed and available on `PATH`:

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

The same classifier can be passed to
`MagikaMimeDetectorProvider::with_media_stream_classifier` when the application
uses registry-based construction. Classifier failures are reported as
`MimeError`; refinement is best-effort only when the classifier succeeds.

## Custom ONNX Runtime Configuration

To use an ONNX Runtime library supplied by the application, disable the bundled
binary and enable `ort`'s dynamic loader. These declarations are copy-pasteable
and keep the versions aligned with `qubit-magika` 0.15:

```toml
[dependencies]
qubit-magika = { version = "0.15", default-features = false }
qubit-mime = "0.18"
qubit-spi = "0.13"
ort = { version = "=2.0.0-rc.12", default-features = false, features = ["std", "ndarray", "load-dynamic", "api-24"] }
```

Initialize the runtime before constructing the detector. Use `onnxruntime.dll`
on Windows, `libonnxruntime.dylib` on macOS, or `libonnxruntime.so` on Linux:

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

Set `ORT_LIBRARY_PATH` to the complete shared-library path and ensure any
execution-provider libraries are discoverable by the dynamic loader. The
`ort::init_from` call must happen before the first detector (or any other
`ort`) API is used; the environment is process-global and can only be
committed once.

The repository's Linux CI checks this dynamic-loader setup with ONNX Runtime
1.24.2: it initializes the shared library, constructs a detector, and verifies
that Python content is classified as `text/x-python`. `ORT_LIBRARY_PATH` must
name the shared-library file itself, rather than its containing directory.

## Advanced Usage

The provider accepts these selection names: `magika`,
`magika-mime-detector`, and `magikamimedetector`. The canonical name is
`magika`.

`MagikaMimeDetector` also implements the `qubit-mime` backend operations for
seekable readers, local files, and synchronous or asynchronous provider paths.
Reader detection restores the original reader position. Content-backend reader
detection classifies the remaining resource from the current cursor, while the
generic detector API keeps whole-resource semantics. Provider-path detection
enforces `max_bytes` cumulatively across range requests. Range-capable providers use
bounded windows and conditional ETag reads when available; providers without
range support may probe one extra byte to detect overflow and reject oversized
resources. Unknown and undefined Magika types become `Ok(None)` so filename
fallback follows the selected `MimeDetectionPolicy`.
The asynchronous path awaits feature extraction before synchronously running
inference while holding the shared session lock. Inference and lock waiting can
occupy the async executor thread; move detection to a blocking worker or service
boundary when that isolation is required.

## Migration Guide

When upgrading a pre-0.14 integration, apply these changes in order:

1. Upgrade the coordinated dependencies to `qubit-mime 0.18`, `qubit-spi
   0.13`, and Rust 1.94.
2. Keep provider registration and detector configuration separate: register
   `MagikaMimeDetectorProvider`, select `magika`, then call
   `create_configured(&MimeConfig)`.
3. Replace per-request `MagikaMimeDetector::new()` calls with one startup
   detector shared through `Arc`.
4. If you use provider paths, treat `max_bytes` as a cumulative budget across
   all reads. Range-capable providers may therefore perform multiple bounded
   reads, while non-range providers reject known oversized resources.
5. If the application owns ONNX Runtime, disable the default feature and use
   the custom configuration shown above before creating the detector.

The provider names `magika`, `magika-mime-detector`, and
`magikamimedetector` remain accepted; use `magika` for new configuration.

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

- Provider discovery requires the opt-in `inventory` feature; the default build
  uses explicit registration. Inventory submits only the unconfigured provider.
- Detector construction initializes the model and runtime; create one detector
  and share it instead of constructing one per input.
- Inference on one detector is serialized internally.
- Async provider-path detection does not move model inference to a dedicated
  blocking executor. Account for this when choosing the application's runtime
  and worker arrangement.
- `qubit-magika` does not promise a MIME result for every input; callers must
  handle `Ok(None)` and `MimeError`.
- This crate exposes Magika's mapped MIME result; it does not define a separate
  MIME registry or replace `qubit-mime` configuration.

## Further Reading

- [README](../README.md)
- [中文用户手册](user_guide.zh_CN.md)
- [API documentation](https://docs.rs/qubit-magika)
- [Example](../examples/basic.rs)
