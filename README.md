# Qubit Magika

[![Rust CI](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-magika/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-magika/coverage-badge.json)](https://qubit-ltd.github.io/rs-magika/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-magika.svg?color=blue)](https://crates.io/crates/qubit-magika)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-magika` adds a Magika-backed `qubit_mime::MimeDetector` for applications
that inspect uploads whose filename may not match their contents. It provides
content-aware MIME detection without putting Magika and ONNX Runtime
dependencies into `qubit-mime` itself. An application can initialize one
detector during startup and share it across its detection calls.

## Overview

The crate enables `bundled-onnxruntime` by default so ordinary builds can link
and run without a separately installed ONNX Runtime. Disable default features if
your application provides a compatible ONNX Runtime shared library. With `ort`'s
dynamic loader, initialize that library with `ort::init_from` before creating a
detector. The Linux CI runs a real Python-content inference with this setup in
the [`dynamic-loader` job](https://github.com/qubit-ltd/rs-magika/blob/main/.github/workflows/ci.yml); see the
[user guide](doc/user_guide.md) for the dependency and startup example.

## Installation

```toml
[dependencies]
qubit-mime = "0.22"
qubit-magika = "0.18"
qubit-spi = "0.14"
qubit-operation = "0.3.2"
```

The crates.io releases needed by this example are not all available yet (`qubit-magika` 0.18, `qubit-mime` 0.22, `qubit-fs` 0.4, and `qubit-operation` 0.3.2 or newer). On 2026-11-24, registry search returned 0.16.0, 0.21.1, 0.3.3, and 0.3.1 respectively, and `--registry-only` could not resolve `qubit-magika = "^0.18"`. Fixed source and `--registry-only` select dependency sources. Until those releases are available, use the fixed sibling source checkouts in [the dependency table](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/ci/local-path-dependencies.tsv) and run `python3 .infra/check-readme-examples.py` from the repository root. The checker compiles the extracted English and Chinese consumers with default features; `--run-example` additionally runs both consumers with default and `inventory` features. The locked `readme_consumer` fixture is an isolated dynamic-loading application with Magika default features disabled, so its graph differs from this root manifest's bundled default. Dynamic-loader CI first checks the `dynamic_loader` feature graph and the `readme_consumer` graphs with default and `inventory`; it then compiles `readme_consumer` and runs it with both feature configurations using isolated ONNX Runtime loading.

## Quick Start

```rust
use std::error::Error;
use std::sync::Arc;

use qubit_magika::{MagikaMimeDetectorProvider, ManagedInferenceRuntime};
use qubit_mime::{
    MimeConfig,
    MimeDetector,
    MimeDetectorRegistry,
    MimeDetectionPolicy,
};
use qubit_operation::{ManagerOptions, OperationManager};
use qubit_spi::ProviderSelection;

// Keep the registry and runtime with the application for its lifetime.
fn create_detector() -> Result<(
    MimeDetectorRegistry,
    Arc<ManagedInferenceRuntime>,
    Arc<dyn MimeDetector>,
), Box<dyn Error>> {
    let manager = OperationManager::new(ManagerOptions::default())?;
    let runtime = Arc::new(ManagedInferenceRuntime::new(
        manager,
        MimeConfig::default().max_buffer_size(),
    )?);
    let registry = MimeDetectorRegistry::default();
    registry.register(
        MagikaMimeDetectorProvider::new().managed_runtime(Arc::clone(&runtime)),
    )?;
    let selection = ProviderSelection::named("magika")?;
    let provider = registry.resolve_selected(&selection)?;
    let detector = provider.create_configured(&MimeConfig::default())?;
    Ok((registry, runtime, detector))
}

// Verify uploaded content when the supplied filename is misleading.
fn inspect_upload(detector: &dyn MimeDetector) -> Result<Option<String>, qubit_mime::MimeError> {
    detector.detect(
        b"#!/usr/bin/env python3\nprint('hello')\n",
        Some("upload.txt"),
        MimeDetectionPolicy::VerifyContent,
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    let (_registry, runtime, detector) = create_detector()?;
    assert_eq!(
        Some("text/x-python".to_owned()),
        inspect_upload(detector.as_ref())?,
    );
    let report = runtime.shutdown_blocking(
        qubit_operation::ShutdownMode::Drain,
        std::time::Duration::from_secs(5),
    );
    assert!(report.worker_stopped);
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

### Operation-aware async provider detection

Applications that need cancellation and release accounting for async provider
sampling can call `MagikaMimeDetector::detect_async_provider_path_managed` or
`detect_bytes_managed` with an `OperationScope`. Managed calls attach their
work to that scope so its report can track release separately from the
business result. Create the runtime and detector at startup, pass the scope to
each managed request, and drain the runtime during application shutdown.
The returned detection result is the business outcome. The scope observer
reports settlement separately: a bounded wait may return `Pending(snapshot)`;
it may also return terminal `Ready(report)` while `all_released()` is false.
That negative report can describe resources owned elsewhere in the scope and
does not by itself show that Magika retains capacity. Magika keeps capacity
while this request's Provider I/O completion is unconfirmed or a worker may
still access request resources. The local future, reader, and driver still drop when the request is
stopped; this does not confirm that native or remote I/O completed. Built-in
cleanup has no way to receive completion evidence for discarded provider I/O,
and retrying its current policy cannot release that capacity. A
`RecoveryHandle` can inspect the state, retry the policy, or transfer
responsibility. `worker_stopped` reports worker join independently of
settlement and release.

The inventory submits a synchronous `MimeDetectorSpec` factory only. It does
not add a provider to `qubit-mime`'s separate async provider registry or attach
an application scope. Selecting this sync-only `magika` provider through the
scope-aware async `MimeRuntime` route returns `AsyncUnsupported`; it does not
fall back to synchronous inference. The legacy content hook normally waits for
its internal operation to settle; the legacy Provider hook normally waits for
its business result. Runtime cancellation can make either return a stop error
early. Use the managed API when settlement needs separate observation. Locally
stopping a request does not release capacity while provider I/O or worker
access remains unconfirmed. See the
[managed lifecycle guide](doc/user_guide.md#migration-guide) for shutdown and
worker reporting semantics.

The inventory-enabled `MimeDetectorRegistry::builtin()` discovers the
unconfigured sync provider; its default selection remains `repository`, so
select `magika` explicitly. An independent `MimeDetectorRegistry::default()`
starts empty and is useful for application-specific registration, as in the
example above. Registering the same provider name twice is an error. Include
`use qubit_magika as _;` when relying on inventory so the linker retains its
submission.

To prepare missing local dependency checkouts in a source repository, use the
[preparation script](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/bin/prepare-local-path-dependencies.sh).
From the repository root, use the read-only validation command
[`check-locked-local-path-dependencies.sh`](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/lib/check-locked-local-path-dependencies.sh)
(`.infra/lib/check-locked-local-path-dependencies.sh`).
It checks every fixed sibling commit and worktree state, and matches the
pinned-dependency check in [CI](https://github.com/qubit-ltd/rs-magika/blob/main/.github/workflows/ci.yml).
Preparation leaves existing Git
checkouts untouched, so restore a drifted checkout to its pinned revision
manually before checking again. It refuses to overwrite occupied non-checkout
paths. Normal and dynamic-fixture bootstrap modes share the same preparation
helper. Publishing a newly prepared missing checkout requires an exclusive
rename into the target path. The helper uses GNU `mv -Tn` (or `gmv` where GNU
coreutils use that command name) when available. On Darwin, if neither command
is available, it uses Python 3's `renamex_np` (default executable `python3`;
override it with `RS_INFRA_PYTHON`). Other platforms require GNU `mv -Tn` or
`gmv` to publish a missing checkout. Reusing an existing valid worktree and strict
read-only validation do not require a publisher. The package does not
ship `.infra`, so its script and [pinned revision list](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/ci/local-path-dependencies.tsv)
are linked from the source repository. After preparing the sibling layout, the
following commands verify the locked manifests:

```bash
cargo metadata --locked --offline --format-version 1 --manifest-path Cargo.toml
cargo metadata --locked --offline --format-version 1 --manifest-path tests/fixtures/dynamic_loader/Cargo.toml
cargo metadata --locked --offline --format-version 1 --manifest-path tests/fixtures/readme_consumer/Cargo.toml
```

For a standalone application manifest using these fixed checkouts, see the
[fixed-source application example in the user guide](doc/user_guide.md#conceptual-model).

Managed requests reserve runtime capacity, the logical `max_bytes` sampling
budget, and the scope child before provider I/O. Closed, full, or cancelled
requests are rejected before provider calls. Dropping a pending provider future
only cancels local polling: when native or remote completion cannot be observed,
capacity remains reserved. The settlement observer can still reach a terminal
`Ready(report)` with `all_released() == false`. See the
[managed lifecycle guide](doc/user_guide.md#migration-guide) for this boundary,
worker error reporting, and shutdown timeouts.

Unknown and undefined Magika predictions have no MIME candidate and produce
`Ok(None)`, allowing the selected `MimeDetectionPolicy` to decide filename
fallback. A shutdown timeout does not forcibly interrupt native inference or
provider I/O. `worker_stopped == false` means join was not confirmed before
the deadline; `true` means the worker joined, independently of resource
settlement. `worker_panicked` records an uncaught panic observed during join.
A panic caught while running inference is reported as a failed operation and
a faulted runtime. If join was not confirmed, a later shutdown call can
continue observing the retained worker handle.

Provider selection and detector configuration are separate inputs. A
`ProviderSelection` decides which registered provider may create the service;
`MimeConfig` controls the detector instance after that provider is resolved.
Without inventory, the app explicitly registers the provider during startup. Enable
`qubit-magika`'s `inventory` feature to submit
`MagikaMimeDetectorProvider::new()` to the MIME detector inventory: linked
applications then discover `magika` through `MimeDetectorRegistry::builtin()`
and `global()`. The stable default remains `repository`; select `magika`
explicitly when needed. A provider with a media-stream classifier still needs
explicit registration with `with_media_stream_classifier`.
Reference the crate in the application (`use qubit_magika as _;`) so the linker
retains its inventory submission.
Inventory submits the unconfigured provider only. To share an application-owned
runtime, register `MagikaMimeDetectorProvider::new().managed_runtime(...)`
explicitly and keep the runtime alive for the detector lifetime; the
[provider runtime example](doc/user_guide.md#sharing-an-application-managed-inference-runtime)
shows the setup and shared budget/shutdown behavior.
Detector construction synchronously waits for the worker to initialize the
embedded Magika model and ONNX Runtime session. Build it during application
startup, create it once, and share it (for example with `Arc`). Inference calls
on its shared runtime are serialized internally.

Synchronous borrowed content, reader, and local-file calls sample on the caller
thread; model work runs on the worker and the call waits for its result. A
matching repository rule may return without model work. Borrowed sync content
is sampled without rejecting it solely because its logical length exceeds
`MimeConfig::max_buffer_size()`. The `MimeDetectorBackend`
`guess_from_content_async` hook checks the detector's per-request owned-payload
limit when first polled, reserves the shared runtime's request and aggregate
owned-payload byte capacity, then copies accepted input for worker sampling and
inference. Its future waits asynchronously and has no parent operation scope.
`detect_bytes_managed` applies the same per-request and runtime aggregate
budgets while tracking its explicit scope. Provider `max_bytes` limits
cumulative logical sample and fallback-read bytes. These logical budgets,
sampled lengths, and copied payload bytes do not guarantee a total process
memory bound; provider buffers and transient allocations may add memory. After
shutdown, ruled sync inputs can still return directly without using the closed
worker; requests requiring model inference fail through the closed runtime.

### Media refinement support

A configured media-stream classifier refines eligible high-level inputs only:

| Detection entry point | Classifier behavior |
| --- | --- |
| Complete content (`detect_by_content` / `detect`) | May classify the complete bytes when precise detection and a matching MIME mapping are configured |
| Seekable reader | May classify the full reader under the same configuration |
| Local file | May classify the file under the same configuration |
| Provider path (`detect_path` / `detect_async_path`) | Not called, including random-access sessions, legacy range reads, and `read_all` fallback |
| Direct `MimeContentBackend` call | Returns MIME candidates; high-level selection and refinement are not part of this backend result |

Setting a classifier on the builder or provider does not enable provider-path
media refinement. The MIME mapping and precise-detection settings must also
match for a high-level input to be refined.

Provider-path detection uses `max_bytes` as the cumulative request budget for
Magika's byte sampling. When the filesystem supports random-access sessions,
Magika opens once and reads each sample from that same opened object into its
caller buffer. The open-time length bounds readable offsets but is not a
content snapshot. If opening returns `UnsupportedOperation` or
`UnsupportedCapability`, detection uses the existing path-based route; other open failures and session read errors
propagate without reopening by path. The legacy route uses bounded windows and
an ETag condition when conditional reads are available. Without range support,
`qubit-fs::read_all` may probe one byte beyond `max_bytes` to detect overflow;
known oversized resources return `MimeError::BufferLimitExceeded`, while an
overflow discovered during `read_all` returns `MimeError::FileSystem`. Unknown and
undefined Magika types return `Ok(None)`, allowing the selected
`MimeDetectionPolicy` to apply filename fallback consistently.
Provider-native asynchronous I/O is awaited by the calling future and can remain
pending if the provider never completes it. Once sampling is ready, Magika
sampling and inference run on the session-owning worker. Legacy hooks have no
parent-scope lifecycle observation; use managed APIs when scope tracking is
required.

## Learn More

- [Core random-access session guide](https://github.com/qubit-ltd/rs-fs/blob/main/doc/user_guide.md#sampling-the-same-opened-object-at-several-offsets)
- [核心随机读取会话说明](https://github.com/qubit-ltd/rs-fs/blob/main/doc/user_guide.zh_CN.md#从同一已打开对象的多个位置采样)
- [English user guide](doc/user_guide.md)
- [中文用户手册](doc/user_guide.zh_CN.md)
- [API documentation](https://docs.rs/qubit-magika)
- [中文 README](README.zh_CN.md)
- [Locked local dependency revisions](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/ci/local-path-dependencies.tsv)

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project checks (available in the source repository)
./.infra/bin/project-ci-check.sh

# Check code coverage
./.infra/bin/coverage.sh
```

The `.infra` scripts are not included in the published Cargo package. See the
[source repository](https://github.com/qubit-ltd/rs-magika) for those scripts.

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run the source repository's
[`align-ci.sh`](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/bin/align-ci.sh)
to format code and [`project-ci-check.sh`](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/bin/project-ci-check.sh)
before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-magika](https://github.com/qubit-ltd/rs-magika)
