# qubit-magika User Guide

[中文用户手册](user_guide.zh_CN.md) | [README](../README.md) | [API documentation](https://docs.rs/qubit-magika)

## Purpose and Audience

This guide is for Rust applications that use `qubit-mime` and need Magika to
classify file content. It covers `qubit-magika` 0.18, which requires Rust 1.94.
The crate supplies the `MagikaMimeDetector` implementation and an optional
`MagikaMimeDetectorProvider`; it does not replace `qubit-mime`'s detector
selection or MIME policy.

## Conceptual Model

There are two separate startup decisions:

1. Keep an application-owned `MimeDetectorRegistry`, register
   `MagikaMimeDetectorProvider`, and select the provider name `magika`.
2. Create a detector with `MimeConfig`, which controls the surrounding
   `qubit-mime` selection and refinement behavior.

For explicit registration, create an isolated registry with
`MimeDetectorRegistry::default()` and retain it in application state. With the
`inventory` feature, `MimeDetectorRegistry::builtin()` also discovers the
linked `magika` provider; `MimeDetectorRegistry::global()` is the process-wide
registry. In both inventory registries, `repository` remains the default
selection. Avoid registering the inventory-submitted provider a second time.

The detector uses repository filename rules for filename-only detection and
Magika inference for content, reader, and local-file detection. Creating a new
runtime initializes Magika's embedded model and the ONNX Runtime session
synchronously before construction returns. The direct constructors
`MagikaMimeDetector::new()` and `from_mime_config(...)` create a new runtime;
injecting an existing runtime through `MagikaMimeDetectorBuilder::managed_runtime`
or `MagikaMimeDetectorProvider::managed_runtime` reuses it without initializing
another session. Inference calls made through one detector are serialized
internally, so one initialized detector can be shared with `Arc`.

For link-time discovery, enable `qubit-magika = { version = "0.18", features =
["inventory"] }`. This submits `MagikaMimeDetectorProvider::new()` to the MIME
detector inventory; `MimeDetectorRegistry::builtin()` then includes `magika`
while retaining `repository` as its default. A provider configured with a
media-stream classifier must still be constructed and registered explicitly.
Reference the crate in the application with `use qubit_magika as _;` so its
inventory submission is linked.

The async provider registry is a separate capability: this inventory entry is
a synchronous factory, so selecting `magika` with
`MimeDetectorRegistry::create_async_backend` returns
`MimeError::AsyncUnsupported`. This does not remove the legacy async hooks on
`MimeDetector`; those hooks can run worker inference or provider reads but do
not accept a parent operation scope. Use the explicit managed API when
application code must attach provider sampling to an operation scope.

The source checkout includes dependency preparation helpers that are not
included in the published crate. To prepare missing sibling checkouts, run
[`prepare-local-path-dependencies.sh`](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/bin/prepare-local-path-dependencies.sh)
without arguments. Preparation leaves any existing Git checkout untouched,
even when its revision or working tree differs from the lock file. To validate
the sibling layout read-only, run
[`check-locked-local-path-dependencies.sh`](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/lib/check-locked-local-path-dependencies.sh)
from the repository root. It checks every pinned commit and worktree state,
reports missing, dirty, or wrong-revision checkouts, and does not repair them.
This is the same pinned-dependency check used by
[CI](https://github.com/qubit-ltd/rs-magika/blob/main/.github/workflows/ci.yml).
Compare the result with the fixed revisions in
[`local-path-dependencies.tsv`](https://github.com/qubit-ltd/rs-magika/blob/main/.infra/ci/local-path-dependencies.tsv)
and resolve drift deliberately. The normal and
dynamic-fixture bootstrap modes share the same preparation helper. Publishing
a newly prepared missing checkout requires an exclusive rename into the target
path. The helper uses GNU `mv -Tn` (or `gmv` where GNU coreutils use that name)
when available. On Darwin, if neither GNU publisher is available, it uses
Python 3's `renamex_np` (default executable `python3`; override it with
`RS_INFRA_PYTHON`). Other platforms require GNU `mv -Tn` or `gmv` to publish a
missing checkout. Preparing an existing valid worktree and running strict
read-only validation do not require a publisher.

For an application that needs these unpublished releases, place its workspace
root beside the pinned checkouts listed in the source repository, with the
application directory named `app`. Keep this manifest at `app/Cargo.toml`:

<!-- fixed-source-application-start -->
```toml
[workspace]

[package]
name = "my-app"
version = "0.1.0"
edition = "2024"

[dependencies]
qubit-magika = { version = "0.18", path = "../rs-magika" }
qubit-mime = { version = "0.22", path = "../rs-mime" }
qubit-fs = { version = "0.4", path = "../rs-fs", default-features = false, features = ["async"] }
qubit-operation = { version = "0.3.2", path = "../rs-operation" }
qubit-spi = "0.14"

[patch.crates-io]
qubit-operation = { path = "../rs-operation" }
```
<!-- fixed-source-application-end -->

Dependency paths are relative to the package directory; workspace patches are
relative to the workspace root. Prepare the fixed sibling checkouts with the
source repository's preparation helper, verify them with
`check-locked-local-path-dependencies.sh`, then check the independent
application lock with Cargo. The local source checker uses this same root
manifest shape. Registry-only consumers omit all paths and patches and use
their own lock file.

The published default feature, `bundled-onnxruntime`, is enabled by this root
manifest. To use the synchronous provider inventory, change the Magika entry
to `qubit-magika = { version = "0.18", path = "../rs-magika", features = ["inventory"] }`.
Fixed source and `--registry-only` select dependency sources. The README checker compiles the extracted English and Chinese consumers with default features; `--run-example` additionally runs both with default and `inventory` features. The repository's locked [`readme_consumer` fixture](https://github.com/qubit-ltd/rs-magika/blob/main/tests/fixtures/readme_consumer/Cargo.toml) is an isolated dynamic-loading consumer with Magika default features disabled, so its graph differs from this root manifest's bundled default. Dynamic-loading CI filters feature-graph metadata for the host, checks the separate [`dynamic_loader` fixture's](https://github.com/qubit-ltd/rs-magika/blob/main/tests/fixtures/dynamic_loader/Cargo.toml) feature graph, then compiles its ordinary binary to verify that normal application dependencies can build `ort::init_from`. It next checks `readme_consumer` with default and `inventory` features and a negative graph for a dependency limited to another platform, compiles the default consumer, and runs both configurations using isolated ONNX Runtime loading.

The application manifest above keeps the direct dependencies used by the
example. `qubit-local-files ^0.6` is a production dependency brought in by
`qubit-mime`; applications do not need to list it directly. As of 2026-11-25,
the latest non-yanked crates.io versions are `qubit-magika` 0.16.0,
`qubit-mime` 0.21.1, `qubit-fs` 0.3.3, `qubit-operation` 0.3.1, and
`qubit-local-files` 0.5.4. The required Magika 0.18, MIME 0.22, FS 0.4, and
Operation 0.3.2 releases are not yet available, so the registry-only check
cannot resolve `qubit-magika = "^0.18"`. Until they are published, use the
fixed source-checkout procedure above and compile the guide examples with
`python3 .infra/check-managed-guide-examples.py` from the source checkout.

## Scenario

An application accepts an uploaded Python script and needs a content-derived
MIME type while also retaining filename detection for ordinary paths. The
success criteria are one detector initialized at startup, `text/x-python`
returned for Python content, and `application/pdf` returned for a `.pdf`
filename.

## Installation and Minimal Configuration

Add the crate versions used by this release:

As of 2026-11-25, the latest non-yanked crates.io versions are
`qubit-magika` 0.16.0, `qubit-mime` 0.21.1, `qubit-fs` 0.3.3,
`qubit-operation` 0.3.1, and `qubit-local-files` 0.5.4. This guide needs
Magika 0.18, MIME 0.22, FS 0.4, and Operation 0.3.2. `qubit-local-files
^0.6` is a production dependency of `qubit-mime`, so the application need not
declare it directly. Until the required releases are available, follow the
fixed source-checkout procedure in [Conceptual Model](#conceptual-model) and
run `python3 .infra/check-managed-guide-examples.py` from the source checkout.

```toml
[dependencies]
qubit-mime = "0.22"
qubit-magika = "0.18"
qubit-spi = "0.14"
qubit-fs = "0.4" # needed when calling detect_path directly
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

fn create_registry() -> Result<MimeDetectorRegistry, Box<dyn Error>> {
    let registry = MimeDetectorRegistry::default();
    registry.register(MagikaMimeDetectorProvider::new())?;
    Ok(registry)
}

fn create_detector(registry: &MimeDetectorRegistry) -> Result<Arc<dyn MimeDetector>, Box<dyn Error>> {
    let selection = ProviderSelection::named("magika")?;
    let provider = registry.resolve_selected(&selection)?;
    Ok(provider.create_configured(&MimeConfig::default())?)
}
```

Call both functions during startup and keep the registry alongside the shared
detector. This lets application code resolve the provider later without
depending on process-global registry state.

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
`MagikaMimeDetector::from_mime_config` accept MIME configuration. The latter
only applies that configuration; it does not attach a media-stream classifier.
Use the builder for a classifier, or configure
`MagikaMimeDetectorProvider::with_media_stream_classifier` and register the
provider when constructing through a registry.

## Media Refinement Support

A configured media-stream classifier is used only for eligible high-level
inputs, and precise detection must be enabled. When an input has a filename
extension, that extension must be enabled and the initially detected MIME type
must be one of the MIME types paired with that extension. Without an extension,
the initially detected MIME type is matched against configured ambiguous
mappings. If it matches mappings that imply different audio/video pairings,
refinement keeps the original MIME selection. Reusing the same pairing in
multiple mappings is not a conflict. An extension and initial MIME type that
do not form a configured pair are not refined.

| Detection entry point | Classifier behavior |
| --- | --- |
| Complete content (`detect_by_content` / `detect`) | May classify the complete bytes |
| Seekable reader | May classify the full reader and attempts to restore its position |
| Local file | May classify the file |
| Provider path (`detect_path` / `detect_async_path`) | Not called, including session, legacy range, and `read_all` fallback routes |
| Direct `MimeContentBackend` call | Returns MIME candidates; high-level selection and refinement are separate |

Configuring a classifier on `MagikaMimeDetectorBuilder` or
`MagikaMimeDetectorProvider` does not make provider-path detection invoke it.
The path backend returns candidate MIME types for the higher-level detector to
select; it does not provide the complete content source used by the current
refinement route.

## Provider-Path Example

Use `detect_path` when the bytes live behind a `qubit-fs` provider rather than a
local `std::fs::File`. The filesystem and logical path are owned by the
application; `max_bytes` bounds cumulative logical sampling reads. If routing
uses `read_all`, it also limits the accepted payload and may allow one extra
byte to be probed for overflow:

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

When random-access sessions are supported, Magika opens the path once and
samples several offsets from that same opened object. The length reported when
opening bounds readable offsets; it is not a snapshot of the object's contents.
Session routing is preferred, and only `UnsupportedOperation` or
`UnsupportedCapability` returned while opening the session triggers the
path-based fallback. Other open errors and all session read errors propagate
without reopening by path. For a filesystem that advertises range reads,
Magika requests bounded windows and can use a conditional ETag read. The legacy
route opens by path separately for each segment, so the ETag condition is what
can detect changes between segments; without conditional reads it does not
provide a cross-segment content snapshot. This range route is used only when
the resource length is known. If range reads are unsupported, or the length is
unknown or unavailable because `stat` is unsupported, detection uses
`qubit-fs::read_all` even if the provider advertises range capability. The
cumulative `max_bytes` budget applies across all Magika logical sampling
reads and limits the payload accepted by `read_all`. That full read may probe
one extra byte to detect overflow, including when range capability is
advertised but length is unknown. When range reads cannot be used, a known length above the budget is
rejected before the full read with `MimeError::BufferLimitExceeded`; a known
oversized resource that uses range sampling is not rejected on length alone.
Overflow discovered during `read_all` returns `MimeError::FileSystem`. The
asynchronous equivalent is `detect_async_path` on `AsyncFileSystem`.

## Media Classifier Example

Media refinement is optional. It follows the matching and conflict rules in
[Media Refinement Support](#media-refinement-support). For example, the built-in
ffprobe classifier can distinguish audio-only, video-only, and audio-video
content. `ffprobe` must be
installed and available on `PATH`:

Enable precise detection before starting the application by setting
`QUBIT_MIME_ENABLE_PRECISE_DETECTION=true` (configuration key
`mime.enable.precise.detection`). `MimeConfig::from_env()` reads this setting.
See [Media Refinement Support](#media-refinement-support) for the extension, MIME mapping, and conflict rules. The default ambiguous mappings cover `webm` (`video/webm` or `audio/webm`) and `ogg` (`video/ogg` or `audio/ogg`).

```rust
use std::sync::Arc;

use qubit_magika::MagikaMimeDetector;
use qubit_mime::{FfprobeCommandMediaStreamClassifier, MimeConfig};

fn create_media_aware_detector() -> qubit_mime::MimeResult<MagikaMimeDetector> {
    let config = MimeConfig::from_env()?;
    let classifier = Arc::new(FfprobeCommandMediaStreamClassifier::new());
    MagikaMimeDetector::builder()
        .mime_config(config)
        .media_stream_classifier(Some(classifier))
        .build()
}
```

For registry construction, pass the same classifier to
`MagikaMimeDetectorProvider::with_media_stream_classifier` and pass the enabled
configuration to `create_configured(&config)`. `MagikaMimeDetector::from_mime_config`
accepts a configuration but does not attach a classifier; use the builder when
both settings are needed. A classifier result that cannot refine the selected
candidate leaves that selection unchanged. Errors raised by the classifier
propagate as `MimeError`.

### Sharing an application-managed inference runtime

The shared runtime and the operation-aware example below use these direct
dependencies. Keep this block alongside the managed examples when copying them
into an application:

<!-- managed-dependencies-start -->
```toml
[dependencies]
qubit-magika = "0.18"
qubit-mime = "0.22"
qubit-operation = "0.3.2"
qubit-fs = { version = "0.4", default-features = false, features = ["async"] }
```
<!-- managed-dependencies-end -->

If these registry versions are not available, use the fixed source-checkout
procedure in [Conceptual Model](#conceptual-model) before compiling this block.

Create the runtime during application startup and register a configured
provider in an isolated registry explicitly:

<!-- provider-runtime-example-start -->
```rust
use std::error::Error;
use std::sync::Arc;

use qubit_magika::{MagikaMimeDetectorProvider, ManagedInferenceRuntime};
use qubit_mime::{MimeConfig, MimeDetectorRegistry};
use qubit_operation::{ManagerOptions, OperationManager};

fn register_provider() -> Result<(MimeDetectorRegistry, Arc<ManagedInferenceRuntime>), Box<dyn Error>> {
    let manager = OperationManager::new(ManagerOptions::default())?;
    let runtime = Arc::new(ManagedInferenceRuntime::new(
        manager,
        MimeConfig::default().max_buffer_size(),
    )?);
    let registry = MimeDetectorRegistry::default();
    registry.register(
        MagikaMimeDetectorProvider::new().managed_runtime(Arc::clone(&runtime)),
    )?;
    Ok((registry, runtime))
}
```
<!-- provider-runtime-example-end -->

Keep both returned values in application state. Resolve `magika` from the
returned registry with `resolve_selected`, then call
`create_configured(&MimeConfig)`; do not discard the registry after
registration. Shut the runtime down after detectors stop accepting requests.
Detectors created by this provider share its worker, request capacity, byte
budget, and shutdown domain. `ManagedInferenceRuntime::new` sets the aggregate
`max_in_flight_bytes` limit across all detectors sharing that runtime; each
detector's `MimeConfig::max_buffer_size` is a separate per-request limit. Set
the aggregate limit to cover the combined payloads you expect to have in
flight, and ensure it can admit a request at each detector's configured
maximum. Inventory submits only
`MagikaMimeDetectorProvider::new()` without a runtime; register a configured
provider explicitly in an independent registry when sharing an
application-owned runtime.

## Custom ONNX Runtime Configuration

To use an ONNX Runtime library supplied by the application, disable the bundled
binary and enable `ort`'s dynamic loader. These declarations are copy-pasteable
and keep the versions aligned with `qubit-magika` 0.18:

If the registry prerequisites are not yet available, use the fixed
source-checkout procedure in [Conceptual Model](#conceptual-model); the
managed and custom ONNX dependency blocks use the same procedure.

<!-- dynamic-loader-application-start -->
```toml
[dependencies]
qubit-magika = { version = "0.18", path = "../rs-magika", default-features = false }
qubit-mime = { version = "0.22", path = "../rs-mime" }
qubit-operation = { version = "0.3.2", path = "../rs-operation" }
qubit-fs = { version = "0.4", path = "../rs-fs", default-features = false, features = ["async"] }
qubit-spi = "0.14"
ort = { version = ">=2.0.0-rc.12,<2.1", default-features = false, features = ["std", "ndarray", "load-dynamic", "api-24"] }

[patch.crates-io]
qubit-operation = { path = "../rs-operation" }
```

The `api-24` feature requires an ONNX Runtime 1.24 or newer shared library;
Linux CI verifies this configuration with ONNX Runtime 1.24.2.
<!-- dynamic-loader-application-end -->

Initialize the runtime before constructing the detector. Use `onnxruntime.dll`
on Windows, `libonnxruntime.dylib` on macOS, or `libonnxruntime.so` on Linux:

<!-- dynamic-loader-rust-example-start -->
```rust,no_run
use std::error::Error;
use std::path::PathBuf;

use qubit_magika::MagikaMimeDetector;

fn create_detector() -> Result<MagikaMimeDetector, Box<dyn Error>> {
    let runtime = std::env::var_os("ORT_LIBRARY_PATH")
        .map(PathBuf::from)
        .ok_or("ORT_LIBRARY_PATH must name the ONNX Runtime shared library")?;
    if !ort::init_from(runtime)?.commit() {
        return Err("ONNX Runtime environment was already initialized".into());
    }
    Ok(MagikaMimeDetector::new()?)
}
```
<!-- dynamic-loader-rust-example-end -->

Set `ORT_LIBRARY_PATH` to the complete shared-library path and ensure any
execution-provider libraries are discoverable by the dynamic loader. The
`ort::init_from` call must happen before the first detector (or any other
`ort`) API is used; the environment is process-global and can only be
committed once.

The repository's [`dynamic_loader` fixture](https://github.com/qubit-ltd/rs-magika/blob/main/tests/fixtures/dynamic_loader/Cargo.toml)
uses the same ONNX Runtime version and feature set. The
[`dynamic-loader` job in Linux CI](https://github.com/qubit-ltd/rs-magika/blob/main/.github/workflows/ci.yml)
filters feature-graph metadata for the host, checks this fixture's graph, and
compiles its ordinary binary to verify that normal application dependencies
can build `ort::init_from`. Run this command from the repository root:

```bash
cargo +1.94.0 check --locked --manifest-path tests/fixtures/dynamic_loader/Cargo.toml --bin magika-dynamic-loader-fixture
```

CI then checks the locked `readme_consumer` graphs with default and `inventory`
features plus a negative graph for a dependency limited to another platform,
compiles the default consumer, and runs both configurations with ONNX Runtime
1.24.2, initializing the shared library, constructing a detector, and verifying
that Python content is classified as `text/x-python`. These graph checks ensure
bundled runtime downloads are disabled. `ORT_LIBRARY_PATH` must name the
shared-library file itself, rather than its containing directory.

## Advanced Usage

The provider accepts these selection names: `magika`,
`magika-mime-detector`, and `magikamimedetector`. The canonical name is
`magika`.

`MagikaMimeDetector` also implements the `qubit-mime` backend operations for
seekable readers, local files, and synchronous or asynchronous provider paths.
`MimeDetectorBackend::guess_from_reader` detects the whole resource, while
`MimeContentBackend::detect_reader` detects from the current cursor to EOF.
Once the original position is known, reader detection attempts to restore it
even if detection returns an error. A successful restore preserves the
detection result or its original error. If the restore seek fails, its
`MimeError::Io` takes precedence and the reader position is unspecified.
For Provider-path routing, `max_bytes`, range reads, and `read_all` fallback
behavior, see the [Provider-Path Example](#provider-path-example). The legacy
range route opens by path for each segment and applies the same conditional
ETag to every segment when available; without conditional reads, it cannot
guarantee a consistent cross-segment snapshot. Unknown and undefined Magika
types become `Ok(None)` so filename fallback follows the selected
`MimeDetectionPolicy`.

Local-file content detection first checks the path's final component. A
directory or final symlink keeps the existing empty-candidate result; other
special types such as FIFOs and Unix sockets return `MimeError::Io` with
`ErrorKind::Unsupported`. For an ordinary file, detection opens the file,
checks the type on that opened handle, and samples through the same handle. On
Unix, the open requests no-follow and nonblocking behavior, so a final symlink
is not followed and opening a FIFO does not wait for a writer. If the opened
handle is not a regular file, detection rejects it with `ErrorKind::Unsupported`.
Other platforms check the opened handle too, but do not get that Unix open-stage
guarantee. If the path changes between the initial check and open, detection can
return an I/O error; this does not protect path ancestors or guarantee a content
snapshot while reading.

Media refinement is a separate step and applies only to ordinary local files.
Directories and symlinks skip refinement; with `Freedesktop`, an available
filename candidate can be selected, while `VerifyContent` returns `None`
without content. The configured `MediaStreamClassifier::classify_file` receives
the path and may open it independently, so content detection's validated
handle does not cover media refinement. A high-level `detect_file` call using
`PreferFilename` can return a unique filename match without inspecting the path
or its type.

One worker owns the Magika session and serializes worker sampling and inference.
For asynchronous provider paths, the worker can wait for provider reads while
sampling; that wait can delay other requests sharing the same runtime. This is
a shared wait and failure domain. Applications that need delay isolation can
use independent runtimes, at the cost of an additional Magika session per
runtime.
Synchronous borrowed content, reader, and local-file entry points sample on the
caller thread, then wait synchronously when model work is needed; repository
rules can return without model work. Borrowed content is not rejected solely
because its logical length exceeds `MimeConfig::max_buffer_size()`. Async
provider routes cooperate with the worker through a capacity-one range bridge;
provider reads are awaited by the calling future. The legacy
`guess_from_content_async` hook checks the detector's per-request owned-payload
limit when first polled, reserves runtime request capacity and aggregate
owned-payload byte capacity before copying the input, and runs sampling and
inference on the worker. It waits asynchronously and has no parent scope.
`detect_bytes_managed` applies the same per-request and shared runtime budgets
while tracking its explicit scope. Provider `max_bytes` bounds cumulative
logical sampling reads and the payload accepted by a `read_all` fallback. That
full read may probe one extra byte, including when range capability is declared
but length is unknown or unavailable. Legacy range sampling requires known
length and declared range support. A known length above the budget is rejected
before a full read only when range sampling cannot be used; overflow discovered
during `read_all` returns `MimeError::FileSystem`. These logical budgets and copied payload
lengths do not cap total process memory, including provider buffers and
transient allocations. After shutdown, ruled sync inputs can still return
directly; requests needing model inference use the closed runtime and fail.

The legacy content hook normally waits for its internal operation to settle;
the legacy Provider hook normally waits for its business result. Runtime
cancellation can make either hook return a stop error early. The managed API
exposes settlement separately: a bounded observation can be `Pending`, and
terminal `Ready(report)` can still have `all_released() == false`. Capacity
remains reserved while provider I/O or worker access is unconfirmed, including
after a local stop request. This behavior is covered by
[`test_unmanaged_content_waits_for_worker_settlement_after_business_result`](../src/managed_runtime/managed_inference_runtime/tests.rs)
and
[`test_legacy_async_hook_returns_cancel_result_while_provider_resources_remain_held`](../tests/provider_path_tests.rs).
Use managed APIs when the application needs release observation independently
of the result.

The async content hook returns the backend's raw Magika candidate list, like
the synchronous content hook. It does not apply high-level MIME selection or
media refinement. Once accepted, it accounts for the owned input against the
shared runtime aggregate byte budget even when a repository rule could
determine the result without model inference. Closed, full, over-budget, or
oversized requests may therefore fail asynchronously instead of returning a
rule result directly. Neither this aggregate payload accounting nor provider
sampling accounting is a total process-memory limit.

## Migration Guide

### Operation-aware async provider requests

Use the managed entry point when an application needs the provider sample reads
to be children of a `qubit-operation` scope. Share one supervisor and detector
for the application lifetime, then pass the request scope explicitly:
Supply the application's `ClockSource` at startup and use the same source for
each observation deadline. `ManagerOptions::default()` has no clock, so
`wait_settlement_until` would otherwise return `ClockUnavailable`.
The caller also supplies `wait_ticks`, converted from the desired wait duration
using that clock's documented tick scale; this value is not seconds.

`MagikaMimeDetectorBuilder::operation_manager` binds the detector runtime to
the application's manager. For an already owned byte buffer, call
`detect_bytes_managed(&scope, bytes)`; for provider-backed data, call
`detect_async_provider_path_managed(&scope, file_system, path, max_bytes)`.
Both APIs associate admitted work with the supplied scope. The legacy
`MimeDetector` hooks do not take a scope, even when they perform asynchronous
work.

<!-- managed-guide-example-start -->
```rust,no_run
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use qubit_fs::{AsyncFileSystem, Path};
use qubit_magika::MagikaMimeDetector;
use qubit_magika::ManagedInferenceShutdownReport;
use qubit_operation::{GroupOptions, ManagerOptions, OperationManager, OperationOptions, OperationOutcome, ShutdownMode};
use qubit_operation::{AsyncOperationObserver, ClockSource, Deadline, MonotonicTime, SettlementWait, ShutdownHandle, ShutdownReport, ShutdownSnapshot, ShutdownWait};

fn prepare_application(clock: Arc<dyn ClockSource>) -> Result<(
    OperationManager,
    qubit_operation::OperationGroup,
    Arc<MagikaMimeDetector>,
    Arc<dyn ClockSource>,
), Box<dyn Error>> {
    let mut options = ManagerOptions::default();
    options.clock = Some(Arc::clone(&clock));
    let manager = OperationManager::new(options)?;
    let service = manager.group(GroupOptions { capacity: 4 })?;
    let detector = Arc::new(
        MagikaMimeDetector::builder()
            .operation_manager(manager.clone())
            .build()?,
    );
    Ok((manager, service, detector, clock))
}

async fn detect_request(
    service: &qubit_operation::OperationGroup,
    detector: &Arc<MagikaMimeDetector>,
    file_system: AsyncFileSystem,
    path: Path,
    clock: &dyn ClockSource,
    wait_ticks: u64,
) -> Result<(OperationOutcome<Option<String>, qubit_mime::MimeError>, AsyncOperationObserver, SettlementWait), Box<dyn Error>> {
    let request_detector = Arc::clone(detector);
    let mut request = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            request_detector
                .detect_async_provider_path_managed(&scope, &file_system, &path, 1024 * 1024)
                .await
        })?;
    let observer = request.observer();
    let outcome = request.wait_outcome().await?;
    match &outcome {
        OperationOutcome::Succeeded(mime) => println!("detection completed: {mime:?}"),
        OperationOutcome::Failed(error) => eprintln!("detection failed: {error}"),
        other => eprintln!("detection did not complete successfully: {other:?}"),
    }
    // The caller converted the desired wait duration to this clock's tick units.
    let now = clock.now();
    let deadline = Deadline::at(MonotonicTime::new(now.domain(), now.ticks().saturating_add(wait_ticks)));
    let settlement = observer.wait_settlement_until(deadline).await?;
    match &settlement {
        SettlementWait::Ready(report) if report.all_released() => println!("request resources released"),
        SettlementWait::Ready(report) => eprintln!("settlement report has unreleased resources: {report:?}"),
        SettlementWait::Pending(snapshot) => eprintln!("settlement is pending at the deadline: {snapshot:?}"),
    }
    Ok((outcome, observer, settlement))
}

enum ShutdownProgress {
    DetectorPending(ManagedInferenceShutdownReport),
    ManagerPending {
        detector: ManagedInferenceShutdownReport,
        handle: ShutdownHandle,
        snapshot: ShutdownSnapshot,
    },
    Complete { detector: ManagedInferenceShutdownReport, manager: ShutdownReport },
}

fn shutdown_application(
    manager: &OperationManager,
    detector: &MagikaMimeDetector,
) -> Result<ShutdownProgress, Box<dyn Error>> {
    let detector_report = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    if !detector_report.worker_stopped {
        return Ok(ShutdownProgress::DetectorPending(detector_report));
    }
    let handle = manager.request_shutdown(ShutdownMode::Drain);
    match handle.wait_for(Duration::from_secs(30))? {
        ShutdownWait::Completed(report) => Ok(ShutdownProgress::Complete { detector: detector_report, manager: report }),
        ShutdownWait::Pending(snapshot) => Ok(ShutdownProgress::ManagerPending { detector: detector_report, handle, snapshot }),
    }
}
```
<!-- managed-guide-example-end -->

`detect_request` returns the exact tuple
`(OperationOutcome<Option<String>, MimeError>, AsyncOperationObserver, SettlementWait)`.
Inspect the business outcome independently: `Succeeded(Some(mime))` contains a
classification, `Succeeded(None)` means there was no classification,
`Failed(error)` carries the detector error, and the other variants report stopped
or otherwise failed operation work. The observer uses the same `ClockSource`
installed in `ManagerOptions`, and the sample creates its deadline from
`clock.now()` in that source's domain. The application converts its desired
wait duration to that source's tick units and passes `wait_ticks`; ticks are not
assumed to be seconds. `wait_settlement_until(deadline)` returns
`Result<SettlementWait, ObservationError>`. `SettlementWait::Pending(snapshot)`
is a bounded observation, not a release confirmation. The deadline must come
from the operation's clock domain. The example matches both variants and checks
`all_released()` on a ready report. `Ready(report)` is terminal but can contain
negative release evidence, including resources owned elsewhere in the scope;
preserve that report for recovery. `Pending(snapshot)` only means this wait
reached its deadline, so callers may continue observing. When this request's
Provider I/O completion is unconfirmed, Magika retains its capacity and the
scope can report an unreleased resource. Stopping the request still drops its
local future, reader, and driver; that does not confirm native or remote I/O
completion. The built-in cleanup policy cannot receive new completion evidence
for discarded provider I/O, so retrying it cannot release that capacity. A
`RecoveryHandle` can inspect the state, retry the registered policy, or transfer
responsibility; resubmitting inference does not release the prior request.
`shutdown_application` returns `DetectorPending` with the
diagnostic report so the caller can retry detector shutdown before closing the
manager. `ManagerPending` retains both its snapshot and handle for another
`wait_for`; `ShutdownProgress::Complete` carries the final manager report. Keep shutdown at a synchronous application
shutdown boundary. An async-only host should run that boundary on its own
blocking executor facility after it stops admission and awaits active requests.

After runtime shutdown, inspect the shared runtime with `detector.runtime_snapshot()`.
The `ManagedInferenceSnapshot` is a non-atomic observation: fields may reflect
different instants, and counts include every detector sharing that runtime.
`faulted` records a caught inference-session panic; `worker_panicked` only records
an uncaught panic observed while joining the worker.
`unconfirmed_provider_io` counts unreleased requests whose Provider operation
completion or cleanup is unconfirmed. This includes dropped pending I/O and an
input destructor that panicked during teardown. The count alone does not show
whether underlying I/O is still running. A snapshot does not prove settlement.
Retain the
operation observer and use `wait_settlement_until(deadline)` with a deadline from
the same clock domain to observe settlement for that operation's scope.
`Pending(snapshot)` means the bounded wait reached its deadline; `Ready(report)`
is terminal but may contain negative release evidence, including resources
owned by other operations in that scope. `admission` in
`ManagedInferenceShutdownReport` is for aggregate service-group counters, while
`worker_stopped` reports only that the worker was joined.

In a real application, keep the service group, supervisor, and detector for the
application lifetime so request scopes come from the same supervisor, and call
the shutdown function after the application has stopped admitting requests.
`max_bytes` bounds cumulative
sample reads and the runtime accounts that request against its shared byte
budget. This is a logical sampling and owned-payload accounting bound, not a
guarantee about total process memory or provider/network activity. Admission
reserves runtime capacity and this byte budget, then commits the optional scope
child before the first provider `open`, `stat`, `read_all`, `length`, or range-read
call. A closed runtime, full capacity, or cancelled scope is rejected before
those provider calls. The `read_all` fallback transfers its buffer into the
async dispatch path without an explicit second full-size clone. That path
converts the owned `Vec` with `into_boxed_slice` before dispatching input to the
worker; conversion may reallocate to trim unused capacity, so this is not a
zero-allocation guarantee. Sampling and
inference run on the session-owning worker; async provider reads are serviced
by the operation future without blocking its executor.

When no provider read is pending, dropping the request closes its sampling
bridge and releases driver ownership. If a read has returned `Ready`, its I/O
completion is known; capacity is released once the worker also finishes. This
applies to both scope-managed and scope-less provider entry points.

The child business outcome is independent of resource release. Successful
inference reports `Succeeded`; a worker error reports `Failed` while the
caller receives the original error. A worker inference panic caught by the
runtime is also exposed as `Failed(MimeError)`; the runtime snapshot sets
`faulted`. The shutdown report's `worker_panicked` flag instead records an
uncaught panic observed while joining the worker. The terminal child result
remains observable after the caller consumes it and after resources are
released.
After a worker panic, the runtime closes admission, fails queued work with a
backend error, and discards its session because Magika and ONNX Runtime do not
document session recovery after an unwind. The runtime cannot be reused for
model requests; create and register a new runtime to resume inference. This
policy addresses Rust panics caught by the worker and does not cover native
crashes or `panic = abort`.

`shutdown_blocking` uses one deadline for the selected shutdown mode and
worker exit. `Drain` waits for accepted service operations to settle and local
provider access to finish. `Cancel` requests cooperative cancellation and
waits for local provider access to finish. If the first-stage wait reaches its
deadline, shutdown makes one final observation of whether the worker has
already exited, then returns before closing service-group admission or taking
the worker sender. A later call on the retained detector or runtime can
advance shutdown if that wait is unfinished. The report does not identify
which phase timed out: `worker_stopped` and `worker_panicked` only report facts
observed by return. Once the mode-specific wait completes, shutdown closes
service-group admission and the worker sender, then tries to join; a queued
operation may still appear in the resource-group snapshot until its executor
retires it. The deadline bounds
observation of request and worker progress; synchronous cancellation
notifications or panic-payload destruction can make the complete call take
longer. Shutdown does not forcibly interrupt native inference or provider
I/O. `worker_stopped == false` means
join was unconfirmed when the call returned and the worker handle is retained;
`true` means join was confirmed by return, not that every operation resource
was released. Shutdown may make one final join check after the deadline, so the
report does not establish when the worker exited. Use the
operation observer's bounded settlement wait to observe a specific request's
scope; `admission` reports aggregate service-group counters and does not replace
that observation. `worker_panicked` records an uncaught panic
observed while joining. Shutdown does not forcibly interrupt native inference
or provider I/O.

The old `MimeDetector` provider hooks remain useful for callers that do not
manage operation scopes. They cannot attach work to a parent scope because the
current upstream trait does not pass one.

When upgrading a pre-0.14 integration, apply these changes in order:

1. Use Rust 1.94 and upgrade to `qubit-magika 0.18`, `qubit-mime 0.22`,
   `qubit-fs 0.4`, `qubit-spi 0.14`, and `qubit-operation 0.3.2`.
2. Keep provider registration and detector configuration separate: register
   `MagikaMimeDetectorProvider`, select `magika`, then call
   `create_configured(&MimeConfig)`.
3. Replace per-request `MagikaMimeDetector::new()` calls with one startup
   detector shared through `Arc`.
4. If you use provider paths, treat `max_bytes` as a cumulative logical
   sampling budget. Known-length range-capable providers may perform multiple
   bounded reads. Unknown or unavailable lengths use `read_all`, even when
   range capability is advertised; it limits the accepted payload and may probe
   one extra byte. When ranges cannot be used, known oversized resources are
   rejected before that full read; overflow found during `read_all` is a
   filesystem error.
5. If the application owns ONNX Runtime, disable the default feature and use
   the custom configuration shown above before creating the detector.

The provider names `magika`, `magika-mime-detector`, and
`magikamimedetector` remain accepted; use `magika` for new configuration.

## Errors and Diagnostics

Direct calls to `MagikaMimeDetector::new()` and
`MagikaMimeDetector::from_mime_config(...)` create and initialize a runtime
and return `MimeError` if initialization fails. `MagikaMimeDetectorProvider::create_configured` returns
`ProviderFailure<MimeError>` and preserves the initialization `MimeError` as
its source. A builder that injects an existing runtime reuses its initialized
session and reports validation errors, such as an insufficient runtime byte
budget, as `MimeError`; it does not reinitialize that runtime. Detection
returns `MimeError` for input I/O failures, incomplete content, detector or
runtime byte limits, admission rejection, and backend
inference failures. The async content hook can return admission errors because
it uses the shared worker even for rule-classifiable content.

`Ok(None)` means that the selected detector found no MIME candidate; it is not
the same as an initialization or I/O error. Handle initialization errors during
startup and handle per-input errors at the boundary where the input is read.

## Troubleshooting

- If `MagikaMimeDetector::new()` or `from_mime_config(...)` fails, check the
  ONNX Runtime feature choice and the runtime/model error carried by
  `MimeError`; these constructors create and initialize a new runtime. If
  `builder().managed_runtime(...)` or a configured provider fails, check that
  the injected runtime's byte budget is at least the detector's configured
  buffer limit. ONNX Runtime initialization errors apply to paths that create a
  new runtime; `create_configured` wraps the original `MimeError` in
  `ProviderFailure<MimeError>`, while builder injection reuses the runtime and
  reports configuration validation errors such as an insufficient byte budget
  directly as `MimeError`.
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
- Creating a runtime initializes the model and ONNX Runtime session
  synchronously. Direct constructors create a runtime; builder or provider
  runtime injection reuses the supplied runtime. Create and share a detector
  instead of constructing one per input.
- Inference on one detector is serialized internally.
- Legacy `MimeDetector` provider hooks do not provide parent-scope lifecycle
  observation. Use the managed entry point to attach admission, child outcome,
  and resource-release observations to a request scope, while accounting for
  the pending-provider completion limitation above.
- `qubit-magika` does not promise a MIME result for every input; callers must
  handle `Ok(None)` and `MimeError`.
- This crate exposes Magika's mapped MIME result; it does not define a separate
  MIME registry or replace `qubit-mime` configuration.

## Further Reading

- [README](../README.md)
- [中文用户手册](user_guide.zh_CN.md)
- [API documentation](https://docs.rs/qubit-magika)
- [Example](../examples/basic.rs)
