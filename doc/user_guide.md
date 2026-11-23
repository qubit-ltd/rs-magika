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

1. Register `MagikaMimeDetectorProvider` with the global
   `MimeDetectorRegistry` and select the provider name `magika`.
2. Create a detector with `MimeConfig`, which controls the surrounding
   `qubit-mime` selection and refinement behavior.

The detector uses repository filename rules for filename-only detection and
Magika inference for content, reader, and local-file detection. Creating a
detector initializes Magika's embedded model and the ONNX Runtime session.
Inference calls made through one detector are serialized internally, so one
initialized detector can be shared with `Arc`.

For link-time discovery, enable `qubit-magika = { version = "0.18", features =
["inventory"] }`. This submits `MagikaMimeDetectorProvider::new()` to the MIME
detector inventory; `MimeDetectorRegistry::builtin()` then includes `magika`
while retaining `repository` as its default. A provider configured with a
media-stream classifier must still be constructed and registered explicitly.
Reference the crate in the application with `use qubit_magika as _;` so its
inventory submission is linked.

The `qubit-mime` async provider registry selects and constructs an
`AsyncMimeDetectorBackend`; its async hooks still do not receive a parent
operation scope. Inventory only registers a provider with that registry. Use
the explicit managed API when application code must attach provider sampling to
an operation scope. To reproduce the tested local dependency layout, run
[`prepare-local-path-dependencies.sh --check-locked`](../.infra/bin/prepare-local-path-dependencies.sh)
and follow the fixed revisions in
[`local-path-dependencies.tsv`](../.infra/ci/local-path-dependencies.tsv).

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

## Media Refinement Support

A configured media-stream classifier is used only for eligible high-level
inputs. Precise detection must be enabled and the detected MIME type or
filename must match a configured ambiguous MIME mapping.

| Detection entry point | Classifier behavior |
| --- | --- |
| Complete content (`detect_by_content` / `detect`) | May classify the complete bytes |
| Seekable reader | May classify the full reader and restore its position |
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
provide a cross-segment content snapshot. The cumulative `max_bytes` budget
applies across all Magika sampling reads. Without range support, a known resource
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

### Sharing an application-managed inference runtime

The shared runtime and the operation-aware example below use these direct
dependencies. Keep this block alongside the managed examples when copying them
into an application:

<!-- managed-dependencies-start -->
```toml
[dependencies]
qubit-magika = "0.18"
qubit-mime = "0.22"
qubit-operation = "0.3"
qubit-fs = { version = "0.4", default-features = false, features = ["async"] }
```
<!-- managed-dependencies-end -->

Create the runtime during application startup and register a configured
provider explicitly:

<!-- provider-runtime-example-start -->
```rust
use std::error::Error;
use std::sync::Arc;

use qubit_magika::{MagikaMimeDetectorProvider, ManagedInferenceRuntime};
use qubit_mime::{MimeConfig, MimeDetectorRegistry};
use qubit_operation::{ManagerOptions, OperationManager};

fn register_provider() -> Result<Arc<ManagedInferenceRuntime>, Box<dyn Error>> {
    let manager = OperationManager::new(ManagerOptions::default())?;
    let runtime = Arc::new(ManagedInferenceRuntime::new(
        manager,
        MimeConfig::default().max_buffer_size(),
    )?);
    MimeDetectorRegistry::global().register(
        MagikaMimeDetectorProvider::new().managed_runtime(Arc::clone(&runtime)),
    )?;
    Ok(runtime)
}
```
<!-- provider-runtime-example-end -->

Keep the returned `Arc` for the application lifetime and shut the runtime down
after detectors stop accepting requests. Detectors created by this provider
share its worker, request capacity, byte budget, and shutdown domain. The
runtime byte budget must cover each detector's configured `max_buffer_size`.
Inventory submits only `MagikaMimeDetectorProvider::new()` without a runtime;
register a configured provider explicitly when sharing an application-owned
runtime.

## Custom ONNX Runtime Configuration

To use an ONNX Runtime library supplied by the application, disable the bundled
binary and enable `ort`'s dynamic loader. These declarations are copy-pasteable
and keep the versions aligned with `qubit-magika` 0.18:

```toml
[dependencies]
qubit-magika = { version = "0.18", default-features = false }
qubit-mime = "0.22"
qubit-spi = "0.14"
qubit-fs = "0.4"
ort = { version = ">=2.0.0-rc.12,<2.1", default-features = false, features = ["std", "ndarray", "load-dynamic", "api-24"] }
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

The [`dynamic-loader` job in the repository's Linux CI](../.github/workflows/ci.yml)
checks this setup with ONNX Runtime 1.24.2: it initializes the shared library,
constructs a detector, and verifies that Python content is classified as
`text/x-python`. The job checks the Cargo feature graph to ensure bundled runtime
downloads are disabled. `ORT_LIBRARY_PATH` must name the shared-library file
itself, rather than its containing directory.

## Advanced Usage

The provider accepts these selection names: `magika`,
`magika-mime-detector`, and `magikamimedetector`. The canonical name is
`magika`.

`MagikaMimeDetector` also implements the `qubit-mime` backend operations for
seekable readers, local files, and synchronous or asynchronous provider paths.
Reader detection restores the original reader position. Content-backend reader
detection classifies the remaining resource from the current cursor, while the
generic detector API keeps whole-resource semantics. Provider-path detection
enforces `max_bytes` cumulatively across all sampling reads. The legacy route
opens by path for each segment and applies the same conditional ETag to every
segment when available; without that condition, it cannot guarantee a
consistent cross-segment snapshot. Providers without range support may probe one extra byte to
detect overflow and reject oversized resources. Unknown and undefined Magika
types become `Ok(None)` so filename fallback follows the selected
`MimeDetectionPolicy`.

Local content detection opens regular files. Directories and symlinks keep
Magika's existing file-type behavior; other local types such as FIFOs and Unix
sockets return `MimeError::Io` with `ErrorKind::Unsupported`. A high-level
`detect_file` call using `PreferFilename` can return a unique filename match
without inspecting the path or its type. The metadata check and subsequent
open are separate operations, so replacing a path between them is outside this
check's protection.

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
while tracking its explicit scope. Provider `max_bytes` is a cumulative logical
sampling and fallback-read budget. These logical budgets and copied payload
lengths do not cap total process memory, including provider buffers and
transient allocations. After shutdown, ruled sync inputs can still return
directly; requests needing model inference use the closed runtime and fail.

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
    // `clock` is the same source installed in ManagerOptions; ticks are in its units.
    let now = clock.now();
    let deadline = Deadline::at(MonotonicTime::new(now.domain(), now.ticks().saturating_add(30)));
    let settlement = observer.wait_settlement_until(deadline).await?;
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
`clock.now()` in that source's domain. `wait_settlement_until(deadline)` returns
`Result<SettlementWait, ObservationError>`. `SettlementWait::Pending(snapshot)`
is a bounded observation, not a release confirmation. The deadline must come
from the operation's clock domain. Keep the observer to continue checking later,
and inspect a ready report with `SettlementReport::all_released()`. Failed
cleanup retains an unconfirmed resource for recovery observation; retry is
available only through the registered recovery policy/handle, not by resubmitting
the inference request. `shutdown_application` returns `DetectorPending` with the
diagnostic report so the caller can retry detector shutdown before closing the
manager. `ManagerPending` retains both its snapshot and handle for another
`wait_for`; `Completed` carries the final manager report. Keep shutdown at a synchronous application
shutdown boundary. An async-only host should run that boundary on its own
blocking executor facility after it stops admission and awaits active requests.

After runtime shutdown, inspect the shared runtime with `detector.runtime_snapshot()`.
The `ManagedInferenceSnapshot` is a non-atomic observation: fields may reflect
different instants, and counts include every detector sharing that runtime.
`faulted` records a caught inference-session panic; `worker_panicked` only records
an uncaught panic observed while joining the worker. A nonzero
`unconfirmed_provider_io` means a pending Provider operation was dropped before
completion could be confirmed. A snapshot does not prove settlement; retain the
operation observer and use `wait_settlement_until(deadline)` with a deadline from
the same clock domain for bounded release observation.

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
those provider calls. The `read_all` fallback transfers its buffer to the
worker instead of making another full-size copy. Sampling and inference run on
the session-owning worker; async provider reads are serviced by the operation
future without blocking its executor.

Provider future cancellation has a narrower meaning than native I/O
cancellation. The `rs-fs` contract says dropping a pending provider future
stops local polling, but provider-native or remote work may continue. Magika
has no completion observer for that work and never treats future `Drop` as
proof that I/O ended. If a pending provider future is dropped and completion
cannot be observed, the child release observation and runtime/byte capacity
remain `Pending`, potentially indefinitely. Cancellation can stop the
caller from waiting promptly; it does not guarantee that native work was
interrupted.

When no provider read is pending, dropping the request closes its sampling
bridge and releases driver ownership. If a read has returned `Ready`, its I/O
completion is known; capacity is released once the worker also finishes. This
applies to both scope-managed and scope-less provider entry points.

The child business outcome is independent of resource release. Successful
inference reports `Succeeded`; a worker error reports `Failed` while the
caller receives the original error, and a worker panic reports
`Panicked`. The terminal child result remains observable after the caller
consumes the result and after resources are released.
After a worker panic, the runtime closes admission, fails queued work with a
backend error, and discards its session because Magika and ONNX Runtime do not
document session recovery after an unwind. The runtime cannot be reused for
model requests; create and register a new runtime to resume inference. This
policy addresses Rust panics caught by the worker and does not cover native
crashes or `panic = abort`.

`shutdown_blocking` uses one deadline for service admission/settlement and
worker exit. On timeout, `worker_stopped == false` means the worker has not
exited and its handle is retained; a later call can continue observing and
reap it. Shutdown does not forcibly interrupt native inference or provider
I/O.

The old `MimeDetector` provider hooks remain useful for callers that do not
manage operation scopes. They cannot attach work to a parent scope because the
current upstream trait does not pass one.

When upgrading a pre-0.14 integration, apply these changes in order:

1. Upgrade to `qubit-magika 0.18`, `qubit-mime 0.22`, `qubit-fs 0.4`,
   `qubit-spi 0.14`, and Rust 1.94.
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
content, detector or runtime byte limits, admission rejection, and backend
inference failures. The async content hook can return admission errors because
it uses the shared worker even for rule-classifiable content.

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
