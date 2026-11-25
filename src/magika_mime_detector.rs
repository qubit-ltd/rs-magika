// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Magika-backed MIME detector implementation with a session-owning worker.

use std::future::Future;
use std::io::SeekFrom;
use std::path::Path;
use std::pin::Pin;

use magika::ContentType;
use magika::Error;
use magika::FeaturesOrRuled;
use magika::FileType;
use magika::SyncInput;
use qubit_fs::AsyncFileSystem;
use qubit_fs::FileSystem;
use qubit_fs::Path as FsPath;
use qubit_fs::metadata::FileSystemCapability;
use qubit_fs::read::ReadOptions;
use qubit_io::std_io::ReadSeek;
use qubit_mime::ContentRequirement;
use qubit_mime::MimeConfig;
use qubit_mime::MimeContentBackend;
use qubit_mime::MimeDetectorBackend;
use qubit_mime::MimeDetectorCore;
use qubit_mime::MimeError;
use qubit_mime::MimeResult;
use qubit_mime::RepositoryMimeDetector;
use qubit_operation::ChildRequirement;
use qubit_operation::OperationOptions;
use qubit_operation::OperationScope;
use qubit_operation::ShutdownMode;

use crate::internal::AsyncProviderInput;
use crate::internal::ManagedReadObserver;
use crate::internal::PROVIDER_OUTCOME;
use crate::internal::ProviderPathRoute;
use crate::internal::ReadSeekInput;
use crate::internal::ReaderScope;
use crate::internal::StopWatcher;
use crate::internal::SyncProviderInput;
use crate::internal::is_unsupported_provider_error;
use crate::internal::map_operation_outcome;
use crate::internal::map_provider_magika_error;
use crate::internal::select_provider_path_route;
use crate::managed_runtime::ManagedInferenceRuntime;
use crate::managed_runtime::ManagedInferenceShutdownReport;
use crate::managed_runtime::ManagedInferenceSnapshot;
use crate::managed_runtime::request_admission_handoff::RequestAdmissionHandoff;

/// MIME detector backed by Google's Magika model.
///
/// Creating a new runtime initializes Magika's embedded model and ONNX Runtime
/// session synchronously before construction returns. When a runtime is
/// supplied through the builder or provider, it is reused as-is and no second
/// session is initialized. Applications should create the detector once and
/// share it, for example through [`std::sync::Arc`]. One worker owns the Magika
/// session and serializes model inference. Synchronous MIME methods wait for worker
/// results; async provider methods service bounded worker range requests
/// without blocking their executor. The operation-aware provider method
/// additionally registers sampling and inference in an explicit scope.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
///
/// use qubit_magika::MagikaMimeDetector;
/// use qubit_mime::{MimeDetector, MimeError};
///
/// let detector = Arc::new(MagikaMimeDetector::new()?);
/// let mime_type = detector.detect_by_content(b"%PDF-1.7")?;
/// assert_eq!(Some("application/pdf".to_owned()), mime_type);
/// # Ok::<(), MimeError>(())
/// ```
#[derive(Debug)]
pub struct MagikaMimeDetector {
    /// Shared detector behavior used for result selection and refinement.
    pub(crate) core: MimeDetectorCore,
    /// Repository detector used for filename-only detection.
    pub(crate) filename_detector: RepositoryMimeDetector<'static>,
    /// Shared bounded runtime whose worker owns the Magika session.
    pub(crate) runtime: std::sync::Arc<ManagedInferenceRuntime>,
}

impl MagikaMimeDetector {
    /// Creates a detector builder.
    #[must_use]
    #[inline]
    pub fn builder() -> crate::MagikaMimeDetectorBuilder {
        crate::MagikaMimeDetectorBuilder::default()
    }

    /// Creates a detector using [`MimeConfig::default`] and a new Magika
    /// session.
    ///
    /// # Returns
    ///
    /// An initialized detector ready for blocking inference.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::DetectorBackend`] when the default operation
    /// manager or its service group cannot be created, when the worker cannot
    /// start, or when Magika reports a non-I/O initialization failure.
    /// Magika initialization I/O failures are returned as [`MimeError::Io`].
    pub fn new() -> MimeResult<Self> {
        Self::from_mime_config(MimeConfig::default())
    }

    /// Creates a detector using MIME selection/refinement configuration and a
    /// new Magika session.
    ///
    /// Applies MIME configuration for result selection and filename fallback.
    /// This constructor does not attach a media-stream classifier; configure
    /// one through [`crate::MagikaMimeDetectorBuilder`] or
    /// [`crate::MagikaMimeDetectorProvider::with_media_stream_classifier`]
    /// when media refinement is required. Magika's embedded model and runtime
    /// configuration remain owned by the upstream Magika session.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use qubit_magika::MagikaMimeDetector;
    /// use qubit_mime::MimeConfig;
    ///
    /// let detector = MagikaMimeDetector::from_mime_config(MimeConfig::default())?;
    /// # Ok::<(), qubit_mime::MimeError>(())
    /// ```
    ///
    /// # Parameters
    ///
    /// * `config` - MIME configuration used for result selection and filename
    ///   fallback.
    ///
    /// # Returns
    ///
    /// An initialized detector ready for blocking inference.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::DetectorBackend`] when the default operation
    /// manager or its service group cannot be created, when the worker cannot
    /// start, or when Magika reports a non-I/O initialization failure.
    /// Magika initialization I/O failures are returned as [`MimeError::Io`].
    /// A zero `MimeConfig::max_buffer_size()` is rejected with
    /// [`MimeError::DetectorBackend`] when this constructor creates a new
    /// runtime. To reuse a shared runtime, configure it through
    /// [`crate::MagikaMimeDetectorBuilder::managed_runtime`].
    #[inline]
    pub fn from_mime_config(config: MimeConfig) -> MimeResult<Self> {
        Self::builder().mime_config(config).build()
    }

    /// Detect owned bytes as a child operation of `scope`.
    ///
    /// The scope observes result delivery and worker release separately. The
    /// existing `MimeDetector` async hooks do not receive a scope and do not
    /// carry this managed lifecycle guarantee.
    ///
    /// # Parameters
    ///
    /// * `scope` - Parent scope tracking result and worker release.
    /// * `content` - Owned bytes retained until sampling and inference finish.
    ///
    /// # Returns
    ///
    /// A MIME mapping or `None` for an unmapped Magika type.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::BufferLimitExceeded`] when `content` exceeds this
    /// detector's per-request owned-payload maximum, or an admission, shared
    /// runtime aggregate byte budget, Magika, or worker error. These logical
    /// payload budgets do not guarantee a total process-memory limit.
    pub async fn detect_bytes_managed(&self, scope: &OperationScope, content: Vec<u8>) -> MimeResult<Option<String>> {
        let limit = self.max_test_bytes();
        if content.len() > limit {
            return Err(MimeError::BufferLimitExceeded {
                requested: content.len(),
                limit,
            });
        }
        self.runtime.detect_bytes(scope, content).await
    }

    /// Detects an async provider path as a child of `scope`, extracting
    /// Magika samples on the managed worker through bounded range requests.
    ///
    /// This explicit API owns an operation scope. The legacy MIME trait hook
    /// remains scope-less because its upstream contract does not pass one.
    /// Before the first provider call (`open`, `stat`, `read_all`, `length`, or
    /// a range read), the runtime reserves request capacity and the
    /// `max_bytes` accounting budget and commits the optional scope child.
    /// Closed, full, or already-cancelled requests are rejected before
    /// provider I/O. Admission capacity is a permission/accounting reservation;
    /// it does not prove provider completion or resource release. The byte
    /// reservation bounds logical sampling and owned payload accounting; it
    /// is not a bound on total process memory or provider/network work. The
    /// `read_all` fallback moves its returned `Vec` into the async
    /// dispatch path without cloning its contents. That path converts it with
    /// `into_boxed_slice` before dispatching the input to the worker; the
    /// conversion may reallocate when the vector has spare capacity, so this
    /// is not a guarantee of zero-copy operation.
    ///
    /// Dropping a pending provider future cancels local polling only. A
    /// provider's native or remote operation may continue, and this API has no
    /// completion observer for that work. Resource release therefore remains
    /// unconfirmed until completion evidence is available; bounded settlement
    /// observation can still produce a terminal report with unreleased
    /// resources. Future cancellation does not prove that underlying I/O was
    /// interrupted.
    /// The returned future reports the child business outcome; the parent
    /// scope observer separately reports settlement. A bounded observation
    /// can return `Pending(snapshot)` or terminal `Ready(report)` with
    /// `all_released() == false`. A negative report can concern other scope
    /// resources and does not by itself show that this request retains
    /// capacity. This request retains capacity while provider I/O completion
    /// is unconfirmed or the worker may still access request resources.
    /// Built-in cleanup has no input for completion evidence from discarded
    /// provider I/O, so retrying its current policy cannot release that
    /// capacity. A `RecoveryHandle` can inspect the state, retry the policy, or
    /// transfer responsibility.
    ///
    /// A Rust panic caught during session inference makes this future return
    /// `MimeError::DetectorBackend`; the public child `OperationOutcome` is
    /// `Failed(MimeError)`. The runtime also records its internal panic fact
    /// and becomes faulted. This is distinct from
    /// [`ManagedInferenceShutdownReport::worker_panicked`], which reports an
    /// uncaught panic observed while joining the worker.
    ///
    /// # Parameters
    ///
    /// * `scope` - Parent operation scope for cancellation and settlement.
    /// * `file_system` - Async provider serving bounded range reads.
    /// * `path` - Provider path used to open or read the resource.
    /// * `max_bytes` - Cumulative logical sampling and fallback-read budget. It
    ///   is checked against this detector's request limit and the shared
    ///   runtime aggregate budget; it does not cap total process memory or
    ///   provider-owned buffering.
    ///
    /// # Returns
    ///
    /// A MIME mapping or `None` for an unmapped Magika type.
    ///
    /// # Errors
    ///
    /// Returns provider, size-limit, admission, Magika, or worker errors.
    pub async fn detect_async_provider_path_managed(
        &self,
        scope: &OperationScope,
        file_system: &AsyncFileSystem,
        path: &FsPath,
        max_bytes: usize,
    ) -> MimeResult<Option<String>> {
        let limit = self.max_test_bytes();
        if max_bytes > limit {
            return Err(MimeError::BufferLimitExceeded {
                requested: max_bytes,
                limit,
            });
        }
        // This admission is created before the child operation is submitted;
        // attach its resource scope only after the child body starts.
        let admission = self.runtime.reserve_for_provider(None, max_bytes)?;
        let runtime = std::sync::Arc::clone(&self.runtime);
        let file_system = file_system.clone();
        let path = path.clone();
        let options = OperationOptions {
            requirement: ChildRequirement::Optional,
            ..OperationOptions::default()
        };
        let (handoff, mut handoff_guard) = RequestAdmissionHandoff::new(admission);
        let body_handoff = std::sync::Arc::clone(&handoff);
        let mut operation = scope
            .manager()
            .as_async()
            .try_submit(options, move |child_scope| async move {
                body_handoff.body_started();
                let admission = match body_handoff.wait_for_child_admission(&child_scope).await {
                    Some(Ok((admission, None))) => admission,
                    Some(Ok((admission, Some(_input)))) => {
                        drop(admission);
                        return Err(MimeError::detector_backend(
                            "magika",
                            "provider handoff unexpectedly contained worker input",
                        ));
                    }
                    Some(Err(error)) => return Err(error),
                    None => {
                        return Err(MimeError::detector_backend(
                            "magika",
                            "request admission handoff closed before registration",
                        ));
                    }
                };
                Self::detect_async_provider_admitted(runtime, &child_scope, admission, &file_system, &path, max_bytes)
                    .await
            })
            .map_err(|error| {
                MimeError::detector_backend(
                    "magika",
                    format!("operation scope rejected inference: provider operation submission failed: {error:?}"),
                )
            })?;
        let mut outcome = Box::pin(operation.wait_outcome());
        let outcome = match handoff
            .register_on_parent(scope, &mut handoff_guard, &mut outcome)
            .await
        {
            crate::managed_runtime::request_admission_handoff::BodyStartOr::Outcome(outcome) => outcome,
            crate::managed_runtime::request_admission_handoff::BodyStartOr::BodyStarted => outcome.await,
            crate::managed_runtime::request_admission_handoff::BodyStartOr::Closed(reason) => {
                drop(outcome);
                let _ = operation.request_stop(qubit_operation::StopReason::UserCancelled);
                return Err(MimeError::detector_backend("magika", reason.as_str()));
            }
        };
        let outcome = outcome.map_err(|error| {
            MimeError::detector_backend("magika", format!("provider operation outcome unavailable: {error:?}"))
        })?;
        map_operation_outcome(outcome, PROVIDER_OUTCOME)
    }

    /// Runs the shared provider route after its request and byte capacity are
    /// reserved.
    ///
    /// The admission remains owned through sampling and worker inference so
    /// provider operation accounting and cancellation release semantics stay
    /// attached to the accepted request.
    ///
    /// # Parameters
    ///
    /// * `admission` - Capacity reservation created by the selected public
    ///   entry point.
    /// * `runtime` - Shared worker runtime that owns Magika inference.
    /// * `scope` - Operation scope used to retain provider work and observe
    ///   cancellation.
    /// * `file_system` - Async provider serving the requested path.
    /// * `path` - Provider path used for sampling or fallback reading.
    /// * `max_bytes` - Cumulative sampling and fallback-read budget.
    ///
    /// # Returns
    ///
    /// A MIME mapping or `None` when Magika has no MIME mapping.
    ///
    /// # Errors
    ///
    /// Returns provider, size-limit, admission, Magika, or worker errors.
    async fn detect_async_provider_admitted(
        runtime: std::sync::Arc<ManagedInferenceRuntime>,
        scope: &OperationScope,
        mut admission: crate::managed_runtime::request_admission::RequestAdmission,
        file_system: &AsyncFileSystem,
        path: &FsPath,
        max_bytes: usize,
    ) -> MimeResult<Option<String>> {
        admission.retain_in_scope(scope)?;
        let open_result = admission
            .run_provider_operation(
                scope,
                file_system.open_random_access_reader(path, ReadOptions::default()),
            )
            .await?;
        match open_result {
            Ok(reader) => {
                let max_segment = file_system
                    .properties()
                    .limits()
                    .max_read_range_bytes()
                    .maximum()
                    .and_then(|value| usize::try_from(value).ok());
                let input = AsyncProviderInput::from_session(reader, max_bytes, max_segment)
                    .with_stop_state(admission.task_state())
                    .with_stop_watcher(StopWatcher::new(scope.context()));
                return runtime
                    .detect_async_input_admitted(scope, admission, input, max_bytes, map_provider_magika_error)
                    .await;
            }
            Err(error) if is_unsupported_provider_error(&error) => {}
            Err(error) => return Err(error.into()),
        }

        let stat_result = admission.run_provider_operation(scope, file_system.stat(path)).await?;
        let metadata = match stat_result {
            Ok(metadata) => Some(metadata),
            Err(error) if is_unsupported_provider_error(&error) => None,
            Err(error) => return Err(error.into()),
        };
        let capabilities = file_system.properties().capabilities();
        let route = select_provider_path_route(
            metadata.as_ref().and_then(|metadata| metadata.len()),
            metadata.as_ref().and_then(|metadata| metadata.etag().cloned()),
            capabilities.supports(FileSystemCapability::RangeRead),
            capabilities.supports(FileSystemCapability::ConditionalRead),
            max_bytes,
        );
        match route {
            ProviderPathRoute::Range { length, version } => {
                let input =
                    AsyncProviderInput::new_owned(file_system.clone(), path.clone(), length, version, max_bytes)
                        .with_stop_state(admission.task_state())
                        .with_stop_watcher(StopWatcher::new(scope.context()));
                return runtime
                    .detect_async_input_admitted(scope, admission, input, max_bytes, map_provider_magika_error)
                    .await;
            }
            ProviderPathRoute::TooLarge { requested } => {
                return Err(MimeError::BufferLimitExceeded {
                    requested,
                    limit: max_bytes,
                });
            }
            ProviderPathRoute::ReadAll => {}
        }
        let io_in_flight = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut observer = ManagedReadObserver::new(
            Some(admission.task_state()),
            std::sync::Arc::clone(&io_in_flight),
            Some(StopWatcher::new(scope.context())),
        );
        let read_result = admission
            .run_provider_operation_with_status(
                scope,
                file_system.read_all_observed(path, ReadOptions::default(), max_bytes, &mut observer),
                io_in_flight,
            )
            .await?;
        let bytes = match read_result {
            Ok(bytes) => bytes,
            Err(qubit_fs::read::ObservedReadError::Stopped) => {
                return Err(MimeError::detector_backend("magika", "provider read stopped"));
            }
            Err(qubit_fs::read::ObservedReadError::Provider(error)) => return Err(error.into()),
        };
        runtime.detect_bytes_admitted(admission, bytes).await
    }

    /// Stops managed inference admission and observes accepted worker jobs.
    ///
    /// The timeout is one deadline for the mode-specific wait and worker exit
    /// attempt. `Drain` waits for accepted service operations to settle;
    /// `Cancel` waits for local provider access to finish before shutdown
    /// closes the worker sender. If the deadline expires before the worker
    /// exits,
    /// [`ManagedInferenceShutdownReport::worker_stopped`] is false and the
    /// runtime retains the worker handle; a later call can continue observing
    /// and reap it. Shutdown does not forcibly interrupt native inference or
    /// provider I/O.
    ///
    /// # Parameters
    ///
    /// * `mode` - Drain accepted requests or request cooperative cancellation.
    /// * `timeout` - Maximum total wait for the mode-specific request wait and
    ///   worker exit attempt.
    ///
    /// # Returns
    ///
    /// After its mode-specific wait, shutdown closes the worker sender and
    /// attempts to join it. `Drain` waits for accepted service operations to
    /// settle. `Cancel` waits for local provider access to finish first. Queued
    /// operations can remain in the service-group snapshot until their
    /// executor retires them. `worker_stopped` confirms the worker join only;
    /// it can be true while an unconfirmed provider request still retains
    /// capacity. A false value means join is unconfirmed; a later call can
    /// continue observing the retained worker handle. `admission` contains
    /// aggregate service-group counters and is not a per-request release
    /// guarantee. To observe a request's resources, retain its operation
    /// observer and use its bounded settlement wait for that request's scope.
    /// `Pending(snapshot)` means that wait reached its deadline. `Ready(report)`
    /// is terminal but may still contain negative release evidence; resources
    /// owned by other operations in the scope can also appear in its report.
    pub fn shutdown_blocking(
        &self,
        mode: ShutdownMode,
        timeout: std::time::Duration,
    ) -> ManagedInferenceShutdownReport {
        self.runtime.shutdown_blocking(mode, timeout)
    }

    /// Reads non-atomic diagnostic state for this detector's shared runtime.
    ///
    /// Counts include every detector that shares the runtime; this snapshot
    /// does not prove resource release.
    pub fn runtime_snapshot(&self) -> ManagedInferenceSnapshot {
        self.runtime.snapshot()
    }

    /// Gets filename candidates from the repository detector.
    ///
    /// # Parameters
    ///
    /// * `filename` - Filename or path.
    ///
    /// # Returns
    ///
    /// Candidate MIME type names.
    #[inline]
    fn guess_from_filename(&self, filename: &str) -> Vec<String> {
        self.filename_detector.guess_from_filename(filename)
    }

    /// Gets content candidates from a Magika-compatible synchronous input.
    ///
    /// `I` is any input implementing [`SyncInput`]. Borrowed synchronous
    /// content is sampled in the calling sync path and is not rejected merely
    /// because its logical length exceeds `MimeConfig::max_buffer_size()`;
    /// this avoids copying the complete payload. Sampling and feature
    /// extraction run in the calling sync path; model inference is serialized
    /// by the shared worker. Rule-classified inputs do not use the session.
    ///
    /// # Type Parameters
    ///
    /// * `I` - Synchronous Magika input used for sampling and feature
    ///   extraction.
    ///
    /// # Parameters
    ///
    /// * `input` - Content input accepted by Magika.
    ///
    /// # Returns
    ///
    /// Zero or one MIME type candidates.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] for input I/O failures or
    /// [`MimeError::DetectorBackend`] when inference fails.
    fn guess_from_magika_input<I>(&self, input: I) -> MimeResult<Vec<String>>
    where
        I: SyncInput,
    {
        let extracted = FeaturesOrRuled::extract_sync(input).map_err(map_magika_error)?;
        self.guess_from_extracted(extracted, map_magika_error)
    }

    /// Maps ruled classifications directly and serializes only model inference.
    ///
    /// The supplied mapper preserves the error contract of the input route that
    /// extracted the features. Ruled results do not acquire the worker session.
    ///
    /// # Parameters
    ///
    /// * `extracted` - Rules result or features extracted without holding the
    ///   session worker.
    /// * `map_error` - Converts upstream inference errors for the originating
    ///   route.
    ///
    /// # Returns
    ///
    /// Zero or one MIME type candidates.
    ///
    /// # Errors
    ///
    /// Returns a backend error if inference fails.
    fn guess_from_extracted(
        &self,
        extracted: FeaturesOrRuled,
        map_error: fn(Error) -> MimeError,
    ) -> MimeResult<Vec<String>> {
        match extracted {
            FeaturesOrRuled::Ruled(content_type) => Ok(content_type_to_mime(content_type).into_iter().collect()),
            FeaturesOrRuled::Features(features) => Ok(self
                .runtime
                .identify_features_blocking(features, map_error)?
                .into_iter()
                .collect()),
        }
    }

    /// Gets content candidates from a local file using Magika.
    ///
    /// # Parameters
    ///
    /// * `file` - Local file path.
    ///
    /// # Returns
    ///
    /// Zero or one MIME type candidates.
    ///
    /// Only regular files are opened for content detection. Symlinks and
    /// directories retain Magika's file-type handling; other local file types
    /// return [`MimeError::Io`] with [`std::io::ErrorKind::Unsupported`].
    /// Metadata inspection and opening are separate operations, so this check
    /// does not prevent a path from being replaced between them.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] for file metadata/read failures or unsupported
    /// local file types, or [`MimeError::DetectorBackend`] when inference
    /// fails.
    fn guess_from_magika_file(&self, file: &Path) -> MimeResult<Vec<String>> {
        let metadata = std::fs::symlink_metadata(file)?;
        let file_type = if metadata.file_type().is_symlink() {
            FileType::Symlink
        } else if metadata.is_dir() {
            FileType::Directory
        } else if metadata.is_file() {
            return self.guess_from_magika_input(std::fs::File::open(file)?);
        } else {
            return Err(MimeError::Io(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "local MIME content detection requires a regular file",
            )));
        };
        Ok(file_type
            .content_type()
            .and_then(content_type_to_mime)
            .into_iter()
            .collect())
    }

    /// Gets content candidates from a seekable reader.
    ///
    /// `WholeResource` inspects the complete resource, while `Remaining`
    /// inspects the window from the reader's original position to EOF. Once
    /// the original position is known, the reader is restored after detection
    /// succeeds or fails. A successful restore preserves the detection result
    /// or its original error. If the restore seek fails, its I/O error takes
    /// precedence and the reader position is unspecified.
    ///
    /// # Parameters
    ///
    /// * `reader` - Reader to inspect. Its original stream position is restored
    ///   when the restore seek succeeds.
    ///
    /// # Returns
    ///
    /// Zero or one MIME type candidates.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] when reading or seeking fails, including when
    /// the restore seek fails after detection. A restore-seek error takes
    /// precedence over a detection error, and the reader position is
    /// unspecified if restoration fails. Returns [`MimeError::DetectorBackend`]
    /// when Magika inference or worker inference fails and restoration
    /// succeeds.
    fn guess_from_reader_with_scope(&self, reader: &mut dyn ReadSeek, scope: ReaderScope) -> MimeResult<Vec<String>> {
        let original_position = reader.stream_position()?;
        let result = (|| {
            let end = reader.seek(SeekFrom::End(0))?;
            let base_offset = match scope {
                ReaderScope::WholeResource => 0,
                ReaderScope::Remaining => original_position,
            };
            let length = end.checked_sub(base_offset).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "reader end precedes window start")
            })?;
            let mut input = ReadSeekInput::new(reader, base_offset, length);
            self.guess_from_magika_input(&mut input)
        })();
        let restore_result = reader.seek(SeekFrom::Start(original_position));
        match (result, restore_result) {
            (Ok(candidates), Ok(_)) => Ok(candidates),
            (Err(error), Ok(_)) => Err(error),
            (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(MimeError::Io(error)),
        }
    }
}

impl MimeDetectorBackend for MagikaMimeDetector {
    /// Reports that this backend requires complete content.
    fn content_requirement(&self) -> ContentRequirement {
        ContentRequirement::Complete
    }

    /// Gets shared selection and refinement behavior.
    fn core(&self) -> &MimeDetectorCore {
        &self.core
    }

    /// Returns the configured maximum content buffer size.
    fn max_test_bytes(&self) -> usize {
        self.core.max_buffer_size()
    }

    /// Detects a MIME type from repository filename rules.
    ///
    /// # Parameters
    ///
    /// * `filename` - Filename or path to match.
    ///
    /// # Returns
    ///
    /// The returned vector contains zero or more repository MIME candidates.
    #[inline]
    fn guess_from_filename(&self, filename: &str) -> Vec<String> {
        self.guess_from_filename(filename)
    }

    /// Detects MIME candidates from an already extracted native basename.
    ///
    /// # Parameters
    ///
    /// * basename - Native basename to match without splitting path separators
    ///   that may be ordinary filename characters.
    ///
    /// # Returns
    ///
    /// Repository MIME candidates in repository order.
    #[inline]
    fn guess_from_filename_basename(&self, basename: &str) -> Vec<String> {
        MimeDetectorBackend::guess_from_filename_basename(&self.filename_detector, basename)
    }

    /// Detects MIME candidates from complete bytes.
    ///
    /// # Parameters
    ///
    /// * `content` - Complete content bytes to inspect.
    ///
    /// # Returns
    ///
    /// The returned vector contains zero or more MIME candidates. Borrowed
    /// input is sampled without applying the owned-content length limit, so
    /// the input's logical length is not a process-memory bound. Filename and
    /// policy selection are handled by the higher-level detector API.
    ///
    /// # Errors
    /// Returns Magika inference or worker errors. This low-level hook only
    /// maps Magika candidates; media-stream refinement belongs to the higher-
    /// level detector API.
    fn guess_from_content(&self, content: &[u8]) -> MimeResult<Vec<String>> {
        self.guess_from_magika_input(content)
    }

    /// Classifies content asynchronously on the session-owning worker.
    ///
    /// The input is copied only after the runtime accepts the request. The
    /// worker performs feature sampling and inference, so polling this future
    /// does not wait for model execution. Unlike managed APIs, this legacy
    /// hook has no parent operation scope and exposes only its business result,
    /// not a settlement observer. Ordinary completion waits for the internal
    /// operation to settle; runtime `Cancel` may return its stop error first
    /// while the worker still retains input and byte capacity. It uses both
    /// this detector's per-request owned-payload limit and the shared runtime's
    /// aggregate owned-payload byte budget. These logical byte budgets do
    /// not bound all process memory, provider buffers, or transient
    /// allocations. Successful return reports the hook's MIME result; the
    /// hook has no public operation handle or separate settlement result.
    ///
    /// # Parameters
    ///
    /// * `content` - Complete content bytes to inspect.
    ///
    /// # Returns
    ///
    /// Zero or one Magika MIME candidates.
    ///
    /// # Errors
    ///
    /// Returns a size-limit, admission, Magika, or worker error.
    fn guess_from_content_async<'a>(
        &'a self,
        content: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = MimeResult<Vec<String>>> + Send + 'a>> {
        Box::pin(async move {
            let limit = self.max_test_bytes();
            if content.len() > limit {
                return Err(MimeError::BufferLimitExceeded {
                    requested: content.len(),
                    limit,
                });
            }
            Ok(self
                .runtime
                .detect_content_unmanaged(content)
                .await?
                .into_iter()
                .collect())
        })
    }

    /// Detects MIME candidates from the complete resource in a seekable reader.
    ///
    /// Once the original position is known, detection attempts to restore it
    /// even if detection returns an error. A successful restore preserves the
    /// detection result or its original error. If the restore seek fails, its
    /// I/O error is returned and the reader position is unspecified.
    ///
    /// # Parameters
    ///
    /// * `reader` - Seekable content source. Its position is restored when the
    ///   restore seek succeeds.
    ///
    /// # Returns
    ///
    /// Returns zero or more MIME candidates and an empty additional-content
    /// buffer. The higher-level detector selects a MIME type and may apply
    /// media refinement.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] when reading or seeking fails, including when
    /// the restore seek fails after detection. A restore-seek error takes
    /// precedence over a detection error, and the reader position is
    /// unspecified if restoration fails. Returns [`MimeError::DetectorBackend`]
    /// when Magika inference or worker inference fails and restoration
    /// succeeds.
    fn guess_from_reader(&self, reader: &mut dyn ReadSeek) -> MimeResult<(Vec<String>, Vec<u8>)> {
        Ok((
            self.guess_from_reader_with_scope(reader, ReaderScope::WholeResource)?,
            Vec::new(),
        ))
    }

    /// Detects a MIME type from a local file.
    ///
    /// # Parameters
    ///
    /// * `file` - Local file to inspect.
    ///
    /// # Returns
    ///
    /// Zero or more MIME candidates and an empty additional-content buffer.
    /// The higher-level detector selects a MIME type and may apply media
    /// refinement.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] when file metadata or content cannot be read,
    /// or when the local path is neither a regular file, symlink, nor
    /// directory. Non-regular file types return `ErrorKind::Unsupported`.
    /// Metadata inspection and opening are separate operations, so path
    /// replacement between them is not prevented. Returns
    /// [`MimeError::DetectorBackend`] when inference fails or worker inference
    /// fails.
    fn guess_from_file(&self, file: &Path) -> MimeResult<(Vec<String>, Vec<u8>)> {
        Ok((self.guess_from_magika_file(file)?, Vec::new()))
    }

    /// Detects MIME candidates from a synchronous filesystem path.
    ///
    /// The complete file length is reported to Magika, while the cumulative
    /// bytes requested by all reads are bounded by `max_bytes`.
    ///
    /// # Parameters
    ///
    /// * `file_system` - Filesystem used to access the path.
    /// * `path` - File path to inspect.
    /// * `max_bytes` - Maximum cumulative bytes allowed across all reads.
    ///
    /// # Errors
    ///
    /// Returns a MIME error when metadata, filesystem reads, or Magika
    /// inference fails.
    fn guess_from_provider_path(
        &self,
        file_system: &FileSystem,
        path: &FsPath,
        max_bytes: usize,
    ) -> MimeResult<(Vec<String>, Option<Vec<u8>>)> {
        let limit = self.max_test_bytes();
        if max_bytes > limit {
            return Err(MimeError::BufferLimitExceeded {
                requested: max_bytes,
                limit,
            });
        }
        match file_system.open_random_access_reader(path, ReadOptions::default()) {
            Ok(reader) => {
                let max_segment = file_system
                    .properties()
                    .limits()
                    .max_read_range_bytes()
                    .maximum()
                    .and_then(|value| usize::try_from(value).ok());
                let input = SyncProviderInput::from_session(reader, max_bytes, max_segment);
                let extracted = FeaturesOrRuled::extract_sync(input).map_err(map_provider_magika_error)?;
                let candidates = self.guess_from_extracted(extracted, map_provider_magika_error)?;
                return Ok((candidates, None));
            }
            Err(error) if is_unsupported_provider_error(&error) => {}
            Err(error) => return Err(error.into()),
        }
        let metadata = match file_system.stat(path) {
            Ok(metadata) => Some(metadata),
            Err(error) if is_unsupported_provider_error(&error) => None,
            Err(error) => return Err(error.into()),
        };
        let capabilities = file_system.properties().capabilities();
        let route = select_provider_path_route(
            metadata.as_ref().and_then(|metadata| metadata.len()),
            metadata.as_ref().and_then(|metadata| metadata.etag().cloned()),
            capabilities.supports(FileSystemCapability::RangeRead),
            capabilities.supports(FileSystemCapability::ConditionalRead),
            max_bytes,
        );
        match route {
            ProviderPathRoute::Range { length, version } => {
                let input = SyncProviderInput::new(file_system, path, length, version, max_bytes);
                let extracted = FeaturesOrRuled::extract_sync(input).map_err(map_provider_magika_error)?;
                let candidates = self.guess_from_extracted(extracted, map_provider_magika_error)?;
                return Ok((candidates, None));
            }
            ProviderPathRoute::TooLarge { requested } => {
                return Err(MimeError::BufferLimitExceeded {
                    requested,
                    limit: max_bytes,
                });
            }
            ProviderPathRoute::ReadAll => {}
        }
        let bytes = file_system.read_all(path, ReadOptions::default(), max_bytes)?;
        let candidates = self.guess_from_magika_input(bytes.as_slice())?;
        Ok((candidates, None))
    }

    /// Detects MIME candidates from an asynchronous filesystem path using this
    /// detector's shared inference runtime. This legacy MIME hook has no
    /// caller-provided operation scope; it waits for the submitted internal
    /// operation's business outcome on ordinary completion. Runtime
    /// cancellation may return its stop outcome before provider I/O completion
    /// is known or resources are released. This legacy hook has no parent
    /// operation scope and exposes no settlement observer.
    ///
    /// # Parameters
    ///
    /// * `file_system` - Asynchronous filesystem used to access the path.
    /// * `path` - File path to inspect.
    /// * `max_bytes` - Maximum cumulative bytes allowed across all reads.
    ///
    /// # Errors
    ///
    /// Returns a size-limit, provider, admission, Magika, or worker error.
    fn guess_from_async_provider_path<'a>(
        &'a self,
        file_system: &'a AsyncFileSystem,
        path: &'a FsPath,
        max_bytes: usize,
    ) -> Pin<Box<dyn Future<Output = MimeResult<(Vec<String>, Option<Vec<u8>>)>> + Send + 'a>> {
        Box::pin(async move {
            let limit = self.max_test_bytes();
            if max_bytes > limit {
                return Err(MimeError::BufferLimitExceeded {
                    requested: max_bytes,
                    limit,
                });
            }
            let runtime = std::sync::Arc::clone(&self.runtime);
            let admission = runtime.reserve_for_provider(None, max_bytes)?;
            let (handoff, mut handoff_guard) =
                crate::managed_runtime::request_admission_handoff::RequestAdmissionHandoff::new(admission);
            let body_handoff = std::sync::Arc::clone(&handoff);
            let group = runtime.service_group();
            let owned_runtime = std::sync::Arc::clone(&runtime);
            let owned_file_system = file_system.clone();
            let owned_path = path.clone();
            let options = OperationOptions {
                requirement: ChildRequirement::Optional,
                ..OperationOptions::default()
            };
            let mut operation = group
                .as_async()
                .try_submit(options, move |scope| async move {
                    let Some((admission, None)) = body_handoff.take_for_root() else {
                        return Err(MimeError::detector_backend(
                            "magika",
                            body_handoff.closed_reason().as_str(),
                        ));
                    };
                    Self::detect_async_provider_admitted(
                        owned_runtime,
                        &scope,
                        admission,
                        &owned_file_system,
                        &owned_path,
                        max_bytes,
                    )
                    .await
                })
                .map_err(|error| {
                    MimeError::detector_backend("magika", format!("provider operation submission failed: {error:?}"))
                })?;
            let mut outcome = Box::pin(operation.wait_outcome());
            let operation_outcome = match handoff.wait_for_root_or(&mut handoff_guard, &mut outcome).await {
                crate::managed_runtime::request_admission_handoff::BodyStartOr::Outcome(outcome) => outcome,
                crate::managed_runtime::request_admission_handoff::BodyStartOr::BodyStarted => outcome.await,
                crate::managed_runtime::request_admission_handoff::BodyStartOr::Closed(reason) => {
                    drop(outcome);
                    let _ = operation.request_stop(qubit_operation::StopReason::UserCancelled);
                    return Err(MimeError::detector_backend("magika", reason.as_str()));
                }
            };
            let outcome = operation_outcome.map_err(|error| {
                MimeError::detector_backend("magika", format!("provider operation outcome unavailable: {error:?}"))
            })?;
            let result = map_operation_outcome(outcome, PROVIDER_OUTCOME)?;
            Ok((result.into_iter().collect(), None))
        })
    }
}

/// Converts a Magika content type to a MIME type name.
///
/// # Parameters
///
/// * `content_type` - Magika content type.
///
/// # Returns
///
/// MIME type name, or `None` for unknown, undefined, or empty MIME mappings.
#[inline]
pub(crate) fn content_type_to_mime(content_type: ContentType) -> Option<String> {
    owned_mime_type(content_type_mime_name(content_type))
}

/// Returns Magika's MIME name, excluding unknown and undefined classifications.
///
/// A known content type yields `Some`; sentinel types yield `None`.
///
/// # Parameters
///
/// * `content_type` - Magika classification whose canonical MIME name is
///   requested.
///
/// # Returns
///
/// The static MIME name, or `None` for unknown and undefined classifications.
fn content_type_mime_name(content_type: ContentType) -> Option<&'static str> {
    match content_type {
        ContentType::Unknown | ContentType::Undefined => None,
        content_type => Some(content_type.info().mime_type),
    }
}

/// Copies a nonempty MIME name into an owned result.
///
/// Empty or absent names remain `None`, so policy fallback can handle them.
///
/// # Parameters
///
/// * `mime_type` - Optional MIME name to copy.
///
/// # Returns
///
/// An owned, nonempty MIME name, or `None` when the name is absent or empty.
fn owned_mime_type(mime_type: Option<&str>) -> Option<String> {
    mime_type.filter(|mime_type| !mime_type.is_empty()).map(str::to_owned)
}

/// Converts a Magika error to a MIME error.
///
/// # Parameters
///
/// * `error` - Magika error.
///
/// # Returns
///
/// Equivalent MIME error.
#[inline]
pub(crate) fn map_magika_error(error: Error) -> MimeError {
    match error {
        Error::IOError(error) => {
            if crate::internal::async_sample_bridge::is_closed_error(&error) {
                MimeError::detector_backend("magika", "provider inference stopped")
            } else {
                MimeError::Io(error)
            }
        }
        error => MimeError::detector_backend("magika", error.to_string()),
    }
}

impl MimeContentBackend for MagikaMimeDetector {
    /// Reports that Magika requires complete content.
    ///
    /// # Returns
    ///
    /// The complete-content requirement.
    fn content_requirement(&self) -> ContentRequirement {
        ContentRequirement::Complete
    }

    /// Detects MIME candidates from complete content bytes.
    ///
    /// # Parameters
    ///
    /// * `bytes` - Complete content to classify.
    ///
    /// # Returns
    ///
    /// MIME candidates inferred from `bytes`.
    fn detect_bytes(&self, bytes: &[u8]) -> MimeResult<Vec<String>> {
        self.guess_from_magika_input(bytes)
    }

    /// Detects the logical content window from the current reader position to
    /// EOF.
    ///
    /// Once the original position is known, detection attempts to restore it
    /// even if detection returns an error. A successful restore preserves the
    /// detection result or its original error. If the restore seek fails, its
    /// I/O error is returned and the reader position is unspecified.
    ///
    /// # Parameters
    ///
    /// * `reader` - Seekable reader whose remaining content is classified.
    ///
    /// # Returns
    ///
    /// MIME candidates inferred from the remaining content.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] when reading or seeking fails, including when
    /// the restore seek fails after detection. A restore-seek error takes
    /// precedence over a detection error, and the reader position is
    /// unspecified if restoration fails. Returns [`MimeError::DetectorBackend`]
    /// when Magika inference or worker inference fails and restoration
    /// succeeds.
    fn detect_reader(&self, reader: &mut dyn ReadSeek) -> MimeResult<Vec<String>> {
        self.guess_from_reader_with_scope(reader, ReaderScope::Remaining)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::io::Write;

    use magika::ContentType;
    use magika::Error;
    use magika::SyncInput;
    use qubit_io::std_io::ReadSeek;
    use qubit_mime::ContentRequirement;
    use qubit_mime::MimeConfig;
    use qubit_mime::MimeContentBackend;
    use qubit_mime::MimeDetectionPolicy;
    use qubit_mime::MimeDetector;
    use qubit_mime::MimeDetectorBackend;
    use qubit_mime::MimeError;
    use qubit_mime::MimeRepository;
    use qubit_mime::RepositoryMimeDetector;
    use qubit_operation::ChildFailureSummary;
    use qubit_operation::ChildOutcomeKind;
    use qubit_operation::ChildOutcomeSummary;
    use qubit_operation::DiagnosticSummary;
    use qubit_operation::ExecutionFailure;
    use qubit_operation::ExecutionPhase;
    use qubit_operation::ManagerOptions;
    use qubit_operation::OperationManager;
    use qubit_operation::OperationOptions;
    use qubit_operation::OperationOutcome;
    use qubit_operation::PanicSummary;
    use qubit_operation::StopReason;
    use qubit_operation::StopReport;
    use tempfile::NamedTempFile;

    use super::MagikaMimeDetector;
    use super::PROVIDER_OUTCOME;
    use super::content_type_to_mime;
    use super::map_operation_outcome;

    #[test]
    fn test_provider_operation_outcomes_preserve_success_and_classify_failures() {
        let success = map_operation_outcome(OperationOutcome::Succeeded("text/plain"), PROVIDER_OUTCOME);
        assert_eq!("text/plain", success.unwrap());

        let application_error = MimeError::detector_backend("magika", "provider read failed");
        let failed = map_operation_outcome::<&str>(OperationOutcome::Failed(application_error), PROVIDER_OUTCOME);
        assert!(failed.unwrap_err().to_string().contains("provider read failed"));

        let manager = OperationManager::new(ManagerOptions::default()).expect("manager should initialize");
        let handle = manager
            .as_sync()
            .try_submit(OperationOptions::default(), |scope| Ok::<_, MimeError>(scope.id()))
            .expect("operation should be admitted");
        let operation_id = handle.id();
        let child_failure = ChildFailureSummary {
            operation_id,
            outcome: ChildOutcomeSummary {
                operation_id,
                kind: ChildOutcomeKind::Failed,
                summary: DiagnosticSummary::new("child failed"),
            },
        };
        let child_failed =
            map_operation_outcome::<&str>(OperationOutcome::ChildFailed(child_failure), PROVIDER_OUTCOME);
        assert!(child_failed.unwrap_err().to_string().contains("provider child failed"));

        let stopped = map_operation_outcome::<&str>(
            OperationOutcome::Stopped(StopReport {
                reason: StopReason::Timeout,
                accepted_at: None,
            }),
            PROVIDER_OUTCOME,
        );
        assert!(stopped.unwrap_err().to_string().contains("provider inference stopped"));

        let panicked = map_operation_outcome::<&str>(
            OperationOutcome::Panicked(PanicSummary {
                operation_id,
                phase: ExecutionPhase::Running,
                summary: DiagnosticSummary::new("provider panicked"),
            }),
            PROVIDER_OUTCOME,
        );
        assert!(
            panicked
                .unwrap_err()
                .to_string()
                .contains("provider operation panicked")
        );

        let execution_failed = map_operation_outcome::<&str>(
            OperationOutcome::ExecutionFailed(ExecutionFailure {
                operation_id,
                summary: DiagnosticSummary::new("executor unavailable"),
                source: None,
            }),
            PROVIDER_OUTCOME,
        );
        assert!(
            execution_failed
                .unwrap_err()
                .to_string()
                .contains("provider executor failed")
        );
    }

    struct CallerThreadInput {
        caller_thread: std::thread::ThreadId,
        bytes: &'static [u8],
    }

    impl SyncInput for CallerThreadInput {
        fn length(&self) -> magika::Result<u64> {
            assert_eq!(self.caller_thread, std::thread::current().id());
            Ok(self.bytes.len() as u64)
        }

        fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> magika::Result<()> {
            assert_eq!(self.caller_thread, std::thread::current().id());
            let start = usize::try_from(offset).expect("test offset fits usize");
            buffer.copy_from_slice(&self.bytes[start..start + buffer.len()]);
            Ok(())
        }
    }

    #[test]
    fn test_undefined_content_is_not_a_candidate() {
        assert_eq!(None, content_type_to_mime(ContentType::Undefined));
    }

    #[cfg(unix)]
    #[test]
    fn test_filename_backend_preserves_native_basename_separators() {
        let repository = Box::leak(Box::new(
            MimeRepository::from_xml(
                r#"<mime-info xmlns="http://www.freedesktop.org/standards/shared-mime-info">
  <mime-type type="application/x-native-basename">
    <glob pattern="left\right.mimecase" weight="80"/>
  </mime-type>
  <mime-type type="application/x-string-basename">
    <glob pattern="right.mimecase" weight="50"/>
  </mime-type>
</mime-info>"#,
            )
            .expect("custom repository should parse"),
        ));
        let mut detector = MagikaMimeDetector::new().expect("detector should initialize");
        detector.filename_detector = RepositoryMimeDetector::with_repository(repository);

        assert_eq!(
            Some("application/x-native-basename".to_owned()),
            MimeDetector::detect_file(
                &detector,
                std::path::Path::new(r"left\right.mimecase"),
                MimeDetectionPolicy::PreferFilename,
            )
            .expect("filename-preferred native path detection should succeed"),
        );
    }

    #[test]
    fn test_constructor_and_builder_detect_python_content() {
        let detector = MagikaMimeDetector::new().expect("detector should initialize");
        let snapshot = detector.runtime_snapshot();
        assert_eq!(detector.max_buffer_size(), snapshot.max_in_flight_bytes);
        assert_eq!(0, snapshot.reserved_bytes);
        assert_eq!(
            Some("application/pdf".to_owned()),
            detector.guess_from_filename("document.pdf").first().cloned()
        );
        let content_backend: &dyn MimeContentBackend = &detector;
        assert_eq!(ContentRequirement::Complete, content_backend.content_requirement());
        assert_eq!(
            vec!["text/x-python"],
            content_backend
                .detect_bytes(b"#!/usr/bin/env python3\nprint('ok')\n")
                .expect("bytes should classify")
        );
        let detector_backend: &dyn MimeDetectorBackend = &detector;
        assert_eq!(ContentRequirement::Complete, detector_backend.content_requirement());
        assert_eq!(detector.max_buffer_size(), detector_backend.max_test_bytes());
        assert_eq!(
            vec!["application/pdf"],
            detector_backend.guess_from_filename("document.pdf")
        );
        assert_eq!(
            vec!["text/x-python"],
            detector_backend
                .guess_from_content(b"#!/usr/bin/env python3\nprint('ok')\n")
                .expect("content should classify")
        );
        let limit = detector.max_buffer_size();
        let at_limit_borrowed = vec![0; limit];
        let oversized_borrowed = vec![0; limit + 1];
        let at_limit_result = MimeDetector::detect_by_content(&detector, &at_limit_borrowed);
        assert!(
            at_limit_result.is_ok(),
            "borrowed synchronous high-level content at the limit should complete: {at_limit_result:?}"
        );
        let high_level_result = MimeDetector::detect_by_content(&detector, &oversized_borrowed);
        assert!(
            high_level_result.is_ok(),
            "borrowed synchronous high-level content must complete beyond the owned-payload limit: {high_level_result:?}"
        );
        let mut reader = Cursor::new(b"#!/usr/bin/env python3\nprint('ok')\n".to_vec());
        assert_eq!(
            vec!["text/x-python"],
            MimeContentBackend::detect_reader(&detector, &mut reader).expect("content reader should classify")
        );
        let mut reader = Cursor::new(b"#!/usr/bin/env python3\nprint('ok')\n".to_vec());
        assert_eq!(
            (vec!["text/x-python".to_owned()], Vec::new()),
            MimeDetectorBackend::guess_from_reader(&detector, &mut reader).expect("detector reader should classify")
        );
        assert_eq!(
            (vec!["text/x-python".to_owned()], Vec::new()),
            MimeDetectorBackend::guess_from_file(
                &detector,
                std::path::Path::new("tests/fixtures/real_files/script.py"),
            )
            .expect("detector file should classify")
        );
        assert_eq!(
            ContentRequirement::Complete,
            MimeContentBackend::content_requirement(&detector)
        );
        assert_eq!(
            ContentRequirement::Complete,
            MimeDetectorBackend::content_requirement(&detector)
        );
        assert_eq!(
            detector.max_buffer_size(),
            MimeDetectorBackend::max_test_bytes(&detector)
        );
        assert_eq!(
            vec!["application/pdf"],
            MimeDetectorBackend::guess_from_filename(&detector, "document.pdf")
        );
        assert_eq!(
            vec!["text/x-python"],
            MimeDetectorBackend::guess_from_content(&detector, b"#!/usr/bin/env python3\nprint('ok')\n",)
                .expect("fully qualified content should classify")
        );
        let _ = MagikaMimeDetector::builder()
            .mime_config(MimeConfig::default())
            .build()
            .expect("builder should initialize");
        let _ =
            MagikaMimeDetector::from_mime_config(MimeConfig::default()).expect("configured detector should initialize");
    }

    #[test]
    fn test_local_file_and_reader_detect_python_content() {
        let detector = MagikaMimeDetector::new().expect("detector should initialize");
        let mut file = NamedTempFile::new().expect("temporary file should be created");
        file.write_all(b"#!/usr/bin/env python3\nprint('ok')\n")
            .expect("fixture should be written");
        assert_eq!(
            vec!["text/x-python"],
            detector
                .guess_from_magika_file(file.path())
                .expect("file should classify")
        );
        let mut reader = Cursor::new(b"#!/usr/bin/env python3\nprint('ok')\n".to_vec());
        assert_eq!(
            vec!["text/x-python"],
            detector
                .guess_from_reader_with_scope(&mut reader, super::ReaderScope::WholeResource)
                .expect("reader should classify")
        );
        let mut reader = Cursor::new(b"skip#!/usr/bin/env python3\nprint('ok')\n".to_vec());
        reader.set_position(4);
        let reader: &mut dyn ReadSeek = &mut reader;
        assert_eq!(
            vec!["text/x-python"],
            detector
                .guess_from_reader_with_scope(reader, super::ReaderScope::Remaining)
                .expect("remaining reader window should classify")
        );
    }

    #[test]
    fn test_magika_io_errors_map_to_mime_io_errors() {
        let error = super::map_magika_error(Error::IOError(std::io::Error::other("io")));
        assert!(matches!(error, MimeError::Io(_)));
        let interrupted = super::map_magika_error(Error::IOError(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "provider interrupted",
        )));
        assert!(matches!(interrupted, MimeError::Io(_)));
        let closed_bridge =
            super::map_magika_error(Error::IOError(crate::internal::async_sample_bridge::closed_error()));
        assert!(matches!(closed_bridge, MimeError::DetectorBackend { .. }));
        let observed_stop = crate::internal::map_provider_magika_error(Error::IOError(
            crate::internal::provider_input::observed_read_stopped_error(),
        ));
        assert!(matches!(observed_stop, MimeError::DetectorBackend { .. }));
        let session_stop = crate::internal::map_provider_magika_error(Error::IOError(
            crate::internal::session_read_stopped_io_error(),
        ));
        assert!(matches!(session_stop, MimeError::DetectorBackend { .. }));
        let provider_interrupted = crate::internal::map_provider_magika_error(Error::IOError(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "provider interrupted",
        )));
        assert!(matches!(provider_interrupted, MimeError::Io(_)));
    }

    #[test]
    fn test_sync_sampling_runs_on_calling_thread_before_worker_inference() {
        let detector = MagikaMimeDetector::new().expect("detector should initialize");
        let input = CallerThreadInput {
            caller_thread: std::thread::current().id(),
            bytes: b"#!/usr/bin/env python3\nprint('ok')\n",
        };
        assert_eq!(vec!["text/x-python"], detector.guess_from_magika_input(input).unwrap());
    }

    #[test]
    fn test_local_directory_returns_no_mime_candidate() {
        let detector = MagikaMimeDetector::new().expect("detector should initialize");
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        assert!(detector.guess_from_magika_file(directory.path()).unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn test_local_symlink_does_not_follow_target() {
        use std::os::unix::fs::symlink;

        let detector = MagikaMimeDetector::new().expect("detector should initialize");
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let target = directory.path().join("target.py");
        let link = directory.path().join("link.py");
        std::fs::write(&target, b"#!/usr/bin/env python3\nprint('ok')\n").unwrap();
        symlink(&target, &link).unwrap();
        assert!(detector.guess_from_magika_file(&link).unwrap().is_empty());
    }
}
