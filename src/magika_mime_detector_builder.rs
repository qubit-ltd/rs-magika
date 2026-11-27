// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Builder for Magika MIME detectors.

use std::sync::Arc;

use qubit_mime::MediaStreamClassifier;
use qubit_mime::MimeConfig;
use qubit_mime::MimeDetectorCore;
use qubit_mime::MimeError;
use qubit_mime::MimeResult;
use qubit_mime::RepositoryMimeDetector;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;

use crate::MagikaMimeDetector;
use crate::internal::regular_file_media_stream_classifier::RegularFileMediaStreamClassifier;
use crate::managed_runtime::ManagedInferenceRuntime;

/// Builder for a [`MagikaMimeDetector`].
///
/// The configured media-stream classifier refines eligible high-level
/// complete-content and reader detections. Local-file refinement is limited
/// to ordinary files. Directories and symbolic links skip refinement; a
/// filename candidate is returned only when the selected detection policy
/// permits fallback and such a candidate exists. FIFO and socket paths are
/// rejected during content detection's metadata check with
/// `ErrorKind::Unsupported`. It is not invoked by
/// `detect_path` or `detect_async_path`, including their full-read fallback;
/// those provider routes return Magika candidates to `qubit-mime` without
/// media refinement.
///
/// # Examples
///
/// ```
/// use qubit_magika::MagikaMimeDetectorBuilder;
/// use qubit_mime::MimeDetector;
///
/// let detector = MagikaMimeDetectorBuilder::default().build()?;
/// let content = b"#!/bin/sh\n\nset -eu\necho \"hello from a real shell fixture\"\n";
/// let mime = detector.detect_by_content(content)?;
/// assert_eq!(mime.as_deref(), Some("text/x-shellscript"));
/// # Ok::<(), qubit_mime::MimeError>(())
/// ```
#[derive(Default)]
pub struct MagikaMimeDetectorBuilder {
    /// MIME selection and refinement settings copied into the detector.
    mime_config: MimeConfig,
    /// Optional classifier shared with the detector for media refinement.
    media_stream_classifier: Option<Arc<dyn MediaStreamClassifier>>,
    /// Optional shared managed runtime supplied by the application.
    runtime: Option<Arc<ManagedInferenceRuntime>>,
    /// Optional application-owned manager used to create the runtime.
    manager: Option<OperationManager>,
}

impl std::fmt::Debug for MagikaMimeDetectorBuilder {
    /// Formats configuration choices without exposing runtime internals.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Output formatter supplied by the caller.
    ///
    /// # Returns
    ///
    /// Formatting success or the formatter's error.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MagikaMimeDetectorBuilder")
            .field("mime_config", &self.mime_config)
            .field(
                "media_stream_classifier_configured",
                &self.media_stream_classifier.is_some(),
            )
            .field("runtime_configured", &self.runtime.is_some())
            .field("manager_configured", &self.manager.is_some())
            .finish()
    }
}

impl MagikaMimeDetectorBuilder {
    /// Replaces the MIME policy and refinement configuration.
    ///
    /// # Parameters
    ///
    /// * `mime_config` - Policy and refinement settings to use when detecting
    ///   MIME types.
    ///
    /// # Returns
    ///
    /// The builder with the supplied configuration.
    #[must_use]
    #[inline]
    pub fn mime_config(mut self, mime_config: MimeConfig) -> Self {
        self.mime_config = mime_config;
        self
    }

    /// Sets the optional classifier used for precise media refinement.
    ///
    /// This setting applies to eligible high-level complete-content and reader
    /// detections. Local-file refinement is limited to ordinary files;
    /// directories and symbolic links skip refinement. A filename candidate
    /// is returned only when the selected detection policy permits fallback
    /// and such a candidate exists. FIFO and socket paths are rejected during
    /// content detection's metadata check with `ErrorKind::Unsupported`.
    /// Provider-path detection does not invoke the classifier, even when its
    /// filesystem fallback reads the whole file.
    ///
    /// # Parameters
    ///
    /// * `media_stream_classifier` - Classifier to use for media refinement, or
    ///   `None` to disable it.
    ///
    /// # Returns
    ///
    /// The builder with the supplied classifier setting.
    #[must_use]
    #[inline]
    pub fn media_stream_classifier(mut self, media_stream_classifier: Option<Arc<dyn MediaStreamClassifier>>) -> Self {
        self.media_stream_classifier = media_stream_classifier;
        self
    }

    /// Shares an application-managed inference runtime with this detector.
    ///
    /// When this runtime is set, it takes precedence over any supervisor set
    /// with [`Self::operation_manager`], regardless of setter order. The
    /// runtime is reused as-is; `build` does not initialize another Magika
    /// session for it. Its byte budget must be at least the configured MIME
    /// buffer limit.
    ///
    /// # Parameters
    ///
    /// * `runtime` - Runtime whose bounded worker and supervisor are shared.
    ///
    /// # Returns
    ///
    /// The builder configured with the supplied runtime.
    #[must_use]
    pub fn managed_runtime(mut self, runtime: Arc<ManagedInferenceRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Uses an application-owned operation manager for the detector runtime.
    ///
    /// This manager is used only when no runtime was set with
    /// [`Self::managed_runtime`]. If both are configured, the supplied runtime
    /// takes precedence regardless of setter order.
    ///
    /// # Parameters
    ///
    /// * `manager` - Application-owned manager retained by the runtime.
    ///
    /// # Returns
    ///
    /// The builder configured to create its runtime from that supervisor.
    #[must_use]
    pub fn operation_manager(mut self, manager: OperationManager) -> Self {
        self.manager = Some(manager);
        self
    }

    /// Builds a detector, initializing a new Magika session unless a runtime
    /// was supplied.
    ///
    /// # Returns
    ///
    /// A detector configured with the builder's MIME settings and classifier.
    /// A supplied runtime is reused, or a new runtime and Magika session are
    /// initialized using the supplied supervisor (or a default supervisor).
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::DetectorBackend`] if a supplied
    /// runtime's byte budget is smaller than the MIME buffer limit, the default
    /// operation manager or service group cannot be created, the worker cannot
    /// start, or Magika reports a non-I/O initialization failure. Magika
    /// initialization I/O failures are returned as
    /// [`MimeError::Io`]. Errors creating a service group from an
    /// application-owned [`OperationManager`] are mapped
    /// to [`MimeError::DetectorBackend`]. A zero
    /// `MimeConfig::max_buffer_size()` is rejected when this builder creates a
    /// new runtime; a supplied shared runtime may be reused with that zero
    /// per-request limit.
    pub fn build(mut self) -> MimeResult<MagikaMimeDetector> {
        let runtime = match self.take_shared_runtime()? {
            Some(runtime) => runtime,
            None => Arc::new(ManagedInferenceRuntime::new(
                self.take_manager()?,
                self.mime_config.max_buffer_size(),
            )?),
        };
        Ok(self.finish(runtime))
    }

    /// Builds a detector while asynchronously waiting for a new Magika
    /// session to initialize on the inference worker.
    ///
    /// The builder starts the worker on the first poll and awaits model
    /// initialization. A supplied runtime is reused without starting another
    /// worker. Dropping the future during startup abandons the new runtime.
    /// Native initialization already in progress can finish on the worker,
    /// which then exits without serving any requests.
    ///
    /// # Returns
    ///
    /// A detector configured with the builder's MIME settings and classifier.
    ///
    /// # Errors
    ///
    /// Returns the same configuration and initialization errors as
    /// [`Self::build`].
    pub async fn build_async(mut self) -> MimeResult<MagikaMimeDetector> {
        let runtime = match self.take_shared_runtime()? {
            Some(runtime) => runtime,
            None => Arc::new(
                ManagedInferenceRuntime::new_async(self.take_manager()?, self.mime_config.max_buffer_size()).await?,
            ),
        };
        Ok(self.finish(runtime))
    }

    /// Takes the supplied runtime after checking its payload budget.
    ///
    /// # Returns
    ///
    /// The shared runtime, if one was supplied.
    ///
    /// # Errors
    ///
    /// Returns a backend error if the runtime cannot cover the MIME buffer
    /// limit.
    fn take_shared_runtime(&mut self) -> MimeResult<Option<Arc<ManagedInferenceRuntime>>> {
        let runtime = self.runtime.take();
        if runtime
            .as_ref()
            .is_some_and(|runtime| runtime.max_in_flight_bytes() < self.mime_config.max_buffer_size())
        {
            return Err(MimeError::detector_backend(
                "magika",
                "managed runtime byte budget is smaller than the configured MIME buffer limit",
            ));
        }
        Ok(runtime)
    }

    /// Takes the supplied manager or creates a default manager.
    ///
    /// # Returns
    ///
    /// The manager to use when constructing a new worker.
    ///
    /// # Errors
    ///
    /// Returns a backend error if the default manager cannot be created.
    fn take_manager(&mut self) -> MimeResult<OperationManager> {
        match self.manager.take() {
            Some(manager) => Ok(manager),
            None => OperationManager::new(ManagerOptions::default()).map_err(|error| {
                MimeError::detector_backend("magika", format!("could not create operation manager: {error}"))
            }),
        }
    }

    /// Combines MIME configuration and a ready inference runtime.
    ///
    /// # Parameters
    ///
    /// * `runtime` - Shared runtime owning the Magika worker.
    ///
    /// # Returns
    ///
    /// A detector using the prepared runtime.
    fn finish(self, runtime: Arc<ManagedInferenceRuntime>) -> MagikaMimeDetector {
        let mut core = MimeDetectorCore::from_mime_config(self.mime_config.clone());
        core.set_media_stream_classifier(self.media_stream_classifier.map(|classifier| {
            Arc::new(RegularFileMediaStreamClassifier::new(classifier)) as Arc<dyn MediaStreamClassifier>
        }));
        MagikaMimeDetector {
            core,
            filename_detector: RepositoryMimeDetector::from_mime_config(self.mime_config),
            runtime,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Duration;

    use qubit_mime::MimeConfig;
    use qubit_operation::ManagerOptions;
    use qubit_operation::OperationManager;
    use qubit_operation::ShutdownMode;

    use super::MagikaMimeDetectorBuilder;
    use crate::managed_runtime::ManagedInferenceRuntime;

    #[test]
    fn test_builder_runtime_precedence() {
        let supervisor_a = OperationManager::new(ManagerOptions::default()).unwrap();
        let runtime_a = Arc::new(
            ManagedInferenceRuntime::new(supervisor_a.clone(), MimeConfig::default().max_buffer_size())
                .expect("runtime A should initialize"),
        );
        let supervisor_b = OperationManager::new(ManagerOptions::default()).unwrap();

        let runtime_then_supervisor = MagikaMimeDetectorBuilder::default()
            .managed_runtime(Arc::clone(&runtime_a))
            .operation_manager(supervisor_b.clone())
            .build()
            .expect("runtime should take precedence over supervisor");
        let supervisor_then_runtime = MagikaMimeDetectorBuilder::default()
            .operation_manager(supervisor_b.clone())
            .managed_runtime(Arc::clone(&runtime_a))
            .build()
            .expect("runtime should take precedence over supervisor");
        assert!(Arc::ptr_eq(&runtime_a, &runtime_then_supervisor.runtime));
        assert!(Arc::ptr_eq(&runtime_a, &supervisor_then_runtime.runtime));

        supervisor_b.close_admission();
        assert!(block_on(runtime_a.detect_bytes_unmanaged(b"%PDF-1.7".to_vec())).is_ok());

        assert!(
            runtime_a
                .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1))
                .worker_stopped,
            "runtime A worker should exit after shutdown"
        );

        let too_small =
            Arc::new(ManagedInferenceRuntime::new(supervisor_a, 1).expect("nonzero runtime should initialize"));
        let error = MagikaMimeDetectorBuilder::default()
            .managed_runtime(too_small.clone())
            .build()
            .expect_err("build should preserve the configured runtime budget error");
        assert!(error.to_string().contains("managed runtime byte budget"));
        too_small.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        struct ThreadWaker(std::thread::Thread);
        impl Wake for ThreadWaker {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }
        }

        let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
        let mut context = Context::from_waker(&waker);
        let mut future = pin!(future);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => {
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    assert!(!remaining.is_zero(), "builder async detection did not complete");
                    std::thread::park_timeout(remaining);
                }
            }
        }
    }
}
