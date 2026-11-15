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
use qubit_mime::MimeResult;
use qubit_mime::RepositoryMimeDetector;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::supervisor::OperationSupervisor;

use crate::MagikaMimeDetector;
use crate::managed_runtime::ManagedInferenceRuntime;

/// Builder for a [`MagikaMimeDetector`].
///
/// The configured media-stream classifier refines eligible high-level
/// complete-content, reader, and local-file detections. It is not invoked by
/// `detect_path` or `detect_async_path`, including their full-read fallback;
/// those provider routes return Magika candidates to `qubit-mime` without
/// media refinement.
///
/// # Examples
///
/// ```no_run
/// use qubit_magika::MagikaMimeDetectorBuilder;
///
/// let detector = MagikaMimeDetectorBuilder::default().build()?;
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
    /// Optional application-owned supervisor used to create the runtime.
    supervisor: Option<OperationSupervisor>,
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
            .field("supervisor_configured", &self.supervisor.is_some())
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
    /// This setting applies to eligible high-level complete-content, reader,
    /// and local-file detections. Provider-path detection does not invoke the
    /// classifier, even when its filesystem fallback reads the whole file.
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
    /// with [`Self::operation_supervisor`], regardless of setter order. The
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

    /// Uses an application-owned supervisor for the detector's managed runtime.
    ///
    /// This supervisor is used only when no runtime was set with
    /// [`Self::managed_runtime`]. If both are configured, the supplied runtime
    /// takes precedence regardless of setter order.
    ///
    /// # Parameters
    ///
    /// * `supervisor` - Application-owned supervisor retained by the runtime.
    ///
    /// # Returns
    ///
    /// The builder configured to create its runtime from that supervisor.
    #[must_use]
    pub fn operation_supervisor(mut self, supervisor: OperationSupervisor) -> Self {
        self.supervisor = Some(supervisor);
        self
    }

    /// Initializes the Magika session and constructs the detector.
    ///
    /// # Returns
    ///
    /// A detector configured with the builder's MIME settings and classifier.
    /// A supplied runtime is reused, or a new runtime and Magika session are
    /// initialized using the supplied supervisor (or a default supervisor).
    ///
    /// # Errors
    ///
    /// Returns an error if a supplied runtime's byte budget is smaller than
    /// the MIME buffer limit, or if a new runtime or Magika session cannot be
    /// initialized.
    pub fn build(self) -> MimeResult<MagikaMimeDetector> {
        let max_buffer_size = self.mime_config.max_buffer_size();
        let runtime = match self.runtime {
            Some(runtime) if runtime.max_in_flight_bytes() >= max_buffer_size => runtime,
            Some(_) => {
                return Err(qubit_mime::MimeError::detector_backend(
                    "magika",
                    "managed runtime byte budget is smaller than the configured MIME buffer limit",
                ));
            }
            None => {
                let supervisor = self
                    .supervisor
                    .unwrap_or_else(|| OperationSupervisor::new(SupervisorLimits::default()));
                Arc::new(ManagedInferenceRuntime::new(supervisor, max_buffer_size)?)
            }
        };
        let mut core = MimeDetectorCore::from_mime_config(self.mime_config.clone());
        core.set_media_stream_classifier(self.media_stream_classifier);
        Ok(MagikaMimeDetector {
            core,
            filename_detector: RepositoryMimeDetector::from_mime_config(self.mime_config),
            runtime,
        })
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
    use qubit_operation::report::SupervisorLimits;
    use qubit_operation::supervisor::OperationSupervisor;
    use qubit_operation::supervisor::ShutdownMode;

    use super::MagikaMimeDetectorBuilder;
    use crate::managed_runtime::ManagedInferenceRuntime;

    #[test]
    fn test_builder_runtime_precedence() {
        let supervisor_a = OperationSupervisor::new(SupervisorLimits::default());
        let runtime_a = Arc::new(
            ManagedInferenceRuntime::new(supervisor_a.clone(), MimeConfig::default().max_buffer_size())
                .expect("runtime A should initialize"),
        );
        let supervisor_b = OperationSupervisor::new(SupervisorLimits::default());

        let runtime_then_supervisor = MagikaMimeDetectorBuilder::default()
            .managed_runtime(Arc::clone(&runtime_a))
            .operation_supervisor(supervisor_b.clone())
            .build()
            .expect("runtime should take precedence over supervisor");
        let supervisor_then_runtime = MagikaMimeDetectorBuilder::default()
            .operation_supervisor(supervisor_b.clone())
            .managed_runtime(Arc::clone(&runtime_a))
            .build()
            .expect("runtime should take precedence over supervisor");
        assert!(Arc::ptr_eq(&runtime_a, &runtime_then_supervisor.runtime));
        assert!(Arc::ptr_eq(&runtime_a, &supervisor_then_runtime.runtime));

        supervisor_b.close();
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

    #[test]
    fn test_builder_debug_and_application_supervisor_configuration() {
        let builder = MagikaMimeDetectorBuilder::default()
            .media_stream_classifier(None)
            .operation_supervisor(OperationSupervisor::new(SupervisorLimits::default()));
        assert!(format!("{builder:?}").contains("MagikaMimeDetectorBuilder"));
        let detector = builder.build().expect("configured detector should initialize");
        assert!(
            detector
                .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1))
                .worker_stopped
        );
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
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::park(),
            }
        }
    }
}
