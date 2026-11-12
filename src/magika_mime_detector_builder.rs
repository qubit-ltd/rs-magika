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
    ///
    /// # Errors
    ///
    /// Returns an error if Magika cannot initialize its session.
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
    use std::time::Duration;

    use qubit_operation::report::SupervisorLimits;
    use qubit_operation::supervisor::OperationSupervisor;
    use qubit_operation::supervisor::ShutdownMode;

    use super::MagikaMimeDetectorBuilder;

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
}
