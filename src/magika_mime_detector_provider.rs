// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider for registering the Magika MIME detector with `qubit-mime`.

use std::sync::Arc;

use qubit_mime::MediaStreamClassifier;
use qubit_mime::MimeConfig;
use qubit_mime::MimeDetector;
use qubit_mime::MimeDetectorSpec;
use qubit_mime::MimeError;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;
use qubit_spi::error::ProviderFailure;
use qubit_spi::provider_descriptor;

use crate::MagikaMimeDetector;
use crate::managed_runtime::ManagedInferenceRuntime;

/// Provider for [`MagikaMimeDetector`].
///
/// A configured classifier is used by eligible high-level complete-content,
/// reader detections and local-file refinement for ordinary files. Directories
/// and symbolic links skip refinement; a filename candidate is returned only
/// when the selected policy permits fallback and such a candidate exists.
/// FIFO and socket paths are rejected during content detection's metadata
/// check with `ErrorKind::Unsupported`. `detect_path` and
/// `detect_async_path` do not invoke it, including when those paths use a
/// full-read fallback.
///
/// # Examples
///
/// ```
/// use qubit_magika::MagikaMimeDetectorProvider;
/// use qubit_spi::ProviderMetadata;
///
/// let provider = MagikaMimeDetectorProvider::new();
/// assert_eq!(provider.descriptor().id().as_str(), "magika");
/// ```
#[derive(Clone, Default)]
pub struct MagikaMimeDetectorProvider {
    /// Optional classifier used for precise media-stream refinement.
    media_stream_classifier: Option<Arc<dyn MediaStreamClassifier>>,
    /// Optional application-owned runtime shared with created detectors.
    runtime: Option<Arc<ManagedInferenceRuntime>>,
}

impl std::fmt::Debug for MagikaMimeDetectorProvider {
    /// Formats the provider without exposing classifier internals.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Destination for the formatted provider description.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MagikaMimeDetectorProvider")
            .field(
                "media_stream_classifier",
                &self.media_stream_classifier.as_ref().map(|_| "configured"),
            )
            .field("runtime_configured", &self.runtime.is_some())
            .finish()
    }
}

impl MagikaMimeDetectorProvider {
    /// Creates a provider with the default configuration.
    ///
    /// # Returns
    ///
    /// A provider with no media-stream classifier configured;
    /// `create_configured` builds detectors with the supplied
    /// [`MimeConfig`] and without media refinement unless a classifier is
    /// explicitly registered on the provider.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a provider with a classifier for precise media refinement.
    ///
    /// The classifier applies to eligible high-level complete-content,
    /// reader, and local-file detections. It is not called for synchronous or
    /// asynchronous provider-path detection.
    ///
    /// # Parameters
    ///
    /// * `classifier` - Shared classifier used to refine detected media
    ///   streams. For local paths, only ordinary files reach it; directories
    ///   and symbolic links skip refinement. A filename candidate is returned
    ///   only when the selected policy permits fallback and such a candidate
    ///   exists. FIFO and socket paths are rejected during content detection's
    ///   metadata check with `ErrorKind::Unsupported`.
    ///
    /// # Returns
    ///
    /// A provider that uses `classifier` for precise media refinement.
    #[inline]
    pub fn with_media_stream_classifier(classifier: Arc<dyn MediaStreamClassifier>) -> Self {
        Self {
            media_stream_classifier: Some(classifier),
            runtime: None,
        }
    }

    /// Shares an application-managed runtime with detectors created by this
    /// provider. All created detectors use the same bounded worker, so they
    /// share its request and byte budgets and shutdown domain. The runtime's
    /// byte budget must be at least each detector's configured MIME buffer
    /// limit. Providers discovered through inventory are not configured with
    /// an application runtime; register this configured provider explicitly
    /// when sharing is required.
    ///
    /// # Parameters
    ///
    /// * `runtime` - Runtime shared by detectors created from this provider.
    ///
    /// # Returns
    ///
    /// This provider configured to share `runtime`.
    #[must_use]
    pub fn managed_runtime(mut self, runtime: Arc<ManagedInferenceRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }
}

impl ServiceProvider<MimeDetectorSpec> for MagikaMimeDetectorProvider {
    /// Creates a Magika-backed detector.
    ///
    /// # Parameters
    ///
    /// * `config` - MIME detector configuration copied into the created
    ///   detector.
    ///
    /// # Returns
    ///
    /// A shared Magika-backed MIME detector.
    ///
    /// # Errors
    ///
    /// Returns an initialization failure if `config` sets a MIME buffer limit
    /// larger than the byte budget of this provider's managed runtime, the
    /// default operation manager or service group cannot be created, the
    /// worker cannot start, or Magika cannot initialize. The failure preserves
    /// the underlying [`MimeError`] source, including the existing mapping of
    /// Magika initialization I/O failures to [`MimeError::Io`] and manager,
    /// group, worker-start, and non-I/O Magika failures to
    /// [`MimeError::DetectorBackend`]. Magika's upstream error does not
    /// reliably distinguish a missing runtime from an invalid model or
    /// configuration, so `OnAbsence` fallback does not suppress initialization
    /// failures.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use qubit_magika::MagikaMimeDetectorProvider;
    /// use qubit_mime::MimeConfig;
    /// use qubit_spi::ServiceProvider;
    ///
    /// let provider = MagikaMimeDetectorProvider::new();
    /// let detector = provider.create_configured(&MimeConfig::default())?;
    /// # Ok::<(), qubit_spi::error::ProviderFailure<qubit_mime::MimeError>>(())
    /// ```
    fn create_configured(&self, config: &MimeConfig) -> Result<Arc<dyn MimeDetector>, ProviderFailure<MimeError>> {
        let mut builder = MagikaMimeDetector::builder()
            .mime_config(config.clone())
            .media_stream_classifier(self.media_stream_classifier.clone());
        if let Some(runtime) = &self.runtime {
            builder = builder.managed_runtime(Arc::clone(runtime));
        }
        builder
            .build()
            .map(|detector| Arc::new(detector) as Arc<dyn MimeDetector>)
            .map_err(ProviderFailure::initialization_failed)
    }
}

impl ProviderMetadata for MagikaMimeDetectorProvider {
    /// Returns the stable Magika provider identity and selection metadata.
    ///
    /// # Returns
    ///
    /// The `magika` descriptor, accepted aliases, and automatic-selection
    /// priority.
    #[inline]
    fn descriptor(&self) -> ProviderDescriptor {
        provider_descriptor!(
            "magika",
            aliases: ["magika-mime-detector", "magikamimedetector"],
            priority: 20,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;

    use qubit_mime::MediaStreamClassifier;
    use qubit_mime::MediaStreamType;
    use qubit_mime::MimeConfig;
    use qubit_mime::MimeResult;
    use qubit_operation::ManagerOptions;
    use qubit_operation::OperationManager;
    use qubit_operation::ShutdownMode;
    use qubit_spi::ProviderMetadata;
    use qubit_spi::ServiceProvider;

    use super::MagikaMimeDetectorProvider;
    use crate::ManagedInferenceRuntime;

    #[derive(Debug)]
    struct NoopClassifier;

    impl MediaStreamClassifier for NoopClassifier {
        fn classify_file(&self, _file: &Path) -> MimeResult<MediaStreamType> {
            Ok(MediaStreamType::None)
        }

        fn classify_reader(&self, _reader: &mut dyn Read) -> MimeResult<MediaStreamType> {
            Ok(MediaStreamType::None)
        }
    }

    #[test]
    fn test_configured_provider_formats_and_creates_classifier_detector() {
        let default_provider = MagikaMimeDetectorProvider::new();
        assert_eq!("magika", default_provider.descriptor().id().as_str());
        assert!(format!("{default_provider:?}").contains("runtime_configured: false"));
        let classifier = NoopClassifier;
        assert_eq!(
            MediaStreamType::None,
            classifier.classify_file(Path::new("/unused")).unwrap()
        );
        assert_eq!(
            MediaStreamType::None,
            classifier.classify_reader(&mut &b""[..]).unwrap()
        );
        let provider = MagikaMimeDetectorProvider::with_media_stream_classifier(Arc::new(NoopClassifier));
        assert!(format!("{provider:?}").contains("configured"));
        let detector = provider
            .create_configured(&MimeConfig::default())
            .expect("configured Magika provider should initialize");
        drop(detector);
    }

    fn runtime(byte_budget: usize) -> Arc<ManagedInferenceRuntime> {
        let manager = OperationManager::new(ManagerOptions::default()).expect("operation manager should initialize");
        Arc::new(
            ManagedInferenceRuntime::new(manager, byte_budget).expect("managed inference runtime should initialize"),
        )
    }

    #[test]
    fn test_provider_shared_runtime_is_cloned_and_shutdown_is_shared() {
        use qubit_mime::MimeDetectionPolicy;
        use qubit_mime::MimeDetector;

        let shared_runtime = runtime(MimeConfig::default().max_buffer_size());
        let provider = MagikaMimeDetectorProvider::new().managed_runtime(Arc::clone(&shared_runtime));
        let cloned_provider = provider.clone();
        assert!(Arc::ptr_eq(
            provider.runtime.as_ref().expect("runtime should be configured"),
            cloned_provider
                .runtime
                .as_ref()
                .expect("cloned runtime should be configured"),
        ));

        let first = provider
            .create_configured(&MimeConfig::default())
            .expect("first detector should initialize");
        let second = cloned_provider
            .create_configured(&MimeConfig::default())
            .expect("second detector should initialize");
        let report = shared_runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
        assert!(report.worker_stopped);
        for detector in [&first, &second] {
            assert!(
                detector
                    .detect(
                        b"#!/usr/bin/env python3\nprint('hello')\n",
                        None,
                        MimeDetectionPolicy::ContentFirst
                    )
                    .is_err()
            );
        }

        let independent_runtime = runtime(MimeConfig::default().max_buffer_size());
        let independent_provider = MagikaMimeDetectorProvider::new().managed_runtime(Arc::clone(&independent_runtime));
        let independent = independent_provider
            .create_configured(&MimeConfig::default())
            .expect("independent detector should initialize");
        assert!(
            independent
                .detect(
                    b"#!/usr/bin/env python3\nprint('hello')\n",
                    None,
                    MimeDetectionPolicy::ContentFirst
                )
                .is_ok()
        );
        let report = independent_runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
        assert!(report.worker_stopped);
    }

    #[test]
    fn test_provider_shared_runtime_budget_and_classifier_configuration() {
        let too_small = runtime(1);
        let error = MagikaMimeDetectorProvider::new()
            .managed_runtime(Arc::clone(&too_small))
            .create_configured(&MimeConfig::default())
            .expect_err("insufficient runtime budget should fail initialization");
        assert!(error.to_string().contains("managed runtime byte budget"));
        let report = too_small.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
        assert!(report.worker_stopped);

        let shared_runtime = runtime(MimeConfig::default().max_buffer_size());
        let provider = MagikaMimeDetectorProvider::with_media_stream_classifier(Arc::new(NoopClassifier))
            .managed_runtime(Arc::clone(&shared_runtime));
        assert!(format!("{provider:?}").contains("runtime_configured: true"));
        let detector = provider
            .create_configured(&MimeConfig::default())
            .expect("classifier and shared runtime should compose");
        drop(detector);
        let report = shared_runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
        assert!(report.worker_stopped);
    }
}
