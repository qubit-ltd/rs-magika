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

/// Provider for [`MagikaMimeDetector`].
///
/// A configured classifier is used by eligible high-level complete-content,
/// reader, and local-file detections. `detect_path` and `detect_async_path`
/// do not invoke it, including when those paths use a full-read fallback.
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
            .finish()
    }
}

impl MagikaMimeDetectorProvider {
    /// Creates a provider with the default configuration.
    ///
    /// # Returns
    ///
    /// A provider that uses default MIME stream refinement behavior.
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
    ///   streams.
    ///
    /// # Returns
    ///
    /// A provider that uses `classifier` for precise media refinement.
    #[inline]
    pub fn with_media_stream_classifier(classifier: Arc<dyn MediaStreamClassifier>) -> Self {
        Self {
            media_stream_classifier: Some(classifier),
        }
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
    /// Returns [`ProviderFailure`] when Magika or ONNX Runtime cannot
    /// initialize. The error preserves the underlying [`MimeError`] source and
    /// is classified as initialization failure because Magika's upstream error
    /// does not reliably distinguish a missing runtime from an invalid model or
    /// configuration. `OnAbsence` fallback therefore does not suppress it.
    fn create_configured(&self, config: &MimeConfig) -> Result<Arc<dyn MimeDetector>, ProviderFailure<MimeError>> {
        MagikaMimeDetector::builder()
            .mime_config(config.clone())
            .media_stream_classifier(self.media_stream_classifier.clone())
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

    use qubit_mime::MediaStreamClassifier;
    use qubit_mime::MediaStreamType;
    use qubit_mime::MimeConfig;
    use qubit_mime::MimeResult;
    use qubit_spi::ProviderMetadata;
    use qubit_spi::ServiceProvider;

    use super::MagikaMimeDetectorProvider;

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
}
