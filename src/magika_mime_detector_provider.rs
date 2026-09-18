// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider for registering the Magika MIME detector with `qubit-mime`.

use std::sync::Arc;

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
    media_stream_classifier: Option<Arc<dyn qubit_mime::MediaStreamClassifier>>,
}

impl std::fmt::Debug for MagikaMimeDetectorProvider {
    /// Formats the provider without exposing classifier internals.
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
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a provider with a classifier for precise media refinement.
    #[inline]
    pub fn with_media_stream_classifier(classifier: Arc<dyn qubit_mime::MediaStreamClassifier>) -> Self {
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
    use qubit_mime::MimeConfig;
    use qubit_mime::MimeDetector;
    use qubit_spi::ProviderMetadata;
    use qubit_spi::ServiceProvider;

    use super::MagikaMimeDetectorProvider;

    #[test]
    fn provider_constructor_and_configuration_work() {
        let provider = MagikaMimeDetectorProvider::new();
        assert_eq!("magika", provider.descriptor().id().as_str());
        let detector = provider
            .create_configured(&MimeConfig::default())
            .expect("provider should create a detector");
        assert_eq!(
            Some("application/pdf".to_owned()),
            detector
                .detect_by_filename("document.pdf")
                .expect("filename should classify")
        );
    }
}
