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
#[derive(Clone, Default)]
pub struct MagikaMimeDetectorProvider {
    media_stream_classifier: Option<Arc<dyn qubit_mime::MediaStreamClassifier>>,
}

impl std::fmt::Debug for MagikaMimeDetectorProvider {
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
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

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
    #[inline(always)]
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
