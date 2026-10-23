// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Builder for Magika MIME detectors.

use std::sync::Arc;

use magika::Session;
use qubit_mime::MediaStreamClassifier;
use qubit_mime::MimeConfig;
use qubit_mime::MimeDetectorCore;
use qubit_mime::MimeResult;
use qubit_mime::RepositoryMimeDetector;

use crate::MagikaMimeDetector;
use crate::magika_mime_detector::map_magika_error;

/// Builder for a [`MagikaMimeDetector`].
///
/// # Examples
///
/// ```no_run
/// use qubit_magika::MagikaMimeDetectorBuilder;
///
/// let detector = MagikaMimeDetectorBuilder::default().build()?;
/// # Ok::<(), qubit_mime::MimeError>(())
/// ```
#[derive(Debug, Default)]
pub struct MagikaMimeDetectorBuilder {
    /// MIME selection and refinement settings copied into the detector.
    mime_config: MimeConfig,
    /// Optional classifier shared with the detector for media refinement.
    media_stream_classifier: Option<Arc<dyn MediaStreamClassifier>>,
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
        let session = Session::new().map_err(map_magika_error)?;
        let mut core = MimeDetectorCore::from_mime_config(self.mime_config.clone());
        core.set_media_stream_classifier(self.media_stream_classifier);
        Ok(MagikaMimeDetector {
            core,
            filename_detector: RepositoryMimeDetector::from_mime_config(self.mime_config),
            session: std::sync::Mutex::new(session),
        })
    }
}
