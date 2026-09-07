// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
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
#[derive(Debug, Default)]
pub struct MagikaMimeDetectorBuilder {
    mime_config: MimeConfig,
    media_stream_classifier: Option<Arc<dyn MediaStreamClassifier>>,
}

impl MagikaMimeDetectorBuilder {
    /// Replaces the MIME policy and refinement configuration.
    #[must_use]
    pub fn mime_config(mut self, mime_config: MimeConfig) -> Self {
        self.mime_config = mime_config;
        self
    }

    /// Sets the optional classifier used for precise media refinement.
    #[must_use]
    pub fn media_stream_classifier(mut self, media_stream_classifier: Option<Arc<dyn MediaStreamClassifier>>) -> Self {
        self.media_stream_classifier = media_stream_classifier;
        self
    }

    /// Initializes the Magika session and constructs the detector.
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
