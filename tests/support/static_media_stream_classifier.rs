// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::io::Read;
use std::path::Path;

use qubit_mime::{
    MediaStreamClassifier,
    MediaStreamType,
    MimeResult,
};

/// Deterministic classifier used to verify reader-based MIME refinement.
#[derive(Debug)]
pub(crate) struct StaticMediaStreamClassifier {
    /// Classification returned for every source.
    stream_type: MediaStreamType,
    /// Optional first byte required from reader classification.
    expected_first_byte: Option<u8>,
}

impl StaticMediaStreamClassifier {
    /// Creates a deterministic classifier.
    ///
    /// # Parameters
    ///
    /// * `stream_type` - Classification returned for every source.
    /// * `expected_first_byte` - Optional first byte required from the reader.
    ///
    /// # Returns
    ///
    /// A configured deterministic classifier.
    pub(crate) fn new(
        stream_type: MediaStreamType,
        expected_first_byte: Option<u8>,
    ) -> Self {
        Self {
            stream_type,
            expected_first_byte,
        }
    }
}

impl MediaStreamClassifier for StaticMediaStreamClassifier {
    /// Returns the configured classification for a file.
    fn classify_file(&self, _file: &Path) -> MimeResult<MediaStreamType> {
        Ok(self.stream_type)
    }

    /// Optionally consumes one byte and returns the configured classification.
    fn classify_reader(
        &self,
        reader: &mut dyn Read,
    ) -> MimeResult<MediaStreamType> {
        if let Some(expected_first_byte) = self.expected_first_byte {
            let mut buffer = [0_u8; 1];
            reader.read_exact(&mut buffer)?;
            if buffer[0] != expected_first_byte {
                return Err(qubit_mime::MimeError::InvalidClassifierInput {
                    reason: "reader did not start at the beginning".to_owned(),
                });
            }
        }
        Ok(self.stream_type)
    }
}
