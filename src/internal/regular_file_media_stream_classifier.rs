// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Limits local-file media refinement to ordinary files.

use std::fmt::Debug;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use qubit_mime::MediaStreamClassifier;
use qubit_mime::MediaStreamType;
use qubit_mime::MimeError;
use qubit_mime::MimeResult;

/// Classifier adapter that excludes non-regular local paths from media
/// refinement while preserving the wrapped classifier's byte and reader
/// behavior. Its path check uses `symlink_metadata`, so it does not follow a
/// final symbolic link. Non-regular paths return `MediaStreamType::None`;
/// metadata errors and errors from the wrapped classifier are propagated.
pub(crate) struct RegularFileMediaStreamClassifier {
    /// The classifier used after a path passes the regular-file check.
    inner: Arc<dyn MediaStreamClassifier>,
}

impl RegularFileMediaStreamClassifier {
    /// Wraps a classifier at the local-path eligibility boundary.
    ///
    /// # Parameters
    ///
    /// * `inner` - The classifier to wrap.
    pub(crate) fn new(inner: Arc<dyn MediaStreamClassifier>) -> Self {
        Self { inner }
    }
}

impl Debug for RegularFileMediaStreamClassifier {
    /// Formats the adapter and its wrapped classifier.
    ///
    /// # Parameters
    ///
    /// * `formatter` - The formatter to write the debug representation into.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RegularFileMediaStreamClassifier")
            .field("inner", &self.inner)
            .finish()
    }
}

impl MediaStreamClassifier for RegularFileMediaStreamClassifier {
    /// Classifies a path only when `symlink_metadata` observes it as an
    /// ordinary file. Directories, final symlinks, and all other non-regular
    /// types return [`MediaStreamType::None`]. Metadata errors and errors from
    /// the wrapped classifier are propagated. The metadata check and the
    /// classifier's subsequent open are separate operations, so this check
    /// does not prevent the path from being replaced between them.
    ///
    /// # Parameters
    ///
    /// * `file` - The path to inspect and classify.
    fn classify_file(&self, file: &Path) -> MimeResult<MediaStreamType> {
        if std::fs::symlink_metadata(file).map_err(MimeError::Io)?.is_file() {
            self.inner.classify_file(file)
        } else {
            Ok(MediaStreamType::None)
        }
    }

    /// Classifies the bytes available from a reader using the wrapped
    /// classifier.
    ///
    /// # Parameters
    ///
    /// * `reader` - The reader whose bytes should be classified.
    fn classify_reader(&self, reader: &mut dyn Read) -> MimeResult<MediaStreamType> {
        self.inner.classify_reader(reader)
    }

    /// Classifies in-memory content using the wrapped classifier.
    ///
    /// # Parameters
    ///
    /// * `content` - The bytes to classify.
    fn classify_content(&self, content: &[u8]) -> MimeResult<MediaStreamType> {
        self.inner.classify_content(content)
    }
}
