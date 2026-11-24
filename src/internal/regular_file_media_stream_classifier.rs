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

/// Classifier adapter that excludes directories and symbolic links from local
/// media refinement while preserving the wrapped classifier's byte and reader
/// behavior.
pub(crate) struct RegularFileMediaStreamClassifier {
    inner: Arc<dyn MediaStreamClassifier>,
}

impl RegularFileMediaStreamClassifier {
    /// Wraps a classifier at the local-path eligibility boundary.
    pub(crate) fn new(inner: Arc<dyn MediaStreamClassifier>) -> Self {
        Self { inner }
    }
}

impl Debug for RegularFileMediaStreamClassifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RegularFileMediaStreamClassifier")
            .field("inner", &self.inner)
            .finish()
    }
}

impl MediaStreamClassifier for RegularFileMediaStreamClassifier {
    fn classify_file(&self, file: &Path) -> MimeResult<MediaStreamType> {
        if std::fs::symlink_metadata(file)
            .map_err(MimeError::Io)?
            .is_file()
        {
            self.inner.classify_file(file)
        } else {
            Ok(MediaStreamType::None)
        }
    }

    fn classify_reader(&self, reader: &mut dyn Read) -> MimeResult<MediaStreamType> {
        self.inner.classify_reader(reader)
    }

    fn classify_content(&self, content: &[u8]) -> MimeResult<MediaStreamType> {
        self.inner.classify_content(content)
    }
}
