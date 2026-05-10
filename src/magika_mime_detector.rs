/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Magika-backed MIME detector implementation.
// qubit-style: allow coverage-cfg

use std::io::SeekFrom;
use std::path::Path;
use std::sync::{
    Mutex,
    PoisonError,
};

use magika::{
    ContentType,
    SyncInput,
};
use qubit_io::ReadSeek;
use qubit_mime::{
    DetectionSource,
    MimeConfig,
    MimeDetectionPolicy,
    MimeDetector,
    MimeDetectorCore,
    MimeError,
    MimeRepository,
    MimeResult,
    RepositoryMimeDetector,
};

/// MIME detector backed by Google's Magika model.
#[derive(Debug)]
pub struct MagikaMimeDetector {
    /// Shared detector behavior used for result selection and refinement.
    core: MimeDetectorCore,
    /// Repository detector used for filename-only detection.
    filename_detector: RepositoryMimeDetector<'static>,
    /// Magika session. The upstream API needs `&mut Session`, so access is serialized.
    session: Mutex<magika::Session>,
}

impl MagikaMimeDetector {
    /// Creates a Magika-backed detector using default MIME configuration.
    ///
    /// # Returns
    /// Initialized detector.
    ///
    /// # Errors
    /// Returns [`MimeError::DetectorBackend`] when Magika or ONNX Runtime cannot
    /// initialize.
    #[inline]
    pub fn new() -> MimeResult<Self> {
        Self::from_mime_config(MimeConfig::default())
    }

    /// Creates a Magika-backed detector from MIME configuration.
    ///
    /// # Parameters
    /// - `config`: MIME configuration used for result selection and filename
    ///   fallback.
    ///
    /// # Returns
    /// Initialized detector.
    ///
    /// # Errors
    /// Returns [`MimeError::DetectorBackend`] when Magika or ONNX Runtime cannot
    /// initialize.
    pub fn from_mime_config(config: MimeConfig) -> MimeResult<Self> {
        let session = magika::Session::new().map_err(map_magika_error)?;
        Ok(Self {
            core: MimeDetectorCore::from_mime_config(config.clone()),
            filename_detector: RepositoryMimeDetector::from_mime_config(config),
            session: Mutex::new(session),
        })
    }

    /// Gets the shared detector core.
    ///
    /// # Returns
    /// Shared detector core.
    #[inline]
    pub fn core(&self) -> &MimeDetectorCore {
        &self.core
    }

    /// Gets mutable shared detector core.
    ///
    /// # Returns
    /// Mutable shared detector core.
    #[inline]
    pub fn core_mut(&mut self) -> &mut MimeDetectorCore {
        &mut self.core
    }

    /// Gets the repository used for filename detection.
    ///
    /// # Returns
    /// Repository reference.
    #[inline]
    pub fn repository(&self) -> &MimeRepository {
        self.filename_detector.repository()
    }

    /// Gets filename candidates from the repository detector.
    ///
    /// # Parameters
    /// - `filename`: Filename or path.
    ///
    /// # Returns
    /// Candidate MIME type names.
    #[inline]
    fn guess_from_filename(&self, filename: &str) -> Vec<String> {
        self.filename_detector.guess_from_filename(filename)
    }

    /// Gets content candidates from Magika.
    ///
    /// # Parameters
    /// - `input`: Content input accepted by Magika.
    ///
    /// # Returns
    /// Zero or one MIME type candidates.
    ///
    /// # Errors
    /// Returns [`MimeError::DetectorBackend`] when Magika inference fails.
    fn guess_from_magika_input<I>(&self, input: I) -> MimeResult<Vec<String>>
    where
        I: SyncInput,
    {
        let mut session = self.session.lock().map_err(map_session_lock_error)?;
        let file_type = session
            .identify_content_sync(input)
            .map_err(map_magika_error)?;
        Ok(file_type
            .content_type()
            .and_then(content_type_to_mime)
            .into_iter()
            .collect())
    }

    /// Gets content candidates from a local file using Magika.
    ///
    /// # Parameters
    /// - `file`: Local file path.
    ///
    /// # Returns
    /// Zero or one MIME type candidates.
    ///
    /// # Errors
    /// Returns [`MimeError::Io`] for file metadata/read failures, or
    /// [`MimeError::DetectorBackend`] when Magika inference fails.
    fn guess_from_magika_file(&self, file: &Path) -> MimeResult<Vec<String>> {
        let mut session = self.session.lock().map_err(map_session_lock_error)?;
        let file_type = session.identify_file_sync(file).map_err(map_magika_error)?;
        Ok(file_type
            .content_type()
            .and_then(content_type_to_mime)
            .into_iter()
            .collect())
    }
}

impl MimeDetector for MagikaMimeDetector {
    /// Detects a MIME type from repository filename rules.
    #[inline]
    fn detect_by_filename(&self, filename: &str) -> Option<String> {
        self.filename_detector.detect_by_filename(filename)
    }

    /// Detects a MIME type from content bytes using Magika.
    fn detect_by_content(&self, content: &[u8]) -> Option<String> {
        let candidates = self.guess_from_magika_input(content).ok()?;
        candidates.first().map(|mime_type| {
            self.core
                .refine_detected_mime_type(mime_type, None, DetectionSource::Content(content))
        })
    }

    /// Detects a MIME type from bytes and optional filename.
    fn detect(
        &self,
        content: &[u8],
        filename: Option<&str>,
        policy: MimeDetectionPolicy,
    ) -> Option<String> {
        let from_filename = filename
            .map(|filename| self.guess_from_filename(filename))
            .unwrap_or_default();
        let from_content =
            if from_filename.len() == 1 && policy == MimeDetectionPolicy::PreferFilename {
                Vec::new()
            } else {
                self.guess_from_magika_input(content).unwrap_or_default()
            };
        self.core.select_result(
            &from_filename,
            &from_content,
            filename,
            policy,
            DetectionSource::Content(content),
        )
    }

    /// Detects a MIME type from a seekable reader without consuming its position.
    fn detect_reader(
        &self,
        reader: &mut dyn ReadSeek,
        filename: Option<&str>,
        policy: MimeDetectionPolicy,
    ) -> MimeResult<Option<String>> {
        let from_filename = filename
            .map(|filename| self.guess_from_filename(filename))
            .unwrap_or_default();
        let from_content =
            if from_filename.len() == 1 && policy == MimeDetectionPolicy::PreferFilename {
                Vec::new()
            } else {
                self.guess_from_reader(reader)?
            };
        Ok(self.core.select_result(
            &from_filename,
            &from_content,
            filename,
            policy,
            DetectionSource::None,
        ))
    }

    /// Detects a MIME type from a local file.
    fn detect_file(&self, file: &Path, policy: MimeDetectionPolicy) -> MimeResult<Option<String>> {
        let filename = file.to_string_lossy();
        let from_filename = self.guess_from_filename(&filename);
        let from_content =
            if from_filename.len() == 1 && policy == MimeDetectionPolicy::PreferFilename {
                Vec::new()
            } else {
                self.guess_from_magika_file(file)?
            };
        Ok(self.core.select_result(
            &from_filename,
            &from_content,
            Some(&filename),
            policy,
            DetectionSource::Path(file),
        ))
    }
}

impl MagikaMimeDetector {
    /// Gets content candidates from a seekable reader.
    ///
    /// # Parameters
    /// - `reader`: Reader to inspect. The original stream position is restored.
    ///
    /// # Returns
    /// Zero or one MIME type candidates.
    ///
    /// # Errors
    /// Returns [`MimeError::Io`] when seeking or reading fails, or
    /// [`MimeError::DetectorBackend`] when Magika inference fails.
    fn guess_from_reader(&self, reader: &mut dyn ReadSeek) -> MimeResult<Vec<String>> {
        let original_position = reader.stream_position()?;
        let length = reader.seek(SeekFrom::End(0))?;
        let mut input = ReadSeekInput { reader, length };
        let result = self.guess_from_magika_input(&mut input);
        let restore_result = input.reader.seek(SeekFrom::Start(original_position));
        match (result, restore_result) {
            (Ok(candidates), Ok(_)) => Ok(candidates),
            (Err(error), Ok(_)) => Err(error),
            (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(MimeError::Io(error)),
        }
    }
}

/// Magika input wrapper over a seekable reader.
struct ReadSeekInput<'a> {
    /// Wrapped reader.
    reader: &'a mut dyn ReadSeek,
    /// Total input length.
    length: u64,
}

impl SyncInput for ReadSeekInput<'_> {
    /// Returns the input length.
    #[inline]
    fn length(&self) -> magika::Result<u64> {
        Ok(self.length)
    }

    /// Reads exactly `buffer.len()` bytes at `offset`.
    #[inline]
    fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> magika::Result<()> {
        self.reader.seek(SeekFrom::Start(offset))?;
        self.reader.read_exact(buffer)?;
        Ok(())
    }
}

/// Converts a Magika content type to a MIME type name.
///
/// # Parameters
/// - `content_type`: Magika content type.
///
/// # Returns
/// MIME type name, or `None` for undefined content.
#[inline]
fn content_type_to_mime(content_type: ContentType) -> Option<String> {
    let mime_type = content_type.info().mime_type;
    if mime_type.is_empty() || mime_type == "application/undefined" {
        None
    } else {
        Some(mime_type.to_owned())
    }
}

/// Converts a poisoned Magika session lock to a MIME error.
///
/// # Parameters
/// - `error`: Poisoned lock error returned by [`Mutex::lock`].
///
/// # Returns
/// MIME detector backend error carrying the lock poisoning context.
#[inline]
fn map_session_lock_error<T>(error: PoisonError<T>) -> MimeError {
    MimeError::detector_backend("magika", format!("session lock poisoned: {error}"))
}

/// Converts a Magika error to a MIME error.
///
/// # Parameters
/// - `error`: Magika error.
///
/// # Returns
/// Equivalent MIME error.
#[inline]
fn map_magika_error(error: magika::Error) -> MimeError {
    match error {
        magika::Error::IOError(error) => MimeError::Io(error),
        error => MimeError::detector_backend("magika", error.to_string()),
    }
}

/// Exercises Magika session lock error conversion in coverage builds.
///
/// # Returns
/// MIME detector backend error converted from a synthetic poisoned lock error.
#[cfg(coverage)]
pub fn coverage_map_session_lock_error() -> MimeError {
    map_session_lock_error(PoisonError::new(()))
}

/// Exercises undefined content type filtering in coverage builds.
///
/// # Returns
/// `None` when Magika reports its undefined content type.
#[cfg(coverage)]
pub fn coverage_undefined_content_type_to_mime() -> Option<String> {
    content_type_to_mime(ContentType::Undefined)
}

/// Exercises non-I/O Magika error conversion in coverage builds.
///
/// # Returns
/// MIME detector backend error converted from a synthetic ONNX Runtime error.
#[cfg(all(coverage, feature = "ort"))]
pub fn coverage_map_non_io_magika_error() -> MimeError {
    map_magika_error(magika::Error::OrtError(ort::Error::new(
        "coverage non-io error",
    )))
}
