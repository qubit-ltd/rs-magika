// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Magika-backed MIME detector implementation.

use std::io::SeekFrom;
use std::path::Path;
use std::sync::{Mutex, PoisonError};

use magika::{ContentType, SyncInput};
use qubit_io::std_io::ReadSeek;
use qubit_mime::{
    DetectionSource, MimeConfig, MimeDetectionPolicy, MimeDetector, MimeDetectorCore, MimeError,
    MimeRepository, MimeResult, RepositoryMimeDetector,
};

use crate::internal::ReadSeekInput;

/// Blocking MIME detector backed by Google's Magika model.
///
/// Construction initializes Magika's embedded model and ONNX Runtime session,
/// so applications should create the detector once and share it, for example
/// through [`std::sync::Arc`]. Inference methods are blocking. Because Magika
/// requires mutable access to its session, concurrent inference calls on one
/// detector are serialized by an internal mutex.
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
///
/// use qubit_magika::MagikaMimeDetector;
/// use qubit_mime::{MimeDetector, MimeError};
///
/// let detector = Arc::new(MagikaMimeDetector::new()?);
/// let mime_type = detector.detect_by_content(b"%PDF-1.7")?;
/// assert_eq!(Some("application/pdf".to_owned()), mime_type);
/// # Ok::<(), MimeError>(())
/// ```
#[derive(Debug)]
pub struct MagikaMimeDetector {
    /// Shared detector behavior used for result selection and refinement.
    core: MimeDetectorCore,
    /// Repository detector used for filename-only detection.
    filename_detector: RepositoryMimeDetector<'static>,
    /// Magika session. The upstream API needs `&mut Session`, so access is
    /// serialized.
    session: Mutex<magika::Session>,
}

impl MagikaMimeDetector {
    /// Creates a detector using [`MimeConfig::default`] and a new Magika
    /// session.
    ///
    /// # Returns
    ///
    /// An initialized detector ready for blocking inference.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] for runtime I/O failures or
    /// [`MimeError::DetectorBackend`] when Magika or ONNX Runtime cannot
    /// initialize.
    #[inline(always)]
    pub fn new() -> MimeResult<Self> {
        Self::from_mime_config(MimeConfig::default())
    }

    /// Creates a detector using MIME selection/refinement configuration and a
    /// new Magika session.
    ///
    /// The configuration controls repository filename matching and result
    /// refinement. Magika's embedded model and runtime configuration remain
    /// owned by the upstream Magika session.
    ///
    /// # Parameters
    ///
    /// * `config` - MIME configuration used for result selection and filename
    ///   fallback.
    ///
    /// # Returns
    ///
    /// An initialized detector ready for blocking inference.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] for runtime I/O failures or
    /// [`MimeError::DetectorBackend`] when Magika or ONNX Runtime cannot
    /// initialize.
    #[inline]
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
    ///
    /// The shared detector core used for selection and refinement.
    #[inline(always)]
    pub fn core(&self) -> &MimeDetectorCore {
        &self.core
    }

    /// Gets mutable shared detector core.
    ///
    /// # Returns
    ///
    /// Mutable access to the shared detector core.
    #[inline(always)]
    pub fn core_mut(&mut self) -> &mut MimeDetectorCore {
        &mut self.core
    }

    /// Gets the repository used for filename detection.
    ///
    /// # Returns
    ///
    /// The repository used by filename-only detection.
    #[inline(always)]
    pub fn repository(&self) -> &MimeRepository {
        self.filename_detector.repository()
    }

    /// Gets filename candidates from the repository detector.
    ///
    /// # Parameters
    ///
    /// * `filename` - Filename or path.
    ///
    /// # Returns
    ///
    /// Candidate MIME type names.
    #[inline(always)]
    fn guess_from_filename(&self, filename: &str) -> Vec<String> {
        self.filename_detector.guess_from_filename(filename)
    }

    /// Gets content candidates from a Magika-compatible synchronous input.
    ///
    /// `I` is any input implementing [`SyncInput`]. This blocking operation
    /// serializes access to the shared Magika session.
    ///
    /// # Parameters
    ///
    /// * `input` - Content input accepted by Magika.
    ///
    /// # Returns
    ///
    /// Zero or one MIME type candidates.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] for input I/O failures or
    /// [`MimeError::DetectorBackend`] when inference fails or the session lock
    /// is poisoned.
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
    ///
    /// * `file` - Local file path.
    ///
    /// # Returns
    ///
    /// Zero or one MIME type candidates.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] for file metadata/read failures, or
    /// [`MimeError::DetectorBackend`] when inference fails or the session lock
    /// is poisoned.
    fn guess_from_magika_file(&self, file: &Path) -> MimeResult<Vec<String>> {
        let mut session = self.session.lock().map_err(map_session_lock_error)?;
        let file_type = session.identify_file_sync(file).map_err(map_magika_error)?;
        Ok(file_type
            .content_type()
            .and_then(content_type_to_mime)
            .into_iter()
            .collect())
    }

    /// Gets content candidates from a seekable reader.
    ///
    /// # Parameters
    ///
    /// * `reader` - Reader to inspect. The original stream position is
    ///   restored.
    ///
    /// # Returns
    ///
    /// Zero or one MIME type candidates.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] when seeking or reading fails, or
    /// [`MimeError::DetectorBackend`] when Magika inference fails or the
    /// session lock is poisoned.
    fn guess_from_reader(&self, reader: &mut dyn ReadSeek) -> MimeResult<Vec<String>> {
        let original_position = reader.stream_position()?;
        let length = reader.seek(SeekFrom::End(0))?;
        let mut input = ReadSeekInput::new(reader, length);
        let result = self.guess_from_magika_input(&mut input);
        let restore_result = input.reader_mut().seek(SeekFrom::Start(original_position));
        match (result, restore_result) {
            (Ok(candidates), Ok(_)) => Ok(candidates),
            (Err(error), Ok(_)) => Err(error),
            (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(MimeError::Io(error)),
        }
    }
}

impl MimeDetector for MagikaMimeDetector {
    /// Gets the maximum prefix size accepted by filesystem-path detection.
    fn max_buffer_size(&self) -> usize {
        self.core.max_buffer_size()
    }

    /// Detects a MIME type from repository filename rules.
    ///
    /// # Parameters
    ///
    /// * `filename` - Filename or path to match.
    ///
    /// # Returns
    ///
    /// `Ok(Some(_))` contains the selected MIME type; `Ok(None)` means no
    /// repository rule matches.
    #[inline(always)]
    fn detect_by_filename(&self, filename: &str) -> MimeResult<Option<String>> {
        self.filename_detector.detect_by_filename(filename)
    }

    /// Detects a MIME type from content bytes using blocking Magika inference.
    ///
    /// # Parameters
    ///
    /// * `content` - Complete content bytes to inspect.
    ///
    /// # Returns
    ///
    /// `Ok(Some(_))` contains the detected MIME type; `Ok(None)` means Magika
    /// returned no mapped type.
    ///
    /// # Errors
    /// Propagates Magika inference and media-classifier errors.
    fn detect_by_content(&self, content: &[u8]) -> MimeResult<Option<String>> {
        let candidates = self.guess_from_magika_input(content)?;
        candidates
            .first()
            .map(|mime_type| {
                self.core.refine_detected_mime_type(
                    mime_type,
                    None,
                    DetectionSource::Content(content),
                )
            })
            .transpose()
    }

    /// Detects a MIME type from bytes and an optional filename.
    ///
    /// # Parameters
    ///
    /// * `content` - Complete content bytes to inspect.
    /// * `filename` - Optional filename used for repository matching.
    /// * `policy` - Strategy for combining filename and content candidates.
    ///
    /// # Returns
    ///
    /// `Ok(Some(_))` contains the selected MIME type; `Ok(None)` means neither
    /// source yields a candidate.
    ///
    /// # Errors
    /// Propagates Magika inference and media-classifier errors.
    fn detect(
        &self,
        content: &[u8],
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
                self.guess_from_magika_input(content)?
            };
        self.core.select_result(
            &from_filename,
            &from_content,
            filename,
            policy,
            DetectionSource::Content(content),
        )
    }

    /// Detects a MIME type from a seekable reader without consuming its
    /// position.
    ///
    /// # Parameters
    ///
    /// * `reader` - Seekable content source whose position is restored.
    /// * `filename` - Optional filename used for repository matching.
    /// * `policy` - Strategy for combining filename and content candidates.
    ///
    /// # Returns
    ///
    /// The selected MIME type, or `None` when neither source yields a
    /// candidate.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] when reading, seeking, or restoring the reader
    /// fails, or [`MimeError::DetectorBackend`] when Magika inference fails or
    /// the session lock is poisoned.
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
        self.core
            .select_reader_result(&from_filename, &from_content, filename, policy, reader)
    }

    /// Detects a MIME type from a local file.
    ///
    /// # Parameters
    ///
    /// * `file` - Local file to inspect.
    /// * `policy` - Strategy for combining filename and content candidates.
    ///
    /// # Returns
    ///
    /// The selected MIME type, or `None` when neither source yields a
    /// candidate.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] when file metadata or content cannot be read,
    /// or [`MimeError::DetectorBackend`] when Magika inference fails or the
    /// session lock is poisoned.
    fn detect_file(&self, file: &Path, policy: MimeDetectionPolicy) -> MimeResult<Option<String>> {
        let filename = file.to_string_lossy();
        let from_filename = self.guess_from_filename(&filename);
        let from_content =
            if from_filename.len() == 1 && policy == MimeDetectionPolicy::PreferFilename {
                Vec::new()
            } else {
                self.guess_from_magika_file(file)?
            };
        self.core.select_result(
            &from_filename,
            &from_content,
            Some(&filename),
            policy,
            DetectionSource::Path(file),
        )
    }
}

/// Converts a Magika content type to a MIME type name.
///
/// # Parameters
///
/// * `content_type` - Magika content type.
///
/// # Returns
///
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
///
/// * `error` - Poisoned lock error returned by [`Mutex::lock`].
///
/// # Returns
///
/// MIME detector backend error carrying the lock poisoning context.
#[inline]
fn map_session_lock_error<T>(error: PoisonError<T>) -> MimeError {
    MimeError::detector_backend("magika", format!("session lock poisoned: {error}"))
}

/// Converts a Magika error to a MIME error.
///
/// # Parameters
///
/// * `error` - Magika error.
///
/// # Returns
///
/// Equivalent MIME error.
#[inline]
fn map_magika_error(error: magika::Error) -> MimeError {
    match error {
        magika::Error::IOError(error) => MimeError::Io(error),
        error => MimeError::detector_backend("magika", error.to_string()),
    }
}
