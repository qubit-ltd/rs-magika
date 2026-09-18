// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Magika-backed MIME detector implementation.

use std::future::Future;
use std::io::SeekFrom;
use std::path::Path;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::PoisonError;

use magika::AsyncInput;
use magika::ContentType;
use magika::Error;
use magika::FeaturesOrRuled;
use magika::Session;
use magika::SyncInput;
use qubit_fs::AsyncFileSystem;
use qubit_fs::FileSystem;
use qubit_fs::Path as FsPath;
use qubit_fs::read::ReadOptions;
use qubit_io::std_io::ReadSeek;
use qubit_mime::ContentRequirement;
use qubit_mime::MimeConfig;
use qubit_mime::MimeContentBackend;
use qubit_mime::MimeDetectorBackend;
use qubit_mime::MimeDetectorCore;
use qubit_mime::MimeError;
use qubit_mime::MimeResult;
use qubit_mime::RepositoryMimeDetector;

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
    pub(crate) core: MimeDetectorCore,
    /// Repository detector used for filename-only detection.
    pub(crate) filename_detector: RepositoryMimeDetector<'static>,
    /// Magika session. The upstream API needs `&mut Session`, so access is
    /// serialized.
    pub(crate) session: Mutex<Session>,
}

impl MagikaMimeDetector {
    /// Creates a detector builder.
    #[must_use]
    #[inline]
    pub fn builder() -> crate::MagikaMimeDetectorBuilder {
        crate::MagikaMimeDetectorBuilder::default()
    }

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
        Self::builder().mime_config(config).build()
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
    #[inline]
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
        let file_type = session.identify_content_sync(input).map_err(map_magika_error)?;
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
        let mut input = ReadSeekInput::new(reader, 0, length);
        let result = self.guess_from_magika_input(&mut input);
        let restore_result = input.reader_mut().seek(SeekFrom::Start(original_position));
        match (result, restore_result) {
            (Ok(candidates), Ok(_)) => Ok(candidates),
            (Err(error), Ok(_)) => Err(error),
            (Ok(_), Err(error)) | (Err(_), Err(error)) => Err(MimeError::Io(error)),
        }
    }
}

impl MimeDetectorBackend for MagikaMimeDetector {
    /// Reports that this backend requires complete content.
    fn content_requirement(&self) -> ContentRequirement {
        ContentRequirement::Complete
    }

    /// Gets shared selection and refinement behavior.
    fn core(&self) -> &MimeDetectorCore {
        &self.core
    }

    /// Returns the configured maximum content buffer size.
    fn max_test_bytes(&self) -> usize {
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
    /// The returned vector contains zero or more repository MIME candidates.
    #[inline]
    fn guess_from_filename(&self, filename: &str) -> Vec<String> {
        self.guess_from_filename(filename)
    }

    /// Detects MIME candidates from complete bytes.
    ///
    /// # Parameters
    ///
    /// * `content` - Complete content bytes to inspect.
    ///
    /// # Returns
    ///
    /// The returned vector contains zero or more MIME candidates. Filename and
    /// policy selection are handled by the higher-level detector API.
    ///
    /// # Errors
    /// Propagates Magika inference and media-classifier errors.
    fn guess_from_content(&self, content: &[u8]) -> MimeResult<Vec<String>> {
        self.guess_from_magika_input(content)
    }

    /// Detects MIME candidates from a seekable reader without consuming it.
    ///
    /// # Parameters
    ///
    /// * `reader` - Seekable content source whose position is restored.
    ///
    /// # Returns
    ///
    /// The returned vector contains zero or more MIME candidates.
    ///
    /// # Errors
    ///
    /// Returns [`MimeError::Io`] when reading, seeking, or restoring the reader
    /// fails, or [`MimeError::DetectorBackend`] when Magika inference fails or
    /// the session lock is poisoned.
    fn guess_from_reader(&self, reader: &mut dyn ReadSeek) -> MimeResult<(Vec<String>, Vec<u8>)> {
        Ok((self.guess_from_reader(reader)?, Vec::new()))
    }

    /// Detects a MIME type from a local file.
    ///
    /// # Parameters
    ///
    /// * `file` - Local file to inspect.
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
    fn guess_from_file(&self, file: &Path) -> MimeResult<(Vec<String>, Vec<u8>)> {
        Ok((self.guess_from_magika_file(file)?, Vec::new()))
    }

    /// Detects MIME candidates from a synchronous filesystem path.
    ///
    /// The complete file length is reported to Magika, while each read is
    /// bounded by `max_bytes`.
    ///
    /// # Parameters
    ///
    /// * `file_system` - Filesystem used to access the path.
    /// * `path` - File path to inspect.
    /// * `max_bytes` - Maximum bytes allowed in one read.
    ///
    /// # Errors
    ///
    /// Returns a MIME error when metadata, filesystem reads, or Magika
    /// inference fails.
    fn guess_from_provider_path(
        &self,
        file_system: &FileSystem,
        path: &FsPath,
        max_bytes: usize,
    ) -> MimeResult<(Vec<String>, Option<Vec<u8>>)> {
        let metadata = file_system.stat(path)?;
        let length = metadata.len().ok_or(MimeError::CompleteContentRequired)?;
        if length > usize::MAX as u64 {
            return Err(MimeError::BufferLimitExceeded {
                requested: usize::MAX,
                limit: max_bytes,
            });
        }
        let mut input = ProviderSyncInput {
            file_system,
            path,
            length,
            budget: max_bytes,
        };
        Ok((self.guess_from_magika_input(&mut input)?, None))
    }

    /// Detects MIME candidates from an asynchronous filesystem path.
    ///
    /// # Parameters
    ///
    /// * `file_system` - Asynchronous filesystem used to access the path.
    /// * `path` - File path to inspect.
    /// * `max_bytes` - Maximum bytes allowed in one read.
    ///
    /// # Errors
    ///
    /// Returns a MIME error when metadata, filesystem reads, or Magika
    /// inference fails.
    fn guess_from_async_provider_path<'a>(
        &'a self,
        file_system: &'a AsyncFileSystem,
        path: &'a FsPath,
        max_bytes: usize,
    ) -> Pin<Box<dyn Future<Output = MimeResult<(Vec<String>, Option<Vec<u8>>)>> + Send + 'a>> {
        Box::pin(async move {
            let metadata = file_system.stat(path).await?;
            let length = metadata.len().ok_or(MimeError::CompleteContentRequired)?;
            let input = ProviderAsyncInput {
                file_system,
                path,
                length,
                budget: max_bytes,
            };
            let file_type = FeaturesOrRuled::extract_async(input).await.map_err(map_magika_error)?;
            let candidates = match file_type {
                FeaturesOrRuled::Ruled(content_type) => content_type_to_mime(content_type).into_iter().collect(),
                FeaturesOrRuled::Features(features) => {
                    let mut session = self.session.lock().map_err(map_session_lock_error)?;
                    let file_type = session.identify_features_sync(&features).map_err(map_magika_error)?;
                    file_type
                        .content_type()
                        .and_then(content_type_to_mime)
                        .into_iter()
                        .collect()
                }
            };
            Ok((candidates, None))
        })
    }
}

/// Adapts synchronous filesystem reads to Magika's random-access input.
struct ProviderSyncInput<'a> {
    /// Filesystem used for bounded reads.
    file_system: &'a FileSystem,
    /// Path whose contents are being classified.
    path: &'a FsPath,
    /// Complete file length reported to Magika.
    length: u64,
    /// Maximum size of an individual read.
    budget: usize,
}

impl magika::SyncInput for ProviderSyncInput<'_> {
    /// Returns the complete file length.
    fn length(&self) -> magika::Result<u64> {
        Ok(self.length)
    }

    /// Reads one bounded range from the filesystem.
    fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> magika::Result<()> {
        let end = offset
            .checked_add(buffer.len() as u64)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "offset overflow"))?;
        if end > self.length {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "read exceeds input").into());
        }
        if buffer.len() > self.budget {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                format!("qubit-magika-budget:{}:{}", buffer.len(), self.budget),
            )
            .into());
        }
        let bytes = self
            .file_system
            .read_prefix(
                self.path,
                ReadOptions::default()
                    .with_offset(Some(offset))
                    .with_length(Some(buffer.len() as u64)),
                buffer.len(),
            )
            .map_err(|error| error.into_io_error())?
            .into_bytes();
        if bytes.len() != buffer.len() {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short provider read").into());
        }
        buffer.copy_from_slice(&bytes);
        Ok(())
    }
}

/// Adapts asynchronous filesystem reads to Magika's random-access input.
struct ProviderAsyncInput<'a> {
    /// Filesystem used for bounded reads.
    file_system: &'a AsyncFileSystem,
    /// Path whose contents are being classified.
    path: &'a FsPath,
    /// Complete file length reported to Magika.
    length: u64,
    /// Maximum size of an individual read.
    budget: usize,
}

impl AsyncInput for ProviderAsyncInput<'_> {
    /// Returns the complete file length.
    async fn length(&self) -> magika::Result<u64> {
        Ok(self.length)
    }

    /// Reads one bounded range from the filesystem.
    async fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> magika::Result<()> {
        let end = offset
            .checked_add(buffer.len() as u64)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "offset overflow"))?;
        if end > self.length {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "read exceeds input").into());
        }
        if buffer.len() > self.budget {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                format!("qubit-magika-budget:{}:{}", buffer.len(), self.budget),
            )
            .into());
        }
        let bytes = self
            .file_system
            .read_prefix(
                self.path,
                ReadOptions::default()
                    .with_offset(Some(offset))
                    .with_length(Some(buffer.len() as u64)),
                buffer.len(),
            )
            .await
            .map_err(|error| error.into_io_error())?
            .into_bytes();
        if bytes.len() != buffer.len() {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short provider read").into());
        }
        buffer.copy_from_slice(&bytes);
        Ok(())
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
    let mime_type = match content_type {
        ContentType::Unknown => "application/octet-stream",
        ContentType::Undefined => "application/undefined",
        content_type => content_type.info().mime_type,
    };
    if mime_type.is_empty() {
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
pub(crate) fn map_magika_error(error: Error) -> MimeError {
    match error {
        Error::IOError(error) => MimeError::Io(error),
        error => MimeError::detector_backend("magika", error.to_string()),
    }
}

impl MimeContentBackend for MagikaMimeDetector {
    /// Reports that Magika requires complete content.
    fn content_requirement(&self) -> ContentRequirement {
        ContentRequirement::Complete
    }

    /// Detects MIME candidates from complete content bytes.
    fn detect_bytes(&self, bytes: &[u8]) -> MimeResult<Vec<String>> {
        self.guess_from_magika_input(bytes)
    }
}
