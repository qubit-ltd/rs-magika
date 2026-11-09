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

use magika::ContentType;
use magika::Error;
use magika::FeaturesOrRuled;
use magika::FileType;
use magika::Session;
use magika::SyncInput;
use qubit_fs::AsyncFileSystem;
use qubit_fs::FileSystem;
use qubit_fs::Path as FsPath;
use qubit_fs::error::FsErrorKind;
use qubit_fs::metadata::FileSystemCapability;
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

use crate::internal::AsyncProviderInput;
use crate::internal::ReadSeekInput;
use crate::internal::ReaderScope;
use crate::internal::SyncProviderInput;
use crate::internal::map_provider_magika_error;

/// Blocking MIME detector backed by Google's Magika model.
///
/// Construction initializes Magika's embedded model and ONNX Runtime session,
/// so applications should create the detector once and share it, for example
/// through [`std::sync::Arc`]. Inference methods are blocking. Because Magika
/// requires mutable access to its session, concurrent inference calls on one
/// detector are serialized by an internal mutex. Input sampling and feature
/// extraction happen outside that mutex; rule-classified inputs do not need
/// the session. Sampling and waiting for inference are blocking operations.
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
    /// `I` is any input implementing [`SyncInput`]. Sampling and feature
    /// extraction do not hold the shared Magika session lock; model inference
    /// is serialized. Rule-classified inputs do not require the session.
    ///
    /// # Type Parameters
    ///
    /// * `I` - Synchronous Magika input used for sampling and feature
    ///   extraction.
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
        let extracted = FeaturesOrRuled::extract_sync(input).map_err(map_magika_error)?;
        self.guess_from_extracted(extracted, map_magika_error)
    }

    /// Maps ruled classifications directly and serializes only model inference.
    ///
    /// The supplied mapper preserves the error contract of the input route that
    /// extracted the features. Ruled results do not acquire the session lock.
    ///
    /// # Parameters
    ///
    /// * `extracted` - Rules result or features extracted without holding the
    ///   session lock.
    /// * `map_error` - Converts upstream inference errors for the originating
    ///   route.
    ///
    /// # Returns
    ///
    /// Zero or one MIME type candidates.
    ///
    /// # Errors
    ///
    /// Returns a backend error if the session lock is poisoned or inference
    /// fails.
    fn guess_from_extracted(
        &self,
        extracted: FeaturesOrRuled,
        map_error: fn(Error) -> MimeError,
    ) -> MimeResult<Vec<String>> {
        match extracted {
            FeaturesOrRuled::Ruled(content_type) => Ok(content_type_to_mime(content_type).into_iter().collect()),
            FeaturesOrRuled::Features(features) => {
                let mut session = self.session.lock().map_err(map_session_lock_error)?;
                let file_type = session.identify_features_sync(&features).map_err(map_error)?;
                Ok(file_type
                    .content_type()
                    .and_then(content_type_to_mime)
                    .into_iter()
                    .collect())
            }
        }
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
        let metadata = std::fs::symlink_metadata(file)?;
        let file_type = if metadata.file_type().is_symlink() {
            FileType::Symlink
        } else if metadata.is_dir() {
            FileType::Directory
        } else {
            return self.guess_from_magika_input(std::fs::File::open(file)?);
        };
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
    fn guess_from_reader_with_scope(&self, reader: &mut dyn ReadSeek, scope: ReaderScope) -> MimeResult<Vec<String>> {
        let original_position = reader.stream_position()?;
        let result = (|| {
            let end = reader.seek(SeekFrom::End(0))?;
            let base_offset = match scope {
                ReaderScope::WholeResource => 0,
                ReaderScope::Remaining => original_position,
            };
            let length = end.checked_sub(base_offset).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "reader end precedes window start")
            })?;
            let mut input = ReadSeekInput::new(reader, base_offset, length);
            self.guess_from_magika_input(&mut input)
        })();
        let restore_result = reader.seek(SeekFrom::Start(original_position));
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
        Ok((
            self.guess_from_reader_with_scope(reader, ReaderScope::WholeResource)?,
            Vec::new(),
        ))
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
    /// The complete file length is reported to Magika, while the cumulative
    /// bytes requested by all reads are bounded by `max_bytes`.
    ///
    /// # Parameters
    ///
    /// * `file_system` - Filesystem used to access the path.
    /// * `path` - File path to inspect.
    /// * `max_bytes` - Maximum cumulative bytes allowed across all reads.
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
        let limit = self.max_test_bytes();
        if max_bytes > limit {
            return Err(MimeError::BufferLimitExceeded {
                requested: max_bytes,
                limit,
            });
        }
        match file_system.open_random_access_reader(path, ReadOptions::default()) {
            Ok(reader) => {
                let max_segment = file_system
                    .properties()
                    .limits()
                    .max_read_range_bytes()
                    .maximum()
                    .and_then(|value| usize::try_from(value).ok());
                let input = SyncProviderInput::from_session(reader, max_bytes, max_segment);
                let extracted = FeaturesOrRuled::extract_sync(input).map_err(map_provider_magika_error)?;
                let candidates = self.guess_from_extracted(extracted, map_provider_magika_error)?;
                return Ok((candidates, None));
            }
            Err(error)
                if matches!(
                    error.kind(),
                    FsErrorKind::UnsupportedOperation | FsErrorKind::UnsupportedCapability
                ) => {}
            Err(error) => return Err(error.into()),
        }
        let metadata = match file_system.stat(path) {
            Ok(metadata) => Some(metadata),
            Err(error)
                if matches!(
                    error.kind(),
                    FsErrorKind::UnsupportedOperation | FsErrorKind::UnsupportedCapability
                ) =>
            {
                None
            }
            Err(error) => return Err(error.into()),
        };
        let capabilities = file_system.properties().capabilities();
        if let Some(metadata) = metadata.as_ref()
            && let Some(length) = metadata.len()
        {
            if capabilities.supports(FileSystemCapability::RangeRead) {
                let version = capabilities
                    .supports(FileSystemCapability::ConditionalRead)
                    .then(|| metadata.etag().cloned())
                    .flatten();
                let input = SyncProviderInput::new(file_system, path, length, version, max_bytes);
                let extracted = FeaturesOrRuled::extract_sync(input).map_err(map_provider_magika_error)?;
                let candidates = self.guess_from_extracted(extracted, map_provider_magika_error)?;
                return Ok((candidates, None));
            }
            let requested = usize::try_from(length).unwrap_or(usize::MAX);
            if length > max_bytes as u64 {
                return Err(MimeError::BufferLimitExceeded {
                    requested,
                    limit: max_bytes,
                });
            }
        }
        let bytes = file_system.read_all(path, ReadOptions::default(), max_bytes)?;
        let candidates = self.guess_from_magika_input(bytes.as_slice())?;
        Ok((candidates, None))
    }

    /// Detects MIME candidates from an asynchronous filesystem path.
    ///
    /// # Parameters
    ///
    /// * `file_system` - Asynchronous filesystem used to access the path.
    /// * `path` - File path to inspect.
    /// * `max_bytes` - Maximum cumulative bytes allowed across all reads.
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
            let limit = self.max_test_bytes();
            if max_bytes > limit {
                return Err(MimeError::BufferLimitExceeded {
                    requested: max_bytes,
                    limit,
                });
            }
            match file_system
                .open_random_access_reader(path, ReadOptions::default())
                .await
            {
                Ok(reader) => {
                    let max_segment = file_system
                        .properties()
                        .limits()
                        .max_read_range_bytes()
                        .maximum()
                        .and_then(|value| usize::try_from(value).ok());
                    let input = AsyncProviderInput::from_session(reader, max_bytes, max_segment);
                    let candidates = guess_from_async_input(self, input).await?;
                    return Ok((candidates, None));
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        FsErrorKind::UnsupportedOperation | FsErrorKind::UnsupportedCapability
                    ) => {}
                Err(error) => return Err(error.into()),
            }
            let metadata = match file_system.stat(path).await {
                Ok(metadata) => Some(metadata),
                Err(error)
                    if matches!(
                        error.kind(),
                        FsErrorKind::UnsupportedOperation | FsErrorKind::UnsupportedCapability
                    ) =>
                {
                    None
                }
                Err(error) => return Err(error.into()),
            };
            let capabilities = file_system.properties().capabilities();
            if let Some(metadata) = metadata.as_ref()
                && let Some(length) = metadata.len()
            {
                if capabilities.supports(FileSystemCapability::RangeRead) {
                    let version = capabilities
                        .supports(FileSystemCapability::ConditionalRead)
                        .then(|| metadata.etag().cloned())
                        .flatten();
                    let input = AsyncProviderInput::new(file_system, path, length, version, max_bytes);
                    let candidates = guess_from_async_input(self, input).await?;
                    return Ok((candidates, None));
                }
                let requested = usize::try_from(length).unwrap_or(usize::MAX);
                if length > max_bytes as u64 {
                    return Err(MimeError::BufferLimitExceeded {
                        requested,
                        limit: max_bytes,
                    });
                }
            }
            let bytes = file_system.read_all(path, ReadOptions::default(), max_bytes).await?;
            let candidates = self.guess_from_magika_input(bytes.as_slice())?;
            Ok((candidates, None))
        })
    }
}

/// Extracts features through asynchronous provider reads, then runs inference.
///
/// Session locking and inference remain synchronous after feature extraction;
/// provider errors and poisoned locks are returned as `MimeError`.
///
/// # Parameters
///
/// * `detector` - Detector whose session performs model inference.
/// * `input` - Asynchronous provider input to sample.
///
/// # Returns
///
/// Zero or one MIME type candidates.
///
/// # Errors
///
/// Returns I/O, budget, backend, or poisoned-session errors from sampling and
/// inference.
async fn guess_from_async_input(
    detector: &MagikaMimeDetector,
    input: AsyncProviderInput<'_>,
) -> MimeResult<Vec<String>> {
    let file_type = FeaturesOrRuled::extract_async(input)
        .await
        .map_err(map_provider_magika_error)?;
    detector.guess_from_extracted(file_type, map_provider_magika_error)
}

/// Converts a Magika content type to a MIME type name.
///
/// # Parameters
///
/// * `content_type` - Magika content type.
///
/// # Returns
///
/// MIME type name, or `None` for unknown, undefined, or empty MIME mappings.
#[inline]
fn content_type_to_mime(content_type: ContentType) -> Option<String> {
    owned_mime_type(content_type_mime_name(content_type))
}

/// Returns Magika's MIME name, excluding unknown and undefined classifications.
///
/// A known content type yields `Some`; sentinel types yield `None`.
///
/// # Parameters
///
/// * `content_type` - Magika classification whose canonical MIME name is
///   requested.
///
/// # Returns
///
/// The static MIME name, or `None` for unknown and undefined classifications.
fn content_type_mime_name(content_type: ContentType) -> Option<&'static str> {
    match content_type {
        ContentType::Unknown | ContentType::Undefined => None,
        content_type => Some(content_type.info().mime_type),
    }
}

/// Copies a nonempty MIME name into an owned result.
///
/// Empty or absent names remain `None`, so policy fallback can handle them.
///
/// # Parameters
///
/// * `mime_type` - Optional MIME name to copy.
///
/// # Returns
///
/// An owned, nonempty MIME name, or `None` when the name is absent or empty.
fn owned_mime_type(mime_type: Option<&str>) -> Option<String> {
    mime_type.filter(|mime_type| !mime_type.is_empty()).map(str::to_owned)
}

/// Converts a poisoned Magika session lock to a MIME error.
///
/// # Parameters
///
/// * `error` - Poisoned lock error returned by [`Mutex::lock`].
///
/// # Type Parameters
///
/// * `T` - Guard type stored inside the poisoned lock error.
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
    ///
    /// # Returns
    ///
    /// The complete-content requirement.
    fn content_requirement(&self) -> ContentRequirement {
        ContentRequirement::Complete
    }

    /// Detects MIME candidates from complete content bytes.
    ///
    /// # Parameters
    ///
    /// * `bytes` - Complete content to classify.
    ///
    /// # Returns
    ///
    /// MIME candidates inferred from `bytes`.
    fn detect_bytes(&self, bytes: &[u8]) -> MimeResult<Vec<String>> {
        self.guess_from_magika_input(bytes)
    }

    /// Detects the logical content window from the current reader position to
    /// EOF.
    ///
    /// # Parameters
    ///
    /// * `reader` - Seekable reader whose remaining content is classified.
    ///
    /// # Returns
    ///
    /// MIME candidates inferred from the remaining content.
    fn detect_reader(&self, reader: &mut dyn ReadSeek) -> MimeResult<Vec<String>> {
        self.guess_from_reader_with_scope(reader, ReaderScope::Remaining)
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::io::Cursor;
    use std::io::Write;
    use std::sync::Arc;

    use magika::ContentType;
    use magika::Error;
    use magika::SyncInput;
    use qubit_io::std_io::ReadSeek;
    use qubit_mime::ContentRequirement;
    use qubit_mime::MimeConfig;
    use qubit_mime::MimeContentBackend;
    use qubit_mime::MimeDetector;
    use qubit_mime::MimeDetectorBackend;
    use qubit_mime::MimeError;
    use tempfile::NamedTempFile;

    use super::MagikaMimeDetector;
    use super::content_type_to_mime;

    struct LockCheckingInput {
        detector: Arc<MagikaMimeDetector>,
        bytes: &'static [u8],
    }

    impl SyncInput for LockCheckingInput {
        fn length(&self) -> magika::Result<u64> {
            let _guard = self
                .detector
                .session
                .try_lock()
                .map_err(|_| magika::Error::IOError(io::Error::other("session lock held during input length")))?;
            Ok(self.bytes.len() as u64)
        }

        fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> magika::Result<()> {
            let _guard = self
                .detector
                .session
                .try_lock()
                .map_err(|_| magika::Error::IOError(io::Error::other("session lock held during input read")))?;
            let start = usize::try_from(offset).expect("test offset fits usize");
            buffer.copy_from_slice(&self.bytes[start..start + buffer.len()]);
            Ok(())
        }
    }

    #[test]
    fn test_undefined_content_is_not_a_candidate() {
        assert_eq!(None, content_type_to_mime(ContentType::Undefined));
    }

    #[test]
    fn test_constructor_and_builder_detect_python_content() {
        let detector = MagikaMimeDetector::new().expect("detector should initialize");
        assert_eq!(
            Some("application/pdf".to_owned()),
            detector.guess_from_filename("document.pdf").first().cloned()
        );
        let content_backend: &dyn MimeContentBackend = &detector;
        assert_eq!(ContentRequirement::Complete, content_backend.content_requirement());
        assert_eq!(
            vec!["text/x-python"],
            content_backend
                .detect_bytes(b"#!/usr/bin/env python3\nprint('ok')\n")
                .expect("bytes should classify")
        );
        let detector_backend: &dyn MimeDetectorBackend = &detector;
        assert_eq!(ContentRequirement::Complete, detector_backend.content_requirement());
        assert_eq!(detector.max_buffer_size(), detector_backend.max_test_bytes());
        assert_eq!(
            vec!["application/pdf"],
            detector_backend.guess_from_filename("document.pdf")
        );
        assert_eq!(
            vec!["text/x-python"],
            detector_backend
                .guess_from_content(b"#!/usr/bin/env python3\nprint('ok')\n")
                .expect("content should classify")
        );
        let mut reader = Cursor::new(b"#!/usr/bin/env python3\nprint('ok')\n".to_vec());
        assert_eq!(
            vec!["text/x-python"],
            MimeContentBackend::detect_reader(&detector, &mut reader).expect("content reader should classify")
        );
        let mut reader = Cursor::new(b"#!/usr/bin/env python3\nprint('ok')\n".to_vec());
        assert_eq!(
            (vec!["text/x-python".to_owned()], Vec::new()),
            MimeDetectorBackend::guess_from_reader(&detector, &mut reader).expect("detector reader should classify")
        );
        assert_eq!(
            (vec!["text/x-python".to_owned()], Vec::new()),
            MimeDetectorBackend::guess_from_file(
                &detector,
                std::path::Path::new("tests/fixtures/real_files/script.py"),
            )
            .expect("detector file should classify")
        );
        assert_eq!(
            ContentRequirement::Complete,
            MimeContentBackend::content_requirement(&detector)
        );
        assert_eq!(
            ContentRequirement::Complete,
            MimeDetectorBackend::content_requirement(&detector)
        );
        assert_eq!(
            detector.max_buffer_size(),
            MimeDetectorBackend::max_test_bytes(&detector)
        );
        assert_eq!(
            vec!["application/pdf"],
            MimeDetectorBackend::guess_from_filename(&detector, "document.pdf")
        );
        assert_eq!(
            vec!["text/x-python"],
            MimeDetectorBackend::guess_from_content(&detector, b"#!/usr/bin/env python3\nprint('ok')\n",)
                .expect("fully qualified content should classify")
        );
        let _ = MagikaMimeDetector::builder()
            .mime_config(MimeConfig::default())
            .build()
            .expect("builder should initialize");
        let _ =
            MagikaMimeDetector::from_mime_config(MimeConfig::default()).expect("configured detector should initialize");
    }

    #[test]
    fn test_local_file_and_reader_detect_python_content() {
        let detector = MagikaMimeDetector::new().expect("detector should initialize");
        let mut file = NamedTempFile::new().expect("temporary file should be created");
        file.write_all(b"#!/usr/bin/env python3\nprint('ok')\n")
            .expect("fixture should be written");
        assert_eq!(
            vec!["text/x-python"],
            detector
                .guess_from_magika_file(file.path())
                .expect("file should classify")
        );
        let mut reader = Cursor::new(b"#!/usr/bin/env python3\nprint('ok')\n".to_vec());
        assert_eq!(
            vec!["text/x-python"],
            detector
                .guess_from_reader_with_scope(&mut reader, super::ReaderScope::WholeResource)
                .expect("reader should classify")
        );
        let mut reader = Cursor::new(b"skip#!/usr/bin/env python3\nprint('ok')\n".to_vec());
        reader.set_position(4);
        let reader: &mut dyn ReadSeek = &mut reader;
        assert_eq!(
            vec!["text/x-python"],
            detector
                .guess_from_reader_with_scope(reader, super::ReaderScope::Remaining)
                .expect("remaining reader window should classify")
        );
    }

    #[test]
    fn test_magika_io_errors_map_to_mime_io_errors() {
        let error = super::map_magika_error(Error::IOError(std::io::Error::other("io")));
        assert!(matches!(error, MimeError::Io(_)));
    }

    #[test]
    fn test_poisoned_session_errors_map_to_backend_errors() {
        let detector =
            std::sync::Arc::new(MagikaMimeDetector::new().expect("detector should initialize for lock test"));
        let poisoned = std::sync::Arc::clone(&detector);
        std::thread::spawn(move || {
            let _guard = poisoned.session.lock().expect("session lock should be available");
            panic!("poison session lock");
        })
        .join()
        .expect_err("thread should poison the lock");
        let mapped = detector
            .guess_from_magika_input(&include_bytes!("../tests/fixtures/real_files/script.py")[..])
            .expect_err("poisoned session should fail detection");
        assert!(matches!(mapped, MimeError::DetectorBackend { .. }));
    }

    #[test]
    fn test_sync_sampling_leaves_session_lock_available() {
        let detector = Arc::new(MagikaMimeDetector::new().expect("detector should initialize"));
        let input = LockCheckingInput {
            detector: Arc::clone(&detector),
            bytes: b"#!/usr/bin/env python3\nprint('ok')\n",
        };
        assert_eq!(vec!["text/x-python"], detector.guess_from_magika_input(input).unwrap());
    }

    #[test]
    fn test_ruled_sync_input_succeeds_with_poisoned_session() {
        let detector = Arc::new(MagikaMimeDetector::new().expect("detector should initialize"));
        let poisoned = Arc::clone(&detector);
        std::thread::spawn(move || {
            let _guard = poisoned.session.lock().unwrap();
            panic!("poison session lock");
        })
        .join()
        .expect_err("thread should poison the lock");
        assert_eq!(
            content_type_to_mime(ContentType::Empty).into_iter().collect::<Vec<_>>(),
            detector.guess_from_magika_input(&b""[..]).unwrap()
        );
    }

    #[test]
    fn test_local_directory_returns_no_mime_candidate() {
        let detector = MagikaMimeDetector::new().expect("detector should initialize");
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        assert!(detector.guess_from_magika_file(directory.path()).unwrap().is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn test_local_symlink_does_not_follow_target() {
        use std::os::unix::fs::symlink;

        let detector = MagikaMimeDetector::new().expect("detector should initialize");
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let target = directory.path().join("target.py");
        let link = directory.path().join("link.py");
        std::fs::write(&target, b"#!/usr/bin/env python3\nprint('ok')\n").unwrap();
        symlink(&target, &link).unwrap();
        assert!(detector.guess_from_magika_file(&link).unwrap().is_empty());
    }
}
