// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider-neutral random-access inputs for Magika.

use std::io;

use magika::Error;
use magika::Result as MagikaResult;
use qubit_fs::FileSystem;
use qubit_fs::Path;
use qubit_fs::metadata::ResourceVersion;
use qubit_fs::read::ObservedReadError;
use qubit_fs::read::ReadIoObserver;
use qubit_fs::read::ReadOptions;
use qubit_mime::MimeError;

mod internal;

use self::internal::ObservedReadStopped;
use super::cooperative_read::READ_STEPS_PER_POLL;
use super::cooperative_read::yield_once;
use super::read_budget::ReadBudget;

/// Reads exactly one Magika range through a synchronous provider.
///
/// Reserves `buffer.len()` bytes before I/O, requests the optional resource
/// version, and rejects short ranges. Filesystem failures become Magika I/O
/// errors; invalid ranges and exhausted budgets fail before the provider call.
///
/// # Parameters
///
/// * `file_system` - Filesystem that supplies the requested ranges.
/// * `path` - Path of the resource within the filesystem.
/// * `length` - Complete logical resource length reported to Magika.
/// * `version` - Optional version condition applied to every range.
/// * `budget` - Per-detection cumulative read budget.
/// * `buffer` - Destination for the requested bytes.
/// * `offset` - Start offset of the requested range in the resource.
///
/// # Errors
///
/// Returns an I/O error when validation, budget reservation, or any provider
/// range fails.
pub(crate) fn read_sync(
    file_system: &FileSystem,
    path: &Path,
    length: u64,
    version: Option<&ResourceVersion>,
    budget: &mut ReadBudget,
    buffer: &mut [u8],
    offset: u64,
) -> MagikaResult<()> {
    validate_request(length, budget, buffer.len(), offset)?;
    if buffer.is_empty() {
        return Ok(());
    }
    let max_segment = file_system
        .properties()
        .limits()
        .max_read_range_bytes()
        .maximum()
        .map_or(usize::MAX, |maximum| usize::try_from(maximum).unwrap_or(usize::MAX));
    read_sync_segments(file_system, path, version, buffer, offset, max_segment)
}

/// Reads a complete validated request through bounded synchronous ranges.
///
/// # Parameters
///
/// * `file_system` - Filesystem that supplies each segment.
/// * `path` - Path of the resource within the filesystem.
/// * `version` - Optional version condition applied to every segment.
/// * `buffer` - Destination filled by the segments.
/// * `offset` - Start offset of the complete request.
/// * `max_segment` - Maximum byte length of one provider request.
///
/// # Errors
///
/// Returns an I/O error when the segment limit is zero, an offset overflows,
/// or a provider segment fails or returns the wrong length.
pub(crate) fn read_sync_segments(
    file_system: &FileSystem,
    path: &Path,
    version: Option<&ResourceVersion>,
    buffer: &mut [u8],
    offset: u64,
    max_segment: usize,
) -> MagikaResult<()> {
    if max_segment == 0 {
        return Err(invalid_input("provider maximum read range is zero").into());
    }
    let mut completed = 0;
    while completed < buffer.len() {
        let segment_length = (buffer.len() - completed).min(max_segment);
        let segment_offset = offset
            .checked_add(completed as u64)
            .ok_or_else(|| invalid_input("provider input offset overflow"))?;
        let bytes = file_system
            .read_prefix(
                path,
                read_options(segment_offset, segment_length, version),
                segment_length,
            )
            .map_err(|error| error.into_io_error())?
            .into_bytes();
        copy_exact(&mut buffer[completed..completed + segment_length], bytes)?;
        completed += segment_length;
    }
    Ok(())
}

/// Reads one validated request through rs-fs's observed prefix loop.
///
/// The observer sees each actual open/read operation. Cancellation reported by
/// the observer becomes an interrupted I/O error; provider failures retain
/// their filesystem context.
///
/// # Type Parameters
///
/// * `O` - Observer that controls and records provider I/O boundaries.
///
/// # Parameters
///
/// * `file_system` - Asynchronous filesystem that supplies the requested
///   ranges.
/// * `path` - Path of the resource within the filesystem.
/// * `length` - Complete logical resource length reported to Magika.
/// * `options` - Read options carrying the range offset and optional version
///   condition.
/// * `budget` - Per-detection cumulative read budget.
/// * `buffer` - Destination for the requested bytes.
/// * `observer` - Receives provider I/O lifecycle events and may stop before
///   the next operation.
///
/// # Returns
///
/// Success after the requested range has been filled exactly.
///
/// # Errors
///
/// Returns an I/O error when validation, budget reservation, or any provider
/// range fails or the observer stops the operation.
pub(crate) async fn read_async_observed<O: ReadIoObserver>(
    file_system: &qubit_fs::AsyncFileSystem,
    path: &Path,
    length: u64,
    options: ReadOptions,
    budget: &mut ReadBudget,
    buffer: &mut [u8],
    observer: &mut O,
) -> MagikaResult<()> {
    let offset = options.offset().unwrap_or_default();
    validate_request(length, budget, buffer.len(), offset)?;
    if buffer.is_empty() {
        return Ok(());
    }
    let max_segment = file_system
        .properties()
        .limits()
        .max_read_range_bytes()
        .maximum()
        .map_or(usize::MAX, |maximum| usize::try_from(maximum).unwrap_or(usize::MAX));
    read_async_segments_observed(
        file_system,
        path,
        options.if_match(),
        buffer,
        offset,
        max_segment,
        observer,
    )
    .await
}

/// Reads a buffer as bounded asynchronous ranges while observing provider I/O.
///
/// Each segment uses rs-fs's prefix reader, preserving version conditions and
/// exact-length validation. The observer is consulted before every segment and
/// receives events for each open and read.
///
/// # Type Parameters
///
/// * `O` - Observer that controls and records provider I/O boundaries.
///
/// # Parameters
///
/// * `file_system` - Asynchronous filesystem that supplies the segments.
/// * `path` - Path of the resource within the filesystem.
/// * `version` - Optional version condition applied to each segment.
/// * `buffer` - Destination filled by the segments.
/// * `offset` - Start offset of the complete request.
/// * `max_segment` - Maximum byte length of one provider request.
/// * `observer` - Receives provider I/O events and may stop before the next
///   segment.
///
/// # Returns
///
/// Success after every segment has filled its corresponding part of `buffer`.
///
/// # Errors
///
/// Returns an I/O error when the segment limit is zero, an offset overflows,
/// a provider segment fails or is short, or the observer stops the operation.
pub(crate) async fn read_async_segments_observed<O: ReadIoObserver>(
    file_system: &qubit_fs::AsyncFileSystem,
    path: &Path,
    version: Option<&ResourceVersion>,
    buffer: &mut [u8],
    offset: u64,
    max_segment: usize,
    observer: &mut O,
) -> MagikaResult<()> {
    if max_segment == 0 {
        return Err(invalid_input("provider maximum read range is zero").into());
    }
    let mut completed = 0;
    let mut completed_steps = 0;
    while completed < buffer.len() {
        let segment_length = (buffer.len() - completed).min(max_segment);
        let segment_offset = offset
            .checked_add(completed as u64)
            .ok_or_else(|| invalid_input("provider input offset overflow"))?;
        let outcome = file_system
            .read_prefix_observed(
                path,
                read_options(segment_offset, segment_length, version),
                segment_length,
                observer,
            )
            .await;
        let bytes = match outcome {
            Ok(outcome) => outcome.into_bytes(),
            Err(ObservedReadError::Stopped) => {
                return Err(observed_read_stopped_error().into());
            }
            Err(ObservedReadError::Provider(error)) => return Err(error.into_io_error().into()),
        };
        copy_exact(&mut buffer[completed..completed + segment_length], bytes)?;
        completed += segment_length;
        completed_steps += 1;
        if completed < buffer.len() && completed_steps >= READ_STEPS_PER_POLL {
            completed_steps = 0;
            yield_once().await;
        }
    }
    Ok(())
}

/// Validates a random-access read and reserves its requested bytes from the
/// per-detection cumulative budget before the provider is called.
///
/// # Parameters
///
/// * `length` - Logical length reported to Magika.
/// * `budget` - Remaining cumulative read budget for this detection.
/// * `requested` - Number of bytes in this read request.
/// * `offset` - Start of the requested logical range.
///
/// # Returns
///
/// `Ok(())` when the request fits both the logical range and byte budget.
///
/// # Errors
///
/// Returns an I/O error when the range overflows or exceeds `length`, or when
/// reserving `requested` would exceed the cumulative budget.
pub(crate) fn validate_request(
    length: u64,
    budget: &mut ReadBudget,
    requested: usize,
    offset: u64,
) -> MagikaResult<()> {
    if requested == 0 {
        return Ok(());
    }
    let end = offset
        .checked_add(requested as u64)
        .ok_or_else(|| invalid_input("provider input offset overflow"))?;
    if end > length {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "provider input read exceeds its length").into());
    }
    budget
        .consume(requested)
        .map_err(|error| io::Error::new(io::ErrorKind::FileTooLarge, error))?;
    Ok(())
}

/// Builds a provider range request starting at `offset` for `length` bytes.
///
/// When `version` is present, the provider must match that resource version.
///
/// # Parameters
///
/// * `offset` - Start offset of the range.
/// * `length` - Number of bytes requested.
/// * `version` - Optional resource version required by the provider.
///
/// # Returns
///
/// Read options carrying the requested offset, length, and version condition.
#[inline]
pub(crate) fn read_options(offset: u64, length: usize, version: Option<&ResourceVersion>) -> ReadOptions {
    ReadOptions::default()
        .with_offset(Some(offset))
        .with_length(Some(length as u64))
        .with_if_match(version.cloned())
}

/// Copies a provider range into Magika's buffer only when its length matches.
///
/// A short or unexpectedly long range returns a Magika I/O error instead of
/// leaving a partially initialized inference buffer.
///
/// # Parameters
///
/// * `buffer` - Destination for the provider bytes.
/// * `bytes` - Bytes returned by the provider.
///
/// # Errors
///
/// Returns an I/O error when the returned byte count differs from the buffer
/// length.
pub(crate) fn copy_exact(buffer: &mut [u8], bytes: Vec<u8>) -> MagikaResult<()> {
    if bytes.len() != buffer.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "provider returned a short range").into());
    }
    buffer.copy_from_slice(&bytes);
    Ok(())
}

/// Constructs an invalid-input error for a rejected provider request.
///
/// # Parameters
///
/// * `message` - Static explanation of why the request is invalid.
///
/// # Returns
///
/// An I/O error whose kind is [`io::ErrorKind::InvalidInput`].
#[inline]
pub(crate) fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// Converts an upstream Magika error while preserving cumulative budget errors.
///
/// # Parameters
///
/// * `error` - Error returned by Magika while reading provider-backed input.
///
/// # Returns
///
/// A MIME error retaining provider I/O failures and the requested/allowed byte
/// counts for cumulative budget failures.
pub(crate) fn map_provider_magika_error(error: Error) -> MimeError {
    match error {
        Error::IOError(error) => {
            if crate::internal::async_sample_bridge::is_closed_error(&error) {
                return MimeError::detector_backend("magika", "provider inference stopped");
            }
            if error.get_ref().is_some_and(|source| source.is::<ObservedReadStopped>()) {
                return MimeError::detector_backend("magika", "provider read stopped");
            }
            if crate::internal::is_session_stop_error(&error) {
                return MimeError::detector_backend("magika", "provider session read stopped");
            }
            if let Some(MimeError::BufferLimitExceeded { requested, limit }) =
                error.get_ref().and_then(|source| source.downcast_ref::<MimeError>())
            {
                return MimeError::BufferLimitExceeded {
                    requested: *requested,
                    limit: *limit,
                };
            }
            MimeError::Io(error)
        }
        error => MimeError::detector_backend("magika", error.to_string()),
    }
}

/// Creates an interruption marker for an observer-reported provider stop.
///
/// # Returns
///
/// An I/O error carrying the private observer-stop marker as its source.
pub(crate) fn observed_read_stopped_error() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, ObservedReadStopped)
}
