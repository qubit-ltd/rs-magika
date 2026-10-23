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
use qubit_fs::read::ReadOptions;
use qubit_mime::MimeError;

use super::budget_exceeded::BudgetExceeded;
use super::read_budget::ReadBudget;

/// Reads exactly one Magika range through a synchronous provider.
///
/// Reserves `buffer.len()` bytes before I/O, requests the optional resource
/// version, and rejects short ranges. Filesystem failures become Magika I/O
/// errors; invalid ranges and exhausted budgets fail before the provider call.
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
    let bytes = file_system
        .read_prefix(path, read_options(offset, buffer.len(), version), buffer.len())
        .map_err(|error| error.into_io_error())?
        .into_bytes();
    copy_exact(buffer, bytes)
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
pub(crate) fn copy_exact(buffer: &mut [u8], bytes: Vec<u8>) -> MagikaResult<()> {
    if bytes.len() != buffer.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "provider returned a short range").into());
    }
    buffer.copy_from_slice(&bytes);
    Ok(())
}

/// Constructs an invalid-input error for a rejected provider request.
#[inline]
pub(crate) fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

/// Converts an upstream Magika error while preserving cumulative budget errors.
pub(crate) fn map_provider_magika_error(error: Error) -> MimeError {
    match error {
        Error::IOError(error) => {
            if let Some(error) = error
                .get_ref()
                .and_then(|source| source.downcast_ref::<BudgetExceeded>())
            {
                return MimeError::BufferLimitExceeded {
                    requested: error.requested,
                    limit: error.limit,
                };
            }
            MimeError::Io(error)
        }
        error => MimeError::detector_backend("magika", error.to_string()),
    }
}
