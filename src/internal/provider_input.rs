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

#[cfg(test)]
mod tests {
    use magika::Error;
    use qubit_mime::MimeError;

    use super::copy_exact;
    use super::invalid_input;
    use super::map_provider_magika_error;
    use super::validate_request;
    use crate::internal::budget_exceeded::BudgetExceeded;
    use crate::internal::read_budget::ReadBudget;

    #[test]
    fn budget_counts_repeated_reads() {
        let mut budget = ReadBudget::new(8_192);
        assert!(budget.consume(4_096).is_ok());
        assert!(budget.consume(4_096).is_ok());
        let error = budget.consume(4_097).expect_err("third read must exceed budget");
        assert_eq!(12_289, error.requested);
        assert_eq!(8_192, error.limit);
    }

    #[test]
    fn budget_reports_addition_overflow() {
        let mut budget = ReadBudget::new(usize::MAX);
        assert!(budget.consume(usize::MAX).is_ok());
        let error = budget.consume(1).expect_err("overflow must be rejected");
        assert_eq!(usize::MAX, error.requested);
        assert_eq!(usize::MAX, error.limit);
    }

    #[test]
    fn budget_rejects_second_window_when_limit_is_one_window() {
        let mut budget = ReadBudget::new(4_096);
        assert!(budget.consume(4_096).is_ok());
        let error = budget
            .consume(4_096)
            .expect_err("second window must exceed the cumulative limit");
        assert_eq!(8_192, error.requested);
        assert_eq!(4_096, error.limit);
    }

    #[test]
    fn helper_errors_are_displayable() {
        assert_eq!(
            "invalid provider input",
            invalid_input("invalid provider input").to_string()
        );
        assert!(BudgetExceeded { requested: 9, limit: 8 }.to_string().contains("9 > 8"));
    }

    #[test]
    fn request_validation_and_copy_errors_are_mapped() {
        let mut budget = ReadBudget::new(8);
        assert!(validate_request(8, &mut budget, 0, 99).is_ok());
        assert!(validate_request(8, &mut budget, 2, 7).is_err());
        assert!(validate_request(8, &mut budget, 2, u64::MAX).is_err());
        let mut output = [0_u8; 2];
        assert!(copy_exact(&mut output, vec![1]).is_err());
        assert!(copy_exact(&mut output, vec![1, 2]).is_ok());
        let mapped = map_provider_magika_error(Error::IOError(std::io::Error::other("provider failure")));
        assert!(matches!(mapped, MimeError::Io(_)));
    }
}
