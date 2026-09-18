// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider-neutral random-access inputs for Magika.

use std::fmt;
use std::io;

use magika::AsyncInput;
use magika::Error;
use magika::Result as MagikaResult;
use magika::SyncInput;
use qubit_fs::AsyncFileSystem;
use qubit_fs::FileSystem;
use qubit_fs::Path;
use qubit_fs::metadata::ResourceVersion;
use qubit_fs::read::ReadOptions;
use qubit_mime::MimeError;

/// Tracks the total number of bytes requested during one detection.
#[derive(Debug)]
pub(crate) struct ReadBudget {
    used: usize,
    limit: usize,
}

impl ReadBudget {
    /// Creates an empty budget with the supplied limit.
    pub(crate) const fn new(limit: usize) -> Self {
        Self { used: 0, limit }
    }

    /// Reserves one read from the cumulative budget.
    pub(crate) fn consume(&mut self, count: usize) -> std::result::Result<(), BudgetExceeded> {
        let Some(requested) = self.used.checked_add(count) else {
            return Err(BudgetExceeded {
                requested: usize::MAX,
                limit: self.limit,
            });
        };
        if requested > self.limit {
            return Err(BudgetExceeded {
                requested,
                limit: self.limit,
            });
        }
        self.used = requested;
        Ok(())
    }
}

/// Describes a read that exceeds the detector's cumulative budget.
#[derive(Debug)]
pub(crate) struct BudgetExceeded {
    /// Total requested bytes including the rejected read.
    pub(crate) requested: usize,
    /// Configured cumulative limit.
    pub(crate) limit: usize,
}

impl fmt::Display for BudgetExceeded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "provider read budget exceeded: {} > {}",
            self.requested, self.limit
        )
    }
}

impl std::error::Error for BudgetExceeded {}

/// Adapts synchronous filesystem ranges to Magika's random-access input.
pub(crate) struct SyncProviderInput<'a> {
    file_system: &'a FileSystem,
    path: &'a Path,
    length: u64,
    version: Option<ResourceVersion>,
    budget: ReadBudget,
}

impl<'a> SyncProviderInput<'a> {
    /// Creates a synchronous provider input.
    pub(crate) fn new(
        file_system: &'a FileSystem,
        path: &'a Path,
        length: u64,
        version: Option<ResourceVersion>,
        limit: usize,
    ) -> Self {
        Self {
            file_system,
            path,
            length,
            version,
            budget: ReadBudget::new(limit),
        }
    }
}

impl SyncInput for SyncProviderInput<'_> {
    fn length(&self) -> MagikaResult<u64> {
        Ok(self.length)
    }

    fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> MagikaResult<()> {
        read_sync(
            self.file_system,
            self.path,
            self.length,
            self.version.as_ref(),
            &mut self.budget,
            buffer,
            offset,
        )
    }
}

/// Adapts asynchronous filesystem ranges to Magika's random-access input.
pub(crate) struct AsyncProviderInput<'a> {
    file_system: &'a AsyncFileSystem,
    path: &'a Path,
    length: u64,
    version: Option<ResourceVersion>,
    budget: ReadBudget,
}

impl<'a> AsyncProviderInput<'a> {
    /// Creates an asynchronous provider input.
    pub(crate) fn new(
        file_system: &'a AsyncFileSystem,
        path: &'a Path,
        length: u64,
        version: Option<ResourceVersion>,
        limit: usize,
    ) -> Self {
        Self {
            file_system,
            path,
            length,
            version,
            budget: ReadBudget::new(limit),
        }
    }
}

impl AsyncInput for AsyncProviderInput<'_> {
    async fn length(&self) -> MagikaResult<u64> {
        Ok(self.length)
    }

    async fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> MagikaResult<()> {
        validate_request(self.length, &mut self.budget, buffer.len(), offset)?;
        let options = read_options(offset, buffer.len(), self.version.as_ref());
        let bytes = self
            .file_system
            .read_prefix(self.path, options, buffer.len())
            .await
            .map_err(|error| error.into_io_error())?
            .into_bytes();
        copy_exact(buffer, bytes)
    }
}

fn read_sync(
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

fn validate_request(length: u64, budget: &mut ReadBudget, requested: usize, offset: u64) -> MagikaResult<()> {
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

fn read_options(offset: u64, length: usize, version: Option<&ResourceVersion>) -> ReadOptions {
    ReadOptions::default()
        .with_offset(Some(offset))
        .with_length(Some(length as u64))
        .with_if_match(version.cloned())
}

fn copy_exact(buffer: &mut [u8], bytes: Vec<u8>) -> MagikaResult<()> {
    if bytes.len() != buffer.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "provider returned a short range").into());
    }
    buffer.copy_from_slice(&bytes);
    Ok(())
}

fn invalid_input(message: &'static str) -> io::Error {
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
    use super::BudgetExceeded;
    use super::ReadBudget;
    use super::invalid_input;

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
}
