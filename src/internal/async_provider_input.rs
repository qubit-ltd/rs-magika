// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous provider-backed input for Magika feature extraction.

use magika::AsyncInput;
use magika::Result as MagikaResult;
use qubit_fs::AsyncFileSystem;
use qubit_fs::Path;
use qubit_fs::metadata::ResourceVersion;

use super::provider_input::copy_exact;
use super::provider_input::read_options;
use super::provider_input::validate_request;
use super::read_budget::ReadBudget;

/// Adapts asynchronous filesystem ranges to Magika's random-access input.
pub(crate) struct AsyncProviderInput<'a> {
    /// Filesystem supplying the requested ranges.
    file_system: &'a AsyncFileSystem,
    /// Logical resource path within the filesystem.
    path: &'a Path,
    /// Complete logical resource length reported to Magika.
    length: u64,
    /// Optional version constraint applied to each range read.
    version: Option<ResourceVersion>,
    /// Cumulative limit shared by all reads in one detection.
    budget: ReadBudget,
}

impl<'a> AsyncProviderInput<'a> {
    /// Creates an asynchronous input for one resource and cumulative byte
    /// limit.
    ///
    /// The borrowed filesystem and path must remain valid through feature
    /// extraction. `version` constrains each range read when present.
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
    /// Returns the total input length.
    #[inline]
    async fn length(&self) -> MagikaResult<u64> {
        Ok(self.length)
    }

    /// Reads one validated range from the provider.
    ///
    /// Reserves bytes before asynchronous I/O and rejects short ranges or
    /// provider failures as Magika I/O errors.
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
