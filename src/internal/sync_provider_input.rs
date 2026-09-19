// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous provider-backed input for Magika.

use magika::Result as MagikaResult;
use magika::SyncInput;
use qubit_fs::FileSystem;
use qubit_fs::Path;
use qubit_fs::metadata::ResourceVersion;

use super::provider_input::read_sync;
use super::read_budget::ReadBudget;

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
    /// Returns the total input length.
    #[inline]
    fn length(&self) -> MagikaResult<u64> {
        Ok(self.length)
    }

    /// Reads one validated range from the provider.
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
