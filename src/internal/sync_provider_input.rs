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
use super::session_read::read_sync_session;
use qubit_fs::read::RandomAccessReader;

/// Adapts synchronous filesystem ranges to Magika's random-access input.
pub(crate) struct SyncProviderInput<'a> {
    /// Filesystem supplying the requested ranges.
    route: SyncRoute<'a>,
    /// Complete logical resource length reported to Magika.
    length: u64,
    /// Optional version constraint applied to each range read.
    version: Option<ResourceVersion>,
    /// Cumulative limit shared by all reads in one detection.
    budget: ReadBudget,
}

enum SyncRoute<'a> {
    Legacy {
        file_system: &'a FileSystem,
        path: &'a Path,
    },
    Session {
        reader: RandomAccessReader,
        max_segment: Option<usize>,
    },
}

impl<'a> SyncProviderInput<'a> {
    /// Creates a synchronous input for one resource and cumulative byte limit.
    ///
    /// The borrowed filesystem and path must remain valid through inference.
    /// `version` constrains each range read when present.
    ///
    /// # Parameters
    ///
    /// * `file_system` - Filesystem that supplies resource ranges.
    /// * `path` - Logical path of the resource within the filesystem.
    /// * `length` - Complete logical resource length reported to Magika.
    /// * `version` - Optional resource version required for each range read.
    /// * `limit` - Maximum cumulative bytes reserved across reads.
    ///
    /// # Returns
    ///
    /// A synchronous input adapter with a fresh cumulative read budget.
    #[must_use]
    pub(crate) fn new(
        file_system: &'a FileSystem,
        path: &'a Path,
        length: u64,
        version: Option<ResourceVersion>,
        limit: usize,
    ) -> Self {
        Self {
            route: SyncRoute::Legacy { file_system, path },
            length,
            version,
            budget: ReadBudget::new(limit),
        }
    }

    /// Creates a session-backed input retaining the opened object through inference.
    ///
    /// # Parameters
    /// * `reader` - Random-access session opened for this provider path.
    /// * `limit` - Maximum cumulative bytes reserved across reads.
    /// * `max_segment` - Optional finite maximum for an individual provider read.
    ///
    /// # Returns
    /// A synchronous input using the session's open-time length.
    pub(crate) fn from_session(
        reader: RandomAccessReader,
        limit: usize,
        max_segment: Option<usize>,
    ) -> Self {
        let length = reader.len();
        Self {
            route: SyncRoute::Session {
                reader,
                max_segment,
            },
            length,
            version: None,
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
    ///
    /// Reserves bytes before blocking I/O and rejects short ranges or provider
    /// failures as Magika I/O errors.
    fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> MagikaResult<()> {
        match &mut self.route {
            SyncRoute::Legacy { file_system, path } => read_sync(
                file_system,
                path,
                self.length,
                self.version.as_ref(),
                &mut self.budget,
                buffer,
                offset,
            ),
            SyncRoute::Session {
                reader,
                max_segment,
            } => read_sync_session(
                reader,
                self.length,
                &mut self.budget,
                buffer,
                offset,
                *max_segment,
            ),
        }
    }
}
