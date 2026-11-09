// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous provider-backed input for Magika feature extraction.

use std::future::Future;

use magika::AsyncInput;
use magika::Result as MagikaResult;
use qubit_fs::AsyncFileSystem;
use qubit_fs::Path;
use qubit_fs::metadata::ResourceVersion;
use qubit_fs::read::AsyncRandomAccessReader;

use super::async_route::AsyncRoute;
use super::provider_input::read_async;
use super::read_budget::ReadBudget;
use super::session_read::read_async_session;

/// Adapts asynchronous filesystem ranges to Magika's random-access input.
pub(crate) struct AsyncProviderInput<'a> {
    /// Filesystem supplying the requested ranges.
    route: AsyncRoute<'a>,
    /// Complete logical resource length reported to Magika.
    length: u64,
    /// Optional version constraint applied to each range read.
    version: Option<ResourceVersion>,
    /// Cumulative limit shared by all reads in one detection.
    budget: ReadBudget,
}

impl<'a> AsyncProviderInput<'a> {
    /// Creates an asynchronous input for one provider resource.
    ///
    /// The filesystem and path are borrowed until feature extraction
    /// completes. When supplied, `version` is required for every legacy range.
    ///
    /// # Parameters
    ///
    /// * `file_system` - Asynchronous filesystem that supplies resource ranges.
    /// * `path` - Path of the resource within the filesystem.
    /// * `length` - Complete logical resource length reported to Magika.
    /// * `version` - Optional resource version required for each legacy range.
    /// * `limit` - Maximum cumulative bytes reserved across all reads.
    ///
    /// # Returns
    ///
    /// An asynchronous input using the legacy path route and a fresh byte
    /// budget.
    #[must_use]
    pub(crate) fn new(
        file_system: &'a AsyncFileSystem,
        path: &'a Path,
        length: u64,
        version: Option<ResourceVersion>,
        limit: usize,
    ) -> Self {
        Self {
            route: AsyncRoute::Legacy { file_system, path },
            length,
            version,
            budget: ReadBudget::new(limit),
        }
    }

    /// Creates a session-backed input retaining the opened object through
    /// feature extraction.
    ///
    /// # Parameters
    ///
    /// * `reader` - Random-access session opened for this provider path.
    /// * `limit` - Maximum cumulative bytes reserved across reads.
    /// * `max_segment` - Optional finite maximum for an individual provider
    ///   read.
    ///
    /// # Returns
    /// An asynchronous input using the session's open-time length.
    pub(crate) fn from_session(reader: AsyncRandomAccessReader, limit: usize, max_segment: Option<usize>) -> Self {
        let length = reader.len();
        Self {
            route: AsyncRoute::Session { reader, max_segment },
            length,
            version: None,
            budget: ReadBudget::new(limit),
        }
    }
}

impl AsyncInput for AsyncProviderInput<'_> {
    /// Returns the complete logical resource length captured at construction.
    ///
    /// # Returns
    ///
    /// The length of the provider resource.
    #[inline]
    fn length(&self) -> impl Future<Output = MagikaResult<u64>> {
        std::future::ready(Ok(self.length))
    }

    /// Reads one validated range from the provider.
    ///
    /// Reserves bytes before asynchronous I/O and rejects short ranges or
    /// provider failures as Magika I/O errors.
    ///
    /// # Parameters
    ///
    /// * `buffer` - Destination for the requested range.
    /// * `offset` - Start offset of the requested range in the resource.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the range is invalid, exceeds the cumulative
    /// budget, or cannot be read exactly from the provider.
    async fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> MagikaResult<()> {
        match &mut self.route {
            AsyncRoute::Legacy { file_system, path } => {
                read_async(
                    file_system,
                    path,
                    self.length,
                    self.version.as_ref(),
                    &mut self.budget,
                    buffer,
                    offset,
                )
                .await
            }
            AsyncRoute::Session { reader, max_segment } => {
                read_async_session(reader, self.length, &mut self.budget, buffer, offset, *max_segment).await
            }
        }
    }
}
