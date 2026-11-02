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
use std::future::Future;

use super::provider_input::copy_exact;
use super::provider_input::read_options;
use super::provider_input::validate_request;
use super::read_budget::ReadBudget;
use super::session_read::read_async_session;
use qubit_fs::read::AsyncRandomAccessReader;

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

enum AsyncRoute<'a> {
    Legacy {
        file_system: &'a AsyncFileSystem,
        path: &'a Path,
    },
    Session {
        reader: AsyncRandomAccessReader,
        max_segment: Option<usize>,
    },
}

impl<'a> AsyncProviderInput<'a> {
    /// Creates an asynchronous input for one resource and cumulative byte
    /// limit.
    ///
    /// The borrowed filesystem and path must remain valid through feature
    /// extraction. `version` constrains each range read when present.
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

    /// Creates a session-backed input retaining the opened object through feature extraction.
    ///
    /// # Parameters
    /// * `reader` - Random-access session opened for this provider path.
    /// * `limit` - Maximum cumulative bytes reserved across reads.
    /// * `max_segment` - Optional finite maximum for an individual provider read.
    ///
    /// # Returns
    /// An asynchronous input using the session's open-time length.
    pub(crate) fn from_session(
        reader: AsyncRandomAccessReader,
        limit: usize,
        max_segment: Option<usize>,
    ) -> Self {
        let length = reader.len();
        Self {
            route: AsyncRoute::Session {
                reader,
                max_segment,
            },
            length,
            version: None,
            budget: ReadBudget::new(limit),
        }
    }
}

impl AsyncInput for AsyncProviderInput<'_> {
    /// Returns the total input length.
    #[inline]
    fn length(&self) -> impl Future<Output = MagikaResult<u64>> {
        std::future::ready(Ok(self.length))
    }

    /// Reads one validated range from the provider.
    ///
    /// Reserves bytes before asynchronous I/O and rejects short ranges or
    /// provider failures as Magika I/O errors.
    async fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> MagikaResult<()> {
        match &mut self.route {
            AsyncRoute::Legacy { file_system, path } => {
                validate_request(self.length, &mut self.budget, buffer.len(), offset)?;
                let options = read_options(offset, buffer.len(), self.version.as_ref());
                let bytes = file_system
                    .read_prefix(path, options, buffer.len())
                    .await
                    .map_err(|error| error.into_io_error())?
                    .into_bytes();
                copy_exact(buffer, bytes)
            }
            AsyncRoute::Session {
                reader,
                max_segment,
            } => {
                read_async_session(
                    reader,
                    self.length,
                    &mut self.budget,
                    buffer,
                    offset,
                    *max_segment,
                )
                .await
            }
        }
    }
}
