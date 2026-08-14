// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Magika input adapter for seekable readers.

use std::io::SeekFrom;

use magika::Result;
use magika::SyncInput;
use qubit_io::std_io::ReadSeek;

/// Adapts a seekable reader to Magika's synchronous random-access input.
pub(crate) struct ReadSeekInput<'reader> {
    /// Wrapped reader.
    reader: &'reader mut dyn ReadSeek,
    /// Total input length in bytes.
    length: u64,
}

impl<'reader> ReadSeekInput<'reader> {
    /// Creates an adapter for a reader with a known length.
    ///
    /// # Parameters
    ///
    /// * `reader` - Seekable reader used for Magika inference.
    /// * `length` - Total reader length in bytes.
    ///
    /// # Returns
    ///
    /// A random-access Magika input adapter.
    #[inline(always)]
    pub(crate) fn new(reader: &'reader mut dyn ReadSeek, length: u64) -> Self {
        Self { reader, length }
    }

    /// Returns the wrapped reader for position restoration.
    ///
    /// # Returns
    ///
    /// The mutable seekable reader borrowed by this adapter.
    #[inline(always)]
    pub(crate) fn reader_mut(&mut self) -> &mut dyn ReadSeek {
        self.reader
    }
}

impl SyncInput for ReadSeekInput<'_> {
    /// Returns the total input length.
    ///
    /// # Returns
    ///
    /// The length supplied when the adapter was created.
    #[inline(always)]
    fn length(&self) -> Result<u64> {
        Ok(self.length)
    }

    /// Reads exactly `buffer.len()` bytes starting at `offset`.
    ///
    /// # Parameters
    ///
    /// * `buffer` - Destination buffer filled by the read.
    /// * `offset` - Absolute byte offset in the input.
    ///
    /// # Errors
    ///
    /// Returns a Magika I/O error when seeking or reading fails.
    #[inline(always)]
    fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> Result<()> {
        self.reader.seek(SeekFrom::Start(offset))?;
        self.reader.read_exact(buffer)?;
        Ok(())
    }
}
