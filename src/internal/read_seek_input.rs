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
    /// Absolute start offset of the input window.
    base_offset: u64,
    /// Input window length in bytes.
    length: u64,
}

impl<'reader> ReadSeekInput<'reader> {
    /// Creates an adapter for a reader with a known length.
    ///
    /// # Parameters
    ///
    /// * `reader` - Seekable reader used for Magika inference.
    /// * `base_offset` - Absolute reader offset at the start of the window.
    /// * `length` - Window length in bytes.
    ///
    /// # Returns
    ///
    /// A random-access Magika input adapter.
    #[inline(always)]
    pub(crate) fn new(reader: &'reader mut dyn ReadSeek, base_offset: u64, length: u64) -> Self {
        Self {
            reader,
            base_offset,
            length,
        }
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
        let end = offset
            .checked_add(buffer.len() as u64)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "magika input offset overflow"))?;
        if end > self.length {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "magika input read exceeds the configured window",
            )
            .into());
        }
        let absolute = self.base_offset.checked_add(offset).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "magika input base offset overflow")
        })?;
        self.reader.seek(SeekFrom::Start(absolute))?;
        self.reader.read_exact(buffer)?;
        Ok(())
    }
}
