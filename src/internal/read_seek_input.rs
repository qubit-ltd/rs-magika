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
    #[inline]
    pub(crate) fn new(reader: &'reader mut dyn ReadSeek, base_offset: u64, length: u64) -> Self {
        Self {
            reader,
            base_offset,
            length,
        }
    }
}

impl SyncInput for ReadSeekInput<'_> {
    /// Returns the total input length.
    ///
    /// # Returns
    ///
    /// The length supplied when the adapter was created.
    #[inline]
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
    #[inline]
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

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use magika::SyncInput;

    use super::ReadSeekInput;

    #[test]
    fn bounded_input_reads_from_base_offset() {
        let mut reader = Cursor::new(b"prefix-script".to_vec());
        let mut input = ReadSeekInput::new(&mut reader, 7, 6);
        let mut buffer = [0_u8; 6];
        SyncInput::read_at(&mut input, &mut buffer, 0).expect("bounded read should succeed");
        assert_eq!(&buffer, b"script");
        assert_eq!(6, SyncInput::length(&input).expect("length should be available"));
    }

    #[test]
    fn bounded_input_rejects_out_of_range_reads() {
        let mut reader = Cursor::new(b"script".to_vec());
        let mut input = ReadSeekInput::new(&mut reader, 0, 6);
        let mut buffer = [0_u8; 1];
        assert!(SyncInput::read_at(&mut input, &mut buffer, 6).is_err());
    }

    #[test]
    fn bounded_input_rejects_base_offset_overflow() {
        let mut reader = Cursor::new(vec![0_u8; 1]);
        let mut input = ReadSeekInput::new(&mut reader, u64::MAX, 1);
        let mut buffer = [0_u8; 1];
        assert!(SyncInput::read_at(&mut input, &mut buffer, 0).is_err());
    }

    #[test]
    fn bounded_input_rejects_relative_offset_overflow() {
        let mut reader = Cursor::new(vec![0_u8; 1]);
        let mut input = ReadSeekInput::new(&mut reader, 0, u64::MAX);
        let mut buffer = [0_u8; 1];
        assert!(SyncInput::read_at(&mut input, &mut buffer, u64::MAX).is_err());
    }
}
