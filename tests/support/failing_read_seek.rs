// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::io::Cursor;
use std::io::Error;
use std::io::ErrorKind;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;

/// Seekable reader that can fail reads or restoration seeks on demand.
#[derive(Debug)]
pub(crate) struct FailingReadSeek {
    /// Wrapped in-memory reader.
    inner: Cursor<Vec<u8>>,
    /// Whether reads should fail.
    fail_reads: bool,
    /// Start offset that should fail after a read has been attempted.
    fail_restore_to: Option<u64>,
    /// Whether a read has been attempted.
    read_attempted: bool,
}

impl FailingReadSeek {
    /// Creates a seekable reader with configurable failure behavior.
    ///
    /// # Parameters
    ///
    /// * `content` - Bytes exposed by the reader.
    /// * `fail_reads` - Whether every read should fail.
    /// * `fail_restore_to` - Offset whose restoration seek should fail after a
    ///   read attempt.
    ///
    /// # Returns
    ///
    /// A configured seekable test reader.
    pub(crate) fn new(
        content: Vec<u8>,
        fail_reads: bool,
        fail_restore_to: Option<u64>,
    ) -> Self {
        Self {
            inner: Cursor::new(content),
            fail_reads,
            fail_restore_to,
            read_attempted: false,
        }
    }

    /// Returns the current reader position.
    ///
    /// # Returns
    ///
    /// Current byte offset in the wrapped cursor.
    #[inline(always)]
    pub(crate) fn position(&self) -> u64 {
        self.inner.position()
    }
}

impl Read for FailingReadSeek {
    /// Reads from the wrapped cursor or returns a configured read error.
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.read_attempted = true;
        if self.fail_reads {
            Err(Error::new(ErrorKind::UnexpectedEof, "forced read failure"))
        } else {
            self.inner.read(buffer)
        }
    }
}

impl Seek for FailingReadSeek {
    /// Seeks in the wrapped cursor or returns a configured restore error.
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        if let SeekFrom::Start(offset) = position
            && self.read_attempted
            && Some(offset) == self.fail_restore_to
        {
            return Err(Error::other("forced restore failure"));
        }
        self.inner.seek(position)
    }
}
