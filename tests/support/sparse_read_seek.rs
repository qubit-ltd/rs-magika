// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::io;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// A sparse seekable resource which materializes only the requested bytes.
pub(crate) struct SparseReadSeek {
    length: u64,
    position: u64,
    prefix: Arc<Vec<u8>>,
    bytes_read: Arc<AtomicUsize>,
}

impl SparseReadSeek {
    pub(crate) fn new(length: u64, prefix: Vec<u8>) -> Self {
        Self {
            length,
            position: 0,
            prefix: Arc::new(prefix),
            bytes_read: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(crate) fn bytes_read(&self) -> usize {
        self.bytes_read.load(Ordering::Relaxed)
    }
}

impl Read for SparseReadSeek {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.length || output.is_empty() {
            return Ok(0);
        }
        let count = output.len().min((self.length - self.position) as usize);
        for (index, byte) in output[..count].iter_mut().enumerate() {
            let absolute = self.position as usize + index;
            *byte = self.prefix[absolute % self.prefix.len()];
        }
        self.position += count as u64;
        self.bytes_read.fetch_add(count, Ordering::Relaxed);
        Ok(count)
    }
}

impl Seek for SparseReadSeek {
    fn seek(&mut self, origin: SeekFrom) -> io::Result<u64> {
        let position = match origin {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(offset) => self
                .position
                .checked_add_signed(offset)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "sparse reader position overflow"))?,
            SeekFrom::End(offset) => self
                .length
                .checked_add_signed(offset)
                .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "sparse reader end overflow"))?,
        };
        if position > self.length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sparse reader seek exceeds length",
            ));
        }
        self.position = position;
        Ok(position)
    }
}
