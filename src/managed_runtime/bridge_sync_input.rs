// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Magika sync input adapter backed by an asynchronous provider bridge.

use std::sync::Arc;

use magika::SyncInput;

use crate::internal::async_sample_bridge::AsyncSampleBridge;

/// Presents async-provider range requests through Magika's synchronous input
/// API.
pub(super) struct BridgeSyncInput(
    /// Bridge used to transfer each requested range to the async driver.
    pub(super) Arc<AsyncSampleBridge>,
);

impl SyncInput for BridgeSyncInput {
    /// Reports the immutable input length held by the bridge.
    fn length(&self) -> magika::Result<u64> {
        Ok(self.0.length())
    }

    /// Reads a requested range by waiting for the async driver response.
    ///
    /// # Parameters
    ///
    /// * `buffer` - Exact-size destination buffer supplied by Magika.
    /// * `offset` - Start offset of the requested range.
    fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> magika::Result<()> {
        self.0.read_at(offset, buffer).map_err(Into::into)
    }
}
