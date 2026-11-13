// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation guard for the async side of a sample bridge.

use std::sync::Arc;

use crate::internal::async_sample_bridge::AsyncSampleBridge;
use crate::internal::async_sample_bridge::ReadRequest;

/// Driver handle whose drop closes the bridge and wakes a waiting worker.
pub(crate) struct AsyncSampleDriver {
    /// Bridge whose worker requests are served by this future.
    pub(super) bridge: Arc<AsyncSampleBridge>,
}

impl AsyncSampleDriver {
    /// Waits asynchronously for the next worker range request.
    ///
    /// # Returns
    ///
    /// The next request, or `None` after bridge shutdown.
    pub(crate) async fn next_request(&self) -> Option<ReadRequest> {
        self.bridge.next_request().await
    }

    /// Waits for independent bridge shutdown while provider I/O is pending.
    pub(crate) async fn closed(&self) {
        self.bridge.closed().await;
    }
}

impl Drop for AsyncSampleDriver {
    /// Closes the bridge when the async detection future is abandoned.
    fn drop(&mut self) {
        self.bridge.close();
    }
}
