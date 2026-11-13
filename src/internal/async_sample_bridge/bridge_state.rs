// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared state for the async sample bridge.

use std::sync::Arc;
use std::task::Waker;

use super::read_request::ReadRequest;
use super::response::Response;

/// Mutable bridge state protected by the bridge mutex.
pub(super) struct BridgeState {
    /// Request waiting for the async driver to take it.
    pub(super) pending: Option<ReadRequest>,
    /// Response owned by a request currently serviced by the driver.
    pub(super) active: Option<Arc<Response>>,
    /// Whether shutdown has woken both bridge participants.
    pub(super) closed: bool,
    /// Driver task waiting for the next request.
    pub(super) driver_waker: Option<Waker>,
    /// Tasks waiting for bridge closure while a provider future is pending.
    pub(super) close_wakers: Vec<Waker>,
}

impl BridgeState {
    /// Creates an open bridge state with no outstanding request.
    pub(super) const fn new() -> Self {
        Self {
            pending: None,
            active: None,
            closed: false,
            driver_waker: None,
            close_wakers: Vec::new(),
        }
    }
}
