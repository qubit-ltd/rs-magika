// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One range request's response synchronization.

use std::io;
use std::sync::Condvar;
use std::sync::Mutex;

/// Stores one result and wakes the synchronous sampler when it arrives.
pub(super) struct Response {
    /// Provider read result, if the async driver has answered.
    pub(super) result: Mutex<Option<io::Result<Vec<u8>>>>,
    /// Notifies the worker when `result` becomes available.
    pub(super) ready: Condvar,
}

impl Response {
    /// Creates an unanswered response slot.
    pub(super) const fn new() -> Self {
        Self {
            result: Mutex::new(None),
            ready: Condvar::new(),
        }
    }
}

/// Publishes the first response and wakes its blocked worker.
///
/// # Parameters
///
/// * `response` - Request's one-shot response slot.
/// * `result` - Exact bytes or the asynchronous read error.
pub(super) fn set_response(response: &Response, result: io::Result<Vec<u8>>) {
    let mut current = response.result.lock().unwrap_or_else(|error| error.into_inner());
    if current.is_none() {
        *current = Some(result);
        response.ready.notify_all();
    }
}
