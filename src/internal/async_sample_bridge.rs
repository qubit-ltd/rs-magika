// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous sampling requests served by an async range reader.

mod async_sample_driver;
mod bridge_state;
mod read_request;
mod response;

use std::future::poll_fn;
use std::io;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::task::Poll;

pub(crate) use async_sample_driver::AsyncSampleDriver;
pub(crate) use read_request::ReadRequest;

use self::bridge_state::BridgeState;
use self::response::Response;
use self::response::set_response;

/// Coordinates one synchronous worker with one asynchronous range driver.
///
/// The worker may have at most one unanswered request. Closing the bridge wakes
/// a worker waiting on either the request slot or its response.
pub(crate) struct AsyncSampleBridge {
    /// Fixed source length used to validate every requested range.
    length: u64,
    /// Pending request and driver notification protected by a mutex.
    state: Mutex<BridgeState>,
    /// Wakes a worker waiting for the sole request slot.
    changed: Condvar,
}

impl AsyncSampleBridge {
    /// Creates a bridge for a resource with the supplied length.
    ///
    /// # Parameters
    ///
    /// * `length` - Immutable resource length used for range validation.
    ///
    /// # Returns
    ///
    /// A shared bridge in the open state.
    pub(crate) fn new(length: u64) -> Arc<Self> {
        Arc::new(Self {
            length,
            state: Mutex::new(BridgeState::new()),
            changed: Condvar::new(),
        })
    }

    /// Returns the resource length fixed when the bridge was created.
    #[must_use]
    #[inline]
    pub(crate) const fn length(&self) -> u64 {
        self.length
    }

    /// Blocks the sampler until the async driver answers this range or closes.
    ///
    /// # Parameters
    ///
    /// * `offset` - Start offset within the fixed resource length.
    /// * `output` - Exact-size destination requested by the sampler.
    ///
    /// # Errors
    ///
    /// Returns `InvalidInput` for an overflowing range, `UnexpectedEof` for a
    /// range beyond the resource or a short provider response, and
    /// `Interrupted` when the bridge closes before the range is answered.
    pub(crate) fn read_at(self: &Arc<Self>, offset: u64, output: &mut [u8]) -> io::Result<()> {
        let end = offset
            .checked_add(output.len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "sample range offset overflow"))?;
        if end > self.length {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "sample range exceeds resource length",
            ));
        }
        if output.is_empty() {
            return Ok(());
        }

        let response = Arc::new(Response::new());
        let request = ReadRequest::new(offset, output.len(), Arc::clone(&response), Arc::downgrade(self));
        let wake = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            while !state.closed && (state.pending.is_some() || state.active.is_some()) {
                state = self.changed.wait(state).unwrap_or_else(|error| error.into_inner());
            }
            if state.closed {
                return Err(closed_error());
            }
            state.pending = Some(request);
            state.driver_waker.take()
        };
        if let Some(waker) = wake {
            waker.wake();
        }

        let mut result = response.result.lock().unwrap_or_else(|error| error.into_inner());
        while result.is_none() {
            result = response.ready.wait(result).unwrap_or_else(|error| error.into_inner());
        }
        match result.take().expect("response checked") {
            Ok(bytes) if bytes.len() == output.len() => {
                output.copy_from_slice(&bytes);
                Ok(())
            }
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "async range returned a short read",
            )),
            Err(error) => Err(error),
        }
    }

    /// Waits asynchronously for the next worker range request.
    ///
    /// # Returns
    ///
    /// The next request, or `None` after bridge shutdown.
    pub(crate) async fn next_request(self: &Arc<Self>) -> Option<ReadRequest> {
        poll_fn(|cx| {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(request) = state.pending.take() {
                state.active = Some(Arc::clone(&request.response));
                return Poll::Ready(Some(request));
            }
            if state.closed {
                return Poll::Ready(None);
            }
            state.driver_waker = Some(cx.waker().clone());
            Poll::Pending
        })
        .await
    }

    /// Creates the async-side cancellation guard for this bridge.
    ///
    /// # Returns
    ///
    /// A guard that closes this bridge when dropped.
    pub(crate) fn driver(self: &Arc<Self>) -> AsyncSampleDriver {
        AsyncSampleDriver {
            bridge: Arc::clone(self),
        }
    }

    /// Closes the bridge and wakes both participants, including an active read.
    pub(crate) fn close(&self) {
        let (waker, pending, active) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.closed = true;
            (state.driver_waker.take(), state.pending.take(), state.active.take())
        };
        let error = || Err(closed_error());
        if let Some(request) = pending {
            set_response(&request.response, error());
            drop(request);
        }
        if let Some(response) = active {
            set_response(&response, error());
        }
        self.changed.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Creates the interruption returned when a worker's bridge has closed.
fn closed_error() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, "async sample bridge closed")
}
