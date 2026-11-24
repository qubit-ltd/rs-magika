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
mod internal;
mod read_request;
mod response;

use std::future::poll_fn;
use std::io;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
#[cfg(test)]
use std::sync::atomic::Ordering;
use std::task::Poll;

pub(crate) use async_sample_driver::AsyncSampleDriver;
pub(crate) use read_request::ReadRequest;

use self::bridge_state::BridgeState;
use self::internal::BridgeClosed;
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
    /// Test synchronization for proving a reader reached the request-slot wait.
    #[cfg(test)]
    waiting_readers: AtomicUsize,
    /// Test synchronization for proving a worker reached the response wait.
    #[cfg(test)]
    waiting_responses: AtomicUsize,
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
            #[cfg(test)]
            waiting_readers: AtomicUsize::new(0),
            #[cfg(test)]
            waiting_responses: AtomicUsize::new(0),
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
                #[cfg(test)]
                self.waiting_readers.fetch_add(1, Ordering::Release);
                state = self.changed.wait(state).unwrap_or_else(|error| error.into_inner());
                #[cfg(test)]
                self.waiting_readers.fetch_sub(1, Ordering::Release);
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
            #[cfg(test)]
            self.waiting_responses.fetch_add(1, Ordering::Release);
            result = response.ready.wait(result).unwrap_or_else(|error| error.into_inner());
            #[cfg(test)]
            self.waiting_responses.fetch_sub(1, Ordering::Release);
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

    /// Waits until worker cancellation or completion closes the bridge.
    pub(crate) async fn closed(&self) {
        poll_fn(|cx| {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closed {
                return Poll::Ready(());
            }
            if !state.close_wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
                state.close_wakers.push(cx.waker().clone());
            }
            Poll::Pending
        })
        .await
    }

    #[cfg(test)]
    pub(crate) fn wait_for_test_waiters(&self, readers: usize, responses: usize, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        while std::time::Instant::now() < deadline {
            if self.waiting_readers.load(Ordering::Acquire) >= readers
                && self.waiting_responses.load(Ordering::Acquire) >= responses
            {
                return true;
            }
            std::thread::yield_now();
        }
        false
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
        let (waker, close_wakers, pending, active) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.closed = true;
            (
                state.driver_waker.take(),
                std::mem::take(&mut state.close_wakers),
                state.pending.take(),
                state.active.take(),
            )
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
        for waker in close_wakers {
            waker.wake();
        }
    }
}

/// Creates the interruption returned when a worker's bridge has closed.
pub(crate) fn closed_error() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, BridgeClosed)
}

/// Returns whether an I/O error came from closing the internal sample bridge.
///
/// # Parameters
///
/// * `error` - I/O error returned by the sample bridge operation.
///
/// # Returns
///
/// `true` when the error carries the private bridge-close marker.
pub(crate) fn is_closed_error(error: &io::Error) -> bool {
    error.get_ref().is_some_and(|source| source.is::<BridgeClosed>())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use super::AsyncSampleBridge;

    #[test]
    fn test_poisoned_bridge_state_still_serves_a_read() {
        let bridge = AsyncSampleBridge::new(4);
        let poison_bridge = Arc::clone(&bridge);
        let _ = std::thread::spawn(move || {
            let _state = poison_bridge.state.lock().unwrap();
            panic!("poison bridge state for recovery test");
        })
        .join();

        let worker_bridge = Arc::clone(&bridge);
        let worker = std::thread::spawn(move || {
            let mut bytes = [0; 2];
            worker_bridge.read_at(1, &mut bytes).unwrap();
            bytes
        });
        let request = block_on(bridge.next_request()).expect("worker should publish a read request");
        assert_eq!((1, 2), (request.offset(), request.len()));
        request.respond(Ok(vec![7, 9]));
        assert_eq!([7, 9], worker.join().unwrap());
    }

    #[test]
    fn test_close_and_closed_recover_a_poisoned_bridge_state() {
        let bridge = AsyncSampleBridge::new(0);
        let poison_bridge = Arc::clone(&bridge);
        let _ = std::thread::spawn(move || {
            let _state = poison_bridge.state.lock().unwrap();
            panic!("poison bridge state before close");
        })
        .join();

        bridge.close();
        let waker = std::task::Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut closed = std::pin::pin!(bridge.closed());
        assert!(Future::poll(closed.as_mut(), &mut context).is_ready());
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut future = std::pin::pin!(future);
        loop {
            if let Poll::Ready(output) = Future::poll(future.as_mut(), &mut context) {
                return output;
            }
            std::thread::yield_now();
        }
    }
}
