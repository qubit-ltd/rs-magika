// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! A single worker-to-provider range request.

use std::io;
use std::sync::Arc;
use std::sync::Weak;

use super::AsyncSampleBridge;
use super::response::Response;
use super::response::set_response;

/// A worker's range request, handed to the async provider driver.
pub(crate) struct ReadRequest {
    /// Start offset requested by Magika.
    offset: u64,
    /// Number of bytes requested by Magika.
    length: usize,
    /// Shared completion slot observed by the worker.
    pub(super) response: Arc<Response>,
    /// Bridge notified when the request completes or is abandoned.
    bridge: Weak<AsyncSampleBridge>,
    /// Whether the provider driver supplied a response.
    answered: bool,
}

impl ReadRequest {
    /// Creates one unanswered request associated with its bridge response.
    ///
    /// # Parameters
    ///
    /// * `offset` - Requested range start.
    /// * `length` - Number of requested bytes.
    /// * `response` - Shared result slot awaited by the worker.
    /// * `bridge` - Bridge notified after response or abandonment.
    pub(super) fn new(offset: u64, length: usize, response: Arc<Response>, bridge: Weak<AsyncSampleBridge>) -> Self {
        Self {
            offset,
            length,
            response,
            bridge,
            answered: false,
        }
    }

    /// Returns the requested range's starting offset.
    pub(crate) const fn offset(&self) -> u64 {
        self.offset
    }

    /// Returns the requested range's byte length.
    pub(crate) const fn len(&self) -> usize {
        self.length
    }

    /// Completes the worker request with the provider's range result.
    ///
    /// A short successful buffer becomes `UnexpectedEof` when the worker
    /// consumes it. The response is published only once.
    ///
    /// # Parameters
    ///
    /// * `result` - Exact provider bytes or the provider read error.
    pub(crate) fn respond(mut self, result: io::Result<Vec<u8>>) {
        set_response(&self.response, result);
        self.answered = true;
        if let Some(bridge) = self.bridge.upgrade() {
            let mut state = bridge.state.lock().unwrap_or_else(|error| error.into_inner());
            state.active = None;
            bridge.changed.notify_all();
        }
    }

    /// Wakes the worker with interruption when a provider response is dropped.
    fn interrupt_worker(&self) {
        set_response(
            &self.response,
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "async sample request was abandoned",
            )),
        );
        if let Some(bridge) = self.bridge.upgrade() {
            let mut state = bridge.state.lock().unwrap_or_else(|error| error.into_inner());
            if state
                .active
                .as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, &self.response))
            {
                state.active = None;
            }
            if state
                .pending
                .as_ref()
                .is_some_and(|pending| Arc::ptr_eq(&pending.response, &self.response))
            {
                state.pending = None;
            }
            bridge.changed.notify_all();
        }
    }
}

impl Drop for ReadRequest {
    /// Interrupts the worker if the provider future abandons this request.
    fn drop(&mut self) {
        if !self.answered {
            self.interrupt_worker();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::Weak;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;

    use super::super::AsyncSampleBridge;
    use super::ReadRequest;
    use crate::internal::async_sample_bridge::response::Response;

    #[test]
    fn test_abandoned_request_interrupts_worker_after_bridge_drop() {
        let response = Arc::new(Response::new());
        let request = ReadRequest::new(0, 1, Arc::clone(&response), Weak::new());

        drop(request);

        let result = response.result.lock().unwrap();
        assert_eq!(
            std::io::ErrorKind::Interrupted,
            result.as_ref().unwrap().as_ref().unwrap_err().kind(),
        );
    }

    #[test]
    fn test_interrupt_recovers_poisoned_bridge_lock_and_clears_active_read() {
        let bridge = Arc::new(AsyncSampleBridge::new(8));
        let worker_bridge = Arc::clone(&bridge);
        let worker = std::thread::spawn(move || {
            let mut byte = [0; 1];
            worker_bridge.read_at(2, &mut byte)
        });
        let request = block_on(bridge.next_request()).expect("worker should request a range");

        let poisoned_bridge = Arc::clone(&bridge);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned_bridge.state.lock().unwrap();
                panic!("poison bridge state before request interruption");
            })
            .join()
            .is_err()
        );

        drop(request);
        assert_eq!(
            std::io::ErrorKind::Interrupted,
            worker.join().unwrap().unwrap_err().kind()
        );
        let state = bridge.state.lock().unwrap_or_else(|error| error.into_inner());
        assert!(state.active.is_none(), "abandoned request must release its active slot");
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        struct ThreadWake(std::thread::Thread);
        impl Wake for ThreadWake {
            fn wake(self: Arc<Self>) {
                self.0.unpark();
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.0.unpark();
            }
        }

        let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            std::thread::park_timeout(std::time::Duration::from_millis(10));
        }
    }

    #[test]
    fn test_abandoning_pending_request_recovers_poisoned_bridge_state() {
        let bridge = AsyncSampleBridge::new(1);
        let response = Arc::new(Response::new());
        let pending = ReadRequest {
            offset: 0,
            length: 1,
            response: Arc::clone(&response),
            bridge: Arc::downgrade(&bridge),
            answered: true,
        };
        bridge.state.lock().unwrap().pending = Some(pending);

        let poison_bridge = Arc::clone(&bridge);
        assert!(
            std::thread::spawn(move || {
                let _state = poison_bridge.state.lock().unwrap();
                panic!("poison bridge state before abandoning the pending request");
            })
            .join()
            .is_err()
        );

        drop(ReadRequest::new(0, 1, Arc::clone(&response), Arc::downgrade(&bridge)));

        let state = bridge.state.lock().unwrap_or_else(|error| error.into_inner());
        assert!(
            state.pending.is_none(),
            "abandonment should remove the matching pending request"
        );
        assert_eq!(
            std::io::ErrorKind::Interrupted,
            response
                .result
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap_err()
                .kind(),
        );
    }
}
