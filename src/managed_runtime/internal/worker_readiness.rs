// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Session startup notification from the inference worker to its constructor.

use std::future::poll_fn;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::task::Poll;
use std::task::Waker;

use qubit_mime::MimeError;
use qubit_mime::MimeResult;

use crate::internal::waker_notify;

/// Waits for the worker's one session initialization result.
pub(super) struct WorkerReadiness {
    /// State shared with the worker's signal.
    shared: Arc<Readiness>,
}

/// Publishes successful initialization or a terminal startup error.
pub(super) struct WorkerReadySignal {
    /// State retained until the result is published.
    shared: Option<Arc<Readiness>>,
}

/// Result and notifications shared by the two sides of worker startup.
#[derive(Default)]
struct Readiness {
    /// Startup result and the asynchronous constructor's waker.
    state: Mutex<ReadinessState>,
    /// Wakes the synchronous constructor on completion.
    ready: Condvar,
}

/// Startup state accessed under one lock to avoid lost async wakeups.
#[derive(Default)]
struct ReadinessState {
    /// Result set once by the worker and taken once by its constructor.
    result: Option<MimeResult<()>>,
    /// Constructor to wake after the result is available.
    waker: Option<Waker>,
}

impl WorkerReadiness {
    /// Creates the two sides of a startup notification.
    ///
    /// # Returns
    ///
    /// The constructor's waiter and the worker's publishing signal.
    pub(super) fn pair() -> (Self, WorkerReadySignal) {
        let shared = Arc::new(Readiness::default());
        let signal = WorkerReadySignal {
            shared: Some(Arc::clone(&shared)),
        };
        (Self { shared }, signal)
    }

    /// Waits for the result on a synchronous call.
    ///
    /// # Errors
    ///
    /// Returns the initialization error or a backend error if the worker exits
    /// before sending a result.
    pub(super) fn wait_blocking(self) -> MimeResult<()> {
        let mut state = self.shared.state.lock().unwrap_or_else(|error| error.into_inner());
        loop {
            if let Some(result) = state.result.take() {
                return result;
            }
            state = self.shared.ready.wait(state).unwrap_or_else(|error| error.into_inner());
        }
    }

    /// Waits for the result without blocking the polling executor.
    ///
    /// # Errors
    ///
    /// Returns the initialization error or a backend error if the worker exits
    /// before sending a result.
    pub(super) async fn wait_async(self) -> MimeResult<()> {
        poll_fn(|context| {
            let candidate = context.waker().clone();
            let mut state = self.shared.state.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(result) = state.result.take() {
                return Poll::Ready(result);
            }
            let previous = state.waker.replace(candidate);
            drop(state);
            drop(previous);
            Poll::Pending
        })
        .await
    }
}

impl Drop for WorkerReadiness {
    /// Releases a registered constructor waker when startup is cancelled.
    fn drop(&mut self) {
        let waker = self
            .shared
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .waker
            .take();
        drop(waker);
    }
}

impl WorkerReadySignal {
    /// Publishes the model session's result and wakes the constructor.
    ///
    /// # Parameters
    ///
    /// * `result` - Success or the original initialization error.
    pub(super) fn finish(mut self, result: MimeResult<()>) {
        if let Some(shared) = self.shared.take() {
            shared.publish(result);
        }
    }
}

impl Drop for WorkerReadySignal {
    /// Reports a worker exit that did not publish an initialization result.
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take() {
            shared.publish(Err(MimeError::detector_backend(
                "magika",
                "worker exited during initialization",
            )));
        }
    }
}

impl Readiness {
    /// Stores the producer's terminal result before notifying the constructor.
    ///
    /// # Parameters
    ///
    /// * `result` - Worker startup result to publish.
    fn publish(&self, result: MimeResult<()>) {
        let waker = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.result = Some(result);
            state.waker.take()
        };
        self.ready.notify_one();
        if let Some(waker) = waker {
            waker_notify::wake(waker);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;

    use qubit_mime::MimeError;

    use super::WorkerReadiness;

    #[derive(Default)]
    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_readiness_preserves_result_published_before_first_poll() {
        let (ready, signal) = WorkerReadiness::pair();
        signal.finish(Ok(()));

        let mut wait = pin!(ready.wait_async());
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(wait.as_mut().poll(&mut context), Poll::Ready(Ok(()))));
    }

    #[test]
    fn test_worker_exit_reports_error_and_wakes_only_latest_waker() {
        let (ready, signal) = WorkerReadiness::pair();
        let mut wait = pin!(ready.wait_async());
        let original = Arc::new(WakeCount::default());
        let latest = Arc::new(WakeCount::default());
        let original_waker = Waker::from(Arc::clone(&original));
        let latest_waker = Waker::from(Arc::clone(&latest));
        let mut original_context = Context::from_waker(&original_waker);
        let mut latest_context = Context::from_waker(&latest_waker);
        assert!(wait.as_mut().poll(&mut original_context).is_pending());
        assert!(wait.as_mut().poll(&mut latest_context).is_pending());

        drop(signal);

        assert_eq!(original.0.load(Ordering::Relaxed), 0);
        assert_eq!(latest.0.load(Ordering::Relaxed), 1);
        assert!(matches!(
            wait.as_mut().poll(&mut latest_context),
            Poll::Ready(Err(MimeError::DetectorBackend { backend, reason }))
                if backend == "magika" && reason == "worker exited during initialization"
        ));
    }

    #[test]
    fn test_cancelled_constructor_releases_waker_before_worker_exits() {
        let (ready, signal) = WorkerReadiness::pair();
        let count = Arc::new(WakeCount::default());
        let waker = Waker::from(Arc::clone(&count));
        let mut context = Context::from_waker(&waker);
        {
            let mut wait = pin!(ready.wait_async());
            assert!(wait.as_mut().poll(&mut context).is_pending());
        }

        assert_eq!(Arc::strong_count(&count), 2);
        signal.finish(Ok(()));
        assert_eq!(count.0.load(Ordering::Relaxed), 0);
    }
}
