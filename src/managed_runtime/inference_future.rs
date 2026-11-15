// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async and blocking observation of one inference result.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use std::sync::mpsc::Sender;
use std::task::Context;
use std::task::Poll;

use qubit_mime::MimeResult;
use qubit_operation::protocol::ManagedOperationControl;
use qubit_operation::report::StopReason;

use super::task_control::register_waker;
use super::task_state::TaskState;

/// Future that observes result completion while retaining cancellation control.
pub(super) struct InferenceFuture {
    /// Shared result and release state for the admitted job.
    state: Arc<TaskState>,
    /// Control handle used to request cooperative stop on future drop.
    control: Arc<dyn ManagedOperationControl>,
    /// Whether this future has already returned its business result.
    completed: bool,
    /// Test handshake sent after the blocking waiter owns the state lock.
    #[cfg(test)]
    wait_started: Option<Sender<()>>,
}

impl InferenceFuture {
    /// Creates the observer for a submitted inference job.
    ///
    /// # Parameters
    ///
    /// * `state` - State populated by the worker.
    /// * `control` - Managed control retained for cancellation.
    pub(super) fn new(state: Arc<TaskState>, control: Arc<dyn ManagedOperationControl>) -> Self {
        Self {
            state,
            control,
            completed: false,
            #[cfg(test)]
            wait_started: None,
        }
    }

    /// Waits synchronously until the business result is published. This result
    /// notification is independent of worker completion and resource release.
    ///
    /// # Returns
    ///
    /// The MIME inference result produced by the worker.
    pub(super) fn wait_blocking(mut self) -> MimeResult<Option<String>> {
        let mut inner = self.state.inner.lock().unwrap_or_else(|error| error.into_inner());
        #[cfg(test)]
        if let Some(wait_started) = self.wait_started.take() {
            let _ = wait_started.send(());
        }
        loop {
            if let Some(result) = inner.result.take() {
                self.completed = true;
                return result;
            }
            inner = self
                .state
                .released
                .wait(inner)
                .unwrap_or_else(|error| error.into_inner());
        }
    }
}

impl Future for InferenceFuture {
    /// Worker result returned after sampling and inference complete.
    type Output = MimeResult<Option<String>>;

    /// Returns the result when the worker publishes it or registers a waker.
    ///
    /// # Parameters
    ///
    /// * `self` - Pinned observer state for this inference result.
    /// * `cx` - Executor context used to register a notification.
    ///
    /// # Returns
    ///
    /// `Ready` with the result, or `Pending` until worker completion.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = {
            let mut inner = self.state.inner.lock().unwrap_or_else(|error| error.into_inner());
            let result = inner.result.take();
            if result.is_none() {
                register_waker(&mut inner.wakers, cx.waker());
            }
            result
        };
        if let Some(result) = result {
            self.completed = true;
            return Poll::Ready(result);
        }
        Poll::Pending
    }
}

impl Drop for InferenceFuture {
    /// Requests cancellation if the consumer abandons a pending result.
    fn drop(&mut self) {
        if !self.completed {
            self.control.request_stop(StopReason::UserCancelled);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Duration;

    use qubit_operation::protocol::ManagedOperationControl;
    use qubit_operation::report::ChildBusinessOutcome;
    use qubit_operation::report::OperationId;

    use super::InferenceFuture;
    use crate::managed_runtime::task_control::TaskControl;
    use crate::managed_runtime::task_state::TaskState;

    #[test]
    fn test_future_registers_until_result_is_published() {
        let state = Arc::new(TaskState::new(OperationId::new(99), None));
        let poisoned_state = Arc::clone(&state);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned_state.inner.lock().unwrap();
            panic!("poison task result lock before future polling");
        })
        .join();
        let control: Arc<dyn ManagedOperationControl> = Arc::new(TaskControl(Arc::clone(&state)));
        let mut future = Box::pin(InferenceFuture::new(Arc::clone(&state), control));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        state.finish_business(Ok(Some("text/plain".to_owned())), ChildBusinessOutcome::Succeeded);
        assert!(matches!(
            future.as_mut().poll(&mut context),
            Poll::Ready(Ok(Some(mime))) if mime == "text/plain"
        ));
    }

    #[test]
    fn test_blocking_wait_observes_result_published_after_wait_begins() {
        for (operation_id, result, business) in [
            (102, Ok(Some("text/plain".to_owned())), ChildBusinessOutcome::Succeeded),
            (
                103,
                Err(qubit_mime::MimeError::detector_backend("test", "failed")),
                ChildBusinessOutcome::Failed,
            ),
        ] {
            let state = Arc::new(TaskState::new(OperationId::new(operation_id), None));
            let (started, observed, worker_done, released, released_pending, waiter_panicked) =
                std::thread::scope(|scope| {
                    struct CompleteOnDrop(Arc<TaskState>);
                    impl Drop for CompleteOnDrop {
                        fn drop(&mut self) {
                            self.0.mark_worker_done();
                            self.0.mark_released();
                        }
                    }

                    let completion = CompleteOnDrop(Arc::clone(&state));
                    let control: Arc<dyn ManagedOperationControl> = Arc::new(TaskControl(Arc::clone(&state)));
                    let (started_tx, started_rx) = mpsc::channel();
                    let (result_tx, result_rx) = mpsc::channel();
                    let mut future = InferenceFuture::new(Arc::clone(&state), control);
                    future.wait_started = Some(started_tx);
                    let waiter = scope.spawn(move || {
                        let _ = result_tx.send(future.wait_blocking());
                    });

                    let started = started_rx.recv_timeout(Duration::from_secs(2));
                    let observed = if started.is_ok() {
                        state.finish_business(result, business);
                        result_rx.recv_timeout(Duration::from_secs(2))
                    } else {
                        Err(mpsc::RecvTimeoutError::Disconnected)
                    };
                    let (worker_done, released) = {
                        let inner = state.inner.lock().unwrap();
                        (inner.worker_done, inner.released)
                    };
                    let waker = Waker::noop();
                    let mut context = Context::from_waker(waker);
                    let released_pending = matches!(
                        TaskControl(Arc::clone(&state)).poll_released(&mut context),
                        Poll::Pending
                    );
                    drop(completion);
                    let waiter_panicked = waiter.join().is_err();
                    (
                        started,
                        observed,
                        worker_done,
                        released,
                        released_pending,
                        waiter_panicked,
                    )
                });

            assert!(started.is_ok(), "waiter entered Condvar wait: {started:?}");
            assert!(
                observed.is_ok(),
                "result notification wakes waiter before release: {observed:?}"
            );
            assert!(!worker_done);
            assert!(!released);
            assert!(released_pending);
            assert!(!waiter_panicked);
            let observed = observed.expect("result notification wakes waiter before release");
            match operation_id {
                102 => assert_eq!(observed.unwrap().as_deref(), Some("text/plain")),
                _ => assert_eq!(
                    observed.unwrap_err().to_string(),
                    "MIME detector backend 'test' failed: failed"
                ),
            }
            let waker = Waker::noop();
            let mut context = Context::from_waker(waker);
            assert!(matches!(TaskControl(state).poll_released(&mut context), Poll::Ready(_)));
        }
    }

    #[test]
    fn test_dropping_pending_future_requests_cooperative_stop() {
        let state = Arc::new(TaskState::new(OperationId::new(100), None));
        let control: Arc<dyn ManagedOperationControl> = Arc::new(TaskControl(Arc::clone(&state)));
        let future = InferenceFuture::new(Arc::clone(&state), control);
        drop(future);
        assert!(state.stop_requested());
    }

    #[test]
    fn test_blocking_wait_recovers_a_poisoned_result_lock() {
        let state = Arc::new(TaskState::new(OperationId::new(101), None));
        let poisoned_state = Arc::clone(&state);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned_state.inner.lock().unwrap();
            panic!("poison task result lock before blocking wait");
        })
        .join();
        state.finish_business(Ok(None), ChildBusinessOutcome::Succeeded);
        let control: Arc<dyn ManagedOperationControl> = Arc::new(TaskControl(Arc::clone(&state)));
        let future = InferenceFuture::new(state, control);
        assert!(matches!(future.wait_blocking(), Ok(None)));
    }
}
