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

use super::task_control::register_waker;
use super::task_state::TaskState;

/// Future that observes result completion while retaining cancellation control.
pub(super) struct InferenceFuture {
    /// Shared result and release state for the admitted job.
    state: Arc<TaskState>,
    /// Whether this future has already returned its business result.
    completed: bool,
    /// Test handshake sent after the blocking waiter owns the state lock.
    #[cfg(test)]
    wait_started: Option<Sender<()>>,
    /// Test handshake sent after a settled waiter enters the release wait.
    #[cfg(test)]
    release_wait_started: Option<Sender<()>>,
}

impl InferenceFuture {
    /// Creates the observer for a submitted inference job.
    ///
    /// # Parameters
    ///
    /// * `state` - State populated by the worker.
    pub(super) fn new(state: Arc<TaskState>) -> Self {
        Self {
            state,
            completed: false,
            #[cfg(test)]
            wait_started: None,
            #[cfg(test)]
            release_wait_started: None,
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

    /// Waits for the business result and then for the worker to release the
    /// admitted input. Synchronous detector paths preserve their legacy
    /// guarantee that returning the result also releases caller-owned capacity.
    pub(super) fn wait_blocking_settled(mut self) -> MimeResult<Option<String>> {
        let state = Arc::clone(&self.state);
        #[cfg(test)]
        let mut release_wait_started = self.release_wait_started.take();
        let result = self.wait_blocking();
        let mut inner = state.inner.lock().unwrap_or_else(|error| error.into_inner());
        while !inner.released {
            #[cfg(test)]
            if let Some(release_wait_started) = release_wait_started.take() {
                let _ = release_wait_started.send(());
            }
            inner = state.released.wait(inner).unwrap_or_else(|error| error.into_inner());
        }
        result
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
    /// `Ready` with the result, or `Pending` until the result is published.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut candidate = Some(cx.waker().clone());
        let result = {
            let mut inner = self.state.inner.lock().unwrap_or_else(|error| error.into_inner());
            let result = inner.result.take();
            if result.is_none() {
                register_waker(&mut inner.wakers, &mut candidate);
            }
            result
        };
        drop(candidate);
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
            self.state.request_stop();
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

    use qubit_operation::ChildOutcomeKind;

    use super::InferenceFuture;
    use crate::managed_runtime::task_state::TaskState;

    #[test]
    fn test_future_registers_until_result_is_published() {
        struct WakeCounter(std::sync::atomic::AtomicUsize);
        impl std::task::Wake for WakeCounter {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let state = Arc::new(TaskState::new(99, None));
        let poisoned_state = Arc::clone(&state);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned_state.inner.lock().unwrap();
            panic!("poison task result lock before future polling");
        })
        .join();
        let mut future = Box::pin(InferenceFuture::new(Arc::clone(&state)));
        let wake_counter = Arc::new(WakeCounter(std::sync::atomic::AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake_counter));
        let mut context = Context::from_waker(&waker);

        assert!(matches!(future.as_mut().poll(&mut context), Poll::Pending));
        state.finish_business(Ok(Some("text/plain".to_owned())), ChildOutcomeKind::Succeeded);
        assert!(
            wake_counter.0.load(std::sync::atomic::Ordering::SeqCst) >= 1,
            "result publication must wake its registered observer"
        );
        assert!(matches!(
            future.as_mut().poll(&mut context),
            Poll::Ready(Ok(Some(mime))) if mime == "text/plain"
        ));
    }

    #[test]
    fn test_blocking_wait_observes_result_published_after_wait_begins() {
        for (operation_id, result, business) in [
            (102, Ok(Some("text/plain".to_owned())), ChildOutcomeKind::Succeeded),
            (
                103,
                Err(qubit_mime::MimeError::detector_backend("test", "failed")),
                ChildOutcomeKind::Failed,
            ),
        ] {
            struct CompleteOnDrop(Arc<TaskState>);
            impl Drop for CompleteOnDrop {
                fn drop(&mut self) {
                    // Ensure every exit path wakes a blocked waiter. Result
                    // publication is write-once, so this only fills an
                    // otherwise missing result during cleanup.
                    self.0.finish_business(
                        Err(qubit_mime::MimeError::detector_backend("test", "waiter cleanup")),
                        ChildOutcomeKind::Failed,
                    );
                    self.0.mark_worker_done();
                    self.0.mark_released();
                }
            }

            let state = Arc::new(TaskState::new(operation_id, None));
            let completion = CompleteOnDrop(Arc::clone(&state));
            let (started_tx, started_rx) = mpsc::channel();
            let (result_tx, result_rx) = mpsc::channel();
            let mut future = InferenceFuture::new(Arc::clone(&state));
            future.wait_started = Some(started_tx);
            let waiter = std::thread::spawn(move || result_tx.send(future.wait_blocking()));

            started_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("waiter entered Condvar wait");
            let publish_state = Arc::clone(&state);
            crate::tests::internal::bridge_wait::join_before(
                std::thread::spawn(move || publish_state.finish_business(result, business)),
                "publish result to blocking waiter",
                Duration::from_secs(2),
            );
            let observed = result_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("result notification wakes waiter before release");
            let fact_state = Arc::clone(&state);
            let (worker_done, released) = crate::tests::internal::bridge_wait::join_before(
                std::thread::spawn(move || {
                    let inner = fact_state.inner.lock().unwrap();
                    (inner.worker_done, inner.released)
                }),
                "read blocking waiter release facts",
                Duration::from_secs(2),
            );
            assert!(!worker_done);
            assert!(!released);
            drop(completion);
            crate::tests::internal::bridge_wait::join_before(
                waiter,
                "blocking waiter cleanup",
                Duration::from_secs(2),
            )
            .expect("blocking waiter result send must succeed");
            match operation_id {
                102 => assert_eq!(observed.unwrap().as_deref(), Some("text/plain")),
                _ => assert_eq!(
                    observed.unwrap_err().to_string(),
                    "MIME detector backend 'test' failed: failed"
                ),
            }
            assert!(state.inner.lock().unwrap().released);
        }
    }

    #[test]
    fn test_dropping_pending_future_requests_cooperative_stop() {
        let state = Arc::new(TaskState::new(100, None));
        let future = InferenceFuture::new(Arc::clone(&state));
        drop(future);
        assert!(state.stop_requested());
    }

    #[test]
    fn test_blocking_wait_recovers_a_poisoned_result_lock() {
        let state = Arc::new(TaskState::new(101, None));
        let poisoned_state = Arc::clone(&state);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned_state.inner.lock().unwrap();
            panic!("poison task result lock before blocking wait");
        })
        .join();
        state.finish_business(Ok(None), ChildOutcomeKind::Succeeded);
        let future = InferenceFuture::new(state);
        assert!(matches!(future.wait_blocking(), Ok(None)));
    }

    #[test]
    fn test_blocking_wait_settled_waits_for_release_after_result() {
        let state = Arc::new(TaskState::new(104, None));
        let poisoned_state = Arc::clone(&state);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned_state.inner.lock().unwrap();
            panic!("poison result lock before settled wait");
        })
        .join();
        state.finish_business(Ok(Some("text/plain".to_owned())), ChildOutcomeKind::Succeeded);
        let (release_wait_tx, release_wait_rx) = mpsc::channel();
        let (result_tx, result_rx) = mpsc::channel();
        let waiter_state = Arc::clone(&state);
        let waiter = std::thread::spawn(move || {
            let mut future = InferenceFuture::new(waiter_state);
            future.release_wait_started = Some(release_wait_tx);
            let _ = result_tx.send(future.wait_blocking_settled());
        });

        release_wait_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("settled waiter entered the release wait after business result");
        let fact_state = Arc::clone(&state);
        let released = crate::tests::internal::bridge_wait::join_before(
            std::thread::spawn(move || {
                fact_state
                    .inner
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .released
            }),
            "read release fact before releasing settled waiter",
            Duration::from_secs(2),
        );
        assert!(!released, "release must remain pending while the gate is held");
        state.mark_worker_done();
        state.mark_released();

        assert_eq!(
            Some("text/plain"),
            result_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("release notification wakes settled waiter")
                .unwrap()
                .as_deref()
        );
        crate::tests::internal::bridge_wait::join_before(
            waiter,
            "settled waiter thread exit",
            Duration::from_secs(2),
        );
    }
}
