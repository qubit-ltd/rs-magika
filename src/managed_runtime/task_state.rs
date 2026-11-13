// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared business and release state for one accepted worker job.

use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::Weak;
use std::task::Waker;

use qubit_mime::MimeResult;
use qubit_operation::report::ChildBusinessOutcome;
use qubit_operation::report::OperationId;

use super::task_inner::TaskInner;
use crate::internal::async_sample_bridge::AsyncSampleBridge;

/// State observed by both the worker and operation control adapter.
pub(super) struct TaskState {
    /// Stable identity attached to the managed child report.
    pub(super) operation_id: OperationId,
    /// Mutable result and lifecycle facts.
    pub(super) inner: Mutex<TaskInner>,
    /// Blocking notification used by synchronous inference callers.
    pub(super) released: Condvar,
    /// Bridge closed when cancellation stops async sampling.
    bridge: Option<Weak<AsyncSampleBridge>>,
}

impl TaskState {
    /// Creates a queued task state and optional sampling bridge reference.
    ///
    /// # Parameters
    ///
    /// * `operation_id` - Stable identifier reported to the parent scope.
    /// * `bridge` - Weak bridge reference closed when this task is stopped.
    pub(super) fn new(operation_id: OperationId, bridge: Option<Weak<AsyncSampleBridge>>) -> Self {
        Self {
            operation_id,
            inner: Mutex::new(TaskInner::new()),
            released: Condvar::new(),
            bridge,
        }
    }

    /// Reports whether the worker should skip or stop this job.
    pub(super) fn stop_requested(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .stop_requested
    }

    /// Marks stop requested, wakes waiters, and closes pending provider reads.
    pub(super) fn request_stop(&self) {
        let wakers = {
            let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            inner.stop_requested = true;
            std::mem::take(&mut inner.wakers)
        };
        for waker in wakers {
            waker.wake();
        }
        if let Some(bridge) = self.bridge.as_ref().and_then(Weak::upgrade) {
            bridge.close();
        }
    }

    /// Records that the inference worker has begun this job.
    pub(super) fn mark_started(&self) {
        self.inner.lock().unwrap_or_else(|error| error.into_inner()).started = true;
    }

    /// Stores the business result and wakes result observers.
    ///
    /// # Parameters
    ///
    /// * `result` - MIME inference result produced by the worker.
    /// * `business` - Worker outcome before cancellation-state reconciliation.
    pub(super) fn finish_business(&self, result: MimeResult<Option<String>>, business: ChildBusinessOutcome) {
        let wakers = {
            let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            if inner.business.is_none() {
                inner.business = Some(if inner.stop_requested {
                    ChildBusinessOutcome::Stopped
                } else {
                    business
                });
                inner.result = Some(result);
            }
            std::mem::take(&mut inner.wakers)
        };
        wake_all(wakers);
    }

    /// Records that the worker released job input and all capacity permits.
    pub(super) fn mark_released(&self) {
        let wakers = {
            let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            inner.released = true;
            std::mem::take(&mut inner.wakers)
        };
        wake_all(wakers);
        self.released.notify_all();
    }
}

/// Wakes every future registered for a task state transition.
///
/// # Parameters
///
/// * `wakers` - Notifications collected while holding the task lock.
fn wake_all(wakers: Vec<Waker>) {
    for waker in wakers {
        waker.wake();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use qubit_operation::report::ChildBusinessOutcome;
    use qubit_operation::report::OperationId;

    use super::TaskState;

    #[test]
    fn test_lifecycle_recovers_a_poisoned_state_lock() {
        let state = Arc::new(TaskState::new(OperationId::new(2), None));
        let poisoned = Arc::clone(&state);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned.inner.lock().unwrap();
            panic!("poison task state for recovery test");
        })
        .join();

        assert!(!state.stop_requested());
        state.request_stop();
        assert!(state.stop_requested());
        state.mark_started();
        state.finish_business(Ok(None), ChildBusinessOutcome::Stopped);
        state.mark_released();
        let inner = state.inner.lock().unwrap_or_else(|error| error.into_inner());
        assert!(inner.started);
        assert!(inner.released);
        assert!(inner.business.is_some());
    }

    #[test]
    fn test_business_terminal_fact_is_write_once() {
        let state = TaskState::new(OperationId::new(5), None);
        state.finish_business(Ok(Some("first".to_owned())), ChildBusinessOutcome::Succeeded);
        state.finish_business(Err(qubit_mime::MimeError::detector_backend("test", "later")), ChildBusinessOutcome::Failed);

        let inner = state.inner.lock().unwrap();
        assert_eq!(inner.business, Some(ChildBusinessOutcome::Succeeded));
        assert!(matches!(inner.result.as_ref(), Some(Ok(Some(value))) if value == "first"));
    }

    #[test]
    fn test_requested_stop_classifies_the_terminal_business_fact_as_stopped() {
        let state = TaskState::new(OperationId::new(6), None);
        state.request_stop();
        state.finish_business(Ok(Some("late result".to_owned())), ChildBusinessOutcome::Succeeded);

        let inner = state.inner.lock().unwrap();
        assert_eq!(inner.business, Some(ChildBusinessOutcome::Stopped));
    }
}
