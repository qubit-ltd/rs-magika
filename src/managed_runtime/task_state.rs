// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared business and release state for one accepted worker job.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::Weak;
use std::task::Waker;

use qubit_mime::MimeResult;
use qubit_operation::report::ChildBusinessOutcome;
use qubit_operation::report::OperationId;

pub(super) use super::admission_gate::AdmissionGate;
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
    bridge: std::sync::Mutex<Option<Weak<AsyncSampleBridge>>>,
    /// Aggregate owner that releases permits after worker and driver
    /// completion.
    owner: std::sync::Mutex<Option<Arc<super::request_admission::RequestResourceOwner>>>,
    /// Weak back-reference lets aggregate release prune the runtime's strong
    /// registry.
    active_tasks: Option<Weak<Mutex<Vec<Arc<TaskState>>>>>,
    /// Weak shutdown gate shared with admission and provider-operation starts.
    admission_gate: Option<Weak<Mutex<AdmissionGate>>>,
}

impl TaskState {
    /// Creates a queued task state and optional sampling bridge reference.
    ///
    /// # Parameters
    ///
    /// * `operation_id` - Stable identifier reported to the parent scope.
    /// * `bridge` - Weak bridge reference closed when this task is stopped.
    #[cfg(test)]
    pub(super) fn new(operation_id: OperationId, bridge: Option<Weak<AsyncSampleBridge>>) -> Self {
        Self::new_registered(operation_id, bridge, None, None)
    }

    /// Creates a task state tracked strongly by the owning runtime until
    /// release.
    /// Creates a task state and registers its optional runtime back-references.
    ///
    /// # Parameters
    ///
    /// * `operation_id` - Stable operation identifier.
    /// * `bridge` - Optional weak sampling bridge reference.
    /// * `active_tasks` - Runtime registry pruned after aggregate release.
    /// * `admission_gate` - Gate serializing provider starts with shutdown.
    ///
    /// # Returns
    ///
    /// A new state with no result or lifecycle facts recorded.
    pub(super) fn new_registered(
        operation_id: OperationId,
        bridge: Option<Weak<AsyncSampleBridge>>,
        active_tasks: Option<Weak<Mutex<Vec<Arc<TaskState>>>>>,
        admission_gate: Option<Weak<Mutex<AdmissionGate>>>,
    ) -> Self {
        Self {
            operation_id,
            inner: Mutex::new(TaskInner::new()),
            released: Condvar::new(),
            bridge: std::sync::Mutex::new(bridge),
            owner: std::sync::Mutex::new(None),
            active_tasks,
            admission_gate,
        }
    }

    /// Linearizes provider-operation start against cancellation and shutdown.
    pub(super) fn try_start_provider_operation(&self) -> bool {
        let gate = self.admission_gate.as_ref().and_then(Weak::upgrade);
        if self.admission_gate.is_some() && gate.is_none() {
            return false;
        }
        let gate = gate
            .as_ref()
            .map(|gate| gate.lock().unwrap_or_else(|error| error.into_inner()));
        if gate.as_ref().is_some_and(|gate| gate.cancelling) {
            return false;
        }
        !self
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .stop_requested
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
        if let Some(bridge) = self
            .bridge
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .and_then(Weak::upgrade)
        {
            bridge.close();
        }
    }

    /// Attaches the bridge created after request admission.
    ///
    /// # Parameters
    ///
    /// * `bridge` - Weak reference to the sampling bridge.
    pub(super) fn attach_bridge(&self, bridge: Weak<AsyncSampleBridge>) {
        *self.bridge.lock().unwrap_or_else(|error| error.into_inner()) = Some(bridge);
        if self.stop_requested()
            && let Some(bridge) = self
                .bridge
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
                .and_then(Weak::upgrade)
        {
            bridge.close();
        }
    }

    /// Attaches the aggregate resource owner for this task.
    ///
    /// # Parameters
    ///
    /// * `owner` - Owner notified when the worker and driver finish.
    pub(super) fn attach_owner(&self, owner: Arc<super::request_admission::RequestResourceOwner>) {
        *self.owner.lock().unwrap_or_else(|error| error.into_inner()) = Some(owner);
    }

    /// Records completion of the provider sampling driver.
    pub(super) fn mark_driver_done(&self) {
        let owner = self
            .owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .cloned();
        if let Some(owner) = owner {
            owner.driver_done();
        }
    }

    /// Records completion of the worker and wakes blocking observers.
    pub(super) fn mark_worker_done(&self) {
        self.inner.lock().unwrap_or_else(|error| error.into_inner()).worker_done = true;
        self.released.notify_all();
    }

    #[cfg(test)]
    pub(super) fn wait_worker_done_blocking(&self, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        while !inner.worker_done {
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let (next, result) = self
                .released
                .wait_timeout(inner, deadline.saturating_duration_since(now))
                .unwrap_or_else(|error| error.into_inner());
            inner = next;
            if result.timed_out() && !inner.worker_done {
                return false;
            }
        }
        true
    }

    #[cfg(test)]
    pub(super) fn wait_released_blocking(&self, timeout: std::time::Duration) -> bool {
        let deadline = std::time::Instant::now() + timeout;
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        while !inner.released {
            let now = std::time::Instant::now();
            if now >= deadline {
                return false;
            }
            let (next, result) = self
                .released
                .wait_timeout(inner, deadline.saturating_duration_since(now))
                .unwrap_or_else(|error| error.into_inner());
            inner = next;
            if result.timed_out() && !inner.released {
                return false;
            }
        }
        true
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
    /// * `business` - Worker outcome corresponding to the stored result.
    pub(super) fn finish_business(&self, result: MimeResult<Option<String>>, business: ChildBusinessOutcome) {
        let wakers = {
            let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            if inner.business.is_none() {
                inner.business = Some(business);
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
        self.owner.lock().unwrap_or_else(|error| error.into_inner()).take();
        if let Some(active_tasks) = self.active_tasks.as_ref().and_then(Weak::upgrade) {
            active_tasks
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .retain(|task| task.operation_id != self.operation_id);
        }
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
    fn test_stop_closes_existing_and_late_attached_sampling_bridges() {
        let bridge = crate::internal::async_sample_bridge::AsyncSampleBridge::new(0);
        let state = TaskState::new(OperationId::new(8), Some(Arc::downgrade(&bridge)));

        state.request_stop();
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        let mut closed = std::pin::pin!(bridge.closed());
        assert!(std::future::Future::poll(closed.as_mut(), &mut context).is_ready());

        let late_bridge = crate::internal::async_sample_bridge::AsyncSampleBridge::new(0);
        state.attach_bridge(Arc::downgrade(&late_bridge));
        let mut closed = std::pin::pin!(late_bridge.closed());
        assert!(std::future::Future::poll(closed.as_mut(), &mut context).is_ready());
    }

    #[test]
    fn test_release_removes_task_from_runtime_registry() {
        let tasks = Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = Arc::new(TaskState::new_registered(
            OperationId::new(9),
            None,
            Some(Arc::downgrade(&tasks)),
            None,
        ));
        tasks.lock().unwrap().push(Arc::clone(&state));

        state.mark_released();

        assert!(tasks.lock().unwrap_or_else(|error| error.into_inner()).is_empty());
        assert!(state.inner.lock().unwrap().released);
    }

    #[test]
    fn test_provider_start_rejects_closed_gate_and_expired_gate() {
        let gate = Arc::new(std::sync::Mutex::new(super::AdmissionGate::new()));
        gate.lock().unwrap().cancelling = true;
        let state = TaskState::new_registered(OperationId::new(10), None, None, Some(Arc::downgrade(&gate)));
        assert!(!state.try_start_provider_operation());

        let poisoned_gate = Arc::new(std::sync::Mutex::new(super::AdmissionGate::new()));
        let gate_to_poison = Arc::clone(&poisoned_gate);
        assert!(
            std::thread::spawn(move || {
                let _guard = gate_to_poison.lock().unwrap();
                panic!("poison admission gate for recovery test");
            })
            .join()
            .is_err()
        );
        let state = TaskState::new_registered(OperationId::new(15), None, None, Some(Arc::downgrade(&poisoned_gate)));
        assert!(state.try_start_provider_operation());

        let expired = Arc::new(std::sync::Mutex::new(super::AdmissionGate::new()));
        let expired_weak = Arc::downgrade(&expired);
        drop(expired);
        let state = TaskState::new_registered(OperationId::new(11), None, None, Some(expired_weak));
        assert!(!state.try_start_provider_operation());
    }

    #[test]
    fn test_worker_completion_recovers_a_poisoned_lifecycle_lock() {
        let state = Arc::new(TaskState::new(OperationId::new(16), None));
        let poisoned = Arc::clone(&state);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned.inner.lock().unwrap();
                panic!("poison lifecycle lock before worker completion");
            })
            .join()
            .is_err()
        );

        state.mark_worker_done();

        assert!(
            state
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .worker_done
        );
        assert!(state.wait_worker_done_blocking(std::time::Duration::from_millis(1)));
    }

    #[test]
    fn test_provider_start_recovers_poisoned_task_state_lock() {
        let gate = Arc::new(std::sync::Mutex::new(super::AdmissionGate::new()));
        let state = Arc::new(TaskState::new_registered(
            OperationId::new(17),
            None,
            None,
            Some(Arc::downgrade(&gate)),
        ));
        let poisoned = Arc::clone(&state);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned.inner.lock().unwrap();
                panic!("poison task state before provider start");
            })
            .join()
            .is_err()
        );

        assert!(state.try_start_provider_operation());
    }

    #[test]
    fn test_worker_completion_without_owner_wakes_waiters() {
        let state = Arc::new(TaskState::new(OperationId::new(12), None));
        assert!(!state.wait_worker_done_blocking(std::time::Duration::ZERO));
        let poisoned = Arc::clone(&state);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned.inner.lock().unwrap();
                panic!("poison worker state before waiting");
            })
            .join()
            .is_err()
        );
        assert!(!state.wait_worker_done_blocking(std::time::Duration::from_millis(1)));
        state.mark_driver_done();
        state.mark_worker_done();
        assert!(state.wait_worker_done_blocking(std::time::Duration::from_millis(1)));
    }

    #[test]
    fn test_lifecycle_recovers_poisoned_bridge_owner_and_registry_locks() {
        let tasks = Arc::new(std::sync::Mutex::new(Vec::new()));
        let state = Arc::new(TaskState::new_registered(
            OperationId::new(13),
            None,
            Some(Arc::downgrade(&tasks)),
            None,
        ));
        tasks.lock().unwrap().push(Arc::clone(&state));

        let poisoned_bridge = Arc::clone(&state);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned_bridge.bridge.lock().unwrap();
                panic!("poison bridge lock for recovery test");
            })
            .join()
            .is_err()
        );
        let poisoned_owner = Arc::clone(&state);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned_owner.owner.lock().unwrap();
                panic!("poison owner lock for recovery test");
            })
            .join()
            .is_err()
        );
        let poisoned_tasks = Arc::clone(&tasks);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned_tasks.lock().unwrap();
                panic!("poison task registry for recovery test");
            })
            .join()
            .is_err()
        );

        state.request_stop();
        state.mark_driver_done();
        state.mark_released();

        assert!(state.inner.lock().unwrap().released);
        assert!(tasks.lock().unwrap_or_else(|error| error.into_inner()).is_empty());
    }

    #[test]
    fn test_business_terminal_fact_is_write_once() {
        let state = TaskState::new(OperationId::new(5), None);
        state.finish_business(Ok(Some("first".to_owned())), ChildBusinessOutcome::Succeeded);
        state.finish_business(
            Err(qubit_mime::MimeError::detector_backend("test", "later")),
            ChildBusinessOutcome::Failed,
        );

        let inner = state.inner.lock().unwrap();
        assert_eq!(inner.business, Some(ChildBusinessOutcome::Succeeded));
        assert!(matches!(inner.result.as_ref(), Some(Ok(Some(value))) if value == "first"));
    }

    #[test]
    fn test_requested_stop_classifies_the_terminal_business_fact_as_stopped() {
        let state = TaskState::new(OperationId::new(6), None);
        state.request_stop();
        state.finish_business(
            Err(qubit_mime::MimeError::detector_backend(
                "test",
                "stopped before execution",
            )),
            ChildBusinessOutcome::Stopped,
        );

        let inner = state.inner.lock().unwrap();
        assert_eq!(inner.business, Some(ChildBusinessOutcome::Stopped));
        assert!(matches!(inner.result.as_ref(), Some(Err(_))));
    }

    #[test]
    fn test_result_winning_stop_race_keeps_matching_business_outcome() {
        let state = TaskState::new(OperationId::new(7), None);
        state.request_stop();
        state.finish_business(Ok(Some("text/plain".to_owned())), ChildBusinessOutcome::Succeeded);

        let inner = state.inner.lock().unwrap();
        assert_eq!(inner.business, Some(ChildBusinessOutcome::Succeeded));
        assert!(matches!(inner.result.as_ref(), Some(Ok(Some(value))) if value == "text/plain"));
    }
}
