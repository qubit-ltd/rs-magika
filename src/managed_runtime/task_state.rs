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
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use qubit_mime::MimeResult;
use qubit_operation::ChildOutcomeKind;
use qubit_operation::ResourceRef;

pub(super) use super::admission_gate::AdmissionGate;
use super::request_resource_scope_guard::RequestResourceScopeGuard;
use super::task_control::register_waker;
use super::task_inner::TaskInner;
use crate::internal::async_sample_bridge::AsyncSampleBridge;

/// State observed by both the worker and operation control adapter.
pub(crate) struct TaskState {
    /// Stable identity attached to the managed child report.
    pub(super) operation_id: u64,
    /// Mutable result and lifecycle facts.
    pub(super) inner: Mutex<TaskInner>,
    /// Blocking notification used by synchronous inference callers.
    pub(super) released: Condvar,
    /// Bridge closed when cancellation stops async sampling.
    bridge: std::sync::Mutex<Option<Weak<AsyncSampleBridge>>>,
    /// Aggregate owner that releases permits after worker and driver
    /// completion.
    owner: std::sync::Mutex<Option<Arc<super::request_resource_owner::RequestResourceOwner>>>,
    /// User reference to the scope-owned cleanup record, dropped after local
    /// worker and driver access ends.
    scope_resource: std::sync::Mutex<Option<ResourceRef<Arc<super::request_resource_owner::RequestResourceOwner>>>>,
    /// Attached one-request scope ended when local request access completes.
    request_scope: std::sync::Mutex<Option<RequestResourceScopeGuard>>,
    /// Weak back-reference lets aggregate release prune the runtime's strong
    /// registry.
    active_tasks: Option<Weak<Mutex<Vec<Arc<TaskState>>>>>,
    /// Weak shutdown gate shared with admission and provider-operation starts.
    admission_gate: Option<Weak<Mutex<AdmissionGate>>>,
    /// Weak pending admission handoff closed synchronously on request stop.
    admission_handoff:
        Mutex<Option<Weak<super::request_admission_handoff::RequestAdmissionHandoff>>>,
}

impl TaskState {
    /// Creates a queued task state and optional sampling bridge reference.
    ///
    /// # Parameters
    ///
    /// * `operation_id` - Stable identifier reported to the parent scope.
    /// * `bridge` - Weak bridge reference closed when this task is stopped.
    #[cfg(test)]
    pub(super) fn new(operation_id: u64, bridge: Option<Weak<AsyncSampleBridge>>) -> Self {
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
        operation_id: u64,
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
            scope_resource: std::sync::Mutex::new(None),
            request_scope: std::sync::Mutex::new(None),
            active_tasks,
            admission_gate,
            admission_handoff: Mutex::new(None),
        }
    }

    /// Linearizes provider-operation start against cancellation and shutdown.
    pub(crate) fn try_start_provider_operation(&self) -> bool {
        let gate = self.admission_gate.as_ref().and_then(Weak::upgrade);
        if self.admission_gate.is_some() && gate.is_none() {
            return false;
        }
        let gate = gate
            .as_ref()
            .map(|gate| gate.lock().unwrap_or_else(|error| error.into_inner()));
        if gate.as_ref().is_some_and(|gate| gate.cancelling || gate.faulted) {
            return false;
        }
        !self
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .stop_requested
    }

    /// Describes why a provider operation may no longer start.
    pub(super) fn provider_stop_reason(&self) -> &'static str {
        let gate = self.admission_gate.as_ref().and_then(Weak::upgrade);
        let Some(gate) = gate else {
            return "provider operation stopped before start";
        };
        let gate = gate.lock().unwrap_or_else(|error| error.into_inner());
        if gate.faulted {
            "provider operation stopped after worker fault"
        } else if gate.cancelling {
            "provider operation stopped after cancellation"
        } else {
            "provider operation stopped before start"
        }
    }

    /// Reports whether the worker should skip or stop this job.
    pub(crate) fn stop_requested(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .stop_requested
    }

    /// Reports whether worker and provider driver have stopped accessing local
    /// state.
    pub(super) fn access_complete(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .access_complete
    }

    /// Records that this request's input is owned by the worker queue.
    pub(super) fn mark_dispatched(&self) {
        self.inner.lock().unwrap_or_else(|error| error.into_inner()).dispatched = true;
    }

    /// Reports whether the worker queue can deliver this request's result.
    pub(super) fn was_dispatched(&self) -> bool {
        self.inner.lock().unwrap_or_else(|error| error.into_inner()).dispatched
    }

    /// Polls whether stop was requested, registering the caller under the
    /// same lock that protects the stop transition.
    ///
    /// # Parameters
    ///
    /// * `context` - Task context whose waker is registered while stop is
    ///   pending.
    pub(super) fn poll_stop_requested(&self, context: &mut Context<'_>) -> Poll<()> {
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if inner.stop_requested {
            Poll::Ready(())
        } else {
            register_waker(&mut inner.wakers, context.waker());
            Poll::Pending
        }
    }

    /// Waits for a stop request without missing a concurrent state change.
    pub(super) fn wait_stopped(&self) -> impl std::future::Future<Output = ()> + '_ {
        std::future::poll_fn(|context| self.poll_stop_requested(context))
    }

    /// Marks stop requested, wakes waiters, and closes pending provider reads.
    pub(super) fn request_stop(&self) {
        let wakers = {
            let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            inner.stop_requested = true;
            std::mem::take(&mut inner.wakers)
        };
        let handoff = self
            .admission_handoff
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .and_then(Weak::upgrade);
        if let Some(handoff) = handoff {
            handoff.close_for_stop();
        }
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

    /// Registers a pending handoff for immediate closure on stop.
    pub(super) fn attach_admission_handoff(
        &self,
        handoff: Weak<super::request_admission_handoff::RequestAdmissionHandoff>,
    ) {
        *self
            .admission_handoff
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(handoff.clone());
        if self.stop_requested()
            && let Some(handoff) = handoff.upgrade()
        {
            handoff.close_for_stop();
        }
    }

    /// Clears this request's stop notification after the handoff closes or is
    /// delivered to its child.
    pub(super) fn detach_admission_handoff(
        &self,
        handoff: &Arc<super::request_admission_handoff::RequestAdmissionHandoff>,
    ) {
        let mut attached = self
            .admission_handoff
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if attached
            .as_ref()
            .is_some_and(|current| Weak::ptr_eq(current, &Arc::downgrade(handoff)))
        {
            *attached = None;
        }
    }

    #[cfg(test)]
    pub(super) fn has_pending_admission_handoff(&self) -> bool {
        self.admission_handoff
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .is_some()
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
    pub(super) fn attach_owner(
        &self,
        owner: Arc<super::request_resource_owner::RequestResourceOwner>,
        scope_resource: Option<ResourceRef<Arc<super::request_resource_owner::RequestResourceOwner>>>,
    ) {
        *self.owner.lock().unwrap_or_else(|error| error.into_inner()) = Some(owner);
        *self.scope_resource.lock().unwrap_or_else(|error| error.into_inner()) = scope_resource;
    }

    /// Attaches the one-request cleanup scope to this task lifecycle.
    ///
    /// # Parameters
    ///
    /// * `scope` - Guard that closes the request scope with this task
    ///   lifecycle.
    pub(super) fn attach_request_scope(&self, scope: RequestResourceScopeGuard) {
        *self.request_scope.lock().unwrap_or_else(|error| error.into_inner()) = Some(scope);
    }

    /// Clones the request owner while it remains attached to this task.
    pub(super) fn request_resource_owner(&self) -> Option<Arc<super::request_resource_owner::RequestResourceOwner>> {
        self.owner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .cloned()
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

    /// Records a provider operation dropped before completion was confirmed.
    pub(super) fn mark_unconfirmed_provider_io(&self) {
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .unconfirmed_provider_io = true;
        if let Some(owner) = self.request_resource_owner() {
            owner.mark_unconfirmed_provider_io();
        }
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

    /// Stores the business result and wakes synchronous and asynchronous result
    /// observers. This notification does not mark worker completion or resource
    /// release; those lifecycle transitions remain independent.
    ///
    /// # Parameters
    ///
    /// * `result` - MIME inference result produced by the worker.
    /// * `business` - Worker outcome corresponding to the stored result.
    pub(super) fn finish_business(&self, result: MimeResult<Option<String>>, business: ChildOutcomeKind) {
        let wakers = {
            let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            if inner.business.is_none() {
                inner.business = Some(business);
                inner.result = Some(result);
            }
            std::mem::take(&mut inner.wakers)
        };
        self.released.notify_all();
        wake_all(wakers);
    }

    /// Records that the worker released job input and all capacity permits.
    pub(super) fn mark_released(&self) {
        let wakers = {
            let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            inner.released = true;
            inner.access_complete = true;
            std::mem::take(&mut inner.wakers)
        };
        wake_all(wakers);
        self.released.notify_all();
        let owner = self.owner.lock().unwrap_or_else(|error| error.into_inner()).take();
        let mut request_scope = self
            .request_scope
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(scope) = request_scope.as_mut() {
            scope.end();
        }
        let scope_resource = self
            .scope_resource
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        drop(owner);
        drop(scope_resource);
        if let Some(active_tasks) = self.active_tasks.as_ref().and_then(Weak::upgrade) {
            active_tasks
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .retain(|task| task.operation_id != self.operation_id);
        }
    }

    /// Drops the scope reference once worker and driver access is over, while
    /// keeping unknown provider I/O visible to runtime observers.
    pub(super) fn mark_access_complete(&self) {
        let wakers = {
            let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            inner.access_complete = true;
            std::mem::take(&mut inner.wakers)
        };
        wake_all(wakers);
        self.released.notify_all();
        let mut request_scope = self
            .request_scope
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(scope) = request_scope.as_mut() {
            scope.end();
        }
        let scope_resource = self
            .scope_resource
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        drop(scope_resource);
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
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;
    use std::task::Waker;

    use qubit_operation::ChildOutcomeKind;

    use super::TaskState;

    fn poison_mutex<T>(mutex: &Mutex<T>) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = mutex.lock().unwrap();
            panic!("poison mutex for recovery assertion");
        }));
    }

    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_pollable_stop_waiter_registers_and_wakes() {
        let state = Arc::new(TaskState::new(1, None));
        let wake_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake_counter));
        let mut context = std::task::Context::from_waker(&waker);
        let mut wait = Box::pin(state.wait_stopped());

        assert!(std::future::Future::poll(wait.as_mut(), &mut context).is_pending());
        assert_eq!(wake_counter.0.load(Ordering::SeqCst), 0);

        state.request_stop();

        assert_eq!(wake_counter.0.load(Ordering::SeqCst), 1);
        assert!(std::future::Future::poll(wait.as_mut(), &mut context).is_ready());
    }

    #[test]
    fn test_lifecycle_recovers_a_poisoned_state_lock() {
        let state = Arc::new(TaskState::new(2, None));
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
        state.finish_business(Ok(None), ChildOutcomeKind::Stopped);
        state.mark_released();
        let inner = state.inner.lock().unwrap_or_else(|error| error.into_inner());
        assert!(inner.started);
        assert!(inner.released);
        assert!(inner.business.is_some());
    }

    #[test]
    fn test_stop_closes_existing_and_late_attached_sampling_bridges() {
        let bridge = crate::internal::async_sample_bridge::AsyncSampleBridge::new(0);
        let state = TaskState::new(8, Some(Arc::downgrade(&bridge)));

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
        let state = Arc::new(TaskState::new_registered(9, None, Some(Arc::downgrade(&tasks)), None));
        tasks.lock().unwrap().push(Arc::clone(&state));

        state.mark_released();

        assert!(tasks.lock().unwrap_or_else(|error| error.into_inner()).is_empty());
        assert!(state.inner.lock().unwrap().released);
    }

    #[test]
    fn test_provider_start_rejects_closed_gate_and_expired_gate() {
        let gate = Arc::new(std::sync::Mutex::new(super::AdmissionGate::new()));
        gate.lock().unwrap().cancelling = true;
        let state = TaskState::new_registered(10, None, None, Some(Arc::downgrade(&gate)));
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
        let state = TaskState::new_registered(15, None, None, Some(Arc::downgrade(&poisoned_gate)));
        assert!(state.try_start_provider_operation());

        let expired = Arc::new(std::sync::Mutex::new(super::AdmissionGate::new()));
        let expired_weak = Arc::downgrade(&expired);
        drop(expired);
        let state = TaskState::new_registered(11, None, None, Some(expired_weak));
        assert!(!state.try_start_provider_operation());
    }

    #[test]
    fn test_unconfirmed_io_stays_visible_after_local_access_completes() {
        let tasks = Arc::new(Mutex::new(Vec::new()));
        let state = Arc::new(TaskState::new_registered(3, None, Some(Arc::downgrade(&tasks)), None));
        tasks.lock().unwrap().push(Arc::clone(&state));

        assert_eq!(state.provider_stop_reason(), "provider operation stopped before start");

        poison_mutex(&state.inner);
        poison_mutex(&state.owner);
        poison_mutex(&state.scope_resource);
        poison_mutex(&state.bridge);
        poison_mutex(&tasks);

        assert!(!state.access_complete());
        assert!(state.request_resource_owner().is_none());

        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        assert!(state.poll_stop_requested(&mut context).is_pending());
        state.mark_unconfirmed_provider_io();
        assert!(
            state
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .unconfirmed_provider_io
        );
        state.mark_access_complete();
        assert!(state.access_complete());
        assert!(
            state
                .inner
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .unconfirmed_provider_io
        );

        state.request_stop();
        state.mark_released();
        assert!(tasks.lock().unwrap_or_else(|error| error.into_inner()).is_empty());
    }

    #[test]
    fn test_provider_stop_reason_reports_gate_states_and_recovers_poison() {
        let unregistered = TaskState::new(2, None);
        assert_eq!(
            unregistered.provider_stop_reason(),
            "provider operation stopped before start"
        );
        let gate = Arc::new(std::sync::Mutex::new(super::AdmissionGate::new()));
        let state = TaskState::new_registered(4, None, None, Some(Arc::downgrade(&gate)));

        assert_eq!(state.provider_stop_reason(), "provider operation stopped before start");
        gate.lock().unwrap().cancelling = true;
        assert_eq!(
            state.provider_stop_reason(),
            "provider operation stopped after cancellation"
        );

        {
            let mut gate = gate.lock().unwrap();
            gate.cancelling = false;
            gate.faulted = true;
        }
        assert_eq!(
            state.provider_stop_reason(),
            "provider operation stopped after worker fault"
        );

        poison_mutex(&gate);
        assert_eq!(
            state.provider_stop_reason(),
            "provider operation stopped after worker fault"
        );
    }

    #[test]
    fn test_worker_completion_recovers_a_poisoned_lifecycle_lock() {
        let state = Arc::new(TaskState::new(16, None));
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
        let state = Arc::new(TaskState::new_registered(17, None, None, Some(Arc::downgrade(&gate))));
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
        let state = Arc::new(TaskState::new(12, None));
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
        let state = Arc::new(TaskState::new_registered(13, None, Some(Arc::downgrade(&tasks)), None));
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
    fn test_lifecycle_recovers_poisoned_locks_when_attaching_resources() {
        let state = Arc::new(TaskState::new(18, None));
        let poisoned_bridge = Arc::clone(&state);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned_bridge.bridge.lock().unwrap();
                panic!("poison bridge lock before late attachment");
            })
            .join()
            .is_err()
        );
        let poisoned_owner = Arc::clone(&state);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned_owner.owner.lock().unwrap();
                panic!("poison owner lock before attachment");
            })
            .join()
            .is_err()
        );
        let poisoned_inner = Arc::clone(&state);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned_inner.inner.lock().unwrap();
                panic!("poison lifecycle lock before provider completion");
            })
            .join()
            .is_err()
        );

        state.mark_unconfirmed_provider_io();
        state.request_stop();

        let bridge = crate::internal::async_sample_bridge::AsyncSampleBridge::new(0);
        state.attach_bridge(Arc::downgrade(&bridge));
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        let mut closed = std::pin::pin!(bridge.closed());
        assert!(std::future::Future::poll(closed.as_mut(), &mut context).is_ready());

        let active = Arc::new(AtomicUsize::new(0));
        let service = crate::managed_runtime::service_permit::ServicePermit::try_acquire(&active, 1).unwrap();
        let budget = Arc::new(crate::managed_runtime::byte_budget::ByteBudget::new(4));
        let bytes = budget.try_acquire(4).unwrap();
        let owner = Arc::new(crate::managed_runtime::request_admission::RequestResourceOwner::new(
            Arc::downgrade(&state),
            service,
            bytes,
            false,
        ));
        state.attach_owner(Arc::clone(&owner), None);
        state.mark_driver_done();
        owner.worker_done();

        let inner = state.inner.lock().unwrap_or_else(|error| error.into_inner());
        assert!(inner.unconfirmed_provider_io);
        assert!(inner.released);
        assert_eq!(0, active.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(0, budget.used());
    }

    #[test]
    fn test_business_terminal_fact_is_write_once() {
        let state = TaskState::new(5, None);
        state.finish_business(Ok(Some("first".to_owned())), ChildOutcomeKind::Succeeded);
        state.finish_business(
            Err(qubit_mime::MimeError::detector_backend("test", "later")),
            ChildOutcomeKind::Failed,
        );

        let inner = state.inner.lock().unwrap();
        assert_eq!(inner.business, Some(ChildOutcomeKind::Succeeded));
        assert!(matches!(inner.result.as_ref(), Some(Ok(Some(value))) if value == "first"));
    }

    #[test]
    fn test_requested_stop_classifies_the_terminal_business_fact_as_stopped() {
        let state = TaskState::new(6, None);
        state.request_stop();
        state.finish_business(
            Err(qubit_mime::MimeError::detector_backend(
                "test",
                "stopped before execution",
            )),
            ChildOutcomeKind::Stopped,
        );

        let inner = state.inner.lock().unwrap();
        assert_eq!(inner.business, Some(ChildOutcomeKind::Stopped));
        assert!(matches!(inner.result.as_ref(), Some(Err(_))));
    }

    #[test]
    fn test_result_winning_stop_race_keeps_matching_business_outcome() {
        let state = TaskState::new(7, None);
        state.request_stop();
        state.finish_business(Ok(Some("text/plain".to_owned())), ChildOutcomeKind::Succeeded);

        let inner = state.inner.lock().unwrap();
        assert_eq!(inner.business, Some(ChildOutcomeKind::Succeeded));
        assert!(matches!(inner.result.as_ref(), Some(Ok(Some(value))) if value == "text/plain"));
    }
}
