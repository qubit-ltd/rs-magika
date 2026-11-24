// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Transfers a request admission after its child operation body starts.

use std::future::Future;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use qubit_mime::MimeError;
use qubit_operation::OperationScope;

use super::request_admission::RequestAdmission;
use super::worker_input::WorkerInput;
mod internal;

pub(crate) use internal::BodyStartOr;
pub(crate) use internal::HandoffCloseReason;
use internal::HandoffState;
pub(crate) use internal::RequestAdmissionHandoffGuard;
use internal::RequestAdmissionPayload;

/// Coordinates request admission between a caller and a submitted child body.
///
/// For a managed child, the caller registers the request against its parent
/// scope after the child body starts, then transfers the admission or
/// registration error to the waiting child. A root child claims its pending
/// payload under the same lock used to observe closure. If either side stops
/// waiting, the handoff closes and releases any payload it still owns.
pub(crate) struct RequestAdmissionHandoff {
    /// Admission transfer and body-start notifications.
    state: Mutex<HandoffState>,
    /// Request state notified until this handoff closes or reaches its child.
    task_state: Weak<super::task_state::TaskState>,
}

impl RequestAdmissionHandoff {
    /// Creates a handoff and the caller guard that owns its unfinished state.
    ///
    /// # Parameters
    ///
    /// * `admission` - Request admission retained until it can be registered
    ///   with the parent scope.
    ///
    /// # Returns
    ///
    /// The shared handoff and a guard that closes it if the caller abandons
    /// the registration step.
    pub(crate) fn new(admission: RequestAdmission) -> (Arc<Self>, RequestAdmissionHandoffGuard) {
        Self::new_with_input(admission, None)
    }

    /// Creates a handoff that also owns worker input until registration.
    ///
    /// # Parameters
    ///
    /// * `admission` - Request capacity retained until registration succeeds.
    /// * `input` - Optional worker input released with the admission if the
    ///   handoff closes before transfer.
    ///
    /// # Returns
    ///
    /// The shared handoff and its caller guard.
    pub(crate) fn new_with_input(
        admission: RequestAdmission,
        input: Option<WorkerInput>,
    ) -> (Arc<Self>, RequestAdmissionHandoffGuard) {
        let task_state = admission.task_state();
        let handoff = Arc::new(Self {
            state: Mutex::new(HandoffState {
                caller_payload: Some(RequestAdmissionPayload { admission, input }),
                child_admission: None,
                body_started: false,
                closed: false,
                close_reason: None,
                caller_waker: None,
                child_waker: None,
                #[cfg(test)]
                child_waiting: false,
            }),
            task_state: Arc::downgrade(&task_state),
        });
        let guard = RequestAdmissionHandoffGuard {
            handoff: Arc::clone(&handoff),
            active: true,
        };
        task_state.attach_admission_handoff(Arc::downgrade(&handoff));
        (handoff, guard)
    }

    /// Closes this pending handoff when its request is stopped.
    pub(super) fn close_for_stop(self: &Arc<Self>) {
        let reason = "request admission stopped";
        self.close_with_reason(HandoffCloseReason::RequestStopped(reason));
    }

    #[cfg(test)]
    pub(crate) fn child_waiting_for_admission(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .child_waiting
    }

    /// Notifies the caller that the accepted child body has begun.
    pub(crate) fn body_started(&self) {
        let caller_waker = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closed {
                return;
            }
            state.body_started = true;
            state.caller_waker.take()
        };
        if let Some(waker) = caller_waker {
            waker.wake();
        }
    }

    /// Registers the request against the borrowed parent after child body
    /// start.
    ///
    /// An already-ready operation outcome takes priority. Otherwise this waits
    /// for the body-start signal, then registers the request and transfers the
    /// admission or its registration error to the waiting child.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Future that observes the submitted operation's outcome.
    /// * `T` - Submitted operation outcome type.
    ///
    /// # Parameters
    ///
    /// * `parent` - Parent scope that owns the request resource registration.
    /// * `guard` - Caller-owned guard that closes an unfinished handoff.
    /// * `outcome` - Future polled while waiting for the child body to start.
    ///
    /// # Returns
    ///
    /// `Outcome(T)` if the operation finishes first, `BodyStarted` after the
    /// admission or registration error is handed to the child, or
    /// `Closed(reason)` if closure wins. `BodyStarted` does not imply that
    /// parent registration succeeded.
    pub(crate) async fn register_on_parent<F, T>(
        self: &Arc<Self>,
        parent: &OperationScope,
        guard: &mut RequestAdmissionHandoffGuard,
        outcome: &mut F,
    ) -> BodyStartOr<T>
    where
        F: Future<Output = T> + Unpin,
    {
        match self.wait_for_body_start_or(outcome).await {
            BodyStartOr::Outcome(outcome) => {
                self.close();
                guard.finish();
                BodyStartOr::Outcome(outcome)
            }
            BodyStartOr::BodyStarted => {
                let Some(mut payload) = self.take_caller_payload() else {
                    guard.finish();
                    return BodyStartOr::Closed(self.closed_reason());
                };
                let result = match payload.admission.retain_in_scope(parent) {
                    Ok(()) => Ok((payload.admission, payload.input.take())),
                    Err(error) => {
                        drop(payload.input.take());
                        drop(payload.admission);
                        Err(error)
                    }
                };
                if !self.provide_to_child(result) {
                    guard.finish();
                    return BodyStartOr::Closed(self.closed_reason());
                }
                guard.finish();
                BodyStartOr::BodyStarted
            }
            BodyStartOr::Closed(_) => {
                guard.finish();
                BodyStartOr::Closed(self.closed_reason())
            }
        }
    }

    /// Waits for an unmanaged child to claim its payload or finish first.
    ///
    /// An outcome that wins closes the handoff and releases any unclaimed
    /// payload. A body claim disarms the caller guard; a prior close is
    /// returned with its recorded reason.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Future that resolves to the unmanaged child outcome.
    /// * `T` - Unmanaged child outcome type.
    ///
    /// # Parameters
    ///
    /// * `guard` - Caller guard that is disarmed when this wait completes.
    /// * `outcome` - Unmanaged child outcome polled alongside body start.
    pub(crate) async fn wait_for_root_or<F, T>(
        self: &Arc<Self>,
        guard: &mut RequestAdmissionHandoffGuard,
        outcome: &mut F,
    ) -> BodyStartOr<T>
    where
        F: Future<Output = T> + Unpin,
    {
        let result = self.wait_for_body_start_or(outcome).await;
        if matches!(result, BodyStartOr::Outcome(_)) {
            self.close();
        }
        guard.finish();
        result
    }

    /// Claims an unmanaged request payload before the submitted body starts
    /// doing request work, or observes that cancellation closed the handoff.
    ///
    /// The payload claim and close check share the handoff lock, so a stop
    /// cannot release capacity while the body is beginning to use it.
    ///
    /// # Returns
    ///
    /// The admission and optional input when claimed, or `None` if already
    /// claimed or closed.
    pub(crate) fn take_for_root(self: &Arc<Self>) -> Option<(RequestAdmission, Option<WorkerInput>)> {
        let (payload, caller_waker) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closed {
                return None;
            }
            let mut payload = state.caller_payload.take()?;
            state.body_started = true;
            ((payload.admission, payload.input.take()), state.caller_waker.take())
        };
        if let Some(waker) = caller_waker {
            waker.wake();
        }
        self.detach_task_state_notification();
        Some(payload)
    }

    /// Waits for body start or polls the operation outcome, without blocking.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Future that observes the submitted operation's outcome.
    /// * `T` - Submitted operation outcome type.
    ///
    /// # Parameters
    ///
    /// * `outcome` - Future polled until the child body starts or the operation
    ///   completes.
    ///
    /// # Returns
    ///
    /// `Outcome(T)` when the outcome is ready, `BodyStarted` when the child
    /// crossed the start/claim boundary, or `Closed(reason)` when the handoff
    /// closed first. The outcome is polled first, so a ready outcome wins over
    /// a simultaneous start or close. The underlying poll uses `Some(true)`,
    /// `Some(false)`, and `None` for started, closed, and pending respectively.
    pub(crate) async fn wait_for_body_start_or<F, T>(&self, outcome: &mut F) -> BodyStartOr<T>
    where
        F: Future<Output = T> + Unpin,
    {
        poll_fn(|context| {
            if let Poll::Ready(outcome) = Pin::new(&mut *outcome).poll(context) {
                return Poll::Ready(BodyStartOr::Outcome(outcome));
            }
            match self.poll_body_started(context) {
                Some(true) => Poll::Ready(BodyStartOr::BodyStarted),
                Some(false) => Poll::Ready(BodyStartOr::Closed(self.closed_reason())),
                None => Poll::Pending,
            }
        })
        .await
    }

    /// Polls body start and registers the caller waker while holding the state
    /// lock.
    ///
    /// # Parameters
    ///
    /// * `context` - Poll context whose waker is notified when body start is
    ///   reported.
    ///
    /// # Returns
    ///
    /// `Some(true)` when the child body has started or claimed the root
    /// payload, `Some(false)` when the handoff is closed, and `None` while it
    /// remains pending.
    fn poll_body_started(&self, context: &mut Context<'_>) -> Option<bool> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.body_started {
            return Some(true);
        }
        if state.closed {
            return Some(false);
        }
        register_waker(&mut state.caller_waker, context.waker());
        None
    }

    /// Waits for the parent to register this request, or for the child to stop.
    ///
    /// # Parameters
    ///
    /// * `child_scope` - Child scope whose stop signal cancels this wait.
    ///
    /// # Returns
    ///
    /// The registered admission or registration error, or `None` if the
    /// handoff closes before the child receives it.
    pub(crate) async fn wait_for_child_admission(
        self: &Arc<Self>,
        child_scope: &OperationScope,
    ) -> Option<Result<(RequestAdmission, Option<WorkerInput>), MimeError>> {
        let mut stopped = Box::pin(child_scope.context().stop_requested());
        poll_fn(|context| {
            if stopped.as_mut().poll(context).is_ready() {
                self.close_for_stop();
                return Poll::Ready(None);
            }
            let (admission, closed) = {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                if let Some(admission) = state.child_admission.take() {
                    (Some(admission), false)
                } else if state.closed {
                    (None, true)
                } else {
                    register_waker(&mut state.child_waker, context.waker());
                    #[cfg(test)]
                    {
                        state.child_waiting = true;
                    }
                    (None, false)
                }
            };
            if let Some(admission) = admission {
                self.detach_task_state_notification();
                return Poll::Ready(Some(admission));
            }
            if closed {
                return Poll::Ready(None);
            }
            Poll::Pending
        })
        .await
    }

    /// Takes the unregistered request admission for the parent registrar.
    ///
    /// # Returns
    ///
    /// The admission if the caller still owns it.
    fn take_caller_payload(&self) -> Option<RequestAdmissionPayload> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .caller_payload
            .take()
    }

    /// Returns the first recorded reason for closing this handoff.
    ///
    /// # Returns
    ///
    /// The first close reason, or `Abandoned` when no reason has been set.
    pub(crate) fn closed_reason(&self) -> HandoffCloseReason {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .close_reason
            .unwrap_or(HandoffCloseReason::Abandoned)
    }

    #[cfg(test)]
    fn take_caller_admission(&self) -> Option<RequestAdmission> {
        let payload = self.take_caller_payload()?;
        drop(payload.input);
        Some(payload.admission)
    }

    /// Provides the registered admission or its failure to the waiting child.
    ///
    /// # Parameters
    ///
    /// * `admission` - Parent-registered admission or the registration error.
    ///
    /// # Returns
    ///
    /// `true` if the child received the value, or `false` if the handoff was
    /// already closed.
    fn provide_to_child(&self, admission: Result<(RequestAdmission, Option<WorkerInput>), MimeError>) -> bool {
        let (accepted, child_waker, rejected) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closed || state.child_admission.is_some() {
                (false, None, Some(admission))
            } else {
                state.child_admission = Some(admission);
                (true, state.child_waker.take(), None)
            }
        };
        if let Some(Ok((admission, input))) = rejected {
            drop(input);
            drop(admission);
        } else {
            drop(rejected);
        }
        if let Some(waker) = child_waker {
            waker.wake();
        }
        accepted
    }

    /// Closes the gate, releases untransferred admission, and wakes both sides.
    fn close(self: &Arc<Self>) {
        self.close_with_reason(HandoffCloseReason::Abandoned);
    }

    /// Closes once, releases untransferred state, and wakes both participants.
    ///
    /// # Parameters
    ///
    /// * `reason` - Reason recorded if no earlier close has already won.
    fn close_with_reason(self: &Arc<Self>, reason: HandoffCloseReason) {
        let (caller_payload, child_admission, caller_waker, child_waker) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.closed = true;
            state.close_reason.get_or_insert(reason);
            (
                state.caller_payload.take(),
                state.child_admission.take(),
                state.caller_waker.take(),
                state.child_waker.take(),
            )
        };
        if let Some(payload) = caller_payload {
            drop(payload.input);
            drop(payload.admission);
        }
        match child_admission {
            Some(Ok((admission, input))) => {
                drop(input);
                drop(admission);
            }
            other => drop(other),
        }
        self.detach_task_state_notification();
        if let Some(waker) = caller_waker {
            waker.wake();
        }
        if let Some(waker) = child_waker {
            waker.wake();
        }
    }

    /// Removes the weak stop callback when the handoff is no longer pending.
    fn detach_task_state_notification(self: &Arc<Self>) {
        if let Some(task_state) = self.task_state.upgrade() {
            task_state.detach_admission_handoff(self);
        }
    }
}

/// Keeps the latest task waker without storing duplicate registrations.
///
/// # Parameters
///
/// * `slot` - Stored waker slot updated for the current task.
/// * `waker` - Current task waker to store when it differs from the existing
///   registration.
fn register_waker(slot: &mut Option<Waker>, waker: &Waker) {
    if slot.as_ref().is_none_or(|registered| !registered.will_wake(waker)) {
        *slot = Some(waker.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use qubit_operation::ManagerOptions;
    use qubit_operation::OperationManager;
    use qubit_operation::OperationOptions;
    use qubit_operation::OperationOutcome;
    use qubit_operation::ScopeLimits;

    use super::super::byte_budget::ByteBudget;
    use super::super::request_admission::RequestAdmission;
    use super::super::service_permit::ServicePermit;
    use super::super::task_state::TaskState;
    use super::BodyStartOr;
    use super::HandoffCloseReason;
    use super::RequestAdmissionHandoff;
    use crate::managed_runtime::worker_input::WorkerBytes;
    use crate::managed_runtime::worker_input::WorkerInput;

    #[test]
    fn test_child_outcome_before_body_start_releases_admission() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let active_requests = Arc::new(AtomicUsize::new(0));
        let byte_budget = Arc::new(ByteBudget::new(8));
        let active_requests_for_parent = Arc::clone(&active_requests);
        let byte_budget_for_parent = Arc::clone(&byte_budget);
        let mut operation = manager
            .as_async()
            .try_submit(OperationOptions::default(), move |scope| async move {
                let admission = RequestAdmission::new(
                    Arc::new(TaskState::new(800, None)),
                    ServicePermit::try_acquire(&active_requests_for_parent, 1).unwrap(),
                    byte_budget_for_parent.try_acquire(8).unwrap(),
                );
                let (handoff, mut guard) = RequestAdmissionHandoff::new(admission);
                let mut outcome = std::future::ready(());
                assert!(matches!(
                    handoff.register_on_parent(&scope, &mut guard, &mut outcome).await,
                    BodyStartOr::Outcome(())
                ));
                assert_eq!(active_requests_for_parent.load(Ordering::SeqCst), 0);
                assert!(byte_budget_for_parent.try_acquire(8).is_some());
                Ok::<_, ()>(())
            })
            .unwrap();

        assert!(matches!(
            block_on(operation.wait_outcome()).unwrap(),
            OperationOutcome::Succeeded(())
        ));
        assert!(block_on(operation.wait_settlement()).all_released());
    }

    #[test]
    fn test_parent_scope_commit_rejection_drops_input_before_admission() {
        struct DropProbe {
            active: Arc<AtomicUsize>,
            budget: Arc<ByteBudget>,
            state: Arc<TaskState>,
            sender: std::sync::mpsc::Sender<bool>,
        }
        impl Drop for DropProbe {
            fn drop(&mut self) {
                let held = self.active.load(Ordering::SeqCst) == 1
                    && self.budget.try_acquire(8).is_none()
                    && !self.state.wait_released_blocking(std::time::Duration::ZERO);
                self.sender.send(held).unwrap();
            }
        }
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let active_requests = Arc::new(AtomicUsize::new(0));
        let byte_budget = Arc::new(ByteBudget::new(8));
        let active_for_parent = Arc::clone(&active_requests);
        let budget_for_parent = Arc::clone(&byte_budget);
        let (drop_facts_tx, drop_facts_rx) = std::sync::mpsc::channel();
        let (state_tx, state_rx) = std::sync::mpsc::channel();
        let mut parent = manager
            .as_async()
            .try_submit(
                OperationOptions {
                    limits: ScopeLimits {
                        resources: 1,
                        ..ScopeLimits::default()
                    },
                    ..OperationOptions::default()
                },
                move |scope| async move {
                    let _held = scope.manage(()).unwrap();
                    let state = Arc::new(TaskState::new(8991, None));
                    let admission = RequestAdmission::new(
                        Arc::clone(&state),
                        ServicePermit::try_acquire(&active_for_parent, 1).unwrap(),
                        budget_for_parent.try_acquire(8).unwrap(),
                    );
                    let payload = WorkerInput::Bytes(WorkerBytes::with_drop_probe(
                        vec![b'x'].into_boxed_slice(),
                        Box::new(DropProbe {
                            active: Arc::clone(&active_for_parent),
                            budget: Arc::clone(&budget_for_parent),
                            state: Arc::clone(&state),
                            sender: drop_facts_tx,
                        }),
                    ));
                    let (handoff, mut guard) = RequestAdmissionHandoff::new_with_input(admission, Some(payload));
                    let child_handoff = Arc::clone(&handoff);
                    let mut child = scope
                        .manager()
                        .as_async()
                        .try_submit(OperationOptions::default(), move |child_scope| async move {
                            child_handoff.body_started();
                            let result = child_handoff
                                .wait_for_child_admission(&child_scope)
                                .await
                                .expect("handoff provides the registration error");
                            assert!(result.is_err());
                            Ok::<_, ()>(())
                        })
                        .unwrap();
                    let mut outcome = Box::pin(child.wait_outcome());
                    assert!(matches!(
                        handoff.register_on_parent(&scope, &mut guard, &mut outcome).await,
                        super::BodyStartOr::BodyStarted
                    ));
                    assert!(matches!(outcome.await.unwrap(), OperationOutcome::Succeeded(())));
                    state_tx.send(state).unwrap();
                    Ok::<_, ()>(())
                },
            )
            .unwrap();

        assert!(matches!(
            block_on(parent.wait_outcome()).unwrap(),
            OperationOutcome::Succeeded(())
        ));
        assert!(drop_facts_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap());
        assert!(
            state_rx
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap()
                .wait_released_blocking(std::time::Duration::ZERO)
        );
        assert_eq!(0, active_requests.load(Ordering::SeqCst));
        assert!(byte_budget.try_acquire(8).is_some());
        assert!(block_on(parent.wait_settlement()).all_released());
    }

    #[test]
    fn test_body_start_wait_returns_closed_when_stop_closes_handoff() {
        let active_requests = Arc::new(AtomicUsize::new(0));
        let byte_budget = Arc::new(ByteBudget::new(8));
        let admission = RequestAdmission::new(
            Arc::new(TaskState::new(899, None)),
            ServicePermit::try_acquire(&active_requests, 1).unwrap(),
            byte_budget.try_acquire(8).unwrap(),
        );
        let state = admission.task_state();
        let (handoff, _guard) = RequestAdmissionHandoff::new(admission);
        let mut outcome = std::future::pending::<()>();

        state.request_stop();

        assert!(matches!(
            block_on(handoff.wait_for_body_start_or(&mut outcome)),
            BodyStartOr::Closed(HandoffCloseReason::RequestStopped(_))
        ));
        assert_eq!(0, active_requests.load(Ordering::SeqCst));
        assert!(byte_budget.try_acquire(8).is_some());
    }

    #[test]
    fn test_dropping_caller_closes_waiting_child_and_releases_admission() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let active_requests = Arc::new(AtomicUsize::new(0));
        let byte_budget = Arc::new(ByteBudget::new(8));
        let active_requests_for_body = Arc::clone(&active_requests);
        let byte_budget_for_body = Arc::clone(&byte_budget);
        let mut operation = manager
            .as_async()
            .try_submit(OperationOptions::default(), move |scope| async move {
                let admission = RequestAdmission::new(
                    Arc::new(TaskState::new(801, None)),
                    ServicePermit::try_acquire(&active_requests_for_body, 1).unwrap(),
                    byte_budget_for_body.try_acquire(8).unwrap(),
                );
                let (handoff, guard) = RequestAdmissionHandoff::new(admission);
                let body_handoff = Arc::clone(&handoff);
                let mut child = scope
                    .manager()
                    .as_async()
                    .try_submit(OperationOptions::default(), move |child_scope| async move {
                        body_handoff.body_started();
                        assert!(body_handoff.wait_for_child_admission(&child_scope).await.is_none());
                        Ok::<_, ()>(())
                    })
                    .unwrap();
                let mut outcome = Box::pin(child.wait_outcome());
                assert!(matches!(
                    handoff.wait_for_body_start_or(&mut outcome).await,
                    BodyStartOr::BodyStarted
                ));

                drop(guard);
                assert!(matches!(outcome.await.unwrap(), OperationOutcome::Succeeded(())));
                assert_eq!(active_requests_for_body.load(Ordering::SeqCst), 0);
                assert!(byte_budget_for_body.try_acquire(8).is_some());
                Ok::<_, ()>(())
            })
            .unwrap();

        assert!(matches!(
            block_on(operation.wait_outcome()).unwrap(),
            OperationOutcome::Succeeded(())
        ));
        assert!(block_on(operation.wait_settlement()).all_released());
    }

    #[test]
    fn test_runtime_stop_closes_handoff_without_caller_or_child_polling() {
        struct ReleaseObserver {
            state: Arc<TaskState>,
            released: Arc<std::sync::atomic::AtomicBool>,
        }
        impl Drop for ReleaseObserver {
            fn drop(&mut self) {
                self.released.store(
                    self.state.wait_released_blocking(std::time::Duration::ZERO),
                    Ordering::Release,
                );
            }
        }

        struct WakeAfterRelease {
            active_requests: Arc<AtomicUsize>,
            observed_active: Arc<AtomicUsize>,
        }

        impl std::task::Wake for WakeAfterRelease {
            fn wake(self: Arc<Self>) {
                self.observed_active
                    .store(self.active_requests.load(Ordering::SeqCst), Ordering::SeqCst);
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.observed_active
                    .store(self.active_requests.load(Ordering::SeqCst), Ordering::SeqCst);
            }
        }

        let active_requests = Arc::new(AtomicUsize::new(0));
        let byte_budget = Arc::new(ByteBudget::new(8));
        let task_state = Arc::new(TaskState::new(802, None));
        let admission = RequestAdmission::new(
            Arc::clone(&task_state),
            ServicePermit::try_acquire(&active_requests, 1).unwrap(),
            byte_budget.try_acquire(8).unwrap(),
        );
        let state = admission.task_state();
        let released_before_input_drop = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let input =
            super::super::worker_input::WorkerInput::Bytes(super::super::worker_input::WorkerBytes::with_drop_probe(
                Box::new([]),
                Box::new(ReleaseObserver {
                    state: Arc::clone(&task_state),
                    released: Arc::clone(&released_before_input_drop),
                }),
            ));
        let (handoff, _guard) = RequestAdmissionHandoff::new_with_input(admission, Some(input));
        let observed_active = Arc::new(AtomicUsize::new(usize::MAX));
        let waker = std::task::Waker::from(Arc::new(WakeAfterRelease {
            active_requests: Arc::clone(&active_requests),
            observed_active: Arc::clone(&observed_active),
        }));
        let mut context = std::task::Context::from_waker(&waker);
        let mut stopped = Box::pin(state.wait_stopped());
        assert!(std::future::Future::poll(stopped.as_mut(), &mut context).is_pending());

        state.request_stop();

        assert_eq!(
            Some(HandoffCloseReason::RequestStopped(
                "provider operation stopped before start"
            )),
            handoff.state.lock().unwrap().close_reason
        );

        assert_eq!(
            active_requests.load(Ordering::SeqCst),
            0,
            "runtime stop must return an admission that has not reached the child"
        );
        assert!(
            byte_budget.try_acquire(8).is_some(),
            "runtime stop must return the admission byte reservation"
        );
        assert_eq!(0, observed_active.load(Ordering::SeqCst));
        assert!(!released_before_input_drop.load(Ordering::Acquire));
    }

    #[test]
    fn test_runtime_stop_during_registration_leaves_local_admission_for_registrar() {
        let active_requests = Arc::new(AtomicUsize::new(0));
        let byte_budget = Arc::new(ByteBudget::new(8));
        let admission = RequestAdmission::new(
            Arc::new(TaskState::new(803, None)),
            ServicePermit::try_acquire(&active_requests, 1).unwrap(),
            byte_budget.try_acquire(8).unwrap(),
        );
        let state = admission.task_state();
        let (handoff, _guard) = RequestAdmissionHandoff::new(admission);
        let admission = handoff.take_caller_admission().unwrap();

        state.request_stop();

        assert_eq!(1, active_requests.load(Ordering::SeqCst));
        assert!(byte_budget.try_acquire(8).is_none());
        assert!(!handoff.provide_to_child(Ok((admission, None))));
        assert_eq!(0, active_requests.load(Ordering::SeqCst));
        assert!(byte_budget.try_acquire(8).is_some());
    }

    #[test]
    fn test_stop_after_atomic_payload_take_drops_input_before_admission() {
        struct ReleaseObserver {
            state: Arc<TaskState>,
            released: Arc<std::sync::atomic::AtomicBool>,
        }
        impl Drop for ReleaseObserver {
            fn drop(&mut self) {
                self.released.store(
                    self.state.wait_released_blocking(std::time::Duration::ZERO),
                    Ordering::Release,
                );
            }
        }

        let active_requests = Arc::new(AtomicUsize::new(0));
        let byte_budget = Arc::new(ByteBudget::new(8));
        let state = Arc::new(TaskState::new(805, None));
        let admission = RequestAdmission::new(
            Arc::clone(&state),
            ServicePermit::try_acquire(&active_requests, 1).unwrap(),
            byte_budget.try_acquire(8).unwrap(),
        );
        let released_before_input_drop = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let input =
            super::super::worker_input::WorkerInput::Bytes(super::super::worker_input::WorkerBytes::with_drop_probe(
                Box::new([]),
                Box::new(ReleaseObserver {
                    state: Arc::clone(&state),
                    released: Arc::clone(&released_before_input_drop),
                }),
            ));
        let (handoff, _guard) = RequestAdmissionHandoff::new_with_input(admission, Some(input));

        let payload = handoff.take_caller_payload().unwrap();
        state.request_stop();
        assert!(!handoff.provide_to_child(Ok((payload.admission, payload.input))));

        assert!(!released_before_input_drop.load(Ordering::Acquire));
        assert_eq!(0, active_requests.load(Ordering::SeqCst));
        assert!(byte_budget.try_acquire(8).is_some());
    }

    #[test]
    fn test_runtime_stop_closes_child_waiting_for_registration_while_parent_is_unpolled() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let active_requests = Arc::new(AtomicUsize::new(0));
        let byte_budget = Arc::new(ByteBudget::new(8));
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (state_tx, state_rx) = std::sync::mpsc::sync_channel(1);
        let active_for_parent = Arc::clone(&active_requests);
        let budget_for_parent = Arc::clone(&byte_budget);
        let mut parent = manager
            .as_async()
            .try_submit(OperationOptions::default(), move |scope| async move {
                let state = Arc::new(TaskState::new(804, None));
                let admission = RequestAdmission::new(
                    Arc::clone(&state),
                    ServicePermit::try_acquire(&active_for_parent, 1).unwrap(),
                    budget_for_parent.try_acquire(8).unwrap(),
                );
                let (handoff, guard) = RequestAdmissionHandoff::new(admission);
                let body_handoff = Arc::clone(&handoff);
                let mut child = scope
                    .manager()
                    .as_async()
                    .try_submit(OperationOptions::default(), move |child_scope| async move {
                        body_handoff.body_started();
                        started_tx.send(()).unwrap();
                        assert!(body_handoff.wait_for_child_admission(&child_scope).await.is_none());
                        Ok::<_, ()>(())
                    })
                    .unwrap();
                state_tx.send(state).unwrap();
                let _guard = guard;
                assert!(matches!(
                    child.wait_outcome().await.unwrap(),
                    OperationOutcome::Succeeded(())
                ));
                Ok::<_, ()>(())
            })
            .unwrap();

        let state = state_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        started_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        assert_eq!(1, active_requests.load(Ordering::SeqCst));

        state.request_stop();

        assert_eq!(0, active_requests.load(Ordering::SeqCst));
        assert!(byte_budget.try_acquire(8).is_some());
        assert!(matches!(
            block_on(parent.wait_outcome()).unwrap(),
            OperationOutcome::Succeeded(())
        ));
    }

    #[test]
    fn test_child_receiving_admission_detaches_request_stop_notification() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let active_requests = Arc::new(AtomicUsize::new(0));
        let byte_budget = Arc::new(ByteBudget::new(8));
        let active_for_parent = Arc::clone(&active_requests);
        let budget_for_parent = Arc::clone(&byte_budget);
        let mut parent = manager
            .as_async()
            .try_submit(OperationOptions::default(), move |scope| async move {
                let state = Arc::new(TaskState::new(805, None));
                let admission = RequestAdmission::new(
                    Arc::clone(&state),
                    ServicePermit::try_acquire(&active_for_parent, 1).unwrap(),
                    budget_for_parent.try_acquire(8).unwrap(),
                );
                let (handoff, mut guard) = RequestAdmissionHandoff::new(admission);
                let body_handoff = Arc::clone(&handoff);
                let body_state = Arc::clone(&state);
                let mut child = scope
                    .manager()
                    .as_async()
                    .try_submit(OperationOptions::default(), move |child_scope| async move {
                        body_handoff.body_started();
                        let admission = body_handoff
                            .wait_for_child_admission(&child_scope)
                            .await
                            .expect("parent should provide the registered admission")
                            .expect("scope registration should succeed");
                        assert!(!body_state.has_pending_admission_handoff());
                        drop(admission);
                        Ok::<_, ()>(())
                    })
                    .unwrap();
                let mut outcome = Box::pin(child.wait_outcome());
                assert!(matches!(
                    handoff.register_on_parent(&scope, &mut guard, &mut outcome).await,
                    BodyStartOr::BodyStarted
                ));
                assert!(matches!(outcome.await.unwrap(), OperationOutcome::Succeeded(())));
                Ok::<_, ()>(())
            })
            .unwrap();

        assert!(matches!(
            block_on(parent.wait_outcome()).unwrap(),
            OperationOutcome::Succeeded(())
        ));
        assert_eq!(0, active_requests.load(Ordering::SeqCst));
        assert!(byte_budget.try_acquire(8).is_some());
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        let waker = std::task::Waker::noop();
        let mut context = std::task::Context::from_waker(waker);
        let mut future = Box::pin(future);
        loop {
            match std::future::Future::poll(future.as_mut(), &mut context) {
                std::task::Poll::Ready(output) => return output,
                std::task::Poll::Pending => std::thread::yield_now(),
            }
        }
    }
}
