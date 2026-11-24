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
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use qubit_mime::MimeError;
use qubit_operation::OperationScope;

use super::request_admission::RequestAdmission;

/// Shared admission state between the caller and a submitted child body.
pub(crate) struct RequestAdmissionHandoff {
    /// Admission transfer and body-start notifications.
    state: Mutex<HandoffState>,
}

/// Mutable state guarded while admissions and wakers are exchanged.
struct HandoffState {
    /// Request whose scope registration is still owned by the caller.
    caller_admission: Option<RequestAdmission>,
    /// Admission or registration failure handed to the child body.
    child_admission: Option<Result<RequestAdmission, MimeError>>,
    /// Whether the submitted child body has entered its closure.
    body_started: bool,
    /// Whether either party abandoned the handoff.
    closed: bool,
    /// Caller future waiting for the child body to start.
    caller_waker: Option<Waker>,
    /// Child future waiting for the caller to finish registration.
    child_waker: Option<Waker>,
}

/// Closes an unfinished handoff when its caller future is dropped.
pub(crate) struct RequestAdmissionHandoffGuard {
    /// Shared state closed unless registration is handed to the child.
    handoff: Arc<RequestAdmissionHandoff>,
    /// Whether dropping this guard should close the shared handoff.
    active: bool,
}

/// Result of racing body start against the submitted operation outcome.
pub(crate) enum BodyStartOr<T> {
    /// The child body entered and is waiting for its admission.
    BodyStarted,
    /// The child operation finished before its body-start handshake.
    Outcome(T),
}

impl RequestAdmissionHandoff {
    /// Creates a handoff and the caller guard that owns its unfinished state.
    pub(crate) fn new(admission: RequestAdmission) -> (Arc<Self>, RequestAdmissionHandoffGuard) {
        let handoff = Arc::new(Self {
            state: Mutex::new(HandoffState {
                caller_admission: Some(admission),
                child_admission: None,
                body_started: false,
                closed: false,
                caller_waker: None,
                child_waker: None,
            }),
        });
        let guard = RequestAdmissionHandoffGuard {
            handoff: Arc::clone(&handoff),
            active: true,
        };
        (handoff, guard)
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
    /// Returns `Some(outcome)` when the operation finishes before entering its
    /// body. Returns `None` after handing the registered admission to the body.
    pub(crate) async fn register_on_parent<F, T>(
        self: &Arc<Self>,
        parent: &OperationScope,
        guard: &mut RequestAdmissionHandoffGuard,
        outcome: &mut F,
    ) -> Option<T>
    where
        F: Future<Output = T> + Unpin,
    {
        match self.wait_for_body_start_or(outcome).await {
            BodyStartOr::Outcome(outcome) => {
                self.close();
                guard.finish();
                Some(outcome)
            }
            BodyStartOr::BodyStarted => {
                let Some(mut admission) = self.take_caller_admission() else {
                    guard.finish();
                    return None;
                };
                let registration = admission.retain_in_scope(parent).map(|()| admission);
                let _ = self.provide_to_child(registration);
                guard.finish();
                None
            }
        }
    }

    /// Waits for body start or polls the operation outcome, without blocking.
    async fn wait_for_body_start_or<F, T>(&self, outcome: &mut F) -> BodyStartOr<T>
    where
        F: Future<Output = T> + Unpin,
    {
        poll_fn(|context| {
            if self.poll_body_started(context) {
                return Poll::Ready(BodyStartOr::BodyStarted);
            }
            Pin::new(&mut *outcome).poll(context).map(BodyStartOr::Outcome)
        })
        .await
    }

    /// Polls body start and registers the caller waker while holding the state
    /// lock.
    fn poll_body_started(&self, context: &mut Context<'_>) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.body_started {
            return true;
        }
        if !state.closed {
            register_waker(&mut state.caller_waker, context.waker());
        }
        false
    }

    /// Waits for the parent to register this request, or for the child to stop.
    pub(crate) async fn wait_for_child_admission(
        &self,
        child_scope: &OperationScope,
    ) -> Option<Result<RequestAdmission, MimeError>> {
        let mut stopped = Box::pin(child_scope.context().stop_requested());
        poll_fn(|context| {
            if stopped.as_mut().poll(context).is_ready() {
                self.close();
                return Poll::Ready(None);
            }
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if let Some(admission) = state.child_admission.take() {
                return Poll::Ready(Some(admission));
            }
            if state.closed {
                return Poll::Ready(None);
            }
            register_waker(&mut state.child_waker, context.waker());
            Poll::Pending
        })
        .await
    }

    /// Takes the unregistered request admission for the parent registrar.
    fn take_caller_admission(&self) -> Option<RequestAdmission> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .caller_admission
            .take()
    }

    /// Provides the registered admission or its failure to the waiting child.
    fn provide_to_child(&self, admission: Result<RequestAdmission, MimeError>) -> bool {
        let (accepted, child_waker, rejected) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            if state.closed || state.child_admission.is_some() {
                (false, None, Some(admission))
            } else {
                state.child_admission = Some(admission);
                (true, state.child_waker.take(), None)
            }
        };
        drop(rejected);
        if let Some(waker) = child_waker {
            waker.wake();
        }
        accepted
    }

    /// Closes the gate, releases untransferred admission, and wakes both sides.
    fn close(&self) {
        let (caller_admission, child_admission, caller_waker, child_waker) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.closed = true;
            (
                state.caller_admission.take(),
                state.child_admission.take(),
                state.caller_waker.take(),
                state.child_waker.take(),
            )
        };
        drop(caller_admission);
        drop(child_admission);
        if let Some(waker) = caller_waker {
            waker.wake();
        }
        if let Some(waker) = child_waker {
            waker.wake();
        }
    }
}

impl RequestAdmissionHandoffGuard {
    /// Disarms caller-drop closure after the child owns the admission.
    fn finish(&mut self) {
        self.active = false;
    }
}

impl Drop for RequestAdmissionHandoffGuard {
    /// Closes an abandoned handoff and wakes its child outside the mutex.
    fn drop(&mut self) {
        if self.active {
            self.handoff.close();
        }
    }
}

/// Keeps the latest task waker without storing duplicate registrations.
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

    use super::super::byte_budget::ByteBudget;
    use super::super::request_admission::RequestAdmission;
    use super::super::service_permit::ServicePermit;
    use super::super::task_state::TaskState;
    use super::BodyStartOr;
    use super::RequestAdmissionHandoff;

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
                assert!(
                    handoff
                        .register_on_parent(&scope, &mut guard, &mut outcome)
                        .await
                        .is_some()
                );
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
