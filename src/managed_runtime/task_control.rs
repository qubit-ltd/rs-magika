// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Operation protocol adapter for a managed inference task.

use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use qubit_operation::protocol::ManagedOperationControl;
use qubit_operation::report::ChildBusinessSummary;
use qubit_operation::report::ChildSettlement;
use qubit_operation::report::ChildSnapshot;
use qubit_operation::report::DiagnosticSummary;
use qubit_operation::report::OperationPhase;
use qubit_operation::report::ResourcePhase;
use qubit_operation::report::StopReason;

use super::task_state::TaskState;

/// Connects task state transitions to the public operation lifecycle contract.
pub(super) struct TaskControl(
    /// Shared task state reported to the parent operation.
    pub(super) Arc<TaskState>,
);

impl ManagedOperationControl for TaskControl {
    /// Requests cooperative cancellation of the worker job.
    ///
    /// # Parameters
    ///
    /// * `_reason` - Parent stop reason; the worker records stop fact locally.
    fn request_stop(&self, _reason: StopReason) {
        self.0.request_stop();
    }

    /// Returns the latest business and resource phase snapshot.
    ///
    /// # Returns
    ///
    /// Current operation and resource phases.
    fn snapshot(&self) -> ChildSnapshot {
        let inner = self.0.inner.lock().unwrap_or_else(|error| error.into_inner());
        ChildSnapshot {
            operation_id: self.0.operation_id,
            phase: if inner.result.is_some() {
                OperationPhase::ResultReady
            } else if inner.started {
                OperationPhase::Running
            } else {
                OperationPhase::Queued
            },
            resource_phase: if inner.released {
                ResourcePhase::Released
            } else if inner.stop_requested {
                ResourcePhase::Stopping
            } else {
                ResourcePhase::Active
            },
            stop_requested: inner.stop_requested,
            released: inner.released,
        }
    }

    /// Waits for a terminal business outcome and returns its summary.
    ///
    /// # Parameters
    ///
    /// * `cx` - Executor context used while the business result is pending.
    ///
    /// # Returns
    ///
    /// A terminal summary or `Pending` until worker completion.
    fn poll_business(&self, cx: &mut Context<'_>) -> Poll<ChildBusinessSummary> {
        let mut inner = self.0.inner.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(outcome) = inner.business {
            return Poll::Ready(ChildBusinessSummary {
                operation_id: self.0.operation_id,
                outcome,
                summary: DiagnosticSummary::new("Magika inference completed"),
            });
        }
        register_waker(&mut inner.wakers, cx.waker());
        Poll::Pending
    }

    /// Waits until worker-owned inputs and permits have actually been released.
    ///
    /// # Parameters
    ///
    /// * `cx` - Executor context used while resources remain worker-owned.
    ///
    /// # Returns
    ///
    /// Settlement after actual release, or `Pending` while ownership remains.
    fn poll_released(&self, cx: &mut Context<'_>) -> Poll<ChildSettlement> {
        let mut inner = self.0.inner.lock().unwrap_or_else(|error| error.into_inner());
        if inner.released {
            return Poll::Ready(ChildSettlement {
                operation_id: self.0.operation_id,
                released: true,
                summary: DiagnosticSummary::new("Magika worker no longer owns this request"),
            });
        }
        register_waker(&mut inner.wakers, cx.waker());
        Poll::Pending
    }
}

/// Adds one distinct waker to the task transition notification list.
///
/// # Parameters
///
/// * `wakers` - Mutable registrations protected by the task state lock.
/// * `waker` - Current future's executor notification handle.
pub(super) fn register_waker(wakers: &mut Vec<Waker>, waker: &Waker) {
    if !wakers.iter().any(|registered| registered.will_wake(waker)) {
        wakers.push(waker.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use qubit_operation::protocol::ManagedOperationControl;
    use qubit_operation::report::ChildBusinessOutcome;
    use qubit_operation::report::OperationId;
    use qubit_operation::report::OperationPhase;
    use qubit_operation::report::ResourcePhase;
    use qubit_operation::report::StopReason;

    use super::TaskControl;
    use crate::managed_runtime::task_state::TaskState;

    #[test]
    fn test_control_reports_queued_running_stopping_and_released_phases() {
        let state = Arc::new(TaskState::new(OperationId::new(1), None));
        let control = TaskControl(Arc::clone(&state));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        let snapshot = control.snapshot();
        assert_eq!(snapshot.phase, OperationPhase::Queued);
        assert_eq!(snapshot.resource_phase, ResourcePhase::Active);
        assert!(matches!(control.poll_business(&mut context), Poll::Pending));
        assert!(matches!(control.poll_released(&mut context), Poll::Pending));

        state.mark_started();
        assert_eq!(control.snapshot().phase, OperationPhase::Running);
        control.request_stop(StopReason::UserCancelled);
        let snapshot = control.snapshot();
        assert_eq!(snapshot.resource_phase, ResourcePhase::Stopping);
        assert!(snapshot.stop_requested);

        state.finish_business(Ok(None), ChildBusinessOutcome::Stopped);
        assert!(matches!(control.poll_business(&mut context), Poll::Ready(_)));
        state.mark_released();
        assert!(matches!(control.poll_released(&mut context), Poll::Ready(_)));
        let snapshot = control.snapshot();
        assert_eq!(snapshot.phase, OperationPhase::ResultReady);
        assert_eq!(snapshot.resource_phase, ResourcePhase::Released);
    }

    #[test]
    fn test_control_recovers_poisoned_task_state_lock() {
        let state = Arc::new(TaskState::new(OperationId::new(3), None));
        let poisoned = Arc::clone(&state);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned.inner.lock().unwrap();
            panic!("poison task control state for recovery test");
        })
        .join();
        let control = TaskControl(state);
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert_eq!(control.snapshot().phase, OperationPhase::Queued);
        assert!(matches!(control.poll_business(&mut context), Poll::Pending));
        assert!(matches!(control.poll_released(&mut context), Poll::Pending));
    }
}
