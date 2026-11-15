// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Resources reserved before any provider operation starts.

use std::sync::Arc;

use qubit_mime::MimeError;
use qubit_mime::MimeResult;
use qubit_operation::ChildOutcomeKind;
use qubit_operation::OperationScope;
use qubit_operation::OperationManager;
use qubit_operation::ResourceRef;

use super::byte_permit::BytePermit;
use super::job::Job;
pub(super) use super::request_resource_owner::RequestResourceOwner;
use super::task_state::TaskState;
use super::worker_input::WorkerInput;

/// Runtime, byte, task, and optional parent-scope admission for one request.
pub(crate) struct RequestAdmission {
    /// Shared lifecycle state for this request.
    pub(super) state: Arc<TaskState>,
    /// Runtime service capacity held until release.
    service: Option<ResourceRef<()>>,
    /// Parent operation scope reference held until release.
    scope_resources: Vec<ResourceRef<()>>,
    /// Public manager view bound to the parent, when one was supplied.
    parent_manager: Option<OperationManager>,
    /// Input byte capacity held until release.
    bytes: Option<BytePermit>,
    /// Whether worker ownership has been established.
    dispatched: bool,
    /// Whether a provider operation started but has not completed.
    provider_pending: bool,
}

impl RequestAdmission {
    /// Creates an admission lease for a reserved request.
    ///
    /// # Parameters
    ///
    /// * `id` - Stable operation identifier.
    /// * `state` - Shared task state.
    /// * `control` - Operation control registered with the parent scope.
    /// * `service` - Runtime service capacity reservation.
    /// * `bytes` - Payload byte reservation.
    ///
    /// # Returns
    ///
    /// An admission lease holding all reserved resources.
    pub(super) fn new(
        state: Arc<TaskState>,
        service: ResourceRef<()>,
        scope_resources: Vec<ResourceRef<()>>,
        parent_manager: Option<OperationManager>,
        bytes: BytePermit,
    ) -> Self {
        Self {
            state,
            service: Some(service),
            scope_resources,
            parent_manager,
            bytes: Some(bytes),
            dispatched: false,
            provider_pending: false,
        }
    }

    /// Atomically marks the provider operation as started unless shutdown won.
    ///
    /// # Returns
    ///
    /// `Ok(())` when provider I/O may begin; otherwise returns a backend error.
    pub(crate) fn begin_provider_operation(&mut self) -> MimeResult<()> {
        if !self.state.try_start_provider_operation() {
            self.state.request_stop();
            let error = MimeError::detector_backend("magika", "provider operation stopped before start");
            self.settle_error(MimeError::detector_backend(
                "magika",
                "provider operation stopped before start",
            ));
            return Err(error);
        }
        self.provider_pending = true;
        Ok(())
    }

    /// Records that the provider operation completed normally.
    pub(crate) fn provider_operation_completed(&mut self) {
        self.provider_pending = false;
    }

    /// Transfers the request and its reservations to the worker.
    ///
    /// # Parameters
    ///
    /// * `input` - Owned worker input.
    /// * `sender` - Bounded worker queue.
    ///
    /// # Returns
    ///
    /// The task state whose business result can be observed.
    pub(super) fn dispatch(
        mut self,
        input: WorkerInput,
        sender: &std::sync::mpsc::SyncSender<Job>,
    ) -> MimeResult<Arc<TaskState>> {
        let driver_required = matches!(&input, WorkerInput::Bridge(..));
        if let WorkerInput::Bridge(bridge, _, _) = &input {
            self.state.attach_bridge(Arc::downgrade(bridge));
        }
        let job = Job {
            input,
            state: Arc::clone(&self.state),
            owner: {
                let owner = Arc::new(RequestResourceOwner::new(
                    Arc::downgrade(&self.state),
                    self.service.take().expect("admission lease is present"),
                    self.bytes.take().expect("byte lease is present"),
                    std::mem::take(&mut self.scope_resources),
                    driver_required,
                ));
                self.state.attach_owner(Arc::clone(&owner));
                owner
            },
        };
        match sender.try_send(job) {
            Ok(()) => {
                self.dispatched = true;
                Ok(Arc::clone(&self.state))
            }
            Err(error) => {
                let rejected = match error {
                    std::sync::mpsc::TrySendError::Full(job) | std::sync::mpsc::TrySendError::Disconnected(job) => job,
                };
                rejected.state.finish_business(
                    Err(MimeError::detector_backend(
                        "magika",
                        "inference worker queue is full or closed",
                    )),
                    ChildOutcomeKind::Failed,
                );
                let state = Arc::clone(&rejected.state);
                drop(rejected);
                state.mark_released();
                self.dispatched = true;
                Ok(state)
            }
        }
    }

    /// Settles an undispatched admission with a terminal error.
    ///
    /// # Parameters
    ///
    /// * `error` - Error reported to the task result.
    pub(crate) fn settle_error(&mut self, error: MimeError) {
        if !self.dispatched {
            self.state.finish_business(Err(error), ChildOutcomeKind::Failed);
            self.service.take();
            self.bytes.take();
            self.state.mark_released();
            self.dispatched = true;
        }
    }

    pub(super) fn scope_resources(scope: Option<&OperationScope>) -> MimeResult<Vec<ResourceRef<()>>> {
        match scope {
            Some(scope) => Ok(vec![scope.manage(()).map_err(|error| {
                MimeError::detector_backend("magika", format!("operation scope rejected inference: {error:?}"))
            })?]),
            None => Ok(Vec::new()),
        }
    }

    pub(super) fn retain_in_scope(&mut self, scope: &OperationScope) -> MimeResult<()> {
        self.scope_resources.push(scope.manage(()).map_err(|error| {
            MimeError::detector_backend("magika", format!("operation scope rejected inference: {error:?}"))
        })?);
        Ok(())
    }

    pub(super) fn parent_manager(&self) -> Option<OperationManager> {
        self.parent_manager.clone()
    }

}

/// Settles or retains resources when an admission lease is abandoned.
impl Drop for RequestAdmission {
    /// Settles the lease or retains permits for unresolved provider I/O.
    fn drop(&mut self) {
        if !self.dispatched {
            if self.provider_pending {
                // rs-fs has no observer for provider-native completion after
                // its caller future is dropped, so retain committed resources.
                let owner = Arc::new(RequestResourceOwner::new(
                    Arc::downgrade(&self.state),
                    self.service.take().expect("admission lease is present"),
                    self.bytes.take().expect("byte lease is present"),
                    std::mem::take(&mut self.scope_resources),
                    true,
                ));
                owner.worker_done();
                self.state.attach_owner(owner);
                self.dispatched = true;
                return;
            }
            self.settle_error(MimeError::detector_backend("magika", "request admission was abandoned"));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use qubit_operation::OperationId;
    use qubit_operation::ManagerOptions;
    use qubit_operation::OperationManager;

    use super::RequestAdmission;
    use super::RequestResourceOwner;
    use crate::managed_runtime::byte_budget::ByteBudget;
    use crate::managed_runtime::task_control::TaskControl;
    use crate::managed_runtime::task_state::TaskState;

    #[test]
    fn test_worker_completion_alone_keeps_provider_resources_until_driver_completion() {
        let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
        let service = supervisor.service_group(1).unwrap();
        let service = service.try_admit().unwrap();
        let budget = Arc::new(ByteBudget::new(8));
        let permit = budget.try_acquire(8).unwrap();
        let state = Arc::new(TaskState::new(OperationId::new(700), None));
        let owner = Arc::new(RequestResourceOwner::new(Arc::downgrade(&state), service, permit, None, true));
        state.attach_owner(Arc::clone(&owner));

        owner.worker_done();
        assert!(!state.inner.lock().unwrap().released);
        assert!(budget.try_acquire(1).is_none());

        owner.driver_done();
        assert!(state.inner.lock().unwrap().released);
        assert!(budget.try_acquire(8).is_some());
    }

    #[test]
    fn test_driver_completion_alone_keeps_provider_resources_until_worker_completion() {
        let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
        let service = supervisor.service_group(1).unwrap().try_admit().unwrap();
        let budget = Arc::new(ByteBudget::new(8));
        let permit = budget.try_acquire(8).unwrap();
        let state = Arc::new(TaskState::new(OperationId::new(702), None));
        let owner = Arc::new(RequestResourceOwner::new(Arc::downgrade(&state), service, permit, None, true));
        state.attach_owner(Arc::clone(&owner));

        owner.driver_done();
        assert!(!state.inner.lock().unwrap().released);
        assert!(budget.try_acquire(1).is_none());

        owner.worker_done();
        assert!(state.inner.lock().unwrap().released);
        assert!(budget.try_acquire(8).is_some());
    }

    #[test]
    fn test_resource_owner_releases_permits_when_task_state_is_gone() {
        let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
        let service = supervisor.service_group(1).unwrap().try_admit().unwrap();
        let budget = Arc::new(ByteBudget::new(8));
        let permit = budget.try_acquire(8).unwrap();
        let state = Arc::new(TaskState::new(OperationId::new(703), None));
        let owner = RequestResourceOwner::new(Arc::downgrade(&state), service, permit, None, true);
        drop(state);

        owner.driver_done();
        owner.worker_done();

        assert!(budget.try_acquire(8).is_some());
    }

    #[test]
    fn test_attach_scope_reports_child_limit_rejection() {
        use qubit_operation::protocol::ManagedOperationControl;
        use qubit_operation::StopReason;
        use qubit_operation::scope::ScopeOptions;
        use qubit_operation::OperationManager;

        let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
        let service = supervisor.service_group(1).unwrap();
        let mut options = ScopeOptions::default();
        options.limits.child_operations = 0;
        let mut handle = service
            .start(options, move |scope| async move {
                let state = Arc::new(TaskState::new(OperationId::new(704), None));
                let control: Arc<dyn ManagedOperationControl> = Arc::new(TaskControl(state));
                scope.request_cancel(StopReason::UserCancelled);
                assert!(RequestAdmission::attach_scope(Some(&scope), control).is_err());
                Ok::<(), ()>(())
            })
            .unwrap();
        let _ = handle.result_blocking().unwrap();
    }

    #[test]
    fn test_dropped_pending_provider_operation_retains_admission_without_claiming_release() {
        let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
        let service = supervisor.service_group(1).unwrap().try_admit().unwrap();
        let budget = Arc::new(ByteBudget::new(8));
        let permit = budget.try_acquire(8).unwrap();
        let state = Arc::new(TaskState::new(OperationId::new(701), None));
        let control: Arc<dyn qubit_operation::protocol::ManagedOperationControl> =
            Arc::new(TaskControl(Arc::clone(&state)));
        let mut admission = RequestAdmission::new(OperationId::new(701), Arc::clone(&state), control, service, permit);
        admission.begin_provider_operation().unwrap();
        drop(admission);

        assert!(!state.inner.lock().unwrap().released);
        assert!(budget.try_acquire(1).is_none());
    }
}
