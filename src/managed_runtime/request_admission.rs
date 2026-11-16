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
use qubit_operation::ResourceRef;

use super::byte_permit::BytePermit;
use super::job::Job;
pub(super) use super::request_resource_owner::RequestResourceOwner;
use super::service_permit::ServicePermit;
use super::task_state::TaskState;
use super::worker_input::WorkerInput;

/// Runtime, byte, task, and optional parent-scope admission for one request.
pub(crate) struct RequestAdmission {
    /// Shared lifecycle state for this request.
    pub(super) state: Arc<TaskState>,
    /// Runtime service capacity held until release.
    service: Option<ServicePermit>,
    /// Parent operation scope reference held until release.
    scope_resources: Vec<ResourceRef<()>>,
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
    /// * `state` - Shared task state.
    /// * `service` - Runtime service capacity reservation.
    /// * `scope_resources` - References retained in the parent operation scope
    ///   until this request's resources are released. These references keep the
    ///   scope lifecycle active; they are not operation controls.
    /// * `bytes` - Payload byte reservation.
    ///
    /// # Returns
    ///
    /// An admission lease holding all reserved resources.
    pub(super) fn new(
        state: Arc<TaskState>,
        service: ServicePermit,
        scope_resources: Vec<ResourceRef<()>>,
        bytes: BytePermit,
    ) -> Self {
        Self {
            state,
            service: Some(service),
            scope_resources,
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
            let error = MimeError::detector_backend("magika", self.state.provider_stop_reason());
            self.settle_error(MimeError::detector_backend("magika", self.state.provider_stop_reason()));
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

    /// Retains one resource reference in the optional parent scope.
    ///
    /// # Parameters
    ///
    /// * `scope` - Parent scope that owns the inference request.
    ///
    /// # Returns
    ///
    /// The scope reference, or an error when the scope rejects it.
    /// Adds a parent or child scope reference to this request's retained
    /// resources.
    ///
    /// # Parameters
    ///
    /// * `scope` - Operation scope that must remain active until request
    ///   release.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation scope rejects the reference.
    pub(crate) fn retain_in_scope(&mut self, scope: &OperationScope) -> MimeResult<()> {
        self.scope_resources.push(scope.manage(()).map_err(|error| {
            MimeError::detector_backend("magika", format!("operation scope rejected inference: {error:?}"))
        })?);
        Ok(())
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

    use qubit_operation::ManagerOptions;
    use qubit_operation::OperationManager;

    use super::super::service_permit::ServicePermit;
    use super::RequestAdmission;
    use super::RequestResourceOwner;
    use crate::managed_runtime::byte_budget::ByteBudget;
    use crate::managed_runtime::task_state::TaskState;

    fn service_resource() -> ServicePermit {
        ServicePermit::try_acquire(&Arc::new(std::sync::atomic::AtomicUsize::new(0)), 1).unwrap()
    }

    #[test]
    fn test_worker_completion_alone_keeps_provider_resources_until_driver_completion() {
        let service = service_resource();
        let budget = Arc::new(ByteBudget::new(8));
        let permit = budget.try_acquire(8).unwrap();
        let state = Arc::new(TaskState::new(700, None));
        let owner = Arc::new(RequestResourceOwner::new(
            Arc::downgrade(&state),
            service,
            permit,
            Vec::new(),
            true,
        ));
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
        let service = service_resource();
        let budget = Arc::new(ByteBudget::new(8));
        let permit = budget.try_acquire(8).unwrap();
        let state = Arc::new(TaskState::new(702, None));
        let owner = Arc::new(RequestResourceOwner::new(
            Arc::downgrade(&state),
            service,
            permit,
            Vec::new(),
            true,
        ));
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
        let service = service_resource();
        let budget = Arc::new(ByteBudget::new(8));
        let permit = budget.try_acquire(8).unwrap();
        let state = Arc::new(TaskState::new(703, None));
        let owner = RequestResourceOwner::new(Arc::downgrade(&state), service, permit, Vec::new(), true);
        drop(state);

        owner.driver_done();
        owner.worker_done();

        assert!(budget.try_acquire(8).is_some());
    }

    #[test]
    fn test_public_operation_scope_can_retain_admission_resources() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let mut operation = manager
            .try_submit(qubit_operation::OperationOptions::default(), |scope| {
                let budget = Arc::new(ByteBudget::new(8));
                let permit = budget.try_acquire(8).unwrap();
                let mut admission = RequestAdmission::new(
                    Arc::new(TaskState::new(704, None)),
                    service_resource(),
                    Vec::new(),
                    permit,
                );
                admission.retain_in_scope(&scope).unwrap();
                assert_eq!(admission.scope_resources.len(), 1);
                Ok::<_, ()>(())
            })
            .unwrap();
        assert!(matches!(
            operation.wait_outcome().unwrap(),
            qubit_operation::OperationOutcome::Succeeded(())
        ));
    }

    #[test]
    fn test_dropped_pending_provider_operation_retains_admission_without_claiming_release() {
        let service = service_resource();
        let budget = Arc::new(ByteBudget::new(8));
        let permit = budget.try_acquire(8).unwrap();
        let state = Arc::new(TaskState::new(701, None));
        let mut admission = RequestAdmission::new(Arc::clone(&state), service, Vec::new(), permit);
        admission.begin_provider_operation().unwrap();
        drop(admission);

        assert!(!state.inner.lock().unwrap().released);
        assert!(budget.try_acquire(1).is_none());
    }
}
