// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Resources reserved before any provider operation starts.

use std::sync::Arc;

use qubit_mime::MimeError;
use qubit_mime::MimeResult;
use qubit_operation::protocol::ManagedOperationControl;
use qubit_operation::report::ChildBusinessOutcome;
use qubit_operation::report::OperationId;
use qubit_operation::scope::OperationScope;
use qubit_operation::supervisor::ServiceRequest;

use super::byte_permit::BytePermit;
use super::job::Job;
use super::task_state::TaskState;
use super::worker_input::WorkerInput;

/// Runtime, byte, task, and optional parent-scope admission for one request.
pub(crate) struct RequestAdmission {
    pub(super) state: Arc<TaskState>,
    control: Arc<dyn ManagedOperationControl>,
    service: Option<ServiceRequest>,
    bytes: Option<BytePermit>,
    dispatched: bool,
    provider_pending: bool,
}

impl RequestAdmission {
    pub(super) fn new(
        id: OperationId,
        state: Arc<TaskState>,
        control: Arc<dyn ManagedOperationControl>,
        service: ServiceRequest,
        bytes: BytePermit,
    ) -> Self {
        let _ = id;
        Self {
            state,
            control,
            service: Some(service),
            bytes: Some(bytes),
            dispatched: false,
            provider_pending: false,
        }
    }

    pub(crate) fn provider_operation_started(&mut self) {
        self.provider_pending = true;
    }

    pub(crate) fn provider_operation_completed(&mut self) {
        self.provider_pending = false;
    }

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
                    std::sync::mpsc::TrySendError::Full(job)
                    | std::sync::mpsc::TrySendError::Disconnected(job) => job,
                };
                rejected.state.finish_business(
                    Err(MimeError::detector_backend(
                        "magika",
                        "inference worker queue is full or closed",
                    )),
                    ChildBusinessOutcome::Failed,
                );
                let state = Arc::clone(&rejected.state);
                drop(rejected);
                state.mark_released();
                self.dispatched = true;
                Ok(state)
            }
        }
    }

    pub(crate) fn settle_error(&mut self, error: MimeError) {
        if !self.dispatched {
            self.state
                .finish_business(Err(error), ChildBusinessOutcome::Failed);
            self.service.take();
            self.bytes.take();
            self.state.mark_released();
            self.dispatched = true;
        }
    }

    pub(super) fn control(&self) -> Arc<dyn ManagedOperationControl> {
        Arc::clone(&self.control)
    }

    pub(super) fn attach_scope(
        scope: Option<&OperationScope>,
        control: Arc<dyn ManagedOperationControl>,
    ) -> MimeResult<()> {
        if let Some(scope) = scope {
            let mut reservation = scope
                .reserve_child(qubit_operation::scope::ChildRequirement::Required)
                .map_err(|error| {
                    MimeError::detector_backend(
                        "magika",
                        format!("operation scope rejected inference: {error:?}"),
                    )
                })?;
            reservation.attach(control).map_err(|error| {
                MimeError::detector_backend(
                    "magika",
                    format!("could not attach inference: {error:?}"),
                )
            })?;
            reservation.commit().map_err(|error| {
                MimeError::detector_backend(
                    "magika",
                    format!("could not commit inference: {error:?}"),
                )
            })?;
        }
        Ok(())
    }
}

/// Holds runtime and byte capacity until the registered worker and sampling
/// driver have both completed. A dropped Pending provider read intentionally
/// leaves `driver_done` false: the filesystem API cannot prove native work has
/// stopped in that case.
pub(super) struct RequestResourceOwner {
    state: std::sync::Weak<TaskState>,
    resources: std::sync::Mutex<Option<(ServiceRequest, BytePermit)>>,
    facts: std::sync::Mutex<(bool, bool, bool)>,
}

impl RequestResourceOwner {
    fn new(
        state: std::sync::Weak<TaskState>,
        service: ServiceRequest,
        bytes: BytePermit,
        driver_required: bool,
    ) -> Self {
        Self {
            state,
            resources: std::sync::Mutex::new(Some((service, bytes))),
            facts: std::sync::Mutex::new((!driver_required, false, false)),
        }
    }

    pub(super) fn worker_done(&self) {
        self.mark_done(false);
    }
    pub(super) fn driver_done(&self) {
        self.mark_done(true);
    }

    fn mark_done(&self, driver: bool) {
        let complete = {
            let mut facts = self.facts.lock().unwrap_or_else(|error| error.into_inner());
            if driver {
                facts.0 = true;
            } else {
                facts.1 = true;
            }
            if facts.0 && facts.1 && !facts.2 {
                facts.2 = true;
                true
            } else {
                false
            }
        };
        if complete {
            self.resources
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take();
            if let Some(state) = self.state.upgrade() {
                state.mark_released();
            }
        }
    }
}

impl Drop for RequestAdmission {
    fn drop(&mut self) {
        if !self.dispatched {
            if self.provider_pending {
                // rs-fs has no observer for provider-native completion after
                // its caller future is dropped, so retain committed resources.
                let owner = Arc::new(RequestResourceOwner::new(
                    Arc::downgrade(&self.state),
                    self.service.take().expect("admission lease is present"),
                    self.bytes.take().expect("byte lease is present"),
                    true,
                ));
                owner.worker_done();
                self.state.attach_owner(owner);
                self.dispatched = true;
                return;
            }
            self.settle_error(MimeError::detector_backend(
                "magika",
                "request admission was abandoned",
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use qubit_operation::report::OperationId;
    use qubit_operation::report::SupervisorLimits;
    use qubit_operation::supervisor::OperationSupervisor;

    use super::RequestAdmission;
    use super::RequestResourceOwner;
    use crate::managed_runtime::byte_budget::ByteBudget;
    use crate::managed_runtime::task_control::TaskControl;
    use crate::managed_runtime::task_state::TaskState;

    #[test]
    fn worker_completion_alone_keeps_provider_resources_until_driver_completion() {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let service = supervisor.service_group(1).unwrap();
        let service = service.try_admit().unwrap();
        let budget = Arc::new(ByteBudget::new(8));
        let permit = budget.try_acquire(8).unwrap();
        let state = Arc::new(TaskState::new(OperationId::new(700), None));
        let owner = Arc::new(RequestResourceOwner::new(
            Arc::downgrade(&state),
            service,
            permit,
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
    fn dropped_pending_provider_operation_retains_admission_without_claiming_release() {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let service = supervisor.service_group(1).unwrap().try_admit().unwrap();
        let budget = Arc::new(ByteBudget::new(8));
        let permit = budget.try_acquire(8).unwrap();
        let state = Arc::new(TaskState::new(OperationId::new(701), None));
        let control: Arc<dyn qubit_operation::protocol::ManagedOperationControl> =
            Arc::new(TaskControl(Arc::clone(&state)));
        let mut admission = RequestAdmission::new(
            OperationId::new(701),
            Arc::clone(&state),
            control,
            service,
            permit,
        );
        admission.provider_operation_started();
        drop(admission);

        assert!(!state.inner.lock().unwrap().released);
        assert!(budget.try_acquire(1).is_none());
    }
}
