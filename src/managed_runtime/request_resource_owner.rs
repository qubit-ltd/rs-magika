// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate release of runtime and byte reservations.

use qubit_operation::supervisor::ServiceRequest;

use super::byte_permit::BytePermit;
use super::task_state::TaskState;

/// Holds runtime and byte capacity until the registered worker and sampling
/// driver have both completed. A dropped Pending provider read intentionally
/// leaves `driver_done` false: the filesystem API cannot prove native work has
/// stopped in that case.
pub(super) struct RequestResourceOwner {
    /// Weak task state notified on aggregate release.
    state: std::sync::Weak<TaskState>,
    /// Runtime and byte permits retained until both participants finish.
    resources: std::sync::Mutex<Option<(ServiceRequest, BytePermit)>>,
    /// Driver completion, worker completion, and release publication.
    facts: std::sync::Mutex<(bool, bool, bool)>,
}

impl RequestResourceOwner {
    /// Creates an aggregate owner for runtime and byte permits.
    ///
    /// # Parameters
    ///
    /// * `state` - Weak task state notified when all resources are released.
    /// * `service` - Runtime service reservation.
    /// * `bytes` - Payload byte reservation.
    /// * `driver_required` - Whether provider sampling must also finish.
    pub(super) fn new(
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

    /// Records that the worker released its input and completed its work.
    pub(super) fn worker_done(&self) {
        if let Some(state) = self.state.upgrade() {
            state.mark_worker_done();
        }
        self.mark_done(false);
    }
    /// Records that the provider sampling driver completed.
    pub(super) fn driver_done(&self) {
        self.mark_done(true);
    }

    /// Releases permits after both required participants complete.
    ///
    /// # Parameters
    ///
    /// * `driver` - Whether this completion came from the sampling driver.
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
            self.resources.lock().unwrap_or_else(|error| error.into_inner()).take();
            if let Some(state) = self.state.upgrade() {
                state.mark_released();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use qubit_operation::report::OperationId;
    use qubit_operation::report::SupervisorLimits;
    use qubit_operation::supervisor::OperationSupervisor;

    use super::RequestResourceOwner;
    use crate::managed_runtime::byte_budget::ByteBudget;
    use crate::managed_runtime::task_state::TaskState;

    #[test]
    fn test_resource_completion_recovers_poisoned_fact_and_permit_locks() {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let service = supervisor.service_group(1).unwrap().try_admit().unwrap();
        let budget = Arc::new(ByteBudget::new(4));
        let permit = budget.try_acquire(4).unwrap();
        let state = Arc::new(TaskState::new(OperationId::new(14), None));
        let owner = Arc::new(RequestResourceOwner::new(Arc::downgrade(&state), service, permit, true));

        let poisoned_facts = Arc::clone(&owner);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned_facts.facts.lock().unwrap();
                panic!("poison completion facts for recovery test");
            })
            .join()
            .is_err()
        );
        let poisoned_resources = Arc::clone(&owner);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned_resources.resources.lock().unwrap();
                panic!("poison permit lock for recovery test");
            })
            .join()
            .is_err()
        );

        owner.driver_done();
        owner.worker_done();

        assert!(state.inner.lock().unwrap().released);
        assert!(budget.try_acquire(4).is_some());
    }
}
