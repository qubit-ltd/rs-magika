// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate release of runtime and byte reservations.

use qubit_operation::CleanupFailure;
use qubit_operation::CleanupOutcome;
use qubit_operation::CleanupStage;
use qubit_operation::DiagnosticSummary;
use qubit_operation::ResourceId;

use super::byte_permit::BytePermit;
use super::request_resource_completion_facts::RequestResourceCompletionFacts;
use super::service_permit::ServicePermit;
use super::task_state::TaskState;

/// Holds runtime and byte capacity until the registered worker and sampling
/// driver have both stopped accessing its state. Unknown provider I/O keeps the
/// permits in the scope-owned payload until cleanup can report a failure.
pub(super) struct RequestResourceOwner {
    /// Weak task state notified on aggregate release.
    state: std::sync::Weak<TaskState>,
    /// Runtime and byte permits retained until both participants finish.
    resources: std::sync::Mutex<Option<(ServicePermit, BytePermit)>>,
    /// Driver completion, worker completion, and release publication.
    facts: std::sync::Mutex<RequestResourceCompletionFacts>,
    /// Whether a provider future was dropped without completion evidence.
    unconfirmed_provider_io: std::sync::atomic::AtomicBool,
    /// Real resource identity installed after the scope reservation commits.
    resource_id: std::sync::OnceLock<ResourceId>,
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
        service: ServicePermit,
        bytes: BytePermit,
        driver_required: bool,
    ) -> Self {
        Self {
            state,
            resources: std::sync::Mutex::new(Some((service, bytes))),
            facts: std::sync::Mutex::new(RequestResourceCompletionFacts {
                driver_required,
                ..RequestResourceCompletionFacts::default()
            }),
            unconfirmed_provider_io: std::sync::atomic::AtomicBool::new(false),
            resource_id: std::sync::OnceLock::new(),
        }
    }

    /// Installs the identity returned by the committed public scope resource.
    ///
    /// # Parameters
    ///
    /// * `id` - Identity assigned to the committed scope resource.
    pub(super) fn set_resource_id(&self, id: ResourceId) {
        assert!(self.resource_id.set(id).is_ok(), "request resource ID is set once");
    }

    /// Adds the provider driver to the completion barrier before dispatch.
    pub(super) fn require_driver(&self) {
        self.facts
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .driver_required = true;
    }

    /// Records that dropped provider I/O has no completion evidence.
    pub(super) fn mark_unconfirmed_provider_io(&self) {
        self.unconfirmed_provider_io
            .store(true, std::sync::atomic::Ordering::Release);
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
                facts.driver_done = true;
            } else {
                facts.worker_done = true;
            }
            if facts.worker_done && (!facts.driver_required || facts.driver_done) && !facts.completed {
                facts.completed = true;
                true
            } else {
                false
            }
        };
        if complete {
            if let Some(state) = self.state.upgrade() {
                if self.unconfirmed_provider_io.load(std::sync::atomic::Ordering::Acquire) {
                    state.mark_access_complete();
                } else {
                    self.release_resources();
                    state.mark_released();
                }
            } else if !self.unconfirmed_provider_io.load(std::sync::atomic::Ordering::Acquire) {
                self.release_resources();
            }
        }
    }

    /// Drops runtime and byte permits after confirmed local access completion.
    pub(super) fn release_resources(&self) {
        let resources = self.resources.lock().unwrap_or_else(|error| error.into_inner()).take();
        drop(resources);
    }

    /// Returns resources after a scope commit that never published this owner.
    pub(super) fn take_resources(&self) -> Option<(ServicePermit, BytePermit)> {
        self.resources.lock().unwrap_or_else(|error| error.into_inner()).take()
    }

    /// Returns release evidence or retains the owner when provider completion
    /// cannot be confirmed.
    pub(super) fn cleanup_outcome(self: std::sync::Arc<Self>) -> CleanupOutcome<std::sync::Arc<Self>> {
        if self.unconfirmed_provider_io.load(std::sync::atomic::Ordering::Acquire) {
            let resource_id = *self
                .resource_id
                .get()
                .expect("a scoped request owner has a committed resource identity");
            CleanupOutcome::Failed {
                failure: CleanupFailure {
                    resource_id,
                    stage: CleanupStage::AsyncRelease,
                    summary: DiagnosticSummary::new("provider I/O completion is unconfirmed"),
                    source: None,
                },
                resource: self,
            }
        } else {
            self.release_resources();
            if let Some(state) = self.state.upgrade() {
                state.mark_released();
            }
            CleanupOutcome::Released
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::RequestResourceOwner;
    use crate::managed_runtime::byte_budget::ByteBudget;
    use crate::managed_runtime::task_state::TaskState;

    #[test]
    fn test_resource_completion_recovers_poisoned_fact_and_permit_locks() {
        let service = super::super::service_permit::ServicePermit::try_acquire(
            &Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            1,
        )
        .unwrap();
        let budget = Arc::new(ByteBudget::new(4));
        let permit = budget.try_acquire(4).unwrap();
        let state = Arc::new(TaskState::new(14, None));
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
