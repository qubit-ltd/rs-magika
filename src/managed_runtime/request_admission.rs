// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Resources reserved before any provider operation starts.

use std::future::Future;
use std::sync::Arc;

use qubit_mime::MimeError;
use qubit_mime::MimeResult;
use qubit_operation::ChildOutcomeKind;
use qubit_operation::OperationScope;

use super::byte_permit::BytePermit;
use super::job::Job;
use super::request_resource_cleanup::RequestResourceCleanup;
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
    /// Input byte capacity held until release.
    bytes: Option<BytePermit>,
    /// Whether worker ownership has been established.
    dispatched: bool,
    /// Whether a provider operation started but has not completed.
    provider_pending: bool,
    /// Whether the provider future crossed its first-poll start boundary.
    provider_polled: bool,
}

impl RequestAdmission {
    /// Returns the shared state used to observe stop between provider reads.
    pub(crate) fn task_state(&self) -> Arc<TaskState> {
        Arc::clone(&self.state)
    }

    /// Creates an admission lease for a reserved request.
    ///
    /// # Parameters
    ///
    /// * `state` - Shared task state.
    /// * `service` - Runtime service capacity reservation.
    /// * `bytes` - Payload byte reservation.
    ///
    /// # Returns
    ///
    /// An admission lease holding all reserved resources.
    pub(super) fn new(state: Arc<TaskState>, service: ServicePermit, bytes: BytePermit) -> Self {
        Self {
            state,
            service: Some(service),
            bytes: Some(bytes),
            dispatched: false,
            provider_pending: false,
            provider_polled: false,
        }
    }

    /// Polls provider I/O alongside runtime and operation stop signals.
    ///
    /// A stop before the provider future's first poll is a clean rejection;
    /// dropping it after a `Pending` poll remains unconfirmed.
    ///
    /// # Parameters
    ///
    /// * `scope` - Parent operation scope whose stop signal is observed.
    /// * `operation` - Provider operation to poll while admission remains
    ///   active.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Provider future type.
    pub(crate) async fn run_provider_operation<F>(
        &mut self,
        scope: &OperationScope,
        operation: F,
    ) -> MimeResult<F::Output>
    where
        F: Future,
    {
        self.begin_provider_operation()?;
        // This wrapper can identify the first poll boundary precisely; the
        // lower-level begin method retains its historical caller contract.
        self.provider_polled = false;
        let state = Arc::clone(&self.state);
        let mut request_stop = Box::pin(state.wait_stopped());
        let mut scope_stop = Box::pin(scope.context().stop_requested());
        let mut operation = Box::pin(operation);
        let outcome = std::future::poll_fn(|context| {
            if request_stop.as_mut().poll(context).is_ready() {
                return std::task::Poll::Ready(None);
            }
            if scope_stop.as_mut().poll(context).is_ready() {
                state.request_stop();
                return std::task::Poll::Ready(None);
            }
            self.provider_polled = true;
            operation.as_mut().poll(context).map(Some)
        })
        .await;
        match outcome {
            Some(output) => {
                self.provider_operation_completed();
                Ok(output)
            }
            None => {
                if !self.provider_polled {
                    self.provider_operation_completed();
                }
                Err(MimeError::detector_backend("magika", state.provider_stop_reason()))
            }
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
        self.provider_polled = true;
        Ok(())
    }

    /// Records that the provider operation completed normally.
    pub(crate) fn provider_operation_completed(&mut self) {
        self.provider_pending = false;
        self.provider_polled = false;
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
        let owner = match self.state.request_resource_owner() {
            Some(owner) => owner,
            None => {
                let owner = Arc::new(RequestResourceOwner::new(
                    Arc::downgrade(&self.state),
                    self.service.take().expect("admission lease is present"),
                    self.bytes.take().expect("admission lease is present"),
                    false,
                ));
                self.state.attach_owner(Arc::clone(&owner), None);
                owner
            }
        };
        if driver_required {
            owner.require_driver();
        }
        let job = Job {
            input,
            state: Arc::clone(&self.state),
            owner: Arc::clone(&owner),
        };
        match sender.try_send(job) {
            Ok(()) => {
                self.dispatched = true;
                Ok(Arc::clone(&self.state))
            }
            Err(error) => {
                let (rejected, reason) = match error {
                    std::sync::mpsc::TrySendError::Full(job) => (job, "inference worker queue is full"),
                    std::sync::mpsc::TrySendError::Disconnected(job) => (job, "inference worker is disconnected"),
                };
                if let WorkerInput::Bridge(bridge, _, _) = &rejected.input {
                    // No worker owns this bridge, so wake any provider driver
                    // instead of waiting for a read that cannot be requested.
                    bridge.close();
                }
                rejected.state.finish_business(
                    Err(MimeError::detector_backend("magika", reason)),
                    ChildOutcomeKind::Failed,
                );
                let state = Arc::clone(&rejected.state);
                rejected.owner.worker_done();
                if driver_required {
                    rejected.owner.driver_done();
                }
                drop(rejected);
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
            if let Some(owner) = self.state.request_resource_owner() {
                owner.worker_done();
            } else {
                self.service.take();
                self.bytes.take();
                self.state.mark_released();
            }
            self.dispatched = true;
        }
    }

    /// Retains a resource reference in the parent operation scope.
    ///
    /// # Parameters
    ///
    /// * `scope` - Operation scope that must remain active until request
    ///   release.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the resource was retained, or an error when the scope
    /// rejects it.
    pub(crate) fn retain_in_scope(&mut self, scope: &OperationScope) -> MimeResult<()> {
        if self.state.request_resource_owner().is_some() {
            return Ok(());
        }
        let reservation = scope.reserve_resource().map_err(|error| {
            MimeError::detector_backend("magika", format!("operation scope rejected inference: {error:?}"))
        })?;
        let owner = Arc::new(RequestResourceOwner::new(
            Arc::downgrade(&self.state),
            self.service
                .take()
                .expect("unregistered admission retains service capacity"),
            self.bytes.take().expect("unregistered admission retains byte capacity"),
            false,
        ));
        let resource = reservation
            .commit_with(Arc::clone(&owner), Arc::new(RequestResourceCleanup))
            .map_err(|error| {
                drop(error);
                MimeError::detector_backend("magika", "operation scope rejected inference resource commit")
            })?;
        owner.set_resource_id(resource.id());
        self.state.attach_owner(owner, Some(resource));
        Ok(())
    }
}

/// Settles or retains resources when an admission lease is abandoned.
impl Drop for RequestAdmission {
    /// Settles the lease or retains permits for unresolved provider I/O.
    fn drop(&mut self) {
        if !self.dispatched {
            if self.provider_pending && self.provider_polled {
                self.state.mark_unconfirmed_provider_io();
                // rs-fs has no observer for provider-native completion after
                // its caller future is dropped, so retain committed resources.
                let owner = match self.state.request_resource_owner() {
                    Some(owner) => owner,
                    None => {
                        let owner = Arc::new(RequestResourceOwner::new(
                            Arc::downgrade(&self.state),
                            self.service.take().expect("admission lease is present"),
                            self.bytes.take().expect("byte lease is present"),
                            false,
                        ));
                        self.state.attach_owner(Arc::clone(&owner), None);
                        owner
                    }
                };
                owner.mark_unconfirmed_provider_io();
                owner.worker_done();
                self.dispatched = true;
                return;
            }
            self.settle_error(MimeError::detector_backend("magika", "request admission was abandoned"));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use qubit_operation::ManagerOptions;
    use qubit_operation::OperationManager;
    use qubit_operation::OperationOptions;
    use qubit_operation::OperationOutcome;
    use qubit_operation::RecoveryPhase;

    use super::super::service_permit::ServicePermit;
    use super::RequestAdmission;
    use super::RequestResourceOwner;
    use crate::managed_runtime::byte_budget::ByteBudget;
    use crate::managed_runtime::task_state::TaskState;

    fn service_resource() -> ServicePermit {
        ServicePermit::try_acquire(&Arc::new(std::sync::atomic::AtomicUsize::new(0)), 1).unwrap()
    }

    #[test]
    fn test_scope_resource_registration_is_idempotent_for_one_admission() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let mut options = OperationOptions::default();
        options.limits.resources = 1;
        let mut operation = manager
            .try_submit(options, |scope| {
                let budget = Arc::new(ByteBudget::new(1));
                let admission = RequestAdmission::new(
                    Arc::new(TaskState::new(701, None)),
                    service_resource(),
                    budget.try_acquire(1).unwrap(),
                );
                let mut admission = admission;
                admission.retain_in_scope(&scope).unwrap();
                admission.retain_in_scope(&scope).unwrap();
                Ok::<_, ()>(())
            })
            .unwrap();

        assert!(matches!(
            operation.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(())
        ));
        assert!(operation.wait_settlement().unwrap().all_released());
    }

    #[test]
    fn test_pending_scoped_provider_io_is_reported_as_recoverable_failure() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let (state_tx, state_rx) = std::sync::mpsc::sync_channel(1);
        let (provider_started_tx, provider_started_rx) = std::sync::mpsc::sync_channel(1);
        let mut operation = manager
            .as_async()
            .try_submit(OperationOptions::default(), move |scope| async move {
                let budget = Arc::new(ByteBudget::new(1));
                let mut admission = RequestAdmission::new(
                    Arc::new(TaskState::new(705, None)),
                    service_resource(),
                    budget.try_acquire(1).unwrap(),
                );
                admission.retain_in_scope(&scope).unwrap();
                state_tx.send(Arc::clone(&admission.state)).unwrap();
                let pending_io = async move {
                    provider_started_tx.send(()).unwrap();
                    std::future::pending::<Result<(), ()>>().await
                };
                assert!(admission.run_provider_operation(&scope, pending_io).await.is_err());
                Ok::<_, ()>(())
            })
            .unwrap();

        let state = state_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        provider_started_rx
            .recv_timeout(std::time::Duration::from_secs(2))
            .unwrap();
        state.request_stop();

        assert!(matches!(
            block_on(operation.wait_outcome()).unwrap(),
            OperationOutcome::Succeeded(())
        ));
        let settlement = block_on(operation.wait_settlement());
        assert!(!settlement.all_released());
        assert_eq!(settlement.cleanup_failures_total, 1);
        assert_eq!(settlement.recovery_count, 1);
        let recovery = settlement.recoveries[0].clone();
        assert_eq!(recovery.snapshot().phase, RecoveryPhase::Pending);
        assert_eq!(
            settlement.cleanup_failures[0].resource_id,
            recovery.snapshot().resources[0]
        );

        let retry = recovery.try_retry().unwrap().wait().unwrap();
        assert_eq!(retry.recovery.phase, RecoveryPhase::Pending);
        assert_eq!(recovery.snapshot().failures_total, 1);
        assert!(!settlement.all_released());
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        let mut future = Box::pin(future);
        loop {
            match Pin::as_mut(&mut future).poll(&mut context) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn test_worker_completion_alone_keeps_provider_resources_until_driver_completion() {
        let service = service_resource();
        let budget = Arc::new(ByteBudget::new(8));
        let permit = budget.try_acquire(8).unwrap();
        let state = Arc::new(TaskState::new(700, None));
        let owner = Arc::new(RequestResourceOwner::new(Arc::downgrade(&state), service, permit, true));
        state.attach_owner(Arc::clone(&owner), None);

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
        let owner = Arc::new(RequestResourceOwner::new(Arc::downgrade(&state), service, permit, true));
        state.attach_owner(Arc::clone(&owner), None);

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
        let owner = RequestResourceOwner::new(Arc::downgrade(&state), service, permit, true);
        drop(state);

        owner.driver_done();
        owner.worker_done();

        assert!(budget.try_acquire(8).is_some());
    }

    #[test]
    fn test_unconfirmed_provider_io_completes_local_access_but_retains_permits() {
        let service = service_resource();
        let budget = Arc::new(ByteBudget::new(8));
        let permit = budget.try_acquire(8).unwrap();
        let state = Arc::new(TaskState::new(706, None));
        let owner = Arc::new(RequestResourceOwner::new(
            Arc::downgrade(&state),
            service,
            permit,
            false,
        ));
        state.attach_owner(Arc::clone(&owner), None);

        owner.mark_unconfirmed_provider_io();
        owner.worker_done();

        assert!(state.access_complete());
        assert!(!state.inner.lock().unwrap().released);
        assert!(budget.try_acquire(1).is_none());
    }

    #[test]
    fn test_public_operation_scope_can_retain_admission_resources() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let mut operation = manager
            .try_submit(qubit_operation::OperationOptions::default(), |scope| {
                let budget = Arc::new(ByteBudget::new(8));
                let permit = budget.try_acquire(8).unwrap();
                let mut admission =
                    RequestAdmission::new(Arc::new(TaskState::new(704, None)), service_resource(), permit);
                admission.retain_in_scope(&scope).unwrap();
                assert!(admission.state.request_resource_owner().is_some());
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
        let mut admission = RequestAdmission::new(Arc::clone(&state), service, permit);
        admission.begin_provider_operation().unwrap();
        admission.provider_polled = true;
        drop(admission);

        assert!(!state.inner.lock().unwrap().released);
        assert!(budget.try_acquire(1).is_none());
    }
}
