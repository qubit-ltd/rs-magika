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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use qubit_mime::MimeError;
use qubit_mime::MimeResult;
use qubit_operation::ChildOutcomeKind;
use qubit_operation::OperationScope;
use qubit_operation::ResourceScopeError;
use qubit_operation::ResourceScopeOptions;

use super::byte_permit::BytePermit;
use super::job::Job;
use super::request_resource_cleanup::RequestResourceCleanup;
pub(super) use super::request_resource_owner::RequestResourceOwner;
use super::request_resource_scope_guard::RequestResourceScopeGuard;
use super::service_permit::ServicePermit;
use super::task_state::TaskState;
use super::worker_input::WorkerInput;

mod internal;
use internal::RejectedJob;

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
    /// Actual provider poll state for observed aggregate reads.
    provider_io_status: Option<Arc<AtomicBool>>,
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
            provider_io_status: None,
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
        self.run_provider_operation_inner(scope, operation, None).await
    }

    /// Polls an aggregate provider operation while observing actual-I/O state.
    ///
    /// The observer distinguishes provider I/O that remains pending from an
    /// aggregate future that yielded cooperatively between completed reads.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Aggregate provider operation future.
    ///
    /// # Parameters
    ///
    /// * `scope` - Parent operation scope whose stop signal is observed.
    /// * `operation` - Aggregate provider future polled while this admission
    ///   remains active.
    /// * `io_in_flight` - Shared flag set only while provider I/O is pending.
    ///
    /// # Returns
    ///
    /// The provider output, or a MIME error if the operation is stopped.
    ///
    /// # Errors
    ///
    /// Returns a backend error when the operation scope or request is stopped.
    pub(crate) async fn run_provider_operation_with_status<F>(
        &mut self,
        scope: &OperationScope,
        operation: F,
        io_in_flight: Arc<AtomicBool>,
    ) -> MimeResult<F::Output>
    where
        F: Future,
    {
        self.run_provider_operation_inner(scope, operation, Some(io_in_flight))
            .await
    }

    /// Polls provider I/O while tracking whether a pending poll is actual I/O.
    ///
    /// A stop before the first provider poll settles the admission cleanly. A
    /// stop after an I/O poll remains unconfirmed until the provider future
    /// reports completion.
    ///
    /// # Type Parameters
    ///
    /// * `F` - Provider operation future.
    ///
    /// # Parameters
    ///
    /// * `scope` - Parent operation scope whose stop signal is observed.
    /// * `operation` - Provider future polled while admission is active.
    /// * `io_in_flight` - Optional state tracking actual provider I/O.
    ///
    /// # Returns
    ///
    /// The provider output if it completes, or a MIME error if stopped.
    ///
    /// # Errors
    ///
    /// Returns a backend error when the operation scope or request is stopped.
    async fn run_provider_operation_inner<F>(
        &mut self,
        scope: &OperationScope,
        operation: F,
        io_in_flight: Option<Arc<AtomicBool>>,
    ) -> MimeResult<F::Output>
    where
        F: Future,
    {
        self.begin_provider_operation()?;
        if let Some(io_in_flight) = &io_in_flight {
            io_in_flight.store(false, Ordering::Release);
            self.provider_io_status = Some(Arc::clone(io_in_flight));
        }
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
            if io_in_flight.is_none() {
                self.provider_polled = true;
            }
            let result = operation.as_mut().poll(context);
            match result {
                std::task::Poll::Ready(output) => std::task::Poll::Ready(Some(output)),
                std::task::Poll::Pending => {
                    if let Some(io_in_flight) = &io_in_flight {
                        let pending = io_in_flight.load(Ordering::Acquire);
                        self.provider_pending = pending;
                        self.provider_polled = pending;
                    } else {
                        self.provider_polled = true;
                    }
                    std::task::Poll::Pending
                }
            }
        })
        .await;
        match outcome {
            Some(output) => {
                self.provider_operation_completed();
                Ok(output)
            }
            None => {
                if io_in_flight
                    .as_ref()
                    .is_some_and(|io_in_flight| io_in_flight.load(Ordering::Acquire))
                {
                    self.provider_pending = true;
                    self.provider_polled = true;
                } else if !self.provider_polled || io_in_flight.is_some() {
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
        let result = self.try_begin_provider_operation();
        if let Err(error) = result {
            self.settle_error(MimeError::detector_backend("magika", self.state.provider_stop_reason()));
            return Err(error);
        }
        Ok(())
    }

    /// Marks provider work started without settling on rejection, so an
    /// owned provider input can be destroyed before admission is released.
    /// The caller remains responsible for dropping that input before settling
    /// admission when this check fails. On rejection, no provider operation
    /// may begin; propagate the error after dropping the input and admission.
    pub(crate) fn try_begin_provider_operation(&mut self) -> MimeResult<()> {
        if !self.state.try_start_provider_operation() {
            self.state.request_stop();
            return Err(MimeError::detector_backend("magika", self.state.provider_stop_reason()));
        }
        self.provider_pending = true;
        self.provider_polled = true;
        Ok(())
    }

    /// Records that the provider operation completed normally.
    pub(crate) fn provider_operation_completed(&mut self) {
        self.provider_pending = false;
        self.provider_polled = false;
        self.provider_io_status = None;
    }

    /// Requires provider driver completion before this request can release.
    pub(crate) fn require_provider_driver(&mut self) {
        if let Some(owner) = self.state.request_resource_owner() {
            owner.require_driver();
            return;
        }
        let owner = Arc::new(RequestResourceOwner::new(
            Arc::downgrade(&self.state),
            self.service.take().expect("unowned admission retains service capacity"),
            self.bytes.take().expect("unowned admission retains byte capacity"),
            true,
        ));
        self.state.attach_owner(Arc::clone(&owner), None);
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
    /// The task state whose business result can be observed. Queue rejection
    /// is returned as `RejectedJob`; the caller must pass it to
    /// [`Self::settle_rejected_job`] so its input is dropped, its business
    /// result is failed, and its worker ownership is settled.
    pub(super) fn dispatch(
        mut self,
        input: WorkerInput,
        sender: &std::sync::mpsc::SyncSender<Job>,
    ) -> Result<Arc<TaskState>, RejectedJob> {
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
                self.dispatched = true;
                Err(RejectedJob { job: rejected, reason })
            }
        }
    }

    /// Releases a rejected job's input and completes its request state.
    /// This consumes the rejection and performs the required business and
    /// worker settlement; callers must not simply discard `RejectedJob`.
    ///
    /// # Parameters
    ///
    /// * `rejected` - Job and reason returned by a failed worker submission.
    ///
    /// # Returns
    ///
    /// The task state completed with the rejection error.
    pub(super) fn settle_rejected_job(rejected: RejectedJob) -> Arc<TaskState> {
        let Job { input, state, owner } = rejected.job;
        if let WorkerInput::Bridge(bridge, _, _) = &input {
            bridge.close();
        }
        drop(input);
        state.finish_business(
            Err(MimeError::detector_backend("magika", rejected.reason)),
            ChildOutcomeKind::Failed,
        );
        let result = Arc::clone(&state);
        owner.worker_done();
        result
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
        if scope.context().stop_reason().is_some() {
            return Err(MimeError::detector_backend(
                "magika",
                format!("operation scope rejected inference: {:?}", ResourceScopeError::Stopped),
            ));
        }
        let resource_scope = scope
            .resource_scope(ResourceScopeOptions { capacity: 1 })
            .map_err(|error| {
                MimeError::detector_backend("magika", format!("operation scope rejected inference: {error:?}"))
            })?;
        let request_scope = RequestResourceScopeGuard::new(resource_scope);
        let reservation = request_scope.scope().reserve_resource().map_err(|error| {
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
        let resource = match reservation.commit_with(Arc::clone(&owner), Arc::new(RequestResourceCleanup)) {
            Ok(resource) => resource,
            Err(error) => {
                drop(error);
                let (service, bytes) = owner
                    .take_resources()
                    .expect("unpublished request owner still holds admission resources");
                self.service = Some(service);
                self.bytes = Some(bytes);
                return Err(MimeError::detector_backend(
                    "magika",
                    "operation scope rejected inference resource commit",
                ));
            }
        };
        owner.set_resource_id(resource.id());
        self.state.attach_request_scope(request_scope);
        self.state.attach_owner(owner, Some(resource));
        Ok(())
    }
}

/// Settles or retains resources when an admission lease is abandoned.
impl Drop for RequestAdmission {
    /// Settles the lease or retains permits for unresolved provider I/O.
    fn drop(&mut self) {
        if !self.dispatched {
            let provider_io_unconfirmed = self
                .provider_io_status
                .as_ref()
                .is_some_and(|status| status.load(Ordering::Acquire));
            if provider_io_unconfirmed || (self.provider_pending && self.provider_polled) {
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
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Duration;

    use qubit_fs::Path;
    use qubit_operation::ManagerOptions;
    use qubit_operation::OperationManager;
    use qubit_operation::OperationOptions;
    use qubit_operation::OperationOutcome;
    use qubit_operation::RecoveryPhase;

    use super::super::service_permit::ServicePermit;
    use super::RequestAdmission;
    use super::RequestResourceOwner;
    use crate::internal::ManagedReadObserver;
    use crate::internal::StopWatcher;
    use crate::internal::provider_input::read_async_observed;
    use crate::internal::provider_input::read_options;
    use crate::internal::read_budget::ReadBudget;
    use crate::managed_runtime::byte_budget::ByteBudget;
    use crate::managed_runtime::task_state::TaskState;
    use crate::tests::internal::provider_input_fixture::ProviderFileSystemSpi;

    fn service_resource() -> ServicePermit {
        ServicePermit::try_acquire(&Arc::new(std::sync::atomic::AtomicUsize::new(0)), 1).unwrap()
    }

    #[test]
    fn test_scope_resource_registration_is_idempotent_for_one_admission() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let mut options = OperationOptions::default();
        options.limits.resources = 1;
        let operation = manager
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
        let mut operation = operation.into_async();

        assert!(matches!(
            block_on(operation.wait_outcome(), "idempotent resource registration outcome").unwrap(),
            OperationOutcome::Succeeded(())
        ));
        assert!(
            block_on(
                operation.wait_settlement(),
                "idempotent resource registration settlement"
            )
            .all_released()
        );
    }

    #[test]
    fn test_scope_resource_registration_reuses_parent_resource_and_scope_capacity() {
        use std::time::Duration;
        use std::time::Instant;

        use qubit_operation::ScopeLimits;

        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let options = OperationOptions {
            limits: ScopeLimits {
                resources: 1,
                resource_scopes: 1,
                ..ScopeLimits::default()
            },
            ..OperationOptions::default()
        };
        let operation = manager
            .try_submit(options, |scope| {
                let budget = Arc::new(ByteBudget::new(1));
                let first = RequestAdmission::new(
                    Arc::new(TaskState::new(709, None)),
                    service_resource(),
                    budget.try_acquire(1).unwrap(),
                );
                let mut first = first;
                first.retain_in_scope(&scope).unwrap();
                first.settle_error(qubit_mime::MimeError::detector_backend(
                    "magika",
                    "first request complete",
                ));

                let second = RequestAdmission::new(
                    Arc::new(TaskState::new(710, None)),
                    service_resource(),
                    budget.try_acquire(1).unwrap(),
                );
                let mut second = second;
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    match second.retain_in_scope(&scope) {
                        Ok(()) => break,
                        Err(error) if Instant::now() < deadline => {
                            assert!(
                                error.to_string().contains("CapacityExceeded"),
                                "unexpected request scope rejection: {error}"
                            );
                            std::thread::yield_now();
                        }
                        Err(error) => panic!("request scope capacity was not reusable: {error}"),
                    }
                }
                second.settle_error(qubit_mime::MimeError::detector_backend(
                    "magika",
                    "second request complete",
                ));
                Ok::<_, ()>(())
            })
            .unwrap();
        let mut operation = operation.into_async();

        assert!(matches!(
            block_on(operation.wait_outcome(), "reused scope capacity outcome").unwrap(),
            OperationOutcome::Succeeded(())
        ));
        assert!(block_on(operation.wait_settlement(), "reused scope capacity settlement").all_released());
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
            block_on(operation.wait_outcome(), "provider operation outcome").unwrap(),
            OperationOutcome::Succeeded(())
        ));
        let settlement = block_on(operation.wait_settlement(), "provider operation settlement");
        assert!(!settlement.all_released());
        assert_eq!(settlement.cleanup_failures_total, 1);
        assert_eq!(settlement.recovery_count, 1);
        let recovery = settlement.recoveries[0].clone();
        assert_eq!(recovery.snapshot().phase, RecoveryPhase::Pending);
        assert_eq!(
            settlement.cleanup_failures[0].resource_id,
            recovery.snapshot().resources[0]
        );

        let retry = block_on(
            recovery.try_retry().unwrap().wait_async(),
            "pending provider I/O recovery retry",
        );
        assert_eq!(retry.recovery.phase, RecoveryPhase::Pending);
        assert_eq!(recovery.snapshot().failures_total, 1);
        assert!(!settlement.all_released());
    }

    #[test]
    fn test_observed_provider_poll_unwind_retains_admission_resources() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let mut operation = manager
            .as_async()
            .try_submit(OperationOptions::default(), |scope| async move {
                let budget = Arc::new(ByteBudget::new(1));
                let mut admission = RequestAdmission::new(
                    Arc::new(TaskState::new(707, None)),
                    service_resource(),
                    budget.try_acquire(1).unwrap(),
                );
                admission.retain_in_scope(&scope).unwrap();
                let io_in_flight = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let status = Arc::clone(&io_in_flight);
                let panic_during_io_poll = std::future::poll_fn(move |_| {
                    status.store(true, Ordering::Release);
                    panic!("provider future unwound after actual I/O poll started");
                    #[allow(unreachable_code)]
                    Poll::<()>::Pending
                });
                let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    block_on(
                        admission.run_provider_operation_with_status(&scope, panic_during_io_poll, io_in_flight),
                        "provider I/O panic settlement",
                    )
                }));
                assert!(unwind.is_err());
                drop(admission);
                Ok::<_, ()>(())
            })
            .unwrap();

        assert!(matches!(
            block_on(operation.wait_outcome(), "provider operation outcome").unwrap(),
            OperationOutcome::Succeeded(())
        ));
        let settlement = block_on(operation.wait_settlement(), "provider operation settlement");
        assert!(!settlement.all_released());
        assert!(settlement.recovery_count > 0);
        assert!(settlement.cleanup_failures_total > 0);
    }

    #[test]
    fn test_observed_prefix_and_full_read_yields_cancel_without_next_io() {
        for full_read in [false, true] {
            let manager = OperationManager::new(ManagerOptions::default()).unwrap();
            let mut outer = manager
                .as_async()
                .try_submit(OperationOptions::default(), move |scope| async move {
                    let budget = Arc::new(ByteBudget::new(128));
                    let mut admission = RequestAdmission::new(
                        Arc::new(TaskState::new(708, None)),
                        service_resource(),
                        budget.try_acquire(128).unwrap(),
                    );
                    admission.retain_in_scope(&scope).unwrap();
                    let provider = ProviderFileSystemSpi::new(vec![0x41; 128])
                        .with_range()
                        .one_byte_reads();
                    let observations = provider.observations();
                    let file_system = provider.async_file_system();
                    let io_in_flight = Arc::new(AtomicBool::new(false));
                    let mut observer = ManagedReadObserver::new(
                        Some(admission.task_state()),
                        Arc::clone(&io_in_flight),
                        Some(StopWatcher::new(scope.context())),
                    );
                    let mut read_budget = ReadBudget::new(128);
                    let mut bytes = [0; 128];
                    let path = Path::parse("/prefix-yield").unwrap();
                    let read: Pin<Box<dyn Future<Output = magika::Result<()>>>> = if full_read {
                        Box::pin(async {
                            file_system
                                .read_all_observed(&path, Default::default(), 128, &mut observer)
                                .await
                                .map(|_| ())
                                .map_err(|error| match error {
                                    qubit_fs::read::ObservedReadError::Stopped => magika::Error::IOError(
                                        std::io::Error::new(std::io::ErrorKind::Interrupted, "read stopped"),
                                    ),
                                    qubit_fs::read::ObservedReadError::Provider(error) => {
                                        magika::Error::IOError(error.into_io_error())
                                    }
                                })
                        })
                    } else {
                        Box::pin(read_async_observed(
                            &file_system,
                            &path,
                            128,
                            read_options(0, bytes.len(), None),
                            &mut read_budget,
                            &mut bytes,
                            &mut observer,
                        ))
                    };
                    let state_for_cancel = admission.task_state();
                    let mut operation =
                        Box::pin(admission.run_provider_operation_with_status(&scope, read, Arc::clone(&io_in_flight)));
                    let waker = Waker::noop();
                    let mut context = Context::from_waker(waker);

                    let first_poll = operation.as_mut().poll(&mut context);
                    assert!(
                        first_poll.is_pending(),
                        "expected the {full_read} 64-read cooperative yield, got {first_poll:?} after {} bytes",
                        observations.read_bytes.load(Ordering::SeqCst)
                    );
                    assert_eq!(64, observations.read_bytes.load(Ordering::SeqCst));
                    assert!(
                        !io_in_flight.load(Ordering::Acquire),
                        "64 Ready reads followed by aggregate Pending must clear actual-I/O state"
                    );
                    state_for_cancel.request_stop();
                    assert!(matches!(operation.as_mut().poll(&mut context), Poll::Ready(Err(_))));
                    drop(operation);
                    drop(admission);
                    assert_eq!(64, observations.read_bytes.load(Ordering::SeqCst));
                    Ok::<_, ()>(())
                })
                .unwrap();

            assert!(matches!(
                block_on(outer.wait_outcome(), "outer operation outcome").unwrap(),
                OperationOutcome::Succeeded(())
            ));
            assert!(block_on(outer.wait_settlement(), "outer operation settlement").all_released());
        }
    }

    fn block_on<F: Future>(future: F, stage: &str) -> F::Output {
        crate::tests::internal::bridge_wait::poll_before(future, stage, Duration::from_secs(5))
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
        let operation = manager
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
        let mut operation = operation.into_async();
        assert!(matches!(
            block_on(operation.wait_outcome(), "public operation scope resource outcome").unwrap(),
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
