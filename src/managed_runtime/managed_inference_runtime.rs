// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded Magika execution on a session-owning worker.

use std::future::Future;
use std::future::poll_fn;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc;
use std::sync::mpsc::SyncSender;
use std::task::Context;
use std::task::Poll;

use magika::AsyncInput;
use magika::Error as MagikaError;
use magika::Features;
use magika::FeaturesOrRuled;
use magika::Session;
use qubit_mime::MimeError;
use qubit_mime::MimeResult;
use qubit_operation::ChildRequirement;
use qubit_operation::GroupOptions;
use qubit_operation::OperationGroup;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationScope;
use qubit_operation::ShutdownMode;
use qubit_operation::ShutdownReport;
use qubit_operation::ShutdownSnapshot;

use super::bridge_sync_input::BridgeSyncInput;
use super::byte_budget::ByteBudget;
use super::drive_event::DriveEvent;
use super::inference_future::InferenceFuture;
use super::job::Job;
use super::managed_inference_shutdown_report::ManagedInferenceShutdownReport;
use super::managed_inference_snapshot::ManagedInferenceSnapshot;
use super::provider_driver_guard::ProviderDriverGuard;
use super::provider_read_event::ProviderReadEvent;
use super::request_admission::RequestAdmission;
use super::service_permit::ServicePermit;
use super::task_state::AdmissionGate;
use super::task_state::TaskState;
use super::worker_input::WorkerInput;
use super::worker_input::worker_bytes;
use crate::internal::AsyncProviderInput;
use crate::internal::INFERENCE_OUTCOME;
use crate::internal::async_sample_bridge::AsyncSampleBridge;
use crate::internal::map_operation_outcome;
use crate::magika_mime_detector::content_type_to_mime;
use crate::magika_mime_detector::map_magika_error;

#[path = "internal/worker_state.rs"]
mod worker_state;

use self::worker_state::WorkerState;

/// Keeps undispatched input destruction ahead of admission release.
struct UndispatchedRequest {
    admission: Option<RequestAdmission>,
    input: Option<WorkerInput>,
}

impl Drop for UndispatchedRequest {
    fn drop(&mut self) {
        drop(self.input.take());
        drop(self.admission.take());
    }
}

#[path = "internal/worker_loop.rs"]
mod worker_loop;

#[cfg(test)]
mod test_support;

/// Monotonically allocates identifiers for runtime child operations.
static NEXT_CHILD_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Maximum number of admitted requests retained by the runtime.
const SERVICE_CAPACITY: usize = 2;

/// Shareable managed execution runtime. One worker owns its Magika session and
/// one queued request; the supervisor service group admits at most one running
/// and one queued request.
///
/// # Examples
///
/// ```no_run
/// use std::error::Error;
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use qubit_magika::ManagedInferenceRuntime;
/// use qubit_operation::{GroupOptions, ManagerOptions, OperationManager, OperationOptions, ShutdownMode};
///
/// async fn example() -> Result<(), Box<dyn Error>> {
///     let manager = OperationManager::new(ManagerOptions::default())?;
///     let runtime = Arc::new(ManagedInferenceRuntime::new(manager.clone(), 1_048_576)?);
///     let service = manager.group(GroupOptions { capacity: 2 })?;
///     let request_runtime = Arc::clone(&runtime);
///     let mut request = service.as_async().try_submit(OperationOptions::default(), move |scope| async move {
///         request_runtime.detect_bytes(&scope, b"%PDF-1.7".to_vec()).await
///     })?;
///     let _outcome = request.wait_outcome().await?;
///     let _settlement = request.wait_settlement().await;
///     service.close_admission();
///     let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
///     assert!(report.worker_stopped);
///     Ok(())
/// }
/// ```
pub struct ManagedInferenceRuntime {
    /// Admission queue whose capacity is shared with the supervisor group.
    sender: Mutex<Option<SyncSender<Job>>>,
    /// Session-owning worker, joined after service group settlement.
    worker: Mutex<WorkerState>,
    /// Finite managed child admission for active and queued jobs.
    service_group: OperationGroup,
    /// Runtime request slots remain occupied until worker and provider release.
    active_requests: Arc<AtomicUsize>,
    /// Upper bound for bytes retained by one detector configuration.
    max_in_flight_bytes: usize,
    /// Aggregate bytes reserved by the active and queued jobs.
    byte_budget: Arc<ByteBudget>,
    /// Strong active tasks retained until aggregate resource release.
    tasks: Arc<Mutex<Vec<Arc<TaskState>>>>,
    /// Serializes admission and provider-start boundaries with shutdown.
    admission_gate: Arc<Mutex<AdmissionGate>>,
}

impl std::fmt::Debug for ManagedInferenceRuntime {
    /// Formats the runtime without exposing session or payload state.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Output formatter supplied by the caller.
    ///
    /// # Returns
    ///
    /// Formatting success or the formatter's error.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManagedInferenceRuntime")
            .finish_non_exhaustive()
    }
}

impl ManagedInferenceRuntime {
    /// Reads independent diagnostic facts for all requests sharing this
    /// runtime.
    ///
    /// The result is non-atomic and does not prove resources have been
    /// released; use operation settlement to observe release completion.
    pub fn snapshot(&self) -> ManagedInferenceSnapshot {
        let tasks = self.tasks.lock().unwrap_or_else(|error| error.into_inner()).clone();
        let mut admitted_not_started = 0;
        let mut worker_running = 0;
        let mut awaiting_release = 0;
        let mut unconfirmed_provider_io = 0;
        for task in tasks {
            let inner = task.inner.lock().unwrap_or_else(|error| error.into_inner());
            if inner.released {
                continue;
            }
            if !inner.started && !inner.worker_done {
                admitted_not_started += 1;
            } else if inner.started && !inner.worker_done {
                worker_running += 1;
            } else if inner.worker_done {
                awaiting_release += 1;
            }
            if inner.unconfirmed_provider_io {
                unconfirmed_provider_io += 1;
            }
        }
        let (admission_closed, cancelling, faulted) = {
            let gate = self.admission_gate.lock().unwrap_or_else(|error| error.into_inner());
            (gate.closed, gate.cancelling, gate.faulted)
        };
        ManagedInferenceSnapshot {
            admission_closed,
            cancelling,
            faulted,
            admitted_not_started,
            worker_running,
            awaiting_release,
            unconfirmed_provider_io,
            reserved_bytes: self.byte_budget.used(),
            max_in_flight_bytes: self.max_in_flight_bytes,
        }
    }

    /// Create a worker using the application-owned operation manager.
    ///
    /// # Parameters
    ///
    /// * `manager` - Application-owned manager retained by the runtime.
    /// * `max_in_flight_bytes` - Aggregate payload bound across admitted jobs.
    ///
    /// # Returns
    ///
    /// A runtime with an initialized session-owning worker.
    ///
    /// # Errors
    ///
    /// Returns an error for a zero byte limit, worker startup failure, or
    /// Magika session initialization failure.
    pub fn new(manager: OperationManager, max_in_flight_bytes: usize) -> MimeResult<Self> {
        if max_in_flight_bytes == 0 {
            return Err(MimeError::detector_backend(
                "magika",
                "runtime byte budget must be nonzero",
            ));
        }
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let (sender, receiver): (SyncSender<Job>, mpsc::Receiver<Job>) = mpsc::sync_channel(SERVICE_CAPACITY);
        let admission_gate = Arc::new(Mutex::new(AdmissionGate::new()));
        let worker_admission_gate = Arc::clone(&admission_gate);
        let tasks = Arc::new(Mutex::new(Vec::new()));
        let worker_tasks = Arc::clone(&tasks);
        let worker = match std::thread::Builder::new()
            .name("magika-inference".to_owned())
            .spawn(move || {
                let mut session = match Session::new() {
                    Ok(session) => {
                        let _ = ready_sender.send(Ok(()));
                        session
                    }
                    Err(error) => {
                        let _ = ready_sender.send(Err(map_magika_error(error)));
                        return;
                    }
                };
                worker_loop::run_worker(
                    receiver,
                    worker_admission_gate,
                    |input| infer_input(&mut session, input),
                    move |faulted| notify_unqueued_provider_tasks(&worker_tasks, faulted),
                    || {},
                    || {},
                );
            }) {
            Ok(worker) => worker,
            Err(error) => {
                return Err(MimeError::detector_backend(
                    "magika",
                    format!("could not start worker: {error}"),
                ));
            }
        };
        match ready_receiver.recv() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(MimeError::detector_backend(
                    "magika",
                    "worker exited during initialization",
                ));
            }
        }
        let service_group = match manager.group(GroupOptions {
            capacity: SERVICE_CAPACITY,
        }) {
            Ok(service_group) => service_group,
            Err(error) => {
                return Err(MimeError::detector_backend(
                    "magika",
                    format!("could not create operation group: {error}"),
                ));
            }
        };
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(WorkerState::new(worker)),
            service_group,
            active_requests: Arc::new(AtomicUsize::new(0)),
            max_in_flight_bytes,
            byte_budget: Arc::new(ByteBudget::new(max_in_flight_bytes)),
            tasks,
            admission_gate,
        })
    }

    /// Classify owned bytes as a child of `scope`.
    ///
    /// Sampling, feature extraction, and inference run on the runtime worker.
    /// This API is separate from `MimeDetector`'s legacy async hooks, which do
    /// not receive an `OperationScope`.
    ///
    /// # Parameters
    ///
    /// * `scope` - Parent scope that owns the inference lifecycle.
    /// * `bytes` - Owned input retained until the worker releases it.
    ///
    /// # Returns
    ///
    /// A MIME mapping or `None` for an unmapped Magika type.
    ///
    /// # Errors
    ///
    /// Returns an admission, size-limit, Magika, or worker error.
    pub async fn detect_bytes(self: &Arc<Self>, scope: &OperationScope, bytes: Vec<u8>) -> MimeResult<Option<String>> {
        let length = bytes.len();
        if length > self.max_in_flight_bytes() {
            return Err(MimeError::BufferLimitExceeded {
                requested: length,
                limit: self.max_in_flight_bytes(),
            });
        }
        self.submit_operation(Some(scope), WorkerInput::Bytes(worker_bytes(bytes.into_boxed_slice())))
            .await
    }

    /// Classifies borrowed content through the worker without a parent scope.
    ///
    /// Admission and the aggregate byte budget are reserved before copying
    /// the input into worker-owned storage. Sampling, feature extraction, and
    /// inference then run on the session-owning worker.
    ///
    /// # Parameters
    ///
    /// * `content` - Complete content bytes to inspect.
    ///
    /// # Returns
    ///
    /// A MIME mapping or `None` when Magika has no mapping.
    ///
    /// # Errors
    ///
    /// Returns a size-limit, capacity, Magika, or worker error.
    pub(crate) async fn detect_content_unmanaged(self: &Arc<Self>, content: &[u8]) -> MimeResult<Option<String>> {
        self.detect_content_unmanaged_with_copy(content, |content| content.to_vec().into_boxed_slice())
            .await
    }

    /// Reserves runtime capacity before copying borrowed content.
    ///
    /// # Parameters
    ///
    /// * `content` - Borrowed input retained only for this call.
    /// * `copy_content` - One-shot owned copy operation invoked after
    ///   admission.
    ///
    /// # Returns
    ///
    /// The MIME mapping or `None` when Magika has no mapping.
    ///
    /// # Errors
    ///
    /// Returns a size-limit, capacity, Magika, or worker error.
    async fn detect_content_unmanaged_with_copy(
        self: &Arc<Self>,
        content: &[u8],
        copy_content: impl FnOnce(&[u8]) -> Box<[u8]>,
    ) -> MimeResult<Option<String>> {
        let length = content.len();
        if length > self.max_in_flight_bytes() {
            return Err(MimeError::BufferLimitExceeded {
                requested: length,
                limit: self.max_in_flight_bytes(),
            });
        }
        let admission = self.reserve_request(None, length)?;
        let input = WorkerInput::Bytes(worker_bytes(copy_content(content)));
        self.submit_operation_admitted(None, admission, input).await
    }

    /// Classifies owned bytes through the worker without registering a parent.
    ///
    /// # Parameters
    ///
    /// * `bytes` - Owned payload retained until the worker releases the job.
    ///
    /// # Returns
    ///
    /// A MIME mapping, or `None` when Magika has no mapping.
    ///
    /// # Errors
    ///
    /// Returns a size-limit, capacity, Magika, or worker error.
    #[cfg(test)]
    pub(crate) async fn detect_bytes_unmanaged(self: &Arc<Self>, bytes: Vec<u8>) -> MimeResult<Option<String>> {
        let length = bytes.len();
        if length > self.max_in_flight_bytes() {
            return Err(MimeError::BufferLimitExceeded {
                requested: length,
                limit: self.max_in_flight_bytes(),
            });
        }
        self.submit_operation(None, WorkerInput::Bytes(worker_bytes(bytes.into_boxed_slice())))
            .await
    }

    /// Submits worker input through a public operation body.
    ///
    /// # Parameters
    ///
    /// * `parent` - Optional parent scope that owns the submitted child.
    /// * `input` - Input and resources retained by the inference worker.
    ///
    /// # Returns
    ///
    /// The worker's MIME mapping, or `None` when it has no mapping.
    ///
    /// # Errors
    ///
    /// Returns admission, worker, or inference errors.
    async fn submit_operation(
        self: &Arc<Self>,
        parent: Option<&OperationScope>,
        input: WorkerInput,
    ) -> MimeResult<Option<String>> {
        let byte_count = match &input {
            WorkerInput::Bytes(bytes) => bytes.len(),
            WorkerInput::Bridge(_, limit, _) => *limit,
            WorkerInput::Features(_, _) => 0,
        };
        // The request scope is registered inside the accepted child body,
        // after submission succeeds and before that body dispatches work.
        let admission = self.reserve_request(None, byte_count)?;
        self.submit_operation_admitted(parent, admission, input).await
    }

    /// Submits an operation using capacity acquired before input allocation.
    ///
    /// # Parameters
    ///
    /// * `parent` - Optional parent scope for the operation.
    /// * `admission` - Capacity and lifecycle state reserved for the request.
    /// * `input` - Owned input submitted to the inference worker.
    ///
    /// # Returns
    ///
    /// The MIME mapping or `None` when Magika has no mapping.
    ///
    /// # Errors
    ///
    /// Returns an operation, capacity, Magika, or worker error.
    async fn submit_operation_admitted(
        self: &Arc<Self>,
        parent: Option<&OperationScope>,
        admission: RequestAdmission,
        input: WorkerInput,
    ) -> MimeResult<Option<String>> {
        self.submit_operation_admitted_with_hook(parent, admission, input, || {}, |_| {})
            .await
    }

    /// Submits an admitted operation, with a test seam immediately before
    /// supervisor submission.
    ///
    /// # Parameters
    ///
    /// * `parent` - Optional parent scope that owns the submitted operation.
    /// * `admission` - Previously acquired request and byte capacity.
    /// * `input` - Owned input dispatched to the inference worker.
    /// * `before_submit` - One-shot hook called before the supervisor accepts
    ///   the operation.
    ///
    /// # Returns
    ///
    /// The MIME mapping produced by the worker, or an error if submission or
    /// inference fails.
    async fn submit_operation_admitted_with_hook(
        self: &Arc<Self>,
        parent: Option<&OperationScope>,
        admission: RequestAdmission,
        input: WorkerInput,
        before_submit: impl FnOnce(),
        before_retain: impl FnOnce(&OperationScope) + Send + 'static,
    ) -> MimeResult<Option<String>> {
        let runtime = Arc::clone(self);
        let cancellation_won = Arc::new(AtomicBool::new(false));
        let body_cancellation_won = Arc::clone(&cancellation_won);
        let state = Arc::clone(&admission.state);
        let (request_handoff, mut request_handoff_guard, body_payload) = match parent {
            Some(_) => {
                let (handoff, guard) =
                    super::request_admission_handoff::RequestAdmissionHandoff::new_with_input(admission, Some(input));
                (Some(handoff), Some(guard), None)
            }
            None => (
                None,
                None,
                Some(UndispatchedRequest {
                    admission: Some(admission),
                    input: Some(input),
                }),
            ),
        };
        let body_handoff = request_handoff.as_ref().map(Arc::clone);
        let mut before_retain = Some(before_retain);
        let body = move |scope| async move {
            let (admission, input) = if let Some(handoff) = body_handoff {
                handoff.body_started();
                match handoff.wait_for_child_admission(&scope).await {
                    Some(Ok((admission, Some(input)))) => (admission, input),
                    Some(Ok((admission, None))) => {
                        drop(admission);
                        return Err(MimeError::detector_backend(
                            "magika",
                            "request admission handoff omitted worker input",
                        ));
                    }
                    Some(Err(error)) => return Err(error),
                    None => {
                        return Err(MimeError::detector_backend(
                            "magika",
                            "request admission handoff closed before registration",
                        ));
                    }
                }
            } else {
                let mut payload = body_payload.expect("scope-less request retains its payload");
                let mut admission = payload.admission.take().expect("payload retains its admission");
                let input = payload.input.take().expect("payload retains its worker input");
                if let Some(hook) = before_retain.take() {
                    hook(&scope);
                }
                if let Err(error) = admission.retain_in_scope(&scope) {
                    drop(input);
                    drop(admission);
                    return Err(error);
                }
                (admission, input)
            };
            let mut inference = Box::pin(runtime.dispatch(admission, input)?);
            let mut stop = Box::pin(scope.context().stop_requested());
            let mut request_stop = Box::pin(state.wait_stopped());
            poll_fn(|cx: &mut Context<'_>| {
                if let Poll::Ready(result) = inference.as_mut().poll(cx) {
                    return Poll::Ready(result);
                }
                if stop.as_mut().poll(cx).is_ready() || request_stop.as_mut().poll(cx).is_ready() {
                    body_cancellation_won.store(true, std::sync::atomic::Ordering::Release);
                    state.request_stop();
                    return Poll::Ready(Err(MimeError::detector_backend("magika", state.provider_stop_reason())));
                }
                Poll::Pending
            })
            .await
        };
        let options = OperationOptions {
            requirement: ChildRequirement::Optional,
            ..OperationOptions::default()
        };
        before_submit();
        // This operation is an implementation detail behind a MIME Result;
        // preserve the result for the caller to handle instead of replacing
        // the parent operation's own outcome with ChildFailed.
        let mut operation = match parent {
            Some(scope) => scope.manager().as_async().try_submit(options.clone(), body),
            None => self.service_group.as_async().try_submit(options, body),
        }
        .map_err(|error| MimeError::detector_backend("magika", format!("operation submission failed: {error:?}")))?;
        let outcome_result = {
            let mut outcome = Box::pin(operation.wait_outcome());
            match (parent, request_handoff.as_ref(), request_handoff_guard.as_mut()) {
                (Some(parent), Some(handoff), Some(guard)) => {
                    match handoff.register_on_parent(parent, guard, &mut outcome).await {
                        super::request_admission_handoff::BodyStartOr::Outcome(outcome) => outcome,
                        super::request_admission_handoff::BodyStartOr::BodyStarted => outcome.await,
                        super::request_admission_handoff::BodyStartOr::Closed(reason) => {
                            drop(outcome);
                            let _ = operation.request_stop(qubit_operation::StopReason::UserCancelled);
                            return Err(MimeError::detector_backend("magika", reason.as_str()));
                        }
                    }
                }
                _ => outcome.await,
            }
        };
        let outcome = outcome_result.map_err(|error| {
            MimeError::detector_backend("magika", format!("operation outcome unavailable: {error:?}"))
        })?;
        if parent.is_none() && !cancellation_won.load(std::sync::atomic::Ordering::Acquire) {
            // Legacy unmanaged calls have no public handle through which a
            // caller can observe eventual settlement, so return only after
            // this operation's resources have actually been released.
            let _settlement = operation.wait_settlement().await;
        }
        map_operation_outcome(outcome, INFERENCE_OUTCOME)
    }

    /// Reserves operation and byte capacity before a provider performs I/O.
    ///
    /// # Parameters
    ///
    /// * `scope` - Optional parent operation that owns this inference request.
    /// * `max_bytes` - Maximum provider payload retained for the request.
    ///
    /// # Returns
    ///
    /// An admission token that must be used to dispatch or settle the request.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload exceeds the runtime limit or admission
    /// capacity is unavailable.
    pub(crate) fn reserve_for_provider(
        &self,
        scope: Option<&OperationScope>,
        max_bytes: usize,
    ) -> MimeResult<RequestAdmission> {
        if max_bytes > self.max_in_flight_bytes() {
            return Err(MimeError::BufferLimitExceeded {
                requested: max_bytes,
                limit: self.max_in_flight_bytes(),
            });
        }
        self.reserve_request(scope, max_bytes)
    }

    /// Drives provider reads for an already admitted async input.
    ///
    /// # Parameters
    ///
    /// * `admission` - Previously reserved operation and byte capacity.
    /// * `input` - Provider source read on demand by Magika sampling.
    /// * `max_bytes` - Bound used for provider range requests.
    /// * `map_error` - Route-specific conversion for Magika failures.
    ///
    /// # Returns
    ///
    /// A MIME mapping, or `None` when the input has no mapping.
    ///
    /// # Errors
    ///
    /// Returns provider, admission, Magika, or worker errors.
    pub(crate) async fn detect_async_input_admitted(
        &self,
        scope: &OperationScope,
        mut admission: RequestAdmission,
        input: AsyncProviderInput,
        max_bytes: usize,
        map_error: fn(MagikaError) -> MimeError,
    ) -> MimeResult<Option<String>> {
        let state = Arc::clone(&admission.state);
        let state_for_stop = Arc::clone(&state);
        let mut scope_stop = Box::pin(scope.context().stop_requested());
        let mut request_stop = Box::pin(state_for_stop.wait_stopped());
        let stop_state = Arc::clone(&state_for_stop);
        let setup_runtime = self;
        let setup = async move {
            if let Err(error) = admission.try_begin_provider_operation() {
                drop(input);
                drop(admission);
                return Err(error);
            }
            let length_result = input.length().await;
            admission.provider_operation_completed();
            let length = match length_result {
                Ok(length) => length,
                Err(error) => {
                    admission.settle_error(MimeError::detector_backend("magika", "provider length lookup failed"));
                    return Err(map_error(error));
                }
            };
            let bridge = AsyncSampleBridge::new(length);
            let driver = bridge.driver();
            let io_in_flight = input.io_in_flight();
            admission.require_provider_driver();
            let guard = ProviderDriverGuard::new_with_io_status(input, driver, Arc::clone(&state), io_in_flight);
            let inference = setup_runtime.dispatch(admission, WorkerInput::Bridge(bridge, max_bytes, map_error))?;
            Ok::<_, MimeError>((guard, inference))
        };
        let mut setup = Box::pin(setup);
        let (guard, inference) = poll_fn(|context| {
            if scope_stop.as_mut().poll(context).is_ready() {
                stop_state.request_stop();
                return Poll::Ready(Err(MimeError::detector_backend("magika", "provider inference stopped")));
            }
            if request_stop.as_mut().poll(context).is_ready() {
                return Poll::Ready(Err(MimeError::detector_backend("magika", "provider inference stopped")));
            }
            setup.as_mut().poll(context)
        })
        .await?;
        drive_async_input_with_stops(guard, inference, scope_stop, request_stop).await
    }

    #[cfg(test)]
    pub(crate) async fn detect_async_input_admitted_test<I>(
        &self,
        mut admission: RequestAdmission,
        input: I,
        max_bytes: usize,
        map_error: fn(MagikaError) -> MimeError,
    ) -> MimeResult<Option<String>>
    where
        I: AsyncInput + Send,
    {
        if let Err(error) = admission.try_begin_provider_operation() {
            drop(input);
            drop(admission);
            return Err(error);
        }
        let length_result = input.length().await;
        admission.provider_operation_completed();
        let length = match length_result {
            Ok(length) => length,
            Err(error) => {
                admission.settle_error(MimeError::detector_backend("magika", "provider length lookup failed"));
                return Err(map_error(error));
            }
        };
        let bridge = AsyncSampleBridge::new(length);
        let driver = bridge.driver();
        let state = Arc::clone(&admission.state);
        admission.require_provider_driver();
        let guard = ProviderDriverGuard::new(input, driver, Arc::clone(&state));
        let inference = self.dispatch(admission, WorkerInput::Bridge(bridge, max_bytes, map_error))?;
        drive_async_input(guard, inference).await
    }

    /// Dispatches owned bytes using capacity reserved by the caller.
    ///
    /// # Parameters
    ///
    /// * `admission` - Previously reserved operation and byte capacity.
    /// * `bytes` - Owned input retained until the worker releases it.
    ///
    /// # Returns
    ///
    /// A MIME mapping, or `None` when Magika has no mapping.
    ///
    /// # Errors
    ///
    /// Returns an error if worker dispatch or inference fails.
    pub(crate) async fn detect_bytes_admitted(
        &self,
        admission: RequestAdmission,
        bytes: Vec<u8>,
    ) -> MimeResult<Option<String>> {
        let state = Arc::clone(&admission.state);
        let inference = self.dispatch(admission, WorkerInput::Bytes(worker_bytes(bytes.into_boxed_slice())))?;
        await_inference_or_runtime_stop(inference, state).await
    }

    /// Maximum total byte payload retained by running and queued jobs.
    ///
    /// # Returns
    ///
    /// The configured aggregate payload limit.
    pub const fn max_in_flight_bytes(&self) -> usize {
        self.max_in_flight_bytes
    }

    /// Returns the runtime's bounded group for scope-less managed work.
    pub(crate) fn service_group(&self) -> OperationGroup {
        self.service_group.clone()
    }

    /// Runs already extracted features synchronously on the owning worker.
    ///
    /// # Parameters
    ///
    /// * `features` - Owned Magika features retained by the worker.
    /// * `map_error` - Route-specific Magika error conversion.
    ///
    /// # Returns
    ///
    /// The MIME mapping or `None` for an unmapped feature set.
    ///
    /// # Errors
    ///
    /// Returns a capacity, Magika, or worker error.
    pub(crate) fn identify_features_blocking(
        &self,
        features: Features,
        map_error: fn(MagikaError) -> MimeError,
    ) -> MimeResult<Option<String>> {
        self.submit(None, WorkerInput::Features(features, map_error))?
            .wait_blocking_settled()
    }

    /// Reserves operation and byte capacity before sending a job to the worker.
    ///
    /// # Parameters
    ///
    /// * `scope` - Optional parent scope for operation registration.
    /// * `input` - Owned work submitted to the session-owning worker.
    ///
    /// # Returns
    ///
    /// A future observing the worker result.
    ///
    /// # Errors
    ///
    /// Returns an error when capacity, scope registration, or worker admission
    /// rejects the request.
    fn submit(&self, scope: Option<&OperationScope>, input: WorkerInput) -> MimeResult<InferenceFuture> {
        let byte_count = match &input {
            WorkerInput::Bytes(bytes) => bytes.len(),
            WorkerInput::Bridge(_, byte_limit, _) => *byte_limit,
            // Magika keeps Features opaque and does not expose its allocation
            // length. It is a fixed-size model input, while the service group
            // bounds retained feature values to one running and one queued job.
            // The byte budget tracks caller payloads and provider read bounds.
            WorkerInput::Features(_, _) => 0,
        };
        let admission = self.reserve_request(scope, byte_count)?;
        self.dispatch(admission, input)
    }

    /// Acquires byte and service capacity for a request.
    ///
    /// # Parameters
    ///
    /// * `scope` - Optional parent operation that owns the request.
    /// * `byte_count` - Payload bytes reserved until resource release.
    ///
    /// # Returns
    ///
    /// An admission token that owns the acquired capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when shutdown has closed admission or a capacity or
    /// scope registration step fails.
    fn reserve_request(&self, scope: Option<&OperationScope>, byte_count: usize) -> MimeResult<RequestAdmission> {
        self.reserve_request_with_hook(scope, byte_count, || {})
    }

    /// Acquires request capacity and invokes a hook after service admission.
    ///
    /// The hook runs while the admission gate is held, allowing lifecycle tests
    /// to synchronize shutdown with registration without changing production
    /// admission behavior.
    ///
    /// # Parameters
    ///
    /// * `scope` - Optional parent operation that owns the request.
    /// * `byte_count` - Payload bytes reserved until resource release.
    /// * `after_service_reservation` - One-shot synchronization hook.
    ///
    /// # Returns
    ///
    /// An admission token that owns the acquired capacity.
    ///
    /// # Errors
    ///
    /// Returns an error when shutdown has closed admission or a capacity or
    /// scope registration step fails.
    fn reserve_request_with_hook(
        &self,
        scope: Option<&OperationScope>,
        byte_count: usize,
        after_service_reservation: impl FnOnce(),
    ) -> MimeResult<RequestAdmission> {
        let admission_gate = self.admission_gate.lock().unwrap_or_else(|error| error.into_inner());
        if admission_gate.closed {
            let reason = if admission_gate.faulted {
                "runtime admission is closed after worker fault"
            } else if admission_gate.cancelling {
                "runtime admission is closed after cancellation"
            } else {
                "runtime admission is closed"
            };
            return Err(MimeError::detector_backend("magika", reason));
        }
        let byte_permit = self
            .byte_budget
            .try_acquire(byte_count)
            .ok_or_else(|| MimeError::detector_backend("magika", "runtime in-flight byte budget is full"))?;
        let admission = ServicePermit::try_acquire(&self.active_requests, SERVICE_CAPACITY)
            .ok_or_else(|| MimeError::detector_backend("magika", "runtime service capacity is full"))?;
        after_service_reservation();
        let id = NEXT_CHILD_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let state = Arc::new(TaskState::new_registered(
            id,
            None,
            Some(Arc::downgrade(&self.tasks)),
            Some(Arc::downgrade(&self.admission_gate)),
        ));
        self.tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(Arc::clone(&state));
        drop(admission_gate);
        let mut request = RequestAdmission::new(state, admission, byte_permit);
        if let Some(scope) = scope {
            request.retain_in_scope(scope)?;
        }
        Ok(request)
    }

    /// Sends an admitted input to the worker and returns its result observer.
    ///
    /// # Parameters
    ///
    /// * `admission` - Token that owns operation and byte capacity.
    /// * `input` - Owned work sent to the session-owning worker.
    ///
    /// # Returns
    ///
    /// A future that observes the worker's inference result.
    ///
    /// # Errors
    ///
    /// Returns an error if the operation cannot be dispatched.
    fn dispatch(&self, admission: RequestAdmission, input: WorkerInput) -> MimeResult<InferenceFuture> {
        let admission_gate = self.admission_gate.lock().unwrap_or_else(|error| error.into_inner());
        if admission_gate.cancelling || admission_gate.faulted {
            let message = if admission_gate.faulted {
                "runtime admission is closed after worker fault"
            } else {
                "runtime admission is closed after cancellation"
            };
            drop(admission_gate);
            drop(input);
            let mut admission = admission;
            admission.settle_error(MimeError::detector_backend("magika", message));
            return Err(MimeError::detector_backend("magika", message));
        }
        let dispatch_result = {
            let sender = self.sender.lock().unwrap_or_else(|error| error.into_inner());
            admission.state.mark_dispatched();
            match sender.as_ref() {
                Some(sender) => admission.dispatch(input, sender),
                None => {
                    let (sender, _receiver) = mpsc::sync_channel(0);
                    admission.dispatch(input, &sender)
                }
            }
        };
        drop(admission_gate);
        let sent_state = match dispatch_result {
            Ok(state) => state,
            Err(rejected) => RequestAdmission::settle_rejected_job(rejected),
        };
        Ok(InferenceFuture::new(sent_state))
    }

    /// Stops admission and waits up to `timeout` for accepted worker jobs.
    ///
    /// # Parameters
    ///
    /// * `mode` - Drain queued work or request cooperative cancellation.
    /// * `timeout` - Maximum wait for child and worker release.
    ///
    /// # Returns
    ///
    /// Admission and worker termination facts. `worker_stopped` reports the
    /// join only and can be true while an unconfirmed provider request still
    /// retains capacity. False means shutdown has not observed a completed
    /// join; the worker handle remains available for a later call to continue
    /// observing and reap. One deadline covers service admission/settlement
    /// and worker exit. Shutdown does not forcibly interrupt native inference
    /// or provider I/O. A caught inference panic marks
    /// [`ManagedInferenceSnapshot::faulted`]; only an uncaught panic
    /// observed while joining sets
    /// [`ManagedInferenceShutdownReport::worker_panicked`].
    pub fn shutdown_blocking(
        &self,
        mode: ShutdownMode,
        timeout: std::time::Duration,
    ) -> ManagedInferenceShutdownReport {
        self.shutdown_blocking_with_hook(mode, timeout, || {})
    }

    /// Shuts down admission with an optional hook after the cancellation scan.
    ///
    /// The hook is used by lifecycle tests to observe the boundary between
    /// scanning active tasks and waiting for service settlement.
    ///
    /// # Parameters
    ///
    /// * `mode` - Drain queued work or request cooperative cancellation.
    /// * `timeout` - Maximum wait for child and worker release.
    /// * `after_task_scan` - One-shot hook run after active tasks are scanned.
    ///
    /// # Returns
    ///
    /// Admission and worker termination facts, including whether the worker
    /// stopped before the timeout.
    pub(crate) fn shutdown_blocking_with_hook(
        &self,
        mode: ShutdownMode,
        timeout: std::time::Duration,
        after_task_scan: impl FnOnce(),
    ) -> ManagedInferenceShutdownReport {
        let deadline = std::time::Instant::now().checked_add(timeout);
        let mut admission_gate = self.admission_gate.lock().unwrap_or_else(|error| error.into_inner());
        admission_gate.closed = true;
        admission_gate.cancelling |= mode == ShutdownMode::Cancel;
        drop(admission_gate);
        if mode == ShutdownMode::Cancel {
            let tasks = self.tasks.lock().unwrap_or_else(|error| error.into_inner()).clone();
            for task in tasks {
                task.request_stop();
            }
        }
        after_task_scan();
        loop {
            let admission = self.admission_report();
            let tasks = self.tasks.lock().unwrap_or_else(|error| error.into_inner());
            let local_access_complete = tasks.iter().all(|task| task.access_complete());
            drop(tasks);
            if admission.snapshot.active_operations == 0 && local_access_complete {
                break;
            }
            if remaining_timeout(deadline).is_zero() {
                let (worker_stopped, worker_panicked) = self.observe_finished_worker();
                return ManagedInferenceShutdownReport {
                    admission,
                    worker_stopped,
                    worker_panicked,
                };
            }
            std::thread::sleep(remaining_timeout(deadline).min(std::time::Duration::from_millis(1)));
        }
        let admission = self.admission_report();
        self.service_group.close_admission();
        self.sender.lock().unwrap_or_else(|error| error.into_inner()).take();
        loop {
            let (worker_stopped, worker_panicked) = self.observe_finished_worker();
            if worker_stopped {
                return ManagedInferenceShutdownReport {
                    admission,
                    worker_stopped,
                    worker_panicked,
                };
            }
            let remaining = remaining_timeout(deadline);
            if remaining.is_zero() {
                return ManagedInferenceShutdownReport {
                    admission,
                    worker_stopped: false,
                    worker_panicked,
                };
            }
            std::thread::sleep(remaining.min(std::time::Duration::from_millis(1)));
        }
    }

    /// Joins the worker when its thread has already finished and returns the
    /// facts retained across concurrent and subsequent shutdown calls.
    fn observe_finished_worker(&self) -> (bool, bool) {
        let mut worker = self.worker.lock().unwrap_or_else(|error| error.into_inner());
        if worker.handle.as_ref().is_some_and(|handle| handle.is_finished()) {
            let handle = worker.handle.take().expect("finished worker handle is present");
            worker.panicked = handle.join().is_err();
            worker.stopped = true;
        }
        (worker.stopped, worker.panicked)
    }

    /// Captures the runtime group and local request counts.
    ///
    /// # Returns
    ///
    /// A shutdown snapshot for this runtime's managed operations.
    fn admission_report(&self) -> ShutdownReport {
        let group = self.service_group.snapshot();
        ShutdownReport {
            snapshot: ShutdownSnapshot {
                active_operations: group.active_operations,
                queued_operations: 0,
                unconfirmed_users: 0,
                cleaning_resources: 0,
                recovery_groups: 0,
                service_resources: self.active_requests.load(std::sync::atomic::Ordering::Acquire),
            },
        }
    }
}

/// Wakes accepted provider operations whose input has not reached the worker.
///
/// # Parameters
///
/// * `tasks` - Runtime-local active request registry.
/// * `faulted` - Request whose inference panic closed worker delivery.
pub(super) fn notify_unqueued_provider_tasks(tasks: &Mutex<Vec<Arc<TaskState>>>, faulted: &Arc<TaskState>) {
    let waiting = tasks.lock().unwrap_or_else(|error| error.into_inner()).clone();
    for task in waiting {
        if !Arc::ptr_eq(&task, faulted) && !task.was_dispatched() {
            task.request_stop();
        }
    }
}

/// Observes a published worker result or runtime cancellation, whichever is
/// observed first. The inference future retains its stop control; dropping it
/// on cancellation requests worker stop while the worker continues to own its
/// input and admission resources until access is complete.
///
/// # Parameters
///
/// * `inference` - Worker result future retained until result or stop is
///   observed.
/// * `state` - Request lifecycle state used to observe runtime stop.
///
/// # Returns
///
/// The MIME result, or a backend error when runtime stop wins.
async fn await_inference_or_runtime_stop(
    inference: InferenceFuture,
    state: Arc<TaskState>,
) -> MimeResult<Option<String>> {
    let mut inference = Box::pin(inference);
    let mut stopped = Box::pin(state.wait_stopped());
    poll_fn(|context| {
        if let Poll::Ready(result) = inference.as_mut().poll(context) {
            return Poll::Ready(result);
        }
        if stopped.as_mut().poll(context).is_ready() {
            return Poll::Ready(Err(MimeError::detector_backend("magika", state.provider_stop_reason())));
        }
        Poll::Pending
    })
    .await
}

/// Computes the time left before a shared shutdown deadline.
///
/// # Parameters
///
/// * `deadline` - Absolute deadline, or `None` when the wait is unbounded.
///
/// # Returns
///
/// The remaining duration, saturating to zero after expiration.
fn remaining_timeout(deadline: Option<std::time::Instant>) -> std::time::Duration {
    deadline
        .map(|deadline| deadline.saturating_duration_since(std::time::Instant::now()))
        .unwrap_or(std::time::Duration::MAX)
}

/// Applies Magika sampling and model inference on the owning worker thread.
///
/// # Parameters
///
/// * `session` - Worker-owned initialized Magika session.
/// * `input` - Owned bytes, bridged async input, or extracted features.
///
/// # Returns
///
/// The MIME mapping for a classified input, or `None` for unmapped types.
fn infer_input(session: &mut Session, input: WorkerInput) -> MimeResult<Option<String>> {
    match input {
        WorkerInput::Bytes(bytes) => match FeaturesOrRuled::extract_sync(&bytes[..]).map_err(map_magika_error)? {
            FeaturesOrRuled::Ruled(content_type) => Ok(content_type_to_mime(content_type)),
            FeaturesOrRuled::Features(features) => identify_features(session, &features, map_magika_error),
        },
        WorkerInput::Bridge(bridge, _, map_error) => {
            match FeaturesOrRuled::extract_sync(BridgeSyncInput(bridge)).map_err(map_error)? {
                FeaturesOrRuled::Ruled(content_type) => Ok(content_type_to_mime(content_type)),
                FeaturesOrRuled::Features(features) => identify_features(session, &features, map_error),
            }
        }
        WorkerInput::Features(features, map_error) => identify_features(session, &features, map_error),
    }
}

/// Runs one owned feature set through the worker-owned session.
///
/// # Parameters
///
/// * `session` - Session owned by the current worker thread.
/// * `features` - Features extracted from the current input.
/// * `map_error` - Route-specific conversion for Magika failures.
///
/// # Returns
///
/// The recognized MIME type, or `None` when Magika has no mapping.
fn identify_features(
    session: &mut Session,
    features: &Features,
    map_error: fn(MagikaError) -> MimeError,
) -> MimeResult<Option<String>> {
    session
        .identify_features_sync(features)
        .map(|file_type| file_type.content_type().and_then(content_type_to_mime))
        .map_err(map_error)
}

/// Serves sampler range requests while keeping executor work asynchronous.
///
/// # Type Parameters
///
/// * `I` - Provider input whose range reads are awaitable.
///
/// # Parameters
///
/// * `guard` - Owns the provider input and bridge driver.
/// * `inference` - Worker result observer.
///
/// # Returns
///
/// The worker's MIME inference result.
#[cfg(test)]
async fn drive_async_input<I>(guard: ProviderDriverGuard<I>, inference: InferenceFuture) -> MimeResult<Option<String>>
where
    I: AsyncInput,
{
    drive_async_input_with_stops(
        guard,
        inference,
        std::future::pending::<()>(),
        std::future::pending::<()>(),
    )
    .await
}

/// Serves provider requests while observing a stop signal after the worker
/// result has had the first chance to win.
///
/// # Type Parameters
///
/// * `I` - Async provider input used to service worker requests.
/// * `S` - Future that resolves when the owning runtime scope stops.
/// * `R` - Future that resolves when this inference request stops.
///
/// # Parameters
///
/// * `guard` - Provider driver and lifecycle state owned by this request.
/// * `inference` - Worker inference future whose result takes precedence.
/// * `scope_stop` - Owning scope stop notification.
/// * `request_stop` - Request-specific stop notification.
async fn drive_async_input_with_stops<I, S, R>(
    mut guard: ProviderDriverGuard<I>,
    inference: InferenceFuture,
    scope_stop: S,
    request_stop: R,
) -> MimeResult<Option<String>>
where
    I: AsyncInput,
    S: Future,
    R: Future,
{
    let mut inference = Box::pin(inference);
    let mut scope_stop = Box::pin(scope_stop);
    let mut request_stop = Box::pin(request_stop);
    loop {
        let next_request = guard.driver.as_ref().expect("driver remains owned").next_request();
        let mut next_request = Box::pin(next_request);
        let event = poll_fn(|cx| {
            if let Poll::Ready(result) = inference.as_mut().poll(cx) {
                return Poll::Ready(DriveEvent::Result(result));
            }
            if scope_stop.as_mut().poll(cx).is_ready() {
                return Poll::Ready(DriveEvent::Stopped);
            }
            if request_stop.as_mut().poll(cx).is_ready() {
                return Poll::Ready(DriveEvent::Stopped);
            }
            match next_request.as_mut().poll(cx) {
                Poll::Ready(request) => Poll::Ready(DriveEvent::Request(request)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await;
        match event {
            DriveEvent::Result(result) => return result,
            DriveEvent::Stopped => {
                guard.state.request_stop();
                return Err(MimeError::detector_backend("magika", "provider inference stopped"));
            }
            DriveEvent::Request(None) => {
                guard.state.request_stop();
                return finish_after_driver_close(
                    &mut inference,
                    &mut scope_stop,
                    &mut request_stop,
                    guard.state.provider_stop_reason(),
                )
                .await;
            }
            DriveEvent::Request(Some(request)) => {
                if !guard.state.try_start_provider_operation() {
                    guard.state.request_stop();
                    return finish_after_driver_close(
                        &mut inference,
                        &mut scope_stop,
                        &mut request_stop,
                        guard.state.provider_stop_reason(),
                    )
                    .await;
                }
                let mut buffer = vec![0; request.len()];
                let mut read = Box::pin(
                    guard
                        .input
                        .as_mut()
                        .expect("input remains owned")
                        .read_at(&mut buffer, request.offset()),
                );
                let mut closed = Box::pin(guard.driver.as_ref().expect("driver remains owned").closed());
                let read_result = poll_fn(|cx| {
                    if let Poll::Ready(result) = inference.as_mut().poll(cx) {
                        return Poll::Ready(ProviderReadEvent::Result(result));
                    }
                    if scope_stop.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(ProviderReadEvent::Stopped);
                    }
                    if request_stop.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(ProviderReadEvent::Stopped);
                    }
                    if closed.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(ProviderReadEvent::Closed);
                    }
                    if guard.io_in_flight.is_none() {
                        guard.unconfirmed_io.store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                    match read.as_mut().poll(cx) {
                        Poll::Ready(result) => {
                            guard.unconfirmed_io.store(false, std::sync::atomic::Ordering::Relaxed);
                            Poll::Ready(ProviderReadEvent::Read(result))
                        }
                        Poll::Pending => {
                            let pending = guard
                                .io_in_flight
                                .as_ref()
                                .is_none_or(|status| status.load(std::sync::atomic::Ordering::Acquire));
                            guard
                                .unconfirmed_io
                                .store(pending, std::sync::atomic::Ordering::Relaxed);
                            Poll::Pending
                        }
                    }
                })
                .await;
                let read_result = match read_result {
                    ProviderReadEvent::Result(result) => return result,
                    ProviderReadEvent::Stopped => {
                        guard.state.request_stop();
                        drop(read);
                        drop(closed);
                        return Err(MimeError::detector_backend("magika", "provider inference stopped"));
                    }
                    ProviderReadEvent::Closed => {
                        // The read future was dropped on bridge close. rs-fs
                        // does not expose whether provider-native work completed.
                        drop(read);
                        drop(closed);
                        guard.state.request_stop();
                        return finish_after_driver_close(
                            &mut inference,
                            &mut scope_stop,
                            &mut request_stop,
                            guard.state.provider_stop_reason(),
                        )
                        .await;
                    }
                    ProviderReadEvent::Read(result) => result,
                };
                drop(read);
                drop(closed);
                let result = match read_result {
                    Ok(()) => Ok(buffer),
                    Err(MagikaError::IOError(error)) => Err(error),
                    Err(error) => Err(std::io::Error::other(error)),
                };
                request.respond(result);
            }
        }
    }
}

/// Finishes a bridge closed path while observing published outcome and stops.
/// The worker outcome is polled first so a result already published at close
/// remains observable without polling the provider again.
///
/// # Type Parameters
///
/// * `S` - Future that reports the parent scope stopping.
/// * `R` - Future that reports the request stopping.
///
/// # Parameters
///
/// * `inference` - Submitted worker outcome, polled before either stop signal.
/// * `scope_stop` - Parent scope stop signal.
/// * `request_stop` - Request-specific stop signal.
/// * `stop_reason` - Backend error detail returned when a stop wins.
///
/// # Returns
///
/// The already-published inference result, or a backend stop error.
///
/// # Errors
///
/// Returns the worker's inference error or the backend error for the winning
/// stop signal.
async fn finish_after_driver_close<S, R>(
    inference: &mut std::pin::Pin<Box<InferenceFuture>>,
    scope_stop: &mut std::pin::Pin<Box<S>>,
    request_stop: &mut std::pin::Pin<Box<R>>,
    stop_reason: &'static str,
) -> MimeResult<Option<String>>
where
    S: Future,
    R: Future,
{
    poll_fn(|cx| {
        if let Poll::Ready(result) = inference.as_mut().poll(cx) {
            return Poll::Ready(result);
        }
        if scope_stop.as_mut().poll(cx).is_ready() || request_stop.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(MimeError::detector_backend("magika", stop_reason)));
        }
        Poll::Pending
    })
    .await
}

#[cfg(test)]
mod tests;
