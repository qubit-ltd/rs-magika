// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded Magika execution on a session-owning worker.

use std::future::poll_fn;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::sync::mpsc::SyncSender;
use std::task::Poll;

use magika::AsyncInput;
use magika::Error as MagikaError;
use magika::Features;
use magika::FeaturesOrRuled;
use magika::Session;
#[cfg(test)]
use magika::SyncInput;
use qubit_mime::MimeError;
use qubit_mime::MimeResult;
use qubit_operation::protocol::ManagedOperationControl;
use qubit_operation::report::ChildBusinessOutcome;
use qubit_operation::report::OperationId;
use qubit_operation::scope::OperationScope;
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ServiceGroup;
use qubit_operation::supervisor::ShutdownMode;

use super::bridge_sync_input::BridgeSyncInput;
use super::byte_budget::ByteBudget;
use super::drive_event::DriveEvent;
use super::inference_future::InferenceFuture;
use super::job::Job;
use super::managed_inference_shutdown_report::ManagedInferenceShutdownReport;
use super::request_admission::RequestAdmission;
use super::task_control::TaskControl;
use super::task_state::AdmissionGate;
use super::task_state::TaskState;
use super::worker_input::WorkerInput;
use crate::internal::async_sample_bridge::AsyncSampleBridge;
use crate::internal::async_sample_bridge::AsyncSampleDriver;
use crate::magika_mime_detector::content_type_to_mime;
use crate::magika_mime_detector::map_magika_error;

/// Monotonically allocates identifiers for runtime child operations.
static NEXT_CHILD_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// Runs inference and classifies the exact result shape used by both workers.
fn execute_classified<T>(
    work: impl FnOnce() -> MimeResult<T>,
) -> (MimeResult<T>, ChildBusinessOutcome) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)) {
        Ok(Ok(value)) => (Ok(value), ChildBusinessOutcome::Succeeded),
        Ok(Err(error)) => (Err(error), ChildBusinessOutcome::Failed),
        Err(_) => (
            Err(MimeError::detector_backend(
                "magika",
                "inference worker panicked",
            )),
            ChildBusinessOutcome::Panicked,
        ),
    }
}

/// Owned byte classifier used by the worker thread.
#[cfg(test)]
pub(crate) type ManagedInferenceEngine =
    Box<dyn Fn(Vec<u8>) -> MimeResult<Option<String>> + Send + 'static>;

/// Shareable managed execution runtime. One worker owns its Magika session and
/// one queued request; the supervisor service group admits at most one running
/// and one queued request.
///
/// # Examples
///
/// ```no_run
/// use std::time::Duration;
///
/// use qubit_magika::ManagedInferenceRuntime;
/// use qubit_operation::report::SupervisorLimits;
/// use qubit_operation::scope::ScopeOptions;
/// use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};
///
/// let supervisor = OperationSupervisor::new(SupervisorLimits::default());
/// let runtime = std::sync::Arc::new(ManagedInferenceRuntime::new(supervisor.clone(), 1_048_576)?);
/// let service = supervisor.service_group(2)?;
/// let request_runtime = std::sync::Arc::clone(&runtime);
/// let mut request = service.start(ScopeOptions::default(), move |scope| async move {
///     request_runtime.detect_bytes(&scope, b"%PDF-1.7".to_vec()).await
/// })?;
/// let _result = request.result_blocking()?;
/// let _settlement = request.released_blocking()?;
/// let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
/// assert!(report.worker_stopped);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ManagedInferenceRuntime {
    /// Admission queue whose capacity is shared with the supervisor group.
    sender: Mutex<Option<SyncSender<Job>>>,
    /// Session-owning worker, joined after service group settlement.
    worker: Mutex<WorkerState>,
    /// Finite managed child admission for active and queued jobs.
    service_group: ServiceGroup,
    /// Supervisor retained for all child-operation registrations.
    _supervisor: OperationSupervisor,
    /// Upper bound for bytes retained by one detector configuration.
    max_in_flight_bytes: usize,
    /// Aggregate bytes reserved by the active and queued jobs.
    byte_budget: Arc<ByteBudget>,
    /// Strong active tasks retained until aggregate resource release.
    tasks: Arc<Mutex<Vec<Arc<TaskState>>>>,
    /// Serializes admission and provider-start boundaries with shutdown.
    admission_gate: Arc<Mutex<AdmissionGate>>,
}

/// Retains the worker handle until actual exit and remembers its join result.
struct WorkerState {
    handle: Option<std::thread::JoinHandle<()>>,
    stopped: bool,
    panicked: bool,
}

impl WorkerState {
    fn new(handle: std::thread::JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
            stopped: false,
            panicked: false,
        }
    }
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
    /// Create a worker using the application-owned operation supervisor.
    ///
    /// # Parameters
    ///
    /// * `supervisor` - Application-owned supervisor retained by the runtime.
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
    pub fn new(supervisor: OperationSupervisor, max_in_flight_bytes: usize) -> MimeResult<Self> {
        if max_in_flight_bytes == 0 {
            return Err(MimeError::detector_backend(
                "magika",
                "runtime byte budget must be nonzero",
            ));
        }
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let (sender, receiver): (SyncSender<Job>, mpsc::Receiver<Job>) = mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
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
                while let Ok(job) = receiver.recv() {
                    let Job {
                        input,
                        state,
                        owner,
                    } = job;
                    let bridge = match &input {
                        WorkerInput::Bridge(bridge, _, _) => Some(Arc::clone(bridge)),
                        _ => None,
                    };
                    if state.stop_requested() {
                        state.finish_business(
                            Err(MimeError::detector_backend(
                                "magika",
                                "inference stopped before execution",
                            )),
                            ChildBusinessOutcome::Stopped,
                        );
                    } else {
                        state.mark_started();
                        let (result, outcome) =
                            execute_classified(|| infer_input(&mut session, input));
                        state.finish_business(result, outcome);
                    }
                    if let Some(bridge) = bridge {
                        bridge.close();
                    }
                    owner.worker_done();
                }
            })
            .map_err(|error| {
                MimeError::detector_backend("magika", format!("could not start worker: {error}"))
            })?;
        ready_receiver.recv().map_err(|_| {
            MimeError::detector_backend("magika", "worker exited during initialization")
        })??;
        let service_group = supervisor.service_group(2).map_err(|error| {
            MimeError::detector_backend(
                "magika",
                format!("could not create service group: {error}"),
            )
        })?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(WorkerState::new(worker)),
            service_group,
            _supervisor: supervisor,
            max_in_flight_bytes,
            byte_budget: Arc::new(ByteBudget::new(max_in_flight_bytes)),
            tasks: Arc::new(Mutex::new(Vec::new())),
            admission_gate: Arc::new(Mutex::new(AdmissionGate::new())),
        })
    }

    /// Creates a deterministic worker for lifecycle tests.
    ///
    /// # Parameters
    ///
    /// * `supervisor` - Supervisor used by the test's managed child scope.
    /// * `max_in_flight_bytes` - Aggregate payload limit for test jobs.
    /// * `engine` - Gated classifier executed by the worker.
    /// * `after_business` - Gate run after result publication but before
    ///   release.
    #[cfg(test)]
    pub(crate) fn with_engine(
        supervisor: OperationSupervisor,
        max_in_flight_bytes: usize,
        engine: ManagedInferenceEngine,
        after_business: Box<dyn Fn() + Send + 'static>,
    ) -> Self {
        Self::with_engine_and_worker_exit_hook(
            supervisor,
            max_in_flight_bytes,
            engine,
            after_business,
            Box::new(|| {}),
        )
    }

    /// Creates a deterministic worker with a hook after all jobs are released.
    #[cfg(test)]
    pub(crate) fn with_engine_and_worker_exit_hook(
        supervisor: OperationSupervisor,
        max_in_flight_bytes: usize,
        engine: ManagedInferenceEngine,
        after_business: Box<dyn Fn() + Send + 'static>,
        before_worker_exit: Box<dyn Fn() + Send + 'static>,
    ) -> Self {
        let (sender, receiver): (SyncSender<Job>, mpsc::Receiver<Job>) = mpsc::sync_channel(1);
        let worker = std::thread::Builder::new()
            .name("magika-test-inference".to_owned())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    let Job {
                        input,
                        state,
                        owner,
                    } = job;
                    if state.stop_requested() {
                        state.finish_business(
                            Err(MimeError::detector_backend(
                                "magika",
                                "inference stopped before execution",
                            )),
                            ChildBusinessOutcome::Stopped,
                        );
                    } else {
                        state.mark_started();
                        let (result, outcome) = execute_classified(|| match input {
                            WorkerInput::Bytes(bytes) => (engine)(bytes.into_vec()),
                            WorkerInput::Bridge(bridge, _, _) => {
                                let mut input = BridgeSyncInput(bridge);
                                let length = SyncInput::length(&input).unwrap();
                                let mut bytes = vec![0; usize::try_from(length).unwrap()];
                                match SyncInput::read_at(&mut input, &mut bytes, 0) {
                                    Ok(()) => (engine)(bytes),
                                    Err(error) => Err(map_magika_error(error)),
                                }
                            }
                            WorkerInput::Features(_, _) => Err(MimeError::detector_backend(
                                "magika",
                                "test engine only accepts owned bytes",
                            )),
                        });
                        state.finish_business(result, outcome);
                    }
                    after_business();
                    owner.worker_done();
                }
                before_worker_exit();
            })
            .expect("test inference worker should start");
        let service_group = supervisor
            .service_group(2)
            .expect("service group should construct");
        Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(WorkerState::new(worker)),
            service_group,
            _supervisor: supervisor,
            max_in_flight_bytes,
            byte_budget: Arc::new(ByteBudget::new(max_in_flight_bytes)),
            tasks: Arc::new(Mutex::new(Vec::new())),
            admission_gate: Arc::new(Mutex::new(AdmissionGate::new())),
        }
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
    pub async fn detect_bytes(
        &self,
        scope: &OperationScope,
        bytes: Vec<u8>,
    ) -> MimeResult<Option<String>> {
        let length = bytes.len();
        if length > self.max_in_flight_bytes() {
            return Err(MimeError::BufferLimitExceeded {
                requested: length,
                limit: self.max_in_flight_bytes(),
            });
        }
        self.submit(Some(scope), WorkerInput::Bytes(bytes.into_boxed_slice()))?
            .await
    }

    /// Classifies owned bytes through the worker without a parent scope.
    ///
    /// # Parameters
    ///
    /// * `bytes` - Owned input payload retained through worker completion.
    ///
    /// # Returns
    ///
    /// A MIME mapping or `None` for an unmapped Magika type.
    ///
    /// # Errors
    ///
    /// Returns a size-limit, capacity, Magika, or worker error.
    #[cfg(test)]
    pub(crate) async fn detect_bytes_unmanaged(
        &self,
        bytes: Vec<u8>,
    ) -> MimeResult<Option<String>> {
        let length = bytes.len();
        if length > self.max_in_flight_bytes() {
            return Err(MimeError::BufferLimitExceeded {
                requested: length,
                limit: self.max_in_flight_bytes(),
            });
        }
        self.submit(None, WorkerInput::Bytes(bytes.into_boxed_slice()))?
            .await
    }

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

    pub(crate) async fn detect_async_input_admitted<I>(
        &self,
        mut admission: RequestAdmission,
        input: I,
        max_bytes: usize,
        map_error: fn(MagikaError) -> MimeError,
    ) -> MimeResult<Option<String>>
    where
        I: AsyncInput + Send,
    {
        admission.begin_provider_operation()?;
        let length_result = input.length().await;
        admission.provider_operation_completed();
        let length = match length_result {
            Ok(length) => length,
            Err(error) => {
                let error = map_error(error);
                admission.settle_error(MimeError::detector_backend(
                    "magika",
                    "provider length lookup failed",
                ));
                return Err(error);
            }
        };
        let bridge = AsyncSampleBridge::new(length);
        let driver = bridge.driver();
        let state = Arc::clone(&admission.state);
        let inference =
            self.dispatch(admission, WorkerInput::Bridge(bridge, max_bytes, map_error))?;
        let (result, driver_completed) = drive_async_input(driver, input, inference, state.clone()).await;
        if driver_completed {
            state.mark_driver_done();
        }
        result
    }

    pub(crate) async fn detect_bytes_admitted(
        &self,
        admission: RequestAdmission,
        bytes: Vec<u8>,
    ) -> MimeResult<Option<String>> {
        self.dispatch(admission, WorkerInput::Bytes(bytes.into_boxed_slice()))?
            .await
    }

    /// Maximum total byte payload retained by running and queued jobs.
    ///
    /// # Returns
    ///
    /// The configured aggregate payload limit.
    pub const fn max_in_flight_bytes(&self) -> usize {
        self.max_in_flight_bytes
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
            .wait_blocking()
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
    fn submit(
        &self,
        scope: Option<&OperationScope>,
        input: WorkerInput,
    ) -> MimeResult<InferenceFuture> {
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

    fn reserve_request(
        &self,
        scope: Option<&OperationScope>,
        byte_count: usize,
    ) -> MimeResult<RequestAdmission> {
        self.reserve_request_with_hook(scope, byte_count, || {})
    }

    fn reserve_request_with_hook(
        &self,
        scope: Option<&OperationScope>,
        byte_count: usize,
        after_service_reservation: impl FnOnce(),
    ) -> MimeResult<RequestAdmission> {
        let admission_gate = self.admission_gate.lock().unwrap_or_else(|error| error.into_inner());
        if admission_gate.closed {
            return Err(MimeError::detector_backend("magika", "runtime admission is closed"));
        }
        let byte_permit = self.byte_budget.try_acquire(byte_count).ok_or_else(|| {
            MimeError::detector_backend("magika", "runtime in-flight byte budget is full")
        })?;
        let admission = self.service_group.try_admit().map_err(|error| {
            MimeError::detector_backend("magika", format!("runtime admission failed: {error}"))
        })?;
        after_service_reservation();
        let id = NEXT_CHILD_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let state = Arc::new(TaskState::new_registered(
            OperationId::new(id as u128),
            None,
            Some(Arc::downgrade(&self.tasks)),
            Some(Arc::downgrade(&self.admission_gate)),
        ));
        let control: Arc<dyn ManagedOperationControl> = Arc::new(TaskControl(Arc::clone(&state)));
        RequestAdmission::attach_scope(scope, Arc::clone(&control))?;
        self.tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .push(Arc::clone(&state));
        drop(admission_gate);
        Ok(RequestAdmission::new(
            OperationId::new(id as u128),
            state,
            control,
            admission,
            byte_permit,
        ))
    }

    fn dispatch(
        &self,
        admission: RequestAdmission,
        input: WorkerInput,
    ) -> MimeResult<InferenceFuture> {
        let control = admission.control();
        let sent_state = {
            let sender = self
                .sender
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match sender.as_ref() {
                Some(sender) => admission.dispatch(input, sender)?,
                None => {
                    let (sender, _receiver) = mpsc::sync_channel(0);
                    admission.dispatch(input, &sender)?
                }
            }
        };
        Ok(InferenceFuture::new(sent_state, control))
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
    /// Admission and worker termination facts. A false `worker_stopped` means
    /// the worker has not exited; its handle remains available for a later
    /// shutdown call to observe and reap. One deadline covers service
    /// admission/settlement and worker exit. Shutdown does not forcibly
    /// interrupt native inference or provider I/O.
    pub fn shutdown_blocking(
        &self,
        mode: ShutdownMode,
        timeout: std::time::Duration,
    ) -> ManagedInferenceShutdownReport {
        self.shutdown_blocking_with_hook(mode, timeout, || {})
    }

    fn shutdown_blocking_with_hook(
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
        let admission = self
            .service_group
            .shutdown_blocking(mode, remaining_timeout(deadline));
        if !admission.is_complete() {
            return ManagedInferenceShutdownReport {
                admission,
                worker_stopped: false,
                worker_panicked: false,
            };
        }
        self.sender
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        loop {
            let mut worker = self
                .worker
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            match worker.handle.as_ref() {
                Some(handle) if handle.is_finished() => {
                    let handle = worker
                        .handle
                        .take()
                        .expect("finished worker handle is present");
                    worker.panicked = handle.join().is_err();
                    worker.stopped = true;
                    return ManagedInferenceShutdownReport {
                        admission,
                        worker_stopped: worker.stopped,
                        worker_panicked: worker.panicked,
                    };
                }
                Some(_) => {
                    let panicked = worker.panicked;
                    drop(worker);
                    let remaining = remaining_timeout(deadline);
                    if remaining.is_zero() {
                        return ManagedInferenceShutdownReport {
                            admission,
                            worker_stopped: false,
                            worker_panicked: panicked,
                        };
                    }
                    std::thread::sleep(remaining.min(std::time::Duration::from_millis(1)));
                }
                None => {
                    return ManagedInferenceShutdownReport {
                        admission,
                        worker_stopped: worker.stopped,
                        worker_panicked: worker.panicked,
                    };
                }
            }
        }
    }
}

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
        WorkerInput::Bytes(bytes) => {
            match FeaturesOrRuled::extract_sync(&bytes[..]).map_err(map_magika_error)? {
                FeaturesOrRuled::Ruled(content_type) => Ok(content_type_to_mime(content_type)),
                FeaturesOrRuled::Features(features) => {
                    identify_features(session, &features, map_magika_error)
                }
            }
        }
        WorkerInput::Bridge(bridge, _, map_error) => {
            match FeaturesOrRuled::extract_sync(BridgeSyncInput(bridge)).map_err(map_error)? {
                FeaturesOrRuled::Ruled(content_type) => Ok(content_type_to_mime(content_type)),
                FeaturesOrRuled::Features(features) => {
                    identify_features(session, &features, map_error)
                }
            }
        }
        WorkerInput::Features(features, map_error) => {
            identify_features(session, &features, map_error)
        }
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
/// * `driver` - Cancellation guard for the bridge's async side.
/// * `input` - Provider input held by the async caller.
/// * `inference` - Worker result observer.
///
/// # Returns
///
/// The worker's MIME inference result.
async fn drive_async_input<I>(
    driver: AsyncSampleDriver,
    mut input: I,
    inference: InferenceFuture,
    state: Arc<TaskState>,
) -> (MimeResult<Option<String>>, bool)
where
    I: AsyncInput,
{
    let mut inference = Box::pin(inference);
    loop {
        let next_request = driver.next_request();
        let mut next_request = Box::pin(next_request);
        let event = poll_fn(|cx| {
            if let Poll::Ready(result) = inference.as_mut().poll(cx) {
                return Poll::Ready(DriveEvent::Result(result));
            }
            match next_request.as_mut().poll(cx) {
                Poll::Ready(request) => Poll::Ready(DriveEvent::Request(request)),
                Poll::Pending => Poll::Pending,
            }
        })
        .await;
        match event {
            DriveEvent::Result(result) => return (result, true),
            DriveEvent::Request(None) => return (inference.await, true),
            DriveEvent::Request(Some(request)) => {
                if !state.try_start_provider_operation() {
                    state.request_stop();
                    return (inference.await, true);
                }
                let mut buffer = vec![0; request.len()];
                let mut read = Box::pin(input.read_at(&mut buffer, request.offset()));
                let mut closed = Box::pin(driver.closed());
                let read_result = poll_fn(|cx| {
                    if let Poll::Ready(result) = read.as_mut().poll(cx) {
                        return Poll::Ready(Some(result));
                    }
                    if closed.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(None);
                    }
                    Poll::Pending
                })
                .await;
                let Some(read_result) = read_result else {
                    // The read future was dropped on bridge close. rs-fs does
                    // not expose whether provider-native work has completed.
                    drop(read);
                    drop(closed);
                    return (inference.await, false);
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;
    use std::time::Duration;

    use qubit_mime::MimeError;
    use qubit_operation::report::ChildBusinessOutcome;
    use qubit_operation::report::StopReason;
    use qubit_operation::report::SupervisorLimits;
    use qubit_operation::supervisor::OperationSupervisor;
    use qubit_operation::supervisor::ShutdownMode;

    use super::ManagedInferenceRuntime;
    use super::WorkerInput;
    use super::execute_classified;

    #[test]
    fn shutdown_scan_cannot_miss_a_request_between_service_and_task_registration() {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
            supervisor,
            8,
            Box::new(|_| Ok(None)),
            Box::new(|| {}),
        ));
        let reservation_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let reservation_wait = Arc::clone(&reservation_gate);
        let (reserved_tx, reserved_rx) = mpsc::channel();
        let (admission_tx, admission_rx) = mpsc::channel();
        let reserve_runtime = Arc::clone(&runtime);
        let reserver = std::thread::spawn(move || {
            let admission = reserve_runtime
                .reserve_request_with_hook(None, 8, move || {
                    reserved_tx.send(()).unwrap();
                    let (lock, changed) = &*reservation_wait;
                    let mut released = lock.lock().unwrap();
                    while !*released {
                        released = changed.wait(released).unwrap();
                    }
                })
                .unwrap();
            admission_tx.send(admission).unwrap();
        });
        reserved_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let (shutdown_started_tx, shutdown_started_rx) = mpsc::channel();
        let (scan_tx, scan_rx) = mpsc::channel();
        let shutdown_runtime = Arc::clone(&runtime);
        let shutdown = std::thread::spawn(move || {
            shutdown_started_tx.send(()).unwrap();
            shutdown_runtime.shutdown_blocking_with_hook(
                ShutdownMode::Cancel,
                Duration::from_millis(250),
                move || scan_tx.send(()).unwrap(),
            )
        });
        shutdown_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let scanned_before_registration = scan_rx.recv_timeout(Duration::from_millis(50)).is_ok();

        let (lock, changed) = &*reservation_gate;
        *lock.lock().unwrap() = true;
        changed.notify_one();
        let admission = admission_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        if !scanned_before_registration {
            scan_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        let _report = shutdown.join().unwrap();
        reserver.join().unwrap();

        assert!(
            admission.state.stop_requested(),
            "shutdown must stop every request admitted before its task scan"
        );
        drop(admission);
        let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));
    }

    #[test]
    fn committed_provider_admission_checks_stop_before_first_and_later_io() {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let service = supervisor.service_group(2).unwrap();
        let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
            supervisor,
            8,
            Box::new(|_| Ok(None)),
            Box::new(|| {}),
        ));

        let first_runtime = Arc::clone(&runtime);
        let mut first = service.start(qubit_operation::scope::ScopeOptions::default(), move |scope| async move {
            let mut admission = first_runtime.reserve_for_provider(Some(&scope), 8)?;
            scope.request_cancel(StopReason::UserCancelled);
            assert!(admission.begin_provider_operation().is_err());
            Ok::<_, MimeError>(())
        }).unwrap();
        let _ = first.result_blocking();

        let later_runtime = Arc::clone(&runtime);
        let mut later = service.start(qubit_operation::scope::ScopeOptions::default(), move |scope| async move {
            let mut admission = later_runtime.reserve_for_provider(Some(&scope), 8)?;
            admission.begin_provider_operation()?;
            admission.provider_operation_completed();
            scope.request_cancel(StopReason::UserCancelled);
            assert!(admission.begin_provider_operation().is_err());
            Ok::<_, MimeError>(())
        }).unwrap();
        let _ = later.result_blocking();

        let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));
    }

    #[test]
    fn scope_less_pending_read_keeps_budget_after_worker_done() {
        struct ThreadWake(std::thread::Thread);
        impl std::task::Wake for ThreadWake {
            fn wake(self: Arc<Self>) { self.0.unpark(); }
            fn wake_by_ref(self: &Arc<Self>) { self.0.unpark(); }
        }
        struct PendingReadInput(std::sync::mpsc::SyncSender<()>);
        impl magika::AsyncInput for PendingReadInput {
            fn length(&self) -> impl Future<Output = Result<u64, magika::Error>> {
                std::future::ready(Ok(1))
            }
            fn read_at(
                &mut self,
                _buffer: &mut [u8],
                _offset: u64,
            ) -> impl Future<Output = Result<(), magika::Error>> {
                let started = self.0.clone();
                async move {
                    started.send(()).unwrap();
                    std::future::pending::<()>().await;
                    Ok(())
                }
            }
        }
        fn map_error(_: magika::Error) -> MimeError {
            MimeError::detector_backend("magika", "test input failed")
        }

        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
            supervisor,
            8,
            Box::new(|_| Ok(None)),
            Box::new(|| {}),
        ));
        let admission = runtime.reserve_for_provider(None, 8).unwrap();
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let input = PendingReadInput(started_tx);
        let body_runtime = Arc::clone(&runtime);
        let mut future = Box::pin(async move {
            body_runtime
                .detect_async_input_admitted(admission, input, 8, map_error)
                .await
        });
        let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
        let mut context = Context::from_waker(&waker);
        loop {
            match started_rx.try_recv() {
                Ok(()) => break,
                Err(mpsc::TryRecvError::Disconnected) => panic!("provider input dropped before read"),
                Err(mpsc::TryRecvError::Empty) => {}
            }
            assert!(future.as_mut().poll(&mut context).is_pending());
            std::thread::park_timeout(Duration::from_millis(10));
        }
        let state = runtime.tasks.lock().unwrap().first().unwrap().clone();
        drop(future);
        assert!(state.wait_worker_done_blocking(Duration::from_secs(2)));
        assert!(!state.inner.lock().unwrap().released);
        assert!(runtime.reserve_for_provider(None, 1).is_err());
        let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_millis(10));
    }

    #[test]
    fn test_shared_worker_classification_covers_success_and_panic() {
        let (result, outcome) =
            execute_classified(|| Ok::<_, MimeError>(Some("text/plain".to_owned())));
        assert!(matches!(result, Ok(Some(value)) if value == "text/plain"));
        assert_eq!(outcome, ChildBusinessOutcome::Succeeded);

        let (result, outcome): (qubit_mime::MimeResult<()>, _) =
            execute_classified(|| panic!("engine panic"));
        assert!(result.is_err());
        assert_eq!(outcome, ChildBusinessOutcome::Panicked);
    }

    #[test]
    fn test_unmanaged_detection_enforces_limit_and_shutdown_joins_worker() {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        assert!(ManagedInferenceRuntime::new(supervisor.clone(), 0).is_err());

        let runtime = ManagedInferenceRuntime::with_engine(
            supervisor,
            8,
            Box::new(|bytes| Ok(Some(format!("application/x-{}", bytes.len())))),
            Box::new(|| {}),
        );
        assert!(format!("{runtime:?}").contains("ManagedInferenceRuntime"));
        assert_eq!(runtime.max_in_flight_bytes(), 8);
        assert!(matches!(
            block_on(runtime.detect_bytes_unmanaged(vec![0; 9])),
            Err(MimeError::BufferLimitExceeded { .. })
        ));
        assert_eq!(
            Some("application/x-3".to_owned()),
            block_on(runtime.detect_bytes_unmanaged(vec![1, 2, 3])).unwrap()
        );

        let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
        assert!(report.admission.is_complete());
        assert!(report.worker_stopped);
        assert!(!report.worker_panicked);
    }

    #[test]
    fn test_runtime_submit_rejects_aggregate_byte_budget_overflow() {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let engine_gate = Arc::clone(&gate);
        let (started_tx, started_rx) = mpsc::channel();
        let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
            supervisor,
            8,
            Box::new(move |_| {
                started_tx.send(()).unwrap();
                let (lock, ready) = &*engine_gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
                Ok(None)
            }),
            Box::new(|| {}),
        ));
        let poisoned_tasks = Arc::clone(&runtime);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned_tasks.tasks.lock().unwrap();
            panic!("poison runtime task list before submission");
        })
        .join();
        let poisoned_sender = Arc::clone(&runtime);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned_sender.sender.lock().unwrap();
            panic!("poison runtime sender before submission");
        })
        .join();
        let first = runtime
            .submit(None, WorkerInput::Bytes(vec![0; 5].into()))
            .expect("first job should fit the byte budget");
        started_rx.recv_timeout(Duration::from_secs(30)).unwrap();
        assert!(
            runtime
                .submit(None, WorkerInput::Bytes(vec![0; 5].into()))
                .is_err()
        );

        let (lock, ready) = &*gate;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        assert!(matches!(first.wait_blocking(), Ok(None)));
        let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
        assert!(report.worker_stopped);
    }

    #[test]
    fn test_shutdown_recovers_poisoned_runtime_mutexes() {
        let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
            OperationSupervisor::new(SupervisorLimits::default()),
            1,
            Box::new(|_| Ok(None)),
            Box::new(|| {}),
        ));
        let poisoned_tasks = Arc::clone(&runtime);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned_tasks.tasks.lock().unwrap();
            panic!("poison runtime task list for recovery test");
        })
        .join();
        let poisoned_sender = Arc::clone(&runtime);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned_sender.sender.lock().unwrap();
            panic!("poison runtime sender for recovery test");
        })
        .join();
        let poisoned_worker = Arc::clone(&runtime);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned_worker.worker.lock().unwrap();
            panic!("poison runtime worker for recovery test");
        })
        .join();

        let report = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));
        assert!(report.admission.is_complete());
        assert!(report.worker_stopped);
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
}
