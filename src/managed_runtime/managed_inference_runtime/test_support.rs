// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Test-only worker construction helpers for managed inference runtime.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;
use std::sync::mpsc;
use std::sync::mpsc::SyncSender;

use magika::SyncInput;
use qubit_mime::MimeError;
use qubit_mime::MimeResult;
use qubit_operation::ChildOutcomeKind;
use qubit_operation::GroupOptions;
use qubit_operation::OperationManager;

use super::AdmissionGate;
use super::BridgeSyncInput;
use super::ByteBudget;
use super::Job;
use super::ManagedInferenceRuntime;
use super::WorkerInput;
use super::execute_classified;
use super::map_magika_error;
use super::worker_state::WorkerState;

/// Owned byte classifier used by the worker thread.
pub(crate) type ManagedInferenceEngine = Box<dyn Fn(Vec<u8>) -> MimeResult<Option<String>> + Send + 'static>;

impl ManagedInferenceRuntime {
    /// Creates a deterministic worker for lifecycle tests.
    ///
    /// # Parameters
    ///
    /// * `supervisor` - Supervisor used by the test's managed child scope.
    /// * `max_in_flight_bytes` - Aggregate payload limit for test jobs.
    /// * `engine` - Gated classifier executed by the worker.
    /// * `after_business` - Gate run after result publication but before
    ///   release.
    ///
    /// # Returns
    ///
    /// A test runtime whose worker uses `engine` and waits at `after_business`
    /// after publishing each result.
    #[cfg(test)]
    pub(crate) fn with_engine(
        supervisor: OperationManager,
        max_in_flight_bytes: usize,
        engine: ManagedInferenceEngine,
        after_business: Box<dyn Fn() + Send + 'static>,
    ) -> Self {
        Self::with_engine_and_worker_exit_hook(supervisor, max_in_flight_bytes, engine, after_business, Box::new(|| {}))
    }

    /// Creates a deterministic worker with a hook after all jobs are released.
    ///
    /// # Parameters
    ///
    /// * `supervisor` - Supervisor used to register and settle test operations.
    /// * `max_in_flight_bytes` - Aggregate payload limit enforced by the
    ///   runtime.
    /// * `engine` - Classifier run for each accepted test job.
    /// * `after_business` - Hook run after publishing each job result and
    ///   before releasing its operation resources.
    /// * `before_worker_exit` - Hook run after the worker observes channel
    ///   close.
    ///
    /// # Returns
    ///
    /// A test runtime using the supplied classifier and lifecycle hooks.
    #[cfg(test)]
    pub(crate) fn with_engine_and_worker_exit_hook(
        supervisor: OperationManager,
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
                    let Job { input, state, owner } = job;
                    if state.stop_requested() {
                        state.finish_business(
                            Err(MimeError::detector_backend(
                                "magika",
                                "inference stopped before execution",
                            )),
                            ChildOutcomeKind::Stopped,
                        );
                        drop(input);
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
        let service_group = supervisor.group(GroupOptions { capacity: 2 }).expect("operation group should construct");
        Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(WorkerState::new(worker)),
            service_group,
            active_requests: Arc::new(AtomicUsize::new(0)),
            max_in_flight_bytes,
            byte_budget: Arc::new(ByteBudget::new(max_in_flight_bytes)),
            tasks: Arc::new(Mutex::new(Vec::new())),
            admission_gate: Arc::new(Mutex::new(AdmissionGate::new())),
        }
    }
}
