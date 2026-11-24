// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared lifecycle for the managed inference worker.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;

use qubit_mime::MimeError;
use qubit_mime::MimeResult;
use qubit_operation::ChildOutcomeKind;

use super::super::job::Job;
use super::super::task_state::AdmissionGate;
use super::WorkerInput;

/// Converts inference execution into a result and its matching child outcome.
///
/// # Type Parameters
///
/// * `T` - Value produced by the inference closure.
///
/// # Parameters
///
/// * `work` - Inference closure whose panic is contained at the worker
///   boundary.
///
/// # Returns
///
/// The closure result and a lifecycle outcome that distinguishes success,
/// failure, and panic.
pub(super) fn execute_classified<T>(work: impl FnOnce() -> MimeResult<T>) -> (MimeResult<T>, ChildOutcomeKind) {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)) {
        Ok(Ok(value)) => (Ok(value), ChildOutcomeKind::Succeeded),
        Ok(Err(error)) => (Err(error), ChildOutcomeKind::Failed),
        Err(_) => (
            Err(MimeError::detector_backend("magika", "inference worker panicked")),
            ChildOutcomeKind::Panicked,
        ),
    }
}

/// Receives and settles every job on the session-owning worker thread.
///
/// # Parameters
///
/// * `receiver` - Bounded queue of accepted jobs.
/// * `gate` - Admission gate closed after an inference panic.
/// * `infer` - Inference implementation for production or tests.
/// * `on_fault` - Callback after the worker closes admission for a fault; it
///   notifies affected provider tasks that were not queued to the worker.
/// * `after_business` - Hook after result publication and bridge closure.
/// * `before_exit` - Hook after the queue closes or a panic drains it.
pub(super) fn run_worker(
    receiver: mpsc::Receiver<Job>,
    gate: Arc<Mutex<AdmissionGate>>,
    infer: impl FnMut(WorkerInput) -> MimeResult<Option<String>>,
    on_fault: impl Fn(&Arc<super::super::task_state::TaskState>),
    after_business: impl Fn(),
    before_exit: impl FnOnce(),
) {
    run_worker_with_before_first_recv(receiver, gate, infer, || {}, on_fault, after_business, before_exit);
}

/// Runs the worker with an optional deterministic first-receive gate.
///
/// # Parameters
///
/// * `receiver` - Queue of admitted worker jobs.
/// * `gate` - Admission state synchronized with the queue.
/// * `infer` - Inference operation run for each input.
/// * `before_first_recv` - Hook called before receiving the first job.
/// * `on_fault` - Callback after the worker closes admission for a fault; it
///   notifies affected provider tasks that were not queued to the worker.
/// * `after_business` - Hook called after publishing each business result.
/// * `before_exit` - Hook called before the worker exits.
pub(super) fn run_worker_with_before_first_recv(
    receiver: mpsc::Receiver<Job>,
    gate: Arc<Mutex<AdmissionGate>>,
    mut infer: impl FnMut(WorkerInput) -> MimeResult<Option<String>>,
    before_first_recv: impl FnOnce(),
    on_fault: impl Fn(&Arc<super::super::task_state::TaskState>),
    after_business: impl Fn(),
    before_exit: impl FnOnce(),
) {
    before_first_recv();
    while let Ok(job) = receiver.recv() {
        let Job { input, state, owner } = job;
        let bridge = match &input {
            WorkerInput::Bridge(bridge, _, _) => Some(Arc::clone(bridge)),
            _ => None,
        };
        let mut terminal_fault = false;
        let stop_requested = state.stop_requested() || {
            let gate = gate.lock().unwrap_or_else(|error| error.into_inner());
            gate.cancelling || gate.faulted
        };
        if stop_requested {
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
            let (result, outcome) = execute_classified(|| infer(input));
            terminal_fault = outcome == ChildOutcomeKind::Panicked;
            if terminal_fault {
                // Serialize the terminal worker fault with dispatch before
                // publishing the panic result or releasing this job's slot.
                let mut gate = gate.lock().unwrap_or_else(|error| error.into_inner());
                gate.closed = true;
                gate.faulted = true;
                drop(gate);
                on_fault(&state);
            }
            state.finish_business(result, outcome);
        }
        if let Some(bridge) = bridge {
            bridge.close();
        }
        after_business();
        owner.worker_done();
        if terminal_fault {
            // Native session state after an unwind is undocumented.
            settle_queued_jobs(&receiver, "inference session discarded after worker panic");
            break;
        }
    }
    before_exit();
}

/// Completes queued work after the worker session has become unusable.
///
/// # Parameters
///
/// * `receiver` - Queue whose already accepted jobs must be completed.
/// * `reason` - Backend error message reported to queued operations.
fn settle_queued_jobs(receiver: &mpsc::Receiver<Job>, reason: &str) {
    for job in receiver.try_iter() {
        let Job { input, state, owner } = job;
        let bridge = match &input {
            WorkerInput::Bridge(bridge, _, _) => Some(Arc::clone(bridge)),
            _ => None,
        };
        state.finish_business(
            Err(MimeError::detector_backend("magika", reason)),
            ChildOutcomeKind::Failed,
        );
        drop(input);
        if let Some(bridge) = bridge {
            bridge.close();
        }
        owner.worker_done();
    }
}
