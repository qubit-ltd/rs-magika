// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Facts returned after a managed inference shutdown request.

use qubit_operation::ShutdownReport;

/// Runtime shutdown facts, including whether the session-owning worker exited.
///
/// Admission counts come from the service group and local active request count;
/// generic categories that are not mapped are reported as zero. Those zeroes do
/// not establish that no domain request is queued or awaiting provider release.
/// Use [`super::ManagedInferenceRuntime::snapshot`] for a best-effort domain
/// view.
///
/// `worker_stopped` reports the worker join only; it is independent of
/// admission and provider settlement. It may be true while an unconfirmed
/// provider request still retains capacity. A false value means shutdown has
/// not observed a completed join; the runtime retains the handle so a later
/// shutdown call can continue observing and reap it. Shutdown does not
/// forcibly interrupt native inference or provider I/O.
///
/// The request's business outcome, the operation observer's bounded
/// settlement result, and this worker join fact are separate observations.
/// `Pending(snapshot)` means the observer's wait ended at its deadline;
/// `Ready(report)` is terminal but may still contain negative release evidence
/// such as unreleased resources. A negative settlement report does not imply
/// that the worker join is pending, nor does a completed join prove provider
/// I/O completion.
///
/// `worker_panicked` records an uncaught panic reported by joining the
/// worker. A panic caught during inference instead marks the runtime's
/// [`super::ManagedInferenceSnapshot::faulted`] state and is surfaced as a
/// detector error; it does not itself set `worker_panicked`.
#[derive(Clone, Debug)]
pub struct ManagedInferenceShutdownReport {
    /// Service-group request and resource facts.
    pub admission: ShutdownReport,
    /// Whether the inference worker exited and the runtime joined it. This is
    /// independent of request outcomes and provider/resource settlement.
    pub worker_stopped: bool,
    /// Whether joining reported a worker panic.
    pub worker_panicked: bool,
}
