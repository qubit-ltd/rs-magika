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
/// A timeout can leave `worker_stopped` false while the runtime retains the
/// worker handle. Call shutdown again to observe and reap the worker after it
/// exits; shutdown does not forcibly interrupt native inference or provider
/// I/O.
#[derive(Clone, Debug)]
pub struct ManagedInferenceShutdownReport {
    /// Service-group request and resource facts.
    pub admission: ShutdownReport,
    /// Whether the inference worker exited and was joined.
    pub worker_stopped: bool,
    /// Whether joining reported a worker panic.
    pub worker_panicked: bool,
}
