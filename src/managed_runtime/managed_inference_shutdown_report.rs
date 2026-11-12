// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Facts returned after a managed inference shutdown request.

use qubit_operation::supervisor::ShutdownReport;

/// Runtime shutdown facts, including whether the session-owning worker exited.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManagedInferenceShutdownReport {
    /// Service-group request and resource facts.
    pub admission: ShutdownReport,
    /// Whether the inference worker exited and was joined.
    pub worker_stopped: bool,
    /// Whether joining reported a worker panic.
    pub worker_panicked: bool,
}
