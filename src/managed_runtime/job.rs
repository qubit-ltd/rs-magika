// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Admission and byte leases retained by one worker job.

use std::sync::Arc;

use qubit_operation::supervisor::ServiceRequest;

use super::byte_permit::BytePermit;
use super::task_state::TaskState;
use super::worker_input::WorkerInput;

/// Work payload and permits kept alive through worker release.
pub(super) struct Job {
    /// Owned inference input consumed by the worker.
    pub(super) input: WorkerInput,
    /// State reported to the operation observer.
    pub(super) state: Arc<TaskState>,
    /// Supervisor admission retained until the worker stops using the job.
    pub(super) admission: ServiceRequest,
    /// Aggregate payload capacity retained until the worker releases input.
    pub(super) byte_permit: BytePermit,
}
