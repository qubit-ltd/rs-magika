// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Admission and optional input held together until handoff transfer.

use super::super::super::worker_input::WorkerInput;
use super::super::RequestAdmission;

/// Keeps request capacity and its untransferred worker input under one owner.
pub(in crate::managed_runtime::request_admission_handoff) struct RequestAdmissionPayload {
    /// Request capacity retained until the parent scope accepts registration.
    pub(in crate::managed_runtime::request_admission_handoff) admission: RequestAdmission,
    /// Optional worker input released before capacity when transfer is closed.
    pub(in crate::managed_runtime::request_admission_handoff) input: Option<WorkerInput>,
}
