// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mutable synchronization state for a request admission handoff.

use std::task::Waker;

use qubit_mime::MimeError;

use super::super::RequestAdmission;

/// State guarded while admissions and task wakers are exchanged.
pub(in crate::managed_runtime::request_admission_handoff) struct HandoffState {
    /// Request whose scope registration is still owned by the caller.
    pub(in crate::managed_runtime::request_admission_handoff) caller_admission: Option<RequestAdmission>,
    /// Admission or registration failure handed to the child body.
    pub(in crate::managed_runtime::request_admission_handoff) child_admission:
        Option<Result<RequestAdmission, MimeError>>,
    /// Whether the submitted child body has entered its closure.
    pub(in crate::managed_runtime::request_admission_handoff) body_started: bool,
    /// Whether either party abandoned the handoff.
    pub(in crate::managed_runtime::request_admission_handoff) closed: bool,
    /// Caller future waiting for the child body to start.
    pub(in crate::managed_runtime::request_admission_handoff) caller_waker: Option<Waker>,
    /// Child future waiting for the caller to finish registration.
    pub(in crate::managed_runtime::request_admission_handoff) child_waker: Option<Waker>,
}
