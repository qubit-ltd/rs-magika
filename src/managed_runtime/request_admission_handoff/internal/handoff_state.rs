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

use super::super::super::worker_input::WorkerInput;
use super::super::RequestAdmission;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HandoffCloseReason {
    RequestStopped(&'static str),
    Abandoned,
}

impl HandoffCloseReason {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RequestStopped(reason) => reason,
            Self::Abandoned => "request admission handoff closed",
        }
    }
}

/// Admission and input moved together while the handoff is synchronized.
pub(in crate::managed_runtime::request_admission_handoff) struct RequestAdmissionPayload {
    pub(in crate::managed_runtime::request_admission_handoff) admission: RequestAdmission,
    pub(in crate::managed_runtime::request_admission_handoff) input: Option<WorkerInput>,
}

/// State guarded while admissions and task wakers are exchanged.
pub(in crate::managed_runtime::request_admission_handoff) struct HandoffState {
    /// Admission and payload retained until their atomic handoff.
    pub(in crate::managed_runtime::request_admission_handoff) caller_payload:
        Option<RequestAdmissionPayload>,
    /// Admission or registration failure handed to the child body.
    pub(in crate::managed_runtime::request_admission_handoff) child_admission:
        Option<Result<(RequestAdmission, Option<WorkerInput>), MimeError>>,
    /// Whether the submitted child body has entered its closure.
    pub(in crate::managed_runtime::request_admission_handoff) body_started: bool,
    /// Whether either party abandoned the handoff.
    pub(in crate::managed_runtime::request_admission_handoff) closed: bool,
    /// First event that closed this handoff.
    pub(in crate::managed_runtime::request_admission_handoff) close_reason:
        Option<HandoffCloseReason>,
    /// Caller future waiting for the child body to start.
    pub(in crate::managed_runtime::request_admission_handoff) caller_waker: Option<Waker>,
    /// Child future waiting for the caller to finish registration.
    pub(in crate::managed_runtime::request_admission_handoff) child_waker: Option<Waker>,
    /// Test observation that the child has registered while awaiting admission.
    #[cfg(test)]
    pub(in crate::managed_runtime::request_admission_handoff) child_waiting: bool,
}
