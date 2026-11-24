// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private state and ownership types for request admission handoffs.

mod body_start_or;
mod handoff_close_reason;
mod handoff_state;
mod request_admission_handoff_guard;
mod request_admission_payload;

pub(crate) use body_start_or::BodyStartOr;
pub(crate) use handoff_close_reason::HandoffCloseReason;
pub(super) use handoff_state::HandoffState;
pub(crate) use request_admission_handoff_guard::RequestAdmissionHandoffGuard;
pub(super) use request_admission_payload::RequestAdmissionPayload;
