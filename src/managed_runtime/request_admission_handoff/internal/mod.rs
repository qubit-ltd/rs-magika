// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private state and ownership types for request admission handoffs.

mod body_start_or;
mod handoff_state;
mod request_admission_handoff_guard;

pub(super) use body_start_or::BodyStartOr;
pub(super) use handoff_state::HandoffCloseReason;
pub(super) use handoff_state::HandoffState;
pub(super) use handoff_state::RequestAdmissionPayload;
pub(crate) use request_admission_handoff_guard::RequestAdmissionHandoffGuard;
