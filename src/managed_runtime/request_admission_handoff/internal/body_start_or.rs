// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Observation returned while waiting for an operation outcome or body start.

use super::handoff_close_reason::HandoffCloseReason;

/// Result observed while racing body start against operation completion.
/// `BodyStarted` means the caller-specific handoff completed: parent callers
/// delivered admission or its registration error, while root callers claimed
/// the payload. `Closed` may occur before or after the child reports start.
///
/// # Type Parameters
///
/// * `T` - Outcome returned when the operation finishes before body start.
pub(crate) enum BodyStartOr<T> {
    /// The child crossed its start boundary and the caller completed transfer
    /// or root payload claim.
    BodyStarted,
    /// The handoff closed before caller transfer completed.
    Closed(
        /// First reason recorded when the caller or request closed the handoff.
        HandoffCloseReason,
    ),
    /// The child operation finished before its body-start handshake.
    Outcome(
        /// Outcome returned by the submitted child operation.
        T,
    ),
}
