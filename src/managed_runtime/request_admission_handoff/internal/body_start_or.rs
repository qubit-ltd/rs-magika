// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcome of waiting for a request body to start.

use super::handoff_close_reason::HandoffCloseReason;

/// Result of racing body start against the submitted operation outcome.
///
/// # Type Parameters
///
/// * `T` - Outcome returned when the operation finishes before body start.
pub(crate) enum BodyStartOr<T> {
    /// The child crossed its start boundary, and its caller-specific handoff
    /// step completed the transfer or root payload claim.
    BodyStarted,
    /// The handoff closed before the child body started.
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
