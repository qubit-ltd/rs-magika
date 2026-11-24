// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Observation returned while waiting for an operation outcome or body start.

use super::handoff_close_reason::HandoffCloseReason;

/// Result observed while polling body start against operation completion.
/// An `Outcome` means the outcome future returned `Ready` before the start
/// signal was observed in that poll; it does not establish which event
/// physically completed first. The lower-level start wait reports only that
/// signal. `register_on_parent` returns `BodyStarted` after admission or its
/// registration error is delivered to the child, while `wait_for_root_or`
/// returns it after the root child claims the payload. `Closed` may occur
/// before or after the child reports start.
///
/// # Type Parameters
///
/// * `T` - Outcome returned when polling observes operation completion
///   before body start.
pub(crate) enum BodyStartOr<T> {
    /// The child body's start signal was observed. Higher-level caller
    /// methods may perform registration or payload transfer before returning
    /// this variant.
    BodyStarted,
    /// The handoff closed before caller transfer completed.
    Closed(
        /// First reason recorded when the caller or request closed the handoff.
        HandoffCloseReason,
    ),
    /// Polling observed the child outcome before the body-start handshake.
    /// This does not prove the operation completed earlier in wall-clock time.
    Outcome(
        /// Outcome returned by the submitted child operation.
        T,
    ),
}
