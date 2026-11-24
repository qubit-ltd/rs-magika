// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcome of waiting for a request body to start.

/// Result of racing body start against the submitted operation outcome.
///
/// # Type Parameters
///
/// * `T` - Outcome returned when the operation finishes before body start.
pub(in crate::managed_runtime::request_admission_handoff) enum BodyStartOr<T> {
    /// The child body entered and is waiting for its admission.
    BodyStarted,
    /// The child operation finished before its body-start handshake.
    Outcome(
        /// Outcome returned by the submitted child operation.
        T,
    ),
}
