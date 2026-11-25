// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Non-atomic observations of one managed inference runtime's current state.

/// A point-in-time, best-effort view of runtime admission and request state.
///
/// Fields are read independently and may reflect different instants during
/// concurrent activity. The snapshot is diagnostic only: it is not an atomic
/// value, a release proof, or a replacement for waiting for settlement. Counts
/// cover every detector sharing this runtime. A later healthy or recovered
/// state alone does not establish that a previously pending request released
/// its provider or worker resources.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct ManagedInferenceSnapshot {
    /// Whether new runtime admission is closed.
    pub admission_closed: bool,
    /// Whether cooperative cancellation has begun.
    pub cancelling: bool,
    /// Whether the worker session faulted after a caught inference panic.
    /// This is separate from the shutdown report's `worker_panicked`, which
    /// records an uncaught panic observed while joining the worker.
    pub faulted: bool,
    /// Admitted, unreleased requests whose worker has not started processing
    /// them, including provider setup and admission queue time; this is not an
    /// exact queue length.
    pub admitted_not_started: usize,
    /// Unreleased requests whose worker participant has started but not
    /// completed. This can include time while the worker waits for a Provider
    /// sample read to be serviced by the caller's future.
    pub worker_running: usize,
    /// Requests whose worker completed but whose resources remain retained.
    pub awaiting_release: usize,
    /// Unreleased requests whose Provider operation completion or cleanup is
    /// unconfirmed. This includes dropped pending I/O and input teardown that
    /// panicked. The count alone does not show whether underlying I/O is still
    /// running.
    pub unconfirmed_provider_io: usize,
    /// Currently reserved aggregate payload bytes.
    pub reserved_bytes: usize,
    /// Configured aggregate payload byte limit.
    pub max_in_flight_bytes: usize,
}
