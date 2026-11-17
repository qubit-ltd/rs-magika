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
/// cover every detector sharing this runtime.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct ManagedInferenceSnapshot {
    /// Whether new runtime admission is closed.
    pub admission_closed: bool,
    /// Whether cooperative cancellation has begun.
    pub cancelling: bool,
    /// Whether the worker session faulted after a caught inference panic.
    pub faulted: bool,
    /// Unreleased requests not yet started by the worker, including provider
    /// setup and admission queue time; this is not an exact queue length.
    pub admitted_not_started: usize,
    /// Unreleased requests whose worker participant has not completed.
    pub worker_running: usize,
    /// Requests whose worker completed but whose resources remain retained.
    pub awaiting_release: usize,
    /// Unreleased requests dropped while provider I/O completion was unknown.
    pub unconfirmed_provider_io: usize,
    /// Currently reserved aggregate payload bytes.
    pub reserved_bytes: usize,
    /// Configured aggregate payload byte limit.
    pub max_in_flight_bytes: usize,
}
