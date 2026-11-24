// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Completion facts for the request resource release barrier.

/// Completion facts needed to release one request's local access barrier.
#[derive(Default)]
pub(super) struct RequestResourceCompletionFacts {
    /// Whether the provider sampling driver participates in the barrier.
    pub(super) driver_required: bool,
    /// Whether the provider sampling driver has completed.
    pub(super) driver_done: bool,
    /// Whether the worker has completed.
    pub(super) worker_done: bool,
    /// Whether the combined completion barrier has settled.
    pub(super) completed: bool,
}
