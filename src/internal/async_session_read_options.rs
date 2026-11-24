// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Configuration for one asynchronous session read.

/// Segmentation, cancellation, and in-flight state for an asynchronous read.
///
/// # Type Parameters
///
/// * `'a` - Lifetime of the optional marker borrowed during provider polling.
/// * `Stop` - Predicate that reports whether the owning request has stopped.
pub(crate) struct AsyncSessionReadOptions<'a, Stop> {
    /// Maximum bytes requested from the session in one provider operation.
    pub(crate) max_segment: Option<usize>,
    /// Checks whether the owning request stopped before another provider read.
    pub(crate) should_stop: Stop,
    /// Marks actual provider polling as in flight, excluding cooperative
    /// yields.
    pub(crate) io_in_flight: Option<&'a std::sync::atomic::AtomicBool>,
}
