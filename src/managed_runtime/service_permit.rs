// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-local bounded admission for worker and provider requests.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Releases one runtime admission slot when the request's real users finish.
pub(super) struct ServicePermit {
    /// Shared active request count decremented when the permit is dropped.
    active: Arc<AtomicUsize>,
}

impl ServicePermit {
    /// Attempts to acquire one slot from the bounded runtime counter.
    ///
    /// # Parameters
    ///
    /// * `active` - Shared count of currently held slots.
    /// * `capacity` - Maximum number of concurrent requests.
    ///
    /// # Returns
    ///
    /// A permit when a slot is available, or `None` when the limit is reached.
    pub(super) fn try_acquire(active: &Arc<AtomicUsize>, capacity: usize) -> Option<Self> {
        active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < capacity).then_some(current + 1)
            })
            .ok()?;
        Some(Self {
            active: Arc::clone(active),
        })
    }
}

impl Drop for ServicePermit {
    /// Releases this request's runtime-local capacity slot.
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}
