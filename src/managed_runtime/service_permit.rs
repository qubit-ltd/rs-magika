// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Runtime-local bounded admission for worker and provider requests.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Releases one runtime admission slot when the request's real users finish.
pub(super) struct ServicePermit {
    active: Arc<AtomicUsize>,
}

impl ServicePermit {
    /// Attempts to acquire one slot from the bounded runtime counter.
    pub(super) fn try_acquire(active: &Arc<AtomicUsize>, capacity: usize) -> Option<Self> {
        active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < capacity).then_some(current + 1)
            })
            .ok()?;
        Some(Self { active: Arc::clone(active) })
    }
}

impl Drop for ServicePermit {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}
