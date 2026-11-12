// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate byte admission for running and queued inference jobs.

use std::sync::Arc;
use std::sync::Mutex;

use super::byte_permit::BytePermit;

/// Tracks the maximum owned payload bytes admitted by one runtime.
pub(super) struct ByteBudget {
    /// Maximum number of bytes retained across active jobs.
    maximum: usize,
    /// Number of bytes currently reserved by permits.
    used: Mutex<usize>,
}

impl ByteBudget {
    /// Attempts to reserve bytes and returns their release guard.
    ///
    /// # Parameters
    ///
    /// * `amount` - Bytes requested by one inference job.
    ///
    /// # Returns
    ///
    /// A permit when the checked total fits the configured maximum.
    pub(super) fn try_acquire(self: &Arc<Self>, amount: usize) -> Option<BytePermit> {
        let mut used = self.used.lock().unwrap_or_else(|error| error.into_inner());
        let next = used.checked_add(amount)?;
        if next > self.maximum {
            return None;
        }
        *used = next;
        Some(BytePermit::new(Arc::clone(self), amount))
    }

    /// Creates an empty accounting budget with the supplied capacity.
    ///
    /// # Parameters
    ///
    /// * `maximum` - Aggregate in-flight byte limit.
    pub(super) const fn new(maximum: usize) -> Self {
        Self {
            maximum,
            used: Mutex::new(0),
        }
    }

    /// Releases a permit's byte count from the aggregate total.
    ///
    /// # Parameters
    ///
    /// * `amount` - Previously reserved byte count.
    pub(super) fn release(&self, amount: usize) {
        let mut used = self.used.lock().unwrap_or_else(|error| error.into_inner());
        *used = used.saturating_sub(amount);
    }
}

#[cfg(test)]
mod tests {
    use super::ByteBudget;

    #[test]
    fn test_permits_reserve_and_release_aggregate_bytes() {
        let budget = std::sync::Arc::new(ByteBudget::new(5));
        let first = budget.try_acquire(3).expect("first reservation fits");
        assert!(budget.try_acquire(3).is_none());
        drop(first);
        assert!(budget.try_acquire(5).is_some());
    }

    #[test]
    fn test_overflowing_or_overlarge_reservations_are_rejected() {
        let budget = std::sync::Arc::new(ByteBudget::new(usize::MAX));
        let first = budget.try_acquire(usize::MAX).expect("maximum reservation fits");
        assert!(budget.try_acquire(1).is_none());
        drop(first);
        budget.release(1);
        assert!(budget.try_acquire(usize::MAX).is_some());
    }

    #[test]
    fn test_poisoned_budget_lock_is_recovered_for_acquire_and_release() {
        let budget = std::sync::Arc::new(ByteBudget::new(1));
        let poisoned = std::sync::Arc::clone(&budget);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned.used.lock().unwrap();
            panic!("poison budget lock for recovery test");
        })
        .join();
        let permit = budget.try_acquire(1).expect("poisoned lock should be recovered");
        drop(permit);
        budget.release(0);
    }
}
