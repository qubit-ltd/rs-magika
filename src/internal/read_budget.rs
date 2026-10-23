// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cumulative byte budgets for provider-backed Magika inputs.

use super::budget_exceeded::BudgetExceeded;

/// Tracks the total number of bytes requested during one detection.
#[derive(Debug)]
pub(crate) struct ReadBudget {
    /// Bytes already reserved during this detection.
    used: usize,
    /// Maximum cumulative bytes allowed for this detection.
    limit: usize,
}

impl ReadBudget {
    /// Creates an empty per-detection budget with `limit` available bytes.
    ///
    /// # Parameters
    ///
    /// - `limit`: Maximum cumulative number of bytes that may be reserved.
    #[inline]
    pub(crate) const fn new(limit: usize) -> Self {
        Self { used: 0, limit }
    }

    /// Reserves `count` bytes before a provider range read.
    ///
    /// Returns [`BudgetExceeded`] without changing the used total if this read
    /// would exceed the limit or overflow the cumulative count.
    ///
    /// # Parameters
    ///
    /// - `count`: Number of bytes to reserve for the provider range read.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the bytes fit within the remaining budget, or
    /// [`BudgetExceeded`] when the cumulative request exceeds the limit.
    pub(crate) fn consume(&mut self, count: usize) -> Result<(), BudgetExceeded> {
        let Some(requested) = self.used.checked_add(count) else {
            return Err(BudgetExceeded {
                requested: usize::MAX,
                limit: self.limit,
            });
        };
        if requested > self.limit {
            return Err(BudgetExceeded {
                requested,
                limit: self.limit,
            });
        }
        self.used = requested;
        Ok(())
    }
}
