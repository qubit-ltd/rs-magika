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
    used: usize,
    limit: usize,
}

impl ReadBudget {
    /// Creates an empty budget with the supplied limit.
    pub(crate) const fn new(limit: usize) -> Self {
        Self { used: 0, limit }
    }

    /// Reserves one read from the cumulative budget.
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
