// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors raised when a provider input exceeds its cumulative read budget.

use std::fmt;

/// Describes a read that exceeds the detector's cumulative budget.
#[derive(Debug)]
pub(crate) struct BudgetExceeded {
    /// Total requested bytes including the rejected read.
    pub(crate) requested: usize,
    /// Configured cumulative limit.
    pub(crate) limit: usize,
}

impl fmt::Display for BudgetExceeded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "provider read budget exceeded: {} > {}",
            self.requested, self.limit
        )
    }
}

impl std::error::Error for BudgetExceeded {}
