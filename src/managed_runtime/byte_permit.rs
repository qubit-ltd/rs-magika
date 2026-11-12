// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII guard that returns reserved byte capacity after job release.

use std::sync::Arc;

use super::byte_budget::ByteBudget;

/// Returns its reserved bytes to the runtime when dropped.
pub(super) struct BytePermit {
    /// Budget whose aggregate count this guard owns.
    budget: Arc<ByteBudget>,
    /// Number of bytes reserved by this guard.
    amount: usize,
}

impl BytePermit {
    /// Creates a release guard for an already-counted reservation.
    ///
    /// # Parameters
    ///
    /// * `budget` - Shared aggregate accounting state.
    /// * `amount` - Reserved payload bytes.
    pub(super) fn new(budget: Arc<ByteBudget>, amount: usize) -> Self {
        Self { budget, amount }
    }
}

impl Drop for BytePermit {
    /// Returns reserved byte capacity to the shared budget.
    fn drop(&mut self) {
        self.budget.release(self.amount);
    }
}
