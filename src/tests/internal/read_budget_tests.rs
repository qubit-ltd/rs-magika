// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::internal::read_budget::ReadBudget;

#[test]
fn test_read_budget_counts_repeated_reads() {
    let mut budget = ReadBudget::new(8_192);
    assert!(budget.consume(4_096).is_ok());
    assert!(budget.consume(4_096).is_ok());
    let error = budget.consume(4_097).expect_err("third read must exceed budget");
    assert_eq!(12_289, error.requested);
    assert_eq!(8_192, error.limit);
}

#[test]
fn test_read_budget_reports_addition_overflow() {
    let mut budget = ReadBudget::new(usize::MAX);
    assert!(budget.consume(usize::MAX).is_ok());
    let error = budget.consume(1).expect_err("overflow must be rejected");
    assert_eq!(usize::MAX, error.requested);
    assert_eq!(usize::MAX, error.limit);
}

#[test]
fn test_read_budget_rejects_second_window_at_limit() {
    let mut budget = ReadBudget::new(4_096);
    assert!(budget.consume(4_096).is_ok());
    let error = budget
        .consume(4_096)
        .expect_err("second window must exceed the cumulative limit");
    assert_eq!(8_192, error.requested);
    assert_eq!(4_096, error.limit);
}
