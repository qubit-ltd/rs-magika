// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use magika::Error;
use qubit_mime::MimeError;

use crate::internal::budget_exceeded::BudgetExceeded;
use crate::internal::provider_input::copy_exact;
use crate::internal::provider_input::invalid_input;
use crate::internal::provider_input::map_provider_magika_error;
use crate::internal::provider_input::validate_request;
use crate::internal::read_budget::ReadBudget;

#[test]
fn test_provider_input_helper_errors_are_displayable() {
    assert_eq!(
        "invalid provider input",
        invalid_input("invalid provider input").to_string()
    );
    assert!(
        BudgetExceeded { requested: 9, limit: 8 }.to_string().contains("9 > 8"),
        "the budget error should report the requested and allowed byte counts"
    );
}

#[test]
fn test_provider_input_validation_and_copy_errors_are_mapped() {
    let mut budget = ReadBudget::new(8);
    assert!(
        validate_request(8, &mut budget, 0, 99).is_ok(),
        "zero-length reads should not require an in-range offset"
    );
    assert!(
        validate_request(8, &mut budget, 2, 7).is_err(),
        "reads extending beyond the logical length should fail"
    );
    assert!(
        validate_request(8, &mut budget, 2, u64::MAX).is_err(),
        "overflowing read offsets should fail"
    );
    let mut output = [0_u8; 2];
    assert!(
        copy_exact(&mut output, vec![1]).is_err(),
        "short provider ranges should fail"
    );
    assert!(
        copy_exact(&mut output, vec![1, 2]).is_ok(),
        "provider ranges matching the buffer length should be copied"
    );
    let mapped = map_provider_magika_error(Error::IOError(std::io::Error::other("provider failure")));
    assert!(
        matches!(mapped, MimeError::Io(_)),
        "ordinary provider I/O errors should remain MIME I/O errors"
    );

    let error = BudgetExceeded {
        requested: 16,
        limit: 8,
    };
    let mapped = map_provider_magika_error(Error::IOError(std::io::Error::new(
        std::io::ErrorKind::FileTooLarge,
        error,
    )));
    assert!(
        matches!(
            mapped,
            MimeError::BufferLimitExceeded {
                requested: 16,
                limit: 8
            }
        ),
        "provider budget failures should retain their requested and allowed byte counts"
    );
}
