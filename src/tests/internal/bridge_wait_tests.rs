// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::pending;
use std::time::Duration;

use crate::tests::internal::bridge_wait::poll_before;

#[test]
fn test_poll_before_reports_stage_for_never_ready_future() {
    let result = std::panic::catch_unwind(|| {
        poll_before(pending::<()>(), "never-ready lifecycle phase", Duration::from_millis(2));
    });
    let panic = result.expect_err("a never-ready future must time out");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .expect("timeout should use a string panic message");
    assert!(
        message.contains("never-ready lifecycle phase"),
        "timeout stage missing: {message}"
    );
}
