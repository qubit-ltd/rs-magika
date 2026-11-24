// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

/// Waits for a bridge worker to finish before joining it.
///
/// The handle is dropped on timeout, so a failed test cannot block forever
/// while joining a worker that remains stuck.
pub(crate) fn join_before<T>(worker: JoinHandle<T>, stage: &str, timeout: Duration) -> T {
    let deadline = Instant::now() + timeout;
    while !worker.is_finished() && Instant::now() < deadline {
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert!(
        worker.is_finished(),
        "timed out waiting for bridge worker; stage={stage}"
    );
    worker.join().unwrap_or_else(|panic| std::panic::resume_unwind(panic))
}
