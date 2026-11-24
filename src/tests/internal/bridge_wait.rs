// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
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

/// Polls a test future until it completes or its absolute deadline expires.
pub(crate) fn poll_before<F: Future>(future: F, stage: &str, timeout: Duration) -> F::Output {
    let deadline = Instant::now() + timeout;
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "timed out polling bridge future; stage={stage}");
        std::thread::park_timeout(remaining.min(Duration::from_millis(1)));
    }
}

#[cfg(test)]
mod tests {
    use super::poll_before;
    use std::future::pending;
    use std::time::Duration;

    #[test]
    fn test_poll_before_reports_stage_for_never_ready_future() {
        let result = std::panic::catch_unwind(|| {
            poll_before(
                pending::<()>(),
                "never-ready lifecycle phase",
                Duration::from_millis(2),
            );
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
}
