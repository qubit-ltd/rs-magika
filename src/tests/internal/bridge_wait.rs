// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt::Debug;
use std::future::Future;
use std::sync::mpsc::Receiver;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

/// Waits for one expected worker signal, recording the failed stage before
/// releasing its gate.
pub(crate) fn wait_for_stage<T: Debug + PartialEq>(
    events: &Receiver<T>,
    expected: &T,
    stage: &str,
    timeout: Duration,
    release: impl FnOnce(),
) -> Result<(), String> {
    let failure = match events.recv_timeout(timeout) {
        Ok(actual) if &actual == expected => return Ok(()),
        Ok(actual) => format!(
            "unexpected signal at stage={stage}: expected {expected:?}, got {actual:?}"
        ),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            format!("timed out waiting for stage={stage}: expected {expected:?}")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            format!("signal channel disconnected at stage={stage}: expected {expected:?}")
        }
    };
    release();
    Err(failure)
}

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
