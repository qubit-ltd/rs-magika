// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::time::Duration;
use std::time::Instant;

struct ThreadWake {
    thread: std::thread::Thread,
    notified: AtomicBool,
}

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notified.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

/// Waits for a future to become ready, polling again only after a wake.
pub(crate) fn run_ready<F: Future>(future: F) -> F::Output {
    run_until(future, Duration::from_secs(5)).expect("future did not complete before deadline")
}

/// Waits for a future until a monotonic deadline, polling again only after a
/// wake.
pub(crate) fn run_until<F: Future>(future: F, timeout: Duration) -> Result<F::Output, ()> {
    let wake = Arc::new(ThreadWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    let deadline = Instant::now() + timeout;
    loop {
        match Future::poll(Pin::as_mut(&mut future), &mut context) {
            Poll::Ready(value) => return Ok(value),
            Poll::Pending => loop {
                if wake.notified.swap(false, Ordering::AcqRel) {
                    break;
                }
                let now = Instant::now();
                if now >= deadline {
                    return Err(());
                }
                std::thread::park_timeout(deadline.saturating_duration_since(now));
            },
        }
    }
}
