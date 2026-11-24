// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::time::Duration;

use crate::internal::async_sample_bridge::AsyncSampleBridge;
use crate::tests::internal::bridge_wait::join_before;

#[test]
fn test_async_driver_answers_worker_range_request() {
    let bridge = Arc::new(AsyncSampleBridge::new(32));
    assert_eq!(32, bridge.length());
    let worker_bridge = Arc::clone(&bridge);
    let worker = std::thread::spawn(move || {
        let mut bytes = [0; 4];
        worker_bridge.read_at(7, &mut bytes).unwrap();
        bytes
    });
    let request = block_on(bridge.next_request()).expect("worker should request a range");
    assert_eq!((7, 4), (request.offset(), request.len()));
    assert!(
        bridge.wait_for_test_waiters(0, 1, Duration::from_secs(2)),
        "worker should be waiting for the provider response before it is published"
    );
    request.respond(Ok(vec![1, 2, 3, 4]));
    assert_eq!(
        [1, 2, 3, 4],
        join_before(worker, "normal response", Duration::from_secs(2))
    );
}

#[test]
fn test_short_read_and_provider_error_terminate_worker_request() {
    let bridge = Arc::new(AsyncSampleBridge::new(16));
    let worker_bridge = Arc::clone(&bridge);
    let worker = std::thread::spawn(move || {
        let mut bytes = [0; 3];
        worker_bridge.read_at(0, &mut bytes)
    });
    let request = block_on(bridge.next_request()).unwrap();
    assert!(
        bridge.wait_for_test_waiters(0, 1, Duration::from_secs(2)),
        "short-read worker should await its response"
    );
    request.respond(Ok(vec![1]));
    assert_eq!(
        std::io::ErrorKind::UnexpectedEof,
        join_before(worker, "short response", Duration::from_secs(2))
            .unwrap_err()
            .kind()
    );

    let bridge = Arc::new(AsyncSampleBridge::new(16));
    let worker_bridge = Arc::clone(&bridge);
    let worker = std::thread::spawn(move || {
        let mut byte = [0; 1];
        worker_bridge.read_at(0, &mut byte)
    });
    let request = block_on(bridge.next_request()).unwrap();
    assert!(
        bridge.wait_for_test_waiters(0, 1, Duration::from_secs(2)),
        "provider-error worker should await its response"
    );
    request.respond(Err(std::io::Error::other("provider failed")));
    assert_eq!(
        "provider failed",
        join_before(worker, "provider error", Duration::from_secs(2))
            .unwrap_err()
            .to_string()
    );
}

#[test]
fn test_close_wakes_worker_waiting_for_range_response() {
    let bridge = Arc::new(AsyncSampleBridge::new(16));
    let worker_bridge = Arc::clone(&bridge);
    let worker = std::thread::spawn(move || {
        let mut byte = [0; 1];
        worker_bridge.read_at(0, &mut byte)
    });
    let request = block_on(bridge.next_request()).unwrap();
    assert!(
        bridge.wait_for_test_waiters(0, 1, Duration::from_secs(2)),
        "worker should await close before the bridge closes"
    );
    bridge.close();
    assert_eq!(
        std::io::ErrorKind::Interrupted,
        join_before(worker, "explicit close", Duration::from_secs(2)).unwrap_err().kind()
    );
    drop(request);
}

#[test]
fn test_dropping_an_unanswered_request_cancels_worker() {
    let bridge = Arc::new(AsyncSampleBridge::new(16));
    let worker_bridge = Arc::clone(&bridge);
    let worker = std::thread::spawn(move || {
        let mut byte = [0; 1];
        worker_bridge.read_at(0, &mut byte)
    });
    let request = block_on(bridge.next_request()).unwrap();
    assert!(
        bridge.wait_for_test_waiters(0, 1, Duration::from_secs(2)),
        "worker should await unanswered request drop"
    );
    drop(request);
    assert_eq!(
        std::io::ErrorKind::Interrupted,
        join_before(worker, "unanswered request drop", Duration::from_secs(2))
            .unwrap_err()
            .kind()
    );
}

#[test]
fn test_dropping_async_driver_closes_pending_bridge_and_wakes_worker() {
    let bridge = AsyncSampleBridge::new(16);
    let worker_bridge = Arc::clone(&bridge);
    let worker = std::thread::spawn(move || {
        let mut byte = [0; 1];
        worker_bridge.read_at(0, &mut byte)
    });
    let driver = bridge.driver();
    let request = block_on(driver.next_request()).expect("driver should receive request");
    assert!(
        bridge.wait_for_test_waiters(0, 1, Duration::from_secs(2)),
        "worker should await driver drop"
    );
    drop(driver);
    assert_eq!(
        std::io::ErrorKind::Interrupted,
        join_before(worker, "driver drop", Duration::from_secs(2)).unwrap_err().kind(),
    );
    drop(request);
}

#[test]
fn test_invalid_ranges_fail_without_publishing_requests() {
    let bridge = AsyncSampleBridge::new(8);
    let mut byte = [0; 1];
    assert_eq!(
        std::io::ErrorKind::InvalidInput,
        bridge.read_at(u64::MAX, &mut byte).unwrap_err().kind(),
    );
    assert_eq!(
        std::io::ErrorKind::UnexpectedEof,
        bridge.read_at(8, &mut byte).unwrap_err().kind(),
    );
    bridge.close();
    assert_eq!(
        std::io::ErrorKind::Interrupted,
        bridge.read_at(0, &mut byte).unwrap_err().kind(),
    );
}

#[test]
fn test_empty_reads_and_closed_driver_finish_without_range_requests() {
    let bridge = AsyncSampleBridge::new(0);
    let mut empty = [];
    bridge.read_at(0, &mut empty).unwrap();
    bridge.close();
    assert!(block_on(bridge.next_request()).is_none());

    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut closed = std::pin::pin!(bridge.closed());
    assert!(Future::poll(closed.as_mut(), &mut context).is_ready());
}

#[test]
fn test_bridge_close_wakes_pending_request_and_close_waiters() {
    let bridge = AsyncSampleBridge::new(1);
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut request = std::pin::pin!(bridge.next_request());
    assert!(Future::poll(request.as_mut(), &mut context).is_pending());
    let mut closed = std::pin::pin!(bridge.closed());
    assert!(Future::poll(closed.as_mut(), &mut context).is_pending());
    assert!(
        Future::poll(closed.as_mut(), &mut context).is_pending(),
        "re-polling with the same waker should not register a duplicate close waiter"
    );

    bridge.close();

    assert!(matches!(
        Future::poll(request.as_mut(), &mut context),
        Poll::Ready(None)
    ));
    assert!(Future::poll(closed.as_mut(), &mut context).is_ready());
}

#[test]
fn test_closed_wakes_registered_waiter_and_deduplicates_proven_identity() {
    struct CountingWake(AtomicUsize);
    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let bridge = AsyncSampleBridge::new(0);
    let counter = Arc::new(CountingWake(AtomicUsize::new(0)));
    let waker = std::task::Waker::from(Arc::clone(&counter));
    let can_prove_same_waiter = waker.will_wake(&waker.clone());
    let mut context = Context::from_waker(&waker);
    let mut closed = std::pin::pin!(bridge.closed());

    assert!(Future::poll(closed.as_mut(), &mut context).is_pending());
    assert!(Future::poll(closed.as_mut(), &mut context).is_pending());
    bridge.close();

    assert!(Future::poll(closed.as_mut(), &mut context).is_ready());
    let wakes = counter.0.load(Ordering::SeqCst);
    assert!(wakes >= 1, "closing the bridge must wake the registered waiter");
    if can_prove_same_waiter {
        assert_eq!(
            1, wakes,
            "a waker proven equivalent to its clone should be registered once"
        );
    }
}

#[test]
fn test_bridge_never_exposes_two_unanswered_requests() {
    let bridge = Arc::new(AsyncSampleBridge::new(8));
    let first_bridge = Arc::clone(&bridge);
    let first = std::thread::spawn(move || {
        let mut byte = [0; 1];
        first_bridge.read_at(0, &mut byte)
    });
    let first_request = block_on(bridge.next_request()).unwrap();
    assert!(
        bridge.wait_for_test_waiters(0, 1, Duration::from_secs(2)),
        "first worker should await its response"
    );
    let second_bridge = Arc::clone(&bridge);
    let second = std::thread::spawn(move || {
        let mut byte = [0; 1];
        second_bridge.read_at(1, &mut byte)
    });
    assert!(
        bridge.wait_for_test_waiters(1, 0, Duration::from_secs(2)),
        "second worker read should be waiting while the first request remains unanswered"
    );
    assert!(poll_for(bridge.next_request(), Duration::from_millis(30)).is_none());
    first_request.respond(Ok(vec![10]));
    let second_request = block_on(bridge.next_request()).unwrap();
    assert!(
        bridge.wait_for_test_waiters(0, 1, Duration::from_secs(2)),
        "second worker should await its response"
    );
    second_request.respond(Ok(vec![11]));
    join_before(first, "first serialized response", Duration::from_secs(2)).unwrap();
    join_before(second, "second serialized response", Duration::from_secs(2)).unwrap();
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(std::thread::Thread);
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    let start = std::time::Instant::now();
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "future did not complete before test timeout",
        );
        std::thread::park_timeout(Duration::from_millis(10));
    }
}

fn poll_for<F: Future>(future: F, timeout: Duration) -> Option<F::Output> {
    let waker = std::task::Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    let start = std::time::Instant::now();
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return Some(value);
        }
        if start.elapsed() >= timeout {
            return None;
        }
        std::thread::yield_now();
    }
}
