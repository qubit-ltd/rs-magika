// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::time::Duration;

use crate::internal::async_sample_bridge::AsyncSampleBridge;

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
    request.respond(Ok(vec![1, 2, 3, 4]));
    assert_eq!([1, 2, 3, 4], worker.join().unwrap());
}

#[test]
fn test_short_read_and_provider_error_terminate_worker_request() {
    let bridge = Arc::new(AsyncSampleBridge::new(16));
    let worker_bridge = Arc::clone(&bridge);
    let worker = std::thread::spawn(move || {
        let mut bytes = [0; 3];
        worker_bridge.read_at(0, &mut bytes)
    });
    block_on(bridge.next_request()).unwrap().respond(Ok(vec![1]));
    assert_eq!(
        std::io::ErrorKind::UnexpectedEof,
        worker.join().unwrap().unwrap_err().kind()
    );

    let bridge = Arc::new(AsyncSampleBridge::new(16));
    let worker_bridge = Arc::clone(&bridge);
    let worker = std::thread::spawn(move || {
        let mut byte = [0; 1];
        worker_bridge.read_at(0, &mut byte)
    });
    block_on(bridge.next_request())
        .unwrap()
        .respond(Err(std::io::Error::other("provider failed")));
    assert_eq!("provider failed", worker.join().unwrap().unwrap_err().to_string());
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
    bridge.close();
    assert_eq!(
        std::io::ErrorKind::Interrupted,
        worker.join().unwrap().unwrap_err().kind()
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
    drop(block_on(bridge.next_request()).unwrap());
    assert_eq!(
        std::io::ErrorKind::Interrupted,
        worker.join().unwrap().unwrap_err().kind()
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
    drop(driver);
    assert_eq!(
        std::io::ErrorKind::Interrupted,
        worker.join().unwrap().unwrap_err().kind(),
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

    bridge.close();

    assert!(matches!(
        Future::poll(request.as_mut(), &mut context),
        Poll::Ready(None)
    ));
    assert!(Future::poll(closed.as_mut(), &mut context).is_ready());
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
    let second_bridge = Arc::clone(&bridge);
    let second = std::thread::spawn(move || {
        let mut byte = [0; 1];
        second_bridge.read_at(1, &mut byte)
    });
    assert!(poll_for(bridge.next_request(), Duration::from_millis(30)).is_none());
    first_request.respond(Ok(vec![10]));
    let second_request = block_on(bridge.next_request()).unwrap();
    second_request.respond(Ok(vec![11]));
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();
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
