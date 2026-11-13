// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

#![cfg(feature = "bundled-onnxruntime")]

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

use qubit_fs::Path;
use qubit_fs_local::LocalFileSystems;
use qubit_fs_local::LocalResourcePolicy;
use qubit_mime::MimeDetectionPolicy;
use qubit_mime::MimeDetector;
use qubit_mime::MimeDetectorBackend;
use qubit_mime::MimeError;
use qubit_operation::protocol::StopReason;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ShutdownMode;

use crate::support::AsyncProviderGate;
use crate::support::AsyncProviderGatePoint;
use crate::support::ProviderFileSystemSpi;
use crate::support::detector;
use crate::support::run_ready;
use crate::support::run_until;

const PYTHON: &[u8] = b"#!/usr/bin/env python3\nprint('hello')\n";

fn path(value: &str) -> Path {
    Path::parse(value).expect("test path should be valid")
}

fn large_provider(range: bool) -> ProviderFileSystemSpi {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec());
    if range {
        provider.with_range().logical(32 * 1024 * 1024)
    } else {
        provider.without_range().logical(32 * 1024 * 1024)
    }
}

#[test]
fn test_managed_provider_session_api_classifies_and_settles_worker() {
    let detector = Arc::new(
        qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"),
    );
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(1).unwrap();
    let file_system = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .async_file_system();
    let fallback_file_system = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .without_stat()
        .random_open_error(qubit_fs::error::FsErrorKind::UnsupportedOperation)
        .async_file_system();
    let body_detector = Arc::clone(&detector);
    let mut handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            let rejected = body_detector
                .detect_async_provider_path_managed(
                    &scope,
                    &file_system,
                    &path("/managed/oversize.py"),
                    body_detector.max_buffer_size() + 1,
                )
                .await;
            assert!(rejected.is_err());
            let session_result = body_detector
                .detect_async_provider_path_managed(
                    &scope,
                    &file_system,
                    &path("/managed/python.py"),
                    4096,
                )
                .await
                .map(|mime| mime.expect("Python sample should be classified"))?;
            let fallback_result = body_detector
                .detect_async_provider_path_managed(
                    &scope,
                    &fallback_file_system,
                    &path("/managed/fallback.py"),
                    4096,
                )
                .await
                .map(|mime| mime.expect("fallback Python sample should be classified"))?;
            Ok::<_, MimeError>((session_result, fallback_result))
        })
        .unwrap();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        qubit_operation::report::OperationOutcome::Succeeded((session_mime, fallback_mime))
            if session_mime == "text/x-python" && fallback_mime == "text/x-python"
    ));
    assert!(handle.observer().released_blocking().unwrap().all_released);
    let report = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    assert!(report.worker_stopped);
}

#[test]
fn test_managed_provider_admission_rejects_cancelled_scope_before_provider_io() {
    let detector = Arc::new(
        qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"),
    );
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(1).unwrap();
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access();
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let body_detector = Arc::clone(&detector);
    let mut handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            scope.request_cancel(qubit_operation::protocol::StopReason::UserCancelled);
            let result = body_detector
                .detect_async_provider_path_managed(
                    &scope,
                    &file_system,
                    &path("/cancelled.py"),
                    4096,
                )
                .await;
            assert!(result.is_err());
            Ok::<_, MimeError>(())
        })
        .unwrap();
    let _ = handle.result_blocking();
    assert_eq!(0, observations.opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.stats.load(Ordering::SeqCst));
    assert_eq!(0, observations.random_opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.random_reads.load(Ordering::SeqCst));
    let _ = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
}

#[test]
fn test_managed_provider_admission_rejects_closed_runtime_before_provider_io() {
    let detector = Arc::new(
        qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"),
    );
    let report = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    assert!(report.worker_stopped);
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(1).unwrap();
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access();
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let body_detector = Arc::clone(&detector);
    let mut handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            let result = body_detector
                .detect_async_provider_path_managed(&scope, &file_system, &path("/closed.py"), 4096)
                .await;
            assert!(result.is_err());
            Ok::<_, MimeError>(())
        })
        .unwrap();
    let _ = handle.result_blocking();
    assert_eq!(0, observations.opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.stats.load(Ordering::SeqCst));
    assert_eq!(0, observations.random_opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.random_reads.load(Ordering::SeqCst));
}

#[test]
fn test_managed_provider_admission_rejects_full_capacity_before_provider_io() {
    let detector = Arc::new(
        qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"),
    );
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(4).unwrap();
    let gates = [
        crate::support::AsyncProviderGate::new(AsyncProviderGatePoint::Open),
        crate::support::AsyncProviderGate::new(AsyncProviderGatePoint::Open),
    ];
    let mut handles = Vec::new();
    for gate in &gates {
        let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
            .with_random_access()
            .with_async_gate(Arc::clone(gate));
        let file_system = provider.async_file_system();
        let body_detector = Arc::clone(&detector);
        handles.push(
            service
                .start(ScopeOptions::default(), move |scope| async move {
                    body_detector
                        .detect_async_provider_path_managed(
                            &scope,
                            &file_system,
                            &path("/full.py"),
                            4096,
                        )
                        .await
                        .map(|_| ())
                })
                .unwrap(),
        );
    }
    for gate in &gates {
        gate.wait_until_entered(Duration::from_secs(2));
    }

    let rejected = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access();
    let observations = rejected.observations();
    let file_system = rejected.async_file_system();
    let body_detector = Arc::clone(&detector);
    let mut rejected_handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            let result = body_detector
                .detect_async_provider_path_managed(
                    &scope,
                    &file_system,
                    &path("/over-capacity.py"),
                    4096,
                )
                .await;
            assert!(result.is_err());
            Ok::<_, MimeError>(())
        })
        .unwrap();
    let _ = rejected_handle.result_blocking();
    assert_eq!(0, observations.opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.stats.load(Ordering::SeqCst));
    assert_eq!(0, observations.random_opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.random_reads.load(Ordering::SeqCst));
    for gate in &gates {
        gate.release();
    }
    for handle in &mut handles {
        let _ = handle.result_blocking();
    }
    let _ = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
}

#[test]
fn test_pending_provider_read_keeps_managed_release_pending_after_bridge_close() {
    let detector = Arc::new(
        qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"),
    );
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(1).unwrap();
    let gate = AsyncProviderGate::new(AsyncProviderGatePoint::Read);
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .with_async_gate(Arc::clone(&gate));
    let file_system = provider.async_file_system();
    let body_detector = Arc::clone(&detector);
    let handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            body_detector
                .detect_async_provider_path_managed(
                    &scope,
                    &file_system,
                    &path("/pending.py"),
                    4096,
                )
                .await
        })
        .unwrap();
    gate.wait_until_entered(Duration::from_secs(30));
    let observer = handle.observer();
    handle.request_cancel(StopReason::UserCancelled);
    let mut handle = handle;
    let _ = handle.result_blocking();
    let (released_tx, released_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        released_tx
            .send(observer.released_blocking().unwrap())
            .unwrap()
    });
    assert!(released_rx.recv_timeout(Duration::from_millis(50)).is_err());
    let report = detector.shutdown_blocking(ShutdownMode::Cancel, Duration::from_millis(50));
    assert!(!report.admission.is_complete());
    // The async fs contract does not let us certify provider-native completion.
    gate.release();
}

#[test]
fn test_sync_provider_backend_uses_session_range_and_full_read_routes() {
    let detector =
        qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize");
    let routes = [
        ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access(),
        ProviderFileSystemSpi::new(PYTHON.to_vec())
            .with_range()
            .random_open_error(qubit_fs::error::FsErrorKind::UnsupportedOperation),
        ProviderFileSystemSpi::new(PYTHON.to_vec())
            .without_stat()
            .random_open_error(qubit_fs::error::FsErrorKind::UnsupportedOperation),
    ];
    for provider in routes {
        let file_system = provider.file_system();
        let (candidates, returned_bytes) = MimeDetectorBackend::guess_from_provider_path(
            &detector,
            &file_system,
            &path("/managed/sync.py"),
            4096,
        )
        .expect("sync provider route should classify the Python sample");
        assert_eq!(vec!["text/x-python"], candidates);
        assert!(returned_bytes.is_none());
    }
    let report = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    assert!(report.worker_stopped);
}

#[test]
fn test_wake_driver_waits_for_wake_and_times_out_without_repolling() {
    struct PendingUntilWake {
        polls: Arc<std::sync::atomic::AtomicUsize>,
        waker: Arc<std::sync::Mutex<Option<std::task::Waker>>>,
        registered: Arc<std::sync::Condvar>,
    }
    impl Future for PendingUntilWake {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            self.registered.notify_all();
            Poll::Pending
        }
    }

    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let waker_slot = Arc::new(std::sync::Mutex::new(None::<std::task::Waker>));
    let result = run_until(
        PendingUntilWake {
            polls: Arc::clone(&polls),
            waker: Arc::clone(&waker_slot),
            registered: Arc::new(std::sync::Condvar::new()),
        },
        Duration::from_millis(20),
    );
    assert!(
        result.is_err(),
        "a Pending future without a wake must time out"
    );
    assert_eq!(
        1,
        polls.load(Ordering::SeqCst),
        "the driver must not actively repoll"
    );

    let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let waker_slot = Arc::new(std::sync::Mutex::new(None::<std::task::Waker>));
    let released = Arc::new(AtomicBool::new(false));
    let release_flag = Arc::clone(&released);
    let release_waker = Arc::clone(&waker_slot);
    let registered = Arc::new(std::sync::Condvar::new());
    let release_registered = Arc::clone(&registered);
    let releaser = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(1);
        let mut slot = release_waker.lock().unwrap();
        while slot.is_none() {
            let now = Instant::now();
            assert!(now < deadline, "future did not register its waker");
            let (next, result) = release_registered
                .wait_timeout(slot, deadline.saturating_duration_since(now))
                .unwrap();
            slot = next;
            assert!(
                !result.timed_out() || slot.is_some(),
                "future did not register its waker"
            );
        }
        let waker = slot.take().unwrap();
        release_flag.store(true, Ordering::Release);
        waker.wake_by_ref();
        waker.wake_by_ref();
    });
    struct Released {
        polls: Arc<std::sync::atomic::AtomicUsize>,
        released: Arc<AtomicBool>,
        slot: Arc<std::sync::Mutex<Option<std::task::Waker>>>,
        registered: Arc<std::sync::Condvar>,
    }
    impl Future for Released {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            if self.released.load(Ordering::Acquire) {
                Poll::Ready(())
            } else {
                *self.slot.lock().unwrap() = Some(cx.waker().clone());
                self.registered.notify_all();
                Poll::Pending
            }
        }
    }
    run_ready(Released {
        polls,
        released,
        slot: waker_slot,
        registered,
    });
    releaser.join().unwrap();
}

#[test]
fn test_wake_during_poll_is_not_lost_before_park() {
    struct WakeDuringPoll(usize);
    impl Future for WakeDuringPoll {
        type Output = usize;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<usize> {
            self.0 += 1;
            if self.0 == 1 {
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(self.0)
            }
        }
    }
    assert_eq!(2, run_ready(WakeDuringPoll(0)));
}

#[test]
fn test_pending_future_replaces_stale_waker() {
    struct CountWake(Arc<std::sync::atomic::AtomicUsize>);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct RegisterWaker(Option<std::task::Waker>);
    impl Future for RegisterWaker {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            self.0 = Some(cx.waker().clone());
            Poll::Pending
        }
    }

    let old_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let new_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let old_waker = std::task::Waker::from(Arc::new(CountWake(Arc::clone(&old_count))));
    let new_waker = std::task::Waker::from(Arc::new(CountWake(Arc::clone(&new_count))));
    let mut future = Box::pin(RegisterWaker(None));
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(&old_waker))
            .is_pending()
    );
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(&new_waker))
            .is_pending()
    );
    future.0.take().unwrap().wake();
    assert_eq!(0, old_count.load(Ordering::SeqCst));
    assert_eq!(1, new_count.load(Ordering::SeqCst));
}

#[test]
fn test_provider_stat_open_and_read_suspend_until_wake() {
    for point in [
        AsyncProviderGatePoint::Stat,
        AsyncProviderGatePoint::Open,
        AsyncProviderGatePoint::Read,
    ] {
        let gate = AsyncProviderGate::new(point);
        let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
            .with_range()
            .with_random_access()
            .with_async_gate(Arc::clone(&gate));
        let file_system = provider.async_file_system();
        let request_path = path("/script.py");
        let future: Pin<Box<dyn Future<Output = qubit_fs::FsResult<()>>>> = match point {
            AsyncProviderGatePoint::Stat => {
                Box::pin(async move { file_system.stat(&request_path).await.map(|_| ()) })
            }
            AsyncProviderGatePoint::Open => Box::pin(async move {
                file_system
                    .open_reader(&request_path, qubit_fs::read::ReadOptions::default())
                    .await
                    .map(drop)
            }),
            AsyncProviderGatePoint::Read => Box::pin(async move {
                let mut reader = file_system
                    .open_random_access_reader(
                        &request_path,
                        qubit_fs::read::ReadOptions::default(),
                    )
                    .await?;
                reader.read_at(0, &mut [0_u8; 1]).await.map(|_| ())
            }),
        };
        let worker_gate = Arc::clone(&gate);
        let releaser = std::thread::spawn(move || {
            worker_gate.wait_until_entered(Duration::from_secs(1));
            worker_gate.release();
        });
        let result = run_ready(future);
        releaser.join().unwrap();
        result.expect("gated provider operation should resume after wake");
        if point == AsyncProviderGatePoint::Read {
            assert_eq!(
                1,
                provider.observations().session_drops.load(Ordering::SeqCst)
            );
        }
    }
}

#[test]
fn test_dropping_pending_provider_read_releases_session() {
    let gate = AsyncProviderGate::new(AsyncProviderGatePoint::Read);
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .with_async_gate(Arc::clone(&gate));
    let worker_gate = Arc::clone(&gate);
    let observer =
        std::thread::spawn(move || worker_gate.wait_until_entered(Duration::from_secs(1)));
    let file_system = provider.async_file_system();
    let request_path = path("/script.py");
    let mut reader = run_ready(
        file_system
            .open_random_access_reader(&request_path, qubit_fs::read::ReadOptions::default()),
    )
    .expect("session should open before the read is gated");
    let result = run_until(reader.read_at(0, &mut [0_u8; 1]), Duration::from_millis(20));
    assert!(
        result.is_err(),
        "a gated provider read must remain Pending until released"
    );
    observer.join().unwrap();
    drop(reader);
    assert_eq!(
        1,
        provider.observations().session_drops.load(Ordering::SeqCst),
        "dropping the pending detection must release its session"
    );
}

#[test]
fn test_gated_provider_read_error_resumes_after_wake() {
    let gate = AsyncProviderGate::new(AsyncProviderGatePoint::Read);
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .fail_reads()
        .with_async_gate(Arc::clone(&gate));
    let worker_gate = Arc::clone(&gate);
    let releaser = std::thread::spawn(move || {
        worker_gate.wait_until_entered(Duration::from_secs(1));
        worker_gate.release();
    });
    let file_system = provider.async_file_system();
    let request_path = path("/script.py");
    let mut reader = run_ready(
        file_system
            .open_random_access_reader(&request_path, qubit_fs::read::ReadOptions::default()),
    )
    .expect("session should open before the read is gated");
    let result = run_ready(reader.read_at(0, &mut [0_u8; 1]));
    releaser.join().unwrap();
    assert!(
        result.is_err(),
        "the provider read error should propagate after wake"
    );
    drop(reader);
    assert_eq!(
        1,
        provider.observations().session_drops.load(Ordering::SeqCst)
    );
}

#[test]
fn test_provider_path_sync_range_reads_only_bounded_windows() {
    let provider = large_provider(true);
    let observations = provider.observations();
    let result = detector().detect_path(
        &provider.file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    );
    assert_eq!(
        result.expect("range detection should succeed"),
        Some("text/x-python".to_owned())
    );
    assert_eq!(
        observations.stats.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        observations.opens.load(std::sync::atomic::Ordering::SeqCst),
        2
    );
    assert!(
        observations
            .options
            .lock()
            .unwrap()
            .iter()
            .all(|options| options.offset().is_some() && options.length().is_some())
    );
    assert!(
        observations
            .read_bytes
            .load(std::sync::atomic::Ordering::SeqCst)
            <= 8192
    );
}

#[test]
fn test_provider_path_async_range_reads_only_bounded_windows() {
    let provider = large_provider(true);
    let observations = provider.observations();
    let result = run_ready(detector().detect_async_path(
        &provider.async_file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    ));
    assert_eq!(
        result.expect("async range detection should succeed"),
        Some("text/x-python".to_owned())
    );
    assert!(
        observations
            .read_bytes
            .load(std::sync::atomic::Ordering::SeqCst)
            <= 8192
    );
}

#[test]
fn test_provider_path_sync_legacy_ranges_respect_provider_limit() {
    let provider = large_provider(true).with_max_read_range_bytes(2);
    let observations = provider.observations();
    let result = detector().detect_path(
        &provider.file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    );
    assert_eq!(
        result.expect("bounded legacy ranges should succeed"),
        Some("text/x-python".to_owned())
    );
    assert!(
        observations
            .options
            .lock()
            .unwrap()
            .iter()
            .all(|options| { options.length().is_some_and(|length| length <= 2) })
    );
}

#[test]
fn test_provider_path_async_legacy_ranges_respect_provider_limit() {
    let provider = large_provider(true).with_max_read_range_bytes(2);
    let observations = provider.observations();
    let result = run_ready(detector().detect_async_path(
        &provider.async_file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    ));
    assert_eq!(
        result.expect("bounded async legacy ranges should succeed"),
        Some("text/x-python".to_owned())
    );
    assert!(
        observations
            .options
            .lock()
            .unwrap()
            .iter()
            .all(|options| { options.length().is_some_and(|length| length <= 2) })
    );
}

#[test]
fn test_provider_path_sync_uses_one_random_access_session_without_stat() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access();
    let observations = provider.observations();
    let result = detector().detect_path(
        &provider.file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    );
    assert_eq!(
        result.expect("session detection should succeed"),
        Some("text/x-python".to_owned())
    );
    assert_eq!(
        1,
        observations
            .random_opens
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        0,
        observations.stats.load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        0,
        observations.opens.load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        1,
        observations
            .session_drops
            .load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[test]
fn test_provider_path_async_uses_one_random_access_session_without_stat() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access();
    let observations = provider.observations();
    let result = run_ready(detector().detect_async_path(
        &provider.async_file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    ));
    assert_eq!(
        result.expect("async session detection should succeed"),
        Some("text/x-python".to_owned())
    );
    assert_eq!(
        1,
        observations
            .random_opens
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        0,
        observations.stats.load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        0,
        observations.opens.load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        1,
        observations
            .session_drops
            .load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[test]
fn test_provider_path_sync_session_respects_finite_range_limit() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .with_max_read_range_bytes(2);
    let observations = provider.observations();

    assert_eq!(
        detector()
            .detect_path(
                &provider.file_system(),
                &path("/script.py"),
                8192,
                MimeDetectionPolicy::VerifyContent,
            )
            .expect("bounded session detection should succeed"),
        Some("text/x-python".to_owned())
    );
    assert!(
        observations
            .session_read_lengths
            .lock()
            .unwrap()
            .iter()
            .all(|length| *length <= 2)
    );
}

#[test]
fn test_provider_path_async_session_respects_finite_range_limit() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .with_max_read_range_bytes(2);
    let observations = provider.observations();

    assert_eq!(
        run_ready(detector().detect_async_path(
            &provider.async_file_system(),
            &path("/script.py"),
            8192,
            MimeDetectionPolicy::VerifyContent,
        ))
        .expect("bounded async session detection should succeed"),
        Some("text/x-python".to_owned())
    );
    assert!(
        observations
            .session_read_lengths
            .lock()
            .unwrap()
            .iter()
            .all(|length| *length <= 2)
    );
}

#[test]
fn test_rooted_local_filesystem_path_detection_uses_random_access() {
    let root = tempfile::tempdir().expect("temporary root should be created");
    std::fs::write(root.path().join("script.py"), PYTHON).expect("fixture should be written");
    let file_system = LocalFileSystems::rooted(root.path(), LocalResourcePolicy::unbounded())
        .expect("rooted local filesystem should open");

    let result = detector().detect_path(
        &file_system,
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    );

    assert_eq!(
        result.expect("local filesystem detection should succeed"),
        Some("text/x-python".to_owned())
    );
}

#[test]
fn test_provider_path_unsupported_session_open_falls_back_to_legacy_route() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_range()
        .random_open_error(qubit_fs::error::FsErrorKind::UnsupportedOperation);
    let observations = provider.observations();
    let result = detector().detect_path(
        &provider.file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    );
    assert_eq!(
        result.expect("unsupported session should use legacy route"),
        Some("text/x-python".to_owned())
    );
    assert_eq!(
        1,
        observations
            .random_opens
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        1,
        observations.stats.load(std::sync::atomic::Ordering::SeqCst)
    );
    assert!(observations.opens.load(std::sync::atomic::Ordering::SeqCst) > 0);
}

#[test]
fn test_provider_path_async_unsupported_session_open_falls_back_to_legacy_route() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_range()
        .random_open_error(qubit_fs::error::FsErrorKind::UnsupportedCapability);
    let observations = provider.observations();
    let result = run_ready(detector().detect_async_path(
        &provider.async_file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    ));
    assert_eq!(
        result.expect("unsupported async session should use legacy route"),
        Some("text/x-python".to_owned())
    );
    assert_eq!(
        1,
        observations
            .random_opens
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        1,
        observations.stats.load(std::sync::atomic::Ordering::SeqCst)
    );
    assert!(observations.opens.load(std::sync::atomic::Ordering::SeqCst) > 0);
}

#[test]
fn test_provider_path_non_unsupported_session_open_error_propagates() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_range()
        .random_open_error(qubit_fs::error::FsErrorKind::PermissionDenied);
    let observations = provider.observations();
    let error = detector()
        .detect_path(
            &provider.file_system(),
            &path("/script.py"),
            8192,
            MimeDetectionPolicy::VerifyContent,
        )
        .expect_err("permission failures should propagate");
    assert!(
        matches!(error, MimeError::FileSystem(ref error) if error.kind() == qubit_fs::error::FsErrorKind::PermissionDenied)
    );
    assert_eq!(
        0,
        observations.stats.load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        0,
        observations.opens.load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[test]
fn test_provider_path_async_non_unsupported_session_open_error_propagates() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_range()
        .random_open_error(qubit_fs::error::FsErrorKind::PermissionDenied);
    let observations = provider.observations();
    let error = run_ready(detector().detect_async_path(
        &provider.async_file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    ))
    .expect_err("async permission failures should propagate");
    assert!(
        matches!(error, MimeError::FileSystem(ref error) if error.kind() == qubit_fs::error::FsErrorKind::PermissionDenied)
    );
    assert_eq!(
        0,
        observations.stats.load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        0,
        observations.opens.load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[test]
fn test_provider_path_session_read_error_does_not_fall_back_to_path() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .fail_reads();
    let observations = provider.observations();
    let error = detector()
        .detect_path(
            &provider.file_system(),
            &path("/script.py"),
            8192,
            MimeDetectionPolicy::VerifyContent,
        )
        .expect_err("session read failure should propagate");
    assert!(matches!(error, MimeError::Io { .. }));
    assert_eq!(
        1,
        observations
            .random_opens
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        0,
        observations.stats.load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        0,
        observations.opens.load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[test]
fn test_provider_path_async_session_read_error_does_not_fall_back_to_path() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .fail_reads();
    let observations = provider.observations();
    let error = run_ready(detector().detect_async_path(
        &provider.async_file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    ))
    .expect_err("async session read failure should propagate");
    assert!(matches!(error, MimeError::Io(_)));
    assert_eq!(
        1,
        observations
            .random_opens
            .load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        0,
        observations.stats.load(std::sync::atomic::Ordering::SeqCst)
    );
    assert_eq!(
        0,
        observations.opens.load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[test]
fn test_provider_path_without_range_rejects_known_oversize() {
    let provider = large_provider(false);
    let error = detector()
        .detect_path(
            &provider.file_system(),
            &path("/script.py"),
            8192,
            MimeDetectionPolicy::VerifyContent,
        )
        .expect_err("oversize provider input should fail");
    assert!(matches!(
        error,
        MimeError::BufferLimitExceeded {
            requested: 33_554_432,
            limit: 8192
        }
    ));
}

#[test]
fn test_provider_path_without_range_reads_small_resources() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).without_range();
    let result = detector().detect_path(
        &provider.file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    );
    assert_eq!(
        result.expect("small fallback should succeed"),
        Some("text/x-python".to_owned())
    );
    assert!(
        provider
            .observations()
            .options
            .lock()
            .unwrap()
            .iter()
            .all(|options| options.offset().is_none() && options.length().is_none())
    );
}

#[test]
fn test_provider_path_unknown_length_falls_back_without_stat() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .unknown_length()
        .without_stat();
    let observations = provider.observations();
    let result = detector().detect_path(
        &provider.file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    );
    assert_eq!(
        result.expect("unknown length fallback should succeed"),
        Some("text/x-python".to_owned())
    );
    assert_eq!(
        observations.stats.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

#[test]
fn test_provider_path_prefer_filename_skips_provider_access() {
    let provider = large_provider(true);
    let observations = provider.observations();
    let result = detector().detect_path(
        &provider.file_system(),
        &path("/document.pdf"),
        8192,
        MimeDetectionPolicy::PreferFilename,
    );
    assert_eq!(
        result.expect("filename detection should succeed"),
        Some("application/pdf".to_owned())
    );
    assert_eq!(
        observations.stats.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        observations.opens.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[test]
fn test_provider_path_conditional_range_uses_etag() {
    let provider = large_provider(true).with_conditional();
    let result = detector().detect_path(
        &provider.file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    );
    assert_eq!(
        result.expect("conditional range detection should succeed"),
        Some("text/x-python".to_owned())
    );
    assert!(
        provider
            .observations()
            .options
            .lock()
            .unwrap()
            .iter()
            .all(|options| options.if_match().is_some())
    );
}

#[test]
fn test_provider_path_async_conditional_ranges_reuse_exact_etag() {
    let provider = large_provider(true)
        .with_conditional()
        .with_max_read_range_bytes(2);
    let result = run_ready(detector().detect_async_path(
        &provider.async_file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    ));
    assert_eq!(
        result.expect("conditional async range detection should succeed"),
        Some("text/x-python".to_owned())
    );
    assert!(
        provider
            .observations()
            .options
            .lock()
            .unwrap()
            .iter()
            .all(|options| options.if_match().is_some_and(|etag| etag.as_str() == "v1"))
    );
}

#[test]
fn test_provider_path_short_read_is_an_io_error() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_range()
        .short_reads();
    let error = detector()
        .detect_path(
            &provider.file_system(),
            &path("/script.py"),
            8192,
            MimeDetectionPolicy::VerifyContent,
        )
        .expect_err("short provider reads should fail");
    assert!(matches!(error, MimeError::Io { .. }));
}

#[test]
fn test_provider_path_async_short_read_is_an_io_error() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_range()
        .short_reads();
    let error = run_ready(detector().detect_async_path(
        &provider.async_file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    ))
    .expect_err("short async provider ranges must fail");
    assert!(matches!(error, MimeError::Io(_)));
}

#[test]
fn test_provider_path_sync_read_failure_is_an_io_error() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_range()
        .fail_reads();
    let error = detector()
        .detect_path(
            &provider.file_system(),
            &path("/script.py"),
            8192,
            MimeDetectionPolicy::VerifyContent,
        )
        .expect_err("provider read failures should be reported");
    assert!(matches!(error, MimeError::Io(_)));
}

#[test]
fn test_provider_path_async_read_failure_is_an_io_error() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_range()
        .fail_reads();
    let error = run_ready(detector().detect_async_path(
        &provider.async_file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    ))
    .expect_err("async provider read failures should be reported");
    assert!(matches!(error, MimeError::Io(_)));
}

#[test]
fn test_provider_path_async_without_range_rejects_known_oversize() {
    let provider = large_provider(false);
    let error = run_ready(detector().detect_async_path(
        &provider.async_file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    ))
    .expect_err("async oversize provider input should fail");
    assert!(matches!(
        error,
        MimeError::BufferLimitExceeded {
            requested: 33_554_432,
            limit: 8192
        }
    ));
}

#[test]
fn test_provider_path_rejects_requested_limit_above_detector_limit() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_range();
    let error = detector()
        .detect_path(
            &provider.file_system(),
            &path("/script.py"),
            usize::MAX,
            MimeDetectionPolicy::VerifyContent,
        )
        .expect_err("requested limit above detector limit should fail");
    assert!(matches!(error, MimeError::BufferLimitExceeded { .. }));
}

#[test]
fn test_provider_async_path_rejects_requested_limit_above_detector_limit() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_range();
    let error = run_ready(detector().detect_async_path(
        &provider.async_file_system(),
        &path("/script.py"),
        usize::MAX,
        MimeDetectionPolicy::VerifyContent,
    ))
    .expect_err("requested limit above detector limit should fail");
    assert!(matches!(error, MimeError::BufferLimitExceeded { .. }));
}
