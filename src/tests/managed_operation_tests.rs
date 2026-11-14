// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use qubit_mime::MimeConfig;
use qubit_mime::MimeDetectorBackend;
use qubit_operation::protocol::StopReason;
use qubit_operation::report::ChildBusinessOutcome;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ShutdownMode;

use crate::managed_runtime::ManagedInferenceEngine;
use crate::managed_runtime::ManagedInferenceRuntime;

#[test]
fn test_managed_inference_keeps_release_pending_and_executor_heartbeats_until_worker_exits() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(1).unwrap();
    let compute_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (started_tx, started_rx) = mpsc::channel();
    let engine_gate = Arc::clone(&compute_gate);
    let engine: ManagedInferenceEngine = Box::new(move |_bytes| {
        started_tx.send(()).unwrap();
        let (lock, ready) = &*engine_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        Ok(Some("text/plain".to_owned()))
    });
    let (business_tx, business_rx) = mpsc::channel();
    let after_business_gate = Arc::clone(&release_gate);
    let after_business: Box<dyn Fn() + Send> = Box::new(move || {
        business_tx.send(()).unwrap();
        let (lock, ready) = &*after_business_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
    });
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        MimeConfig::default().max_buffer_size(),
        engine,
        after_business,
    ));
    let detector = Arc::new(
        crate::MagikaMimeDetector::builder()
            .managed_runtime(Arc::clone(&runtime))
            .build()
            .unwrap(),
    );
    let heartbeats = Arc::new(AtomicUsize::new(0));
    let body_detector = Arc::clone(&detector);
    let body_heartbeats = Arc::clone(&heartbeats);
    let mut handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            let mut detection = Box::pin(body_detector.detect_bytes_managed(&scope, b"fixture".to_vec()));
            std::future::poll_fn(move |cx| {
                body_heartbeats.fetch_add(1, Ordering::SeqCst);
                cx.waker().wake_by_ref();
                match detection.as_mut().poll(cx) {
                    std::task::Poll::Ready(result) => std::task::Poll::Ready(result.map(|_| ())),
                    std::task::Poll::Pending => std::task::Poll::Pending,
                }
            })
            .await
        })
        .expect("managed operation should be admitted");

    started_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    assert!(heartbeats.load(Ordering::SeqCst) > 1);

    let (lock, ready) = &*compute_gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    business_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        qubit_operation::report::OperationOutcome::Succeeded(())
    ));

    let observer = handle.observer();
    let (released_tx, released_rx) = mpsc::channel();
    std::thread::spawn(move || {
        released_tx.send(observer.released_blocking().unwrap()).unwrap();
    });
    assert!(released_rx.recv_timeout(Duration::from_millis(30)).is_err());

    let (lock, ready) = &*release_gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    let settlement = released_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(settlement.all_released);
    let report = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    assert!(report.worker_stopped);
}

#[test]
fn test_cancelling_managed_inference_does_not_report_release_before_worker_exit() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(1).unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (started_tx, started_rx) = mpsc::channel();
    let engine_gate = Arc::clone(&gate);
    let engine: ManagedInferenceEngine = Box::new(move |_bytes| {
        started_tx.send(()).unwrap();
        let (lock, ready) = &*engine_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        Ok(Some("text/plain".to_owned()))
    });
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        64,
        engine,
        Box::new(|| {}),
    ));
    let body_runtime = Arc::clone(&runtime);
    let handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            body_runtime.detect_bytes(&scope, b"fixture".to_vec()).await.map(|_| ())
        })
        .expect("managed operation should be admitted");

    started_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    let observer = handle.observer();
    handle.request_cancel(StopReason::UserCancelled);
    let (released_tx, released_rx) = mpsc::channel();
    std::thread::spawn(move || {
        released_tx.send(observer.released_blocking().unwrap()).unwrap();
    });
    assert!(released_rx.recv_timeout(Duration::from_millis(30)).is_err());

    let (lock, ready) = &*gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    let settlement = released_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(settlement.all_released);
}

#[test]
fn test_byte_budget_rejects_aggregate_payload_and_shutdown_joins_worker() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(1).unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (started_tx, started_rx) = mpsc::channel();
    let engine_gate = Arc::clone(&gate);
    let engine: ManagedInferenceEngine = Box::new(move |_bytes| {
        started_tx.send(()).unwrap();
        let (lock, ready) = &*engine_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        Ok(Some("text/plain".to_owned()))
    });
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        engine,
        Box::new(|| {}),
    ));
    let body_runtime = Arc::clone(&runtime);
    let (rejected_tx, rejected_rx) = mpsc::channel();
    let handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            let mut first = Box::pin(body_runtime.detect_bytes(&scope, vec![0; 5]));
            std::future::poll_fn(|cx| {
                assert!(first.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            let rejected = body_runtime.detect_bytes(&scope, vec![0; 5]).await.is_err();
            rejected_tx.send(rejected).unwrap();
            first.await.map(|_| ())
        })
        .expect("managed operation should be admitted");

    started_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(rejected_rx.recv_timeout(Duration::from_secs(30)).unwrap());
    let observer = handle.observer();
    let (released_tx, released_rx) = mpsc::channel();
    std::thread::spawn(move || {
        released_tx.send(observer.released_blocking().unwrap()).unwrap();
    });
    assert!(released_rx.recv_timeout(Duration::from_millis(30)).is_err());

    let (lock, ready) = &*gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    assert!(released_rx.recv_timeout(Duration::from_secs(30)).unwrap().all_released);
    let shutdown = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    assert!(shutdown.admission.is_complete());
    assert!(shutdown.worker_stopped);
    assert!(!shutdown.worker_panicked);
}

#[test]
fn test_managed_engine_error_is_delivered_and_reported_as_failed_child() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(1).unwrap();
    let engine: ManagedInferenceEngine = Box::new(|_bytes| {
        Err(qubit_mime::MimeError::detector_backend(
            "fixture",
            "original engine error",
        ))
    });
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        64,
        engine,
        Box::new(|| {}),
    ));
    let body_runtime = Arc::clone(&runtime);
    let (report_tx, report_rx) = mpsc::channel();
    let mut handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            let result = body_runtime.detect_bytes(&scope, b"fixture".to_vec()).await;
            let report = scope.join().await;
            report_tx.send(report).unwrap();
            result.map(|_| ())
        })
        .expect("managed operation should be admitted");

    let outcome = handle.result_blocking().unwrap();
    let qubit_operation::report::OperationOutcome::Failed(error) = outcome else {
        panic!("operation should return the inference error");
    };
    assert!(error.to_string().contains("original engine error"));
    let report = report_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    assert_eq!(report.required_failures.len(), 1);
    assert_eq!(report.required_failures[0].outcome, ChildBusinessOutcome::Failed);
}

#[test]
fn test_service_group_rejects_inference_when_running_and_queued_capacity_is_full() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(1).unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (started_tx, started_rx) = mpsc::channel();
    let engine_gate = Arc::clone(&gate);
    let engine: ManagedInferenceEngine = Box::new(move |_bytes| {
        started_tx.send(()).unwrap();
        let (lock, ready) = &*engine_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        Ok(Some("text/plain".to_owned()))
    });
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        64,
        engine,
        Box::new(|| {}),
    ));
    let body_runtime = Arc::clone(&runtime);
    let (rejected_tx, rejected_rx) = mpsc::channel();
    let mut handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            let mut running = Box::pin(body_runtime.detect_bytes(&scope, b"running".to_vec()));
            std::future::poll_fn(|cx| {
                assert!(running.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            started_rx.recv_timeout(Duration::from_secs(30)).unwrap();

            let mut queued = Box::pin(body_runtime.detect_bytes(&scope, b"queued".to_vec()));
            std::future::poll_fn(|cx| {
                assert!(queued.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            let rejected = body_runtime
                .detect_bytes(&scope, b"rejected".to_vec())
                .await
                .unwrap_err();
            rejected_tx.send(format!("{rejected:?}")).unwrap();

            let (lock, ready) = &*gate;
            *lock.lock().unwrap() = true;
            ready.notify_all();
            running.await?;
            queued.await?;
            scope.request_cancel(StopReason::UserCancelled);
            let scope_rejected = body_runtime
                .detect_bytes(&scope, b"cancelled-scope".to_vec())
                .await
                .unwrap_err();
            rejected_tx.send(format!("{scope_rejected:?}")).unwrap();
            Ok::<(), qubit_mime::MimeError>(())
        })
        .expect("managed operation should be admitted");

    let first_rejection = rejected_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    let second_rejection = rejected_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(first_rejection.contains("runtime admission failed"));
    assert!(
        second_rejection.contains("could not commit inference"),
        "{second_rejection}"
    );
    assert!(matches!(
        handle.result_blocking().unwrap(),
        qubit_operation::report::OperationOutcome::Succeeded(())
    ));
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    assert!(report.admission.is_complete());
    assert!(report.worker_stopped);
}

#[test]
fn test_sync_provider_backend_classifies_a_rooted_local_file() {
    let root = tempfile::tempdir().expect("temporary root should be created");
    std::fs::write(
        root.path().join("script.py"),
        b"#!/usr/bin/env python3\nprint('hello')\n",
    )
    .expect("fixture should be written");
    let file_system =
        qubit_fs_local::LocalFileSystems::rooted(root.path(), qubit_fs_local::LocalResourcePolicy::unbounded())
            .expect("rooted local filesystem should open");
    let detector = crate::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize");
    let path = qubit_fs::Path::parse("/script.py").expect("fixture path should be valid");

    let (candidates, returned_bytes) =
        MimeDetectorBackend::guess_from_provider_path(&detector, &file_system, &path, 4096)
            .expect("sync provider path should classify the Python sample");

    assert_eq!(vec!["text/x-python"], candidates);
    assert!(returned_bytes.is_none());
    let report = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    assert!(report.worker_stopped);
}

#[test]
fn test_cancel_shutdown_reports_blocked_worker_until_real_release() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(1).unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (started_tx, started_rx) = mpsc::channel();
    let engine_gate = Arc::clone(&gate);
    let engine: ManagedInferenceEngine = Box::new(move |_bytes| {
        started_tx.send(()).unwrap();
        let (lock, ready) = &*engine_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = ready.wait(released).unwrap();
        }
        Ok(Some("text/plain".to_owned()))
    });
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        16,
        engine,
        Box::new(|| {}),
    ));
    let body_runtime = Arc::clone(&runtime);
    let mut handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            body_runtime.detect_bytes(&scope, b"held".to_vec()).await.map(|_| ())
        })
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(30)).unwrap();

    let timeout = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_millis(10));
    assert!(!timeout.admission.is_complete());
    assert!(!timeout.worker_stopped);

    let (lock, ready) = &*gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    let _ = handle.result_blocking();
    assert!(handle.observer().released_blocking().unwrap().all_released);
    let complete = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(30));
    assert!(complete.admission.is_complete());
    assert!(complete.worker_stopped);
}

#[test]
fn test_shutdown_timeout_includes_worker_exit_after_child_release() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(1).unwrap();
    let worker_exit_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (worker_exit_tx, worker_exit_rx) = mpsc::channel();
    let gate = Arc::clone(&worker_exit_gate);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine_and_worker_exit_hook(
        supervisor,
        64,
        Box::new(|_bytes| Ok(Some("text/plain".to_owned()))),
        Box::new(|| {}),
        Box::new(move || {
            worker_exit_tx.send(()).unwrap();
            let (lock, ready) = &*gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            panic!("controlled worker exit panic");
        }),
    ));
    let body_runtime = Arc::clone(&runtime);
    let mut handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            body_runtime.detect_bytes(&scope, b"fixture".to_vec()).await.map(|_| ())
        })
        .unwrap();
    let operation_succeeded = matches!(
        handle.result_blocking(),
        Ok(qubit_operation::report::OperationOutcome::Succeeded(()))
    );
    let resources_released = handle
        .observer()
        .released_blocking()
        .is_ok_and(|settlement| settlement.all_released);

    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown_thread = std::thread::spawn(move || {
        shutdown_tx
            .send(shutdown_runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_millis(150)))
            .unwrap();
    });
    let worker_reached_exit = worker_exit_rx.recv_timeout(Duration::from_secs(2)).is_ok();
    let timed_result = shutdown_rx.recv_timeout(Duration::from_millis(300));

    let (first_observer_tx, first_observer_rx) = mpsc::channel();
    let first_runtime = Arc::clone(&runtime);
    let first_observer = std::thread::spawn(move || {
        first_observer_tx
            .send(first_runtime.shutdown_blocking(ShutdownMode::Drain, Duration::ZERO))
            .unwrap();
    });
    let (second_observer_tx, second_observer_rx) = mpsc::channel();
    let second_runtime = Arc::clone(&runtime);
    let second_observer = std::thread::spawn(move || {
        second_observer_tx
            .send(second_runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::ZERO))
            .unwrap();
    });
    let first_observer_result = first_observer_rx.recv_timeout(Duration::from_secs(2));
    let second_observer_result = second_observer_rx.recv_timeout(Duration::from_secs(2));

    // Always release the worker and join the shutdown caller before asserting.
    let (lock, ready) = &*worker_exit_gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    shutdown_thread.join().unwrap();
    first_observer.join().unwrap();
    second_observer.join().unwrap();

    let finish_deadline = std::time::Instant::now() + Duration::from_secs(2);
    let final_report = loop {
        let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::ZERO);
        if report.worker_stopped || std::time::Instant::now() >= finish_deadline {
            break report;
        }
        std::thread::sleep(Duration::from_millis(1));
    };
    let repeated_report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::ZERO);

    let timed_report = timed_result.expect("shutdown should honor its deadline while worker exit is gated");
    let first_observer_report = first_observer_result.expect("first concurrent observer should return");
    let second_observer_report = second_observer_result.expect("second concurrent observer should return");
    assert!(operation_succeeded);
    assert!(resources_released);
    assert!(worker_reached_exit);
    assert!(!timed_report.worker_stopped);
    assert!(!first_observer_report.worker_stopped);
    assert!(!second_observer_report.worker_stopped);
    assert!(final_report.worker_stopped);
    assert!(final_report.worker_panicked);
    assert!(repeated_report.worker_stopped);
    assert!(repeated_report.worker_panicked);
}

#[test]
fn test_public_runtime_detection_preserves_result_release_and_shutdown() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor.clone(),
        MimeConfig::default().max_buffer_size(),
        Box::new(|bytes| Ok(Some(format!("application/x-{}", bytes.len())))),
        Box::new(|| {}),
    ));
    assert!(format!("{runtime:?}").contains("ManagedInferenceRuntime"));
    let detector = Arc::new(
        crate::MagikaMimeDetector::builder()
            .managed_runtime(Arc::clone(&runtime))
            .build()
            .unwrap(),
    );
    let request_detector = Arc::clone(&detector);
    let service = supervisor.service_group(2).unwrap();
    let mut handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            request_detector.detect_bytes_managed(&scope, vec![1, 2, 3]).await
        })
        .unwrap();

    let outcome = handle.result_blocking().unwrap();
    assert!(matches!(
        outcome,
        qubit_operation::report::OperationOutcome::Succeeded(Some(value)) if value == "application/x-3"
    ));
    assert!(handle.observer().released_blocking().unwrap().all_released);
    let shutdown = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    assert!(shutdown.worker_stopped);
    assert!(shutdown.admission.is_complete());
}

#[test]
fn test_managed_bytes_respects_detector_limit() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let limit = MimeConfig::default().max_buffer_size();
    let calls = Arc::new(AtomicUsize::new(0));
    let engine_calls = Arc::clone(&calls);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor.clone(),
        limit.saturating_mul(2),
        Box::new(move |_bytes| {
            engine_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some("text/plain".to_owned()))
        }),
        Box::new(|| {}),
    ));
    let detector = Arc::new(
        crate::MagikaMimeDetector::builder()
            .managed_runtime(Arc::clone(&runtime))
            .build()
            .unwrap(),
    );
    let service = supervisor.service_group(1).unwrap();
    let request_detector = Arc::clone(&detector);
    let request_calls = Arc::clone(&calls);
    let mut handle = service
        .start(ScopeOptions::default(), move |scope| async move {
            let error = request_detector
                .detect_bytes_managed(&scope, vec![0; limit + 1])
                .await
                .unwrap_err();
            assert!(matches!(error, qubit_mime::MimeError::BufferLimitExceeded { requested, limit: actual } if requested == limit + 1 && actual == limit));
            assert_eq!(request_calls.load(Ordering::SeqCst), 0);
            request_detector
                .detect_bytes_managed(&scope, vec![0; limit])
                .await?;
            request_detector
                .detect_bytes_managed(&scope, vec![0; limit - 1])
                .await?;
            Ok::<(), qubit_mime::MimeError>(())
        })
        .unwrap();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        qubit_operation::report::OperationOutcome::Succeeded(())
    ));
    assert!(handle.observer().released_blocking().unwrap().all_released);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        runtime
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30))
            .worker_stopped
    );
}

#[test]
fn test_async_content_yields_while_worker_is_gated() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let engine_gate = Arc::clone(&gate);
    let (started_tx, started_rx) = mpsc::channel();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        MimeConfig::default().max_buffer_size(),
        Box::new(move |_bytes| {
            started_tx.send(()).unwrap();
            let (lock, ready) = &*engine_gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            Ok(Some("text/plain".to_owned()))
        }),
        Box::new(|| {}),
    ));
    let detector = crate::MagikaMimeDetector::builder()
        .managed_runtime(Arc::clone(&runtime))
        .build()
        .unwrap();
    let mut detection = Box::pin(MimeDetectorBackend::guess_from_content_async(&detector, b"fixture"));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());

    // This watchdog also releases a synchronous implementation, turning the
    // regression into a bounded assertion failure instead of a hung suite.
    let watchdog_gate = Arc::clone(&gate);
    let (watchdog_cancel_tx, watchdog_cancel_rx) = mpsc::channel();
    let watchdog = std::thread::spawn(move || {
        if watchdog_cancel_rx.recv_timeout(Duration::from_secs(3)).is_err() {
            let (lock, ready) = &*watchdog_gate;
            *lock.lock().unwrap() = true;
            ready.notify_all();
        }
    });

    let first_poll = detection.as_mut().poll(&mut context);
    assert!(first_poll.is_pending(), "async content hook blocked its polling thread");
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("the worker should enter the gated inference");
    let mut heartbeat = 0;
    let mut heartbeat_task = Box::pin(std::future::poll_fn(|_| {
        heartbeat += 1;
        std::task::Poll::Ready(())
    }));
    assert!(heartbeat_task.as_mut().poll(&mut context).is_ready());
    assert_eq!(heartbeat, 1, "executor work must run while inference is gated");

    let (lock, ready) = &*gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    watchdog_cancel_tx.send(()).unwrap();
    watchdog.join().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let std::task::Poll::Ready(result) = detection.as_mut().poll(&mut context) {
            assert_eq!(result.unwrap(), vec!["text/plain"]);
            break;
        }
        assert!(std::time::Instant::now() < deadline, "worker result did not arrive");
        std::thread::yield_now();
    }
    assert!(
        runtime
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30))
            .worker_stopped
    );
}

#[test]
fn test_async_content_returns_empty_candidate_and_worker_error() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let engine_calls = Arc::clone(&calls);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        MimeConfig::default().max_buffer_size(),
        Box::new(move |bytes| {
            engine_calls.fetch_add(1, Ordering::SeqCst);
            match bytes.as_slice() {
                b"none" => Ok(None),
                b"error" => Err(qubit_mime::MimeError::detector_backend("fixture", "inference failed")),
                _ => Ok(Some("text/plain".to_owned())),
            }
        }),
        Box::new(|| {}),
    ));
    let detector = crate::MagikaMimeDetector::builder()
        .managed_runtime(Arc::clone(&runtime))
        .build()
        .unwrap();
    let unpolled = MimeDetectorBackend::guess_from_content_async(&detector, b"not-polled");
    drop(unpolled);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "an unpolled future must not submit work"
    );
    assert!(
        block_on_content(MimeDetectorBackend::guess_from_content_async(&detector, b"none"))
            .unwrap()
            .is_empty()
    );
    let error = block_on_content(MimeDetectorBackend::guess_from_content_async(&detector, b"error")).unwrap_err();
    assert!(error.to_string().contains("inference failed"));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        runtime
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30))
            .worker_stopped
    );
}

#[test]
fn test_async_content_rejects_detector_limit_and_closed_runtime() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let limit = MimeConfig::default().max_buffer_size();
    let calls = Arc::new(AtomicUsize::new(0));
    let engine_calls = Arc::clone(&calls);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        limit,
        Box::new(move |_bytes| {
            engine_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some("text/plain".to_owned()))
        }),
        Box::new(|| {}),
    ));
    let detector = crate::MagikaMimeDetector::builder()
        .managed_runtime(Arc::clone(&runtime))
        .build()
        .unwrap();
    let error = block_on_content(MimeDetectorBackend::guess_from_content_async(
        &detector,
        &vec![0; limit + 1],
    ))
    .unwrap_err();
    assert!(
        matches!(error, qubit_mime::MimeError::BufferLimitExceeded { requested, limit: actual } if requested == limit + 1 && actual == limit)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    assert!(
        runtime
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30))
            .worker_stopped
    );
    assert!(block_on_content(MimeDetectorBackend::guess_from_content_async(&detector, b"closed")).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn test_unmanaged_content_api_enforces_runtime_byte_limit() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let engine_calls = Arc::clone(&calls);
    let runtime = ManagedInferenceRuntime::with_engine(
        supervisor,
        4,
        Box::new(move |_bytes| {
            engine_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some("text/plain".to_owned()))
        }),
        Box::new(|| {}),
    );
    let error = block_on_content(runtime.detect_content_unmanaged(b"12345")).unwrap_err();
    assert!(matches!(
        error,
        qubit_mime::MimeError::BufferLimitExceeded { requested: 5, limit: 4 }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        block_on_content(runtime.detect_content_unmanaged(b"1234")).unwrap(),
        Some("text/plain".to_owned())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        runtime
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30))
            .worker_stopped
    );
}

#[test]
fn test_async_content_drop_releases_capacity_after_worker_finishes() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let engine_gate = Arc::clone(&gate);
    let (started_tx, started_rx) = mpsc::channel();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        MimeConfig::default().max_buffer_size(),
        Box::new(move |_bytes| {
            started_tx.send(()).unwrap();
            let (lock, ready) = &*engine_gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            Ok(Some("text/plain".to_owned()))
        }),
        Box::new(|| {}),
    ));
    let detector = crate::MagikaMimeDetector::builder()
        .managed_runtime(Arc::clone(&runtime))
        .build()
        .unwrap();
    let mut abandoned = Box::pin(MimeDetectorBackend::guess_from_content_async(&detector, b"first"));
    let context = &mut std::task::Context::from_waker(std::task::Waker::noop());
    assert!(abandoned.as_mut().poll(context).is_pending());
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first request should reach the worker");
    drop(abandoned);

    let mut queued = Box::pin(MimeDetectorBackend::guess_from_content_async(&detector, b"queued"));
    assert!(queued.as_mut().poll(context).is_pending());
    let rejected = block_on_content(MimeDetectorBackend::guess_from_content_async(&detector, b"full"));
    assert!(rejected.is_err(), "running plus queued requests should fill capacity");

    let (lock, ready) = &*gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    assert_eq!(block_on_content(queued).unwrap(), vec!["text/plain"]);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match block_on_content(MimeDetectorBackend::guess_from_content_async(&detector, b"reused")) {
            Ok(candidates) => {
                assert_eq!(candidates, vec!["text/plain"]);
                break;
            }
            Err(_) => {
                assert!(std::time::Instant::now() < deadline, "worker capacity was not released");
                std::thread::yield_now();
            }
        }
    }
    assert!(
        runtime
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30))
            .worker_stopped
    );
}

fn block_on_content<F: Future>(future: F) -> F::Output {
    struct ThreadWake(std::thread::Thread);
    impl std::task::Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(future);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let std::task::Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return result;
        }
        let now = std::time::Instant::now();
        assert!(now < deadline, "async content hook did not complete");
        std::thread::park_timeout(deadline.saturating_duration_since(now));
    }
}
