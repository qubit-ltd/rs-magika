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
use qubit_operation::report::ChildBusinessOutcome;
use qubit_operation::protocol::StopReason;
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
        Err(qubit_mime::MimeError::detector_backend("fixture", "original engine error"))
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
