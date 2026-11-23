// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

#![cfg(feature = "bundled-onnxruntime")]

use std::future::Future;
use std::io::Cursor;
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
use qubit_magika::ManagedInferenceRuntime;
use qubit_mime::MimeDetectionPolicy;
use qubit_mime::MimeDetector;
use qubit_mime::MimeDetectorBackend;
use qubit_mime::MimeError;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::ScopeLimits;
use qubit_operation::ShutdownMode;
use qubit_operation::StopReason;

use crate::support::AsyncProviderGate;
use crate::support::AsyncProviderGatePoint;
use crate::support::ProviderFileSystemSpi;
use crate::support::ShutdownBlocking;
use crate::support::detector;
use crate::support::run_ready;
use crate::support::run_until;

const PYTHON: &[u8] = b"#!/usr/bin/env python3\nprint('hello')\n";

struct ReleaseProviderGateOnDrop(Arc<AsyncProviderGate>);

impl Drop for ReleaseProviderGateOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

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

fn managed_provider_result(
    detector: &'static qubit_magika::MagikaMimeDetector,
    file_system: qubit_fs::AsyncFileSystem,
    max_bytes: usize,
) -> Result<Option<String>, MimeError> {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 4 }).unwrap();
    let mut handle = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            detector
                .detect_async_provider_path_managed(&scope, &file_system, &path("/matrix.py"), max_bytes)
                .await
        })
        .unwrap();
    let outcome = run_ready(handle.wait_outcome()).unwrap();
    let result = match outcome {
        qubit_operation::OperationOutcome::Succeeded(mime) => Ok(mime),
        qubit_operation::OperationOutcome::Failed(error) => Err(error),
        other => panic!("managed provider operation did not complete: {other:?}"),
    };
    assert!(run_ready(handle.wait_settlement()).all_released());
    assert!(
        service
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5))
            .snapshot
            .active_operations
            == 0
    );
    assert!(
        supervisor
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5))
            .snapshot
            .active_operations
            == 0
    );
    result
}

fn assert_provider_observations_equal(
    legacy: &crate::support::ProviderObservations,
    managed: &crate::support::ProviderObservations,
) {
    assert_eq!(
        legacy.stats.load(Ordering::SeqCst),
        managed.stats.load(Ordering::SeqCst)
    );
    assert_eq!(
        legacy.opens.load(Ordering::SeqCst),
        managed.opens.load(Ordering::SeqCst)
    );
    assert_eq!(
        legacy.random_opens.load(Ordering::SeqCst),
        managed.random_opens.load(Ordering::SeqCst)
    );
    assert_eq!(
        legacy.random_reads.load(Ordering::SeqCst),
        managed.random_reads.load(Ordering::SeqCst)
    );
    assert_eq!(
        legacy.session_read_lengths.lock().unwrap().as_slice(),
        managed.session_read_lengths.lock().unwrap().as_slice()
    );
    assert_eq!(
        legacy.session_drops.load(Ordering::SeqCst),
        managed.session_drops.load(Ordering::SeqCst)
    );
    assert_eq!(
        legacy.read_bytes.load(Ordering::SeqCst),
        managed.read_bytes.load(Ordering::SeqCst)
    );
    let options = |observations: &crate::support::ProviderObservations| {
        observations
            .options
            .lock()
            .unwrap()
            .iter()
            .map(|options| {
                (
                    options.offset(),
                    options.length(),
                    options.if_match().map(|etag| etag.as_str().to_owned()),
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(options(legacy), options(managed));
}

fn assert_legacy_managed_provider_equivalent(
    make_provider: impl Fn() -> ProviderFileSystemSpi,
    max_bytes: usize,
    expected_etag: Option<&str>,
) {
    let legacy_provider = make_provider();
    let legacy_observations = legacy_provider.observations();
    let legacy = run_ready(MimeDetectorBackend::guess_from_async_provider_path(
        detector(),
        &legacy_provider.async_file_system(),
        &path("/matrix.py"),
        max_bytes,
    ))
    .map(|(candidates, _)| candidates.into_iter().next());

    let managed_provider = make_provider();
    let managed_observations = managed_provider.observations();
    let managed = managed_provider_result(detector(), managed_provider.async_file_system(), max_bytes);
    let normalized = |result: Result<Option<String>, MimeError>| result.map_err(|error| format!("{error:?}"));
    let legacy = legacy.map_err(|error| format!("{error:?}"));
    assert_eq!(legacy, normalized(managed));
    assert_provider_observations_equal(&legacy_observations, &managed_observations);
    if let Some(expected_etag) = expected_etag {
        for observations in [&legacy_observations, &managed_observations] {
            let options = observations.options.lock().unwrap();
            assert!(!options.is_empty());
            assert!(
                options
                    .iter()
                    .all(|options| { options.if_match().is_some_and(|etag| etag.as_str() == expected_etag) })
            );
        }
    }
}

#[test]
fn test_async_provider_admitted_flow_matches_legacy_and_managed_entrances() {
    assert_legacy_managed_provider_equivalent(
        || ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access(),
        8192,
        None,
    );
    assert_legacy_managed_provider_equivalent(
        || large_provider(true).with_conditional().with_max_read_range_bytes(2),
        8192,
        Some("v1"),
    );
    assert_legacy_managed_provider_equivalent(
        || ProviderFileSystemSpi::new(PYTHON.to_vec()).without_range(),
        8192,
        None,
    );
    assert_legacy_managed_provider_equivalent(
        || {
            ProviderFileSystemSpi::new(PYTHON.to_vec())
                .with_range()
                .random_open_error(qubit_fs::error::FsErrorKind::PermissionDenied)
        },
        8192,
        None,
    );
    assert_legacy_managed_provider_equivalent(|| large_provider(false), 8192, None);
    assert_legacy_managed_provider_equivalent(
        || {
            ProviderFileSystemSpi::new(PYTHON.to_vec())
                .with_random_access()
                .fail_reads()
        },
        8192,
        None,
    );
}

#[test]
fn test_async_provider_admission_failure_matches_both_entrances_without_io() {
    let detector = Arc::new(qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"));
    assert!(
        detector
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30))
            .worker_stopped
    );

    let legacy_provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access();
    let legacy_observations = legacy_provider.observations();
    let legacy = run_ready(MimeDetectorBackend::guess_from_async_provider_path(
        detector.as_ref(),
        &legacy_provider.async_file_system(),
        &path("/matrix.py"),
        8192,
    ))
    .map(|(candidates, _)| candidates.into_iter().next());

    let managed_provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access();
    let managed_observations = managed_provider.observations();
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 1 }).unwrap();
    let body_detector = Arc::clone(&detector);
    let file_system = managed_provider.async_file_system();
    let mut handle = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            let result = body_detector
                .detect_async_provider_path_managed(&scope, &file_system, &path("/matrix.py"), 8192)
                .await;
            Ok::<_, MimeError>(result)
        })
        .unwrap();
    let outcome = run_ready(handle.wait_outcome()).unwrap();
    let managed = match outcome {
        qubit_operation::OperationOutcome::Succeeded(result) => result,
        qubit_operation::OperationOutcome::Failed(error) => Err(error),
        other => panic!("managed admission operation did not complete: {other:?}"),
    };
    assert!(run_ready(handle.wait_settlement()).all_released());
    let _ = service.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    let _ = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    assert_eq!(
        legacy.map_err(|error| format!("{error:?}")),
        managed.map_err(|error| format!("{error:?}"))
    );
    assert_provider_observations_equal(&legacy_observations, &managed_observations);
    assert_eq!(0, legacy_observations.random_opens.load(Ordering::SeqCst));
    assert_eq!(0, legacy_observations.stats.load(Ordering::SeqCst));
    assert_eq!(0, legacy_observations.opens.load(Ordering::SeqCst));
    assert_eq!(0, legacy_observations.random_reads.load(Ordering::SeqCst));
    assert_eq!(0, legacy_observations.read_bytes.load(Ordering::SeqCst));
    assert_eq!(0, managed_observations.stats.load(Ordering::SeqCst));
    assert_eq!(0, managed_observations.opens.load(Ordering::SeqCst));
    assert_eq!(0, managed_observations.random_opens.load(Ordering::SeqCst));
    assert_eq!(0, managed_observations.random_reads.load(Ordering::SeqCst));
    assert_eq!(0, managed_observations.read_bytes.load(Ordering::SeqCst));
}

fn assert_full_capacity_rejects_second_async_provider_route(first_managed: bool) -> (String, String) {
    let detector = Arc::new(qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"));
    let capacity = detector.max_buffer_size();
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 4 }).unwrap();
    let first_gate = AsyncProviderGate::new(AsyncProviderGatePoint::Read);
    let _release_gate = ReleaseProviderGateOnDrop(Arc::clone(&first_gate));
    let first_provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .with_async_gate(Arc::clone(&first_gate));
    let first_observations = first_provider.observations();
    let first_file_system = first_provider.async_file_system();
    let first_detector = Arc::clone(&detector);
    let mut managed_first = None;
    let mut legacy_first = None;

    if first_managed {
        managed_first = Some(
            service
                .as_async()
                .try_submit(OperationOptions::default(), move |scope| async move {
                    first_detector
                        .detect_async_provider_path_managed(
                            &scope,
                            &first_file_system,
                            &path("/capacity-first.py"),
                            capacity,
                        )
                        .await
                })
                .unwrap(),
        );
    } else {
        legacy_first = Some(std::thread::spawn(move || {
            run_ready(MimeDetectorBackend::guess_from_async_provider_path(
                first_detector.as_ref(),
                &first_file_system,
                &path("/capacity-first.py"),
                capacity,
            ))
        }));
    }

    first_gate.wait_until_entered(Duration::from_secs(10));
    let second_provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access();
    let second_observations = second_provider.observations();
    let second_file_system = second_provider.async_file_system();
    let second_result = if first_managed {
        let result = run_ready(MimeDetectorBackend::guess_from_async_provider_path(
            detector.as_ref(),
            &second_file_system,
            &path("/capacity-second.py"),
            1,
        ));
        result.map(|(candidates, _)| candidates.into_iter().next())
    } else {
        let second_detector = Arc::clone(&detector);
        let mut second = service
            .as_async()
            .try_submit(OperationOptions::default(), move |scope| async move {
                second_detector
                    .detect_async_provider_path_managed(&scope, &second_file_system, &path("/capacity-second.py"), 1)
                    .await
            })
            .unwrap();
        let outcome = run_ready(second.wait_outcome()).unwrap();
        let result = match outcome {
            qubit_operation::OperationOutcome::Succeeded(mime) => Ok(mime),
            qubit_operation::OperationOutcome::Failed(error) => Err(error),
            other => panic!("managed capacity rejection did not complete: {other:?}"),
        };
        assert!(run_ready(second.wait_settlement()).all_released());
        result
    };

    // Always wake the admitted request before asserting so its driver and
    // worker can settle and release their reserved capacity.
    first_gate.release();
    let first_result = if let Some(mut handle) = managed_first {
        let outcome = run_ready(handle.wait_outcome()).unwrap();
        let result = match outcome {
            qubit_operation::OperationOutcome::Succeeded(mime) => Ok(mime),
            qubit_operation::OperationOutcome::Failed(error) => Err(error),
            other => panic!("managed capacity holder did not complete: {other:?}"),
        };
        assert!(run_ready(handle.wait_settlement()).all_released());
        result
    } else {
        legacy_first
            .take()
            .unwrap()
            .join()
            .expect("scope-less capacity holder should not panic")
            .map(|(candidates, _)| candidates.into_iter().next())
    };
    assert!(
        service
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(10))
            .snapshot
            .active_operations
            == 0
    );
    assert!(
        supervisor
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(10))
            .snapshot
            .active_operations
            == 0
    );
    assert!(
        detector
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30))
            .worker_stopped
    );

    assert_eq!(
        Some("text/x-python".to_owned()),
        first_result.expect("first capacity holder should classify the fixture")
    );
    let second_error = second_result.expect_err("second request should be rejected by full capacity");
    let (backend, reason) = match second_error {
        MimeError::DetectorBackend { backend, reason } => (backend, reason),
        other => panic!("expected a detector budget error, got {other:?}"),
    };
    assert_eq!("magika", backend);
    assert_eq!("runtime in-flight byte budget is full", reason);
    assert_eq!(0, second_observations.stats.load(Ordering::SeqCst));
    assert_eq!(0, second_observations.opens.load(Ordering::SeqCst));
    assert_eq!(0, second_observations.random_opens.load(Ordering::SeqCst));
    assert_eq!(0, second_observations.random_reads.load(Ordering::SeqCst));
    assert_eq!(0, second_observations.read_bytes.load(Ordering::SeqCst));
    assert!(second_observations.options.lock().unwrap().is_empty());
    assert_eq!(1, first_observations.random_opens.load(Ordering::SeqCst));
    assert!(first_observations.random_reads.load(Ordering::SeqCst) > 0);
    (backend, reason)
}

#[test]
fn test_async_provider_full_capacity_rejects_both_entrypoint_orders_before_io() {
    // Cancellation is scope-specific and cannot be applied to the legacy
    // scope-less entrance; cancelled-scope and pending-capacity behavior stay
    // covered by their dedicated managed and scope-less tests.
    let managed_first = assert_full_capacity_rejects_second_async_provider_route(true);
    let legacy_first = assert_full_capacity_rejects_second_async_provider_route(false);
    assert_eq!(managed_first, legacy_first);
}

#[test]
fn test_scope_admission_rejection_performs_no_io() {
    for stopped in [true, false] {
        let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
        let service = supervisor.group(qubit_operation::GroupOptions { capacity: 2 }).unwrap();
        let config = qubit_mime::MimeConfig::default();
        let runtime = Arc::new(ManagedInferenceRuntime::new(supervisor.clone(), config.max_buffer_size()).unwrap());
        let detector = Arc::new(
            qubit_magika::MagikaMimeDetector::builder()
                .mime_config(config)
                .managed_runtime(Arc::clone(&runtime))
                .build()
                .unwrap(),
        );
        let provider = ProviderFileSystemSpi::new(PYTHON.to_vec());
        let observations = provider.observations();
        let file_system = provider.async_file_system();
        let request_detector = Arc::clone(&detector);
        let options = if stopped {
            OperationOptions::default()
        } else {
            OperationOptions {
                limits: ScopeLimits {
                    resources: 1,
                    ..ScopeLimits::default()
                },
                ..OperationOptions::default()
            }
        };
        let (facts_tx, facts_rx) = std::sync::mpsc::channel();
        let mut operation = service
            .as_async()
            .try_submit(options, move |scope| async move {
                let _held = if stopped { None } else { Some(scope.manage(()).unwrap()) };
                if stopped {
                    let _ = scope.request_stop(StopReason::UserCancelled);
                }
                let error = request_detector
                    .detect_async_provider_path_managed(&scope, &file_system, &path("/scope/rejected.py"), 4096)
                    .await
                    .unwrap_err();
                facts_tx.send(format!("{error:?}")).unwrap();
                Ok::<_, MimeError>(())
            })
            .unwrap();
        let outcome = run_ready(operation.wait_outcome()).expect("operation outcome must be available");
        if stopped {
            assert!(
                matches!(outcome, qubit_operation::OperationOutcome::Succeeded(()))
                    || matches!(outcome, qubit_operation::OperationOutcome::Stopped(ref report) if report.reason == StopReason::UserCancelled),
                "unexpected outcome: {outcome:?}"
            );
        } else {
            assert!(
                matches!(outcome, qubit_operation::OperationOutcome::Succeeded(())),
                "unexpected outcome: {outcome:?}"
            );
        }
        let error = facts_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("scope rejection fact must arrive");
        assert!(
            error.contains("operation scope rejected inference"),
            "unexpected error: {error}"
        );
        assert_eq!(observations.stats.load(Ordering::SeqCst), 0);
        assert_eq!(observations.opens.load(Ordering::SeqCst), 0);
        assert_eq!(observations.random_opens.load(Ordering::SeqCst), 0);
        assert_eq!(observations.random_reads.load(Ordering::SeqCst), 0);
        assert_eq!(observations.read_bytes.load(Ordering::SeqCst), 0);
        let report = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(5));
        assert!(report.worker_stopped);
        assert!(
            service
                .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5))
                .snapshot
                .active_operations
                == 0
        );
        assert!(
            supervisor
                .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5))
                .snapshot
                .active_operations
                == 0
        );
    }
}

#[test]
fn test_managed_guide_reuses_detector_for_two_requests() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 2 }).unwrap();
    let config = qubit_mime::MimeConfig::default();
    let runtime = Arc::new(
        ManagedInferenceRuntime::new(supervisor.clone(), config.max_buffer_size())
            .expect("managed inference runtime should initialize"),
    );
    let detector = Arc::new(
        qubit_magika::MagikaMimeDetector::builder()
            .mime_config(config)
            .managed_runtime(runtime)
            .build()
            .expect("bundled ONNX Runtime should initialize"),
    );

    // Hold the first Provider read pending while a separate future advances on
    // this test thread. The request observer uses the guide's async result and
    // release APIs throughout.
    let gate = AsyncProviderGate::new(AsyncProviderGatePoint::Open);
    let first_provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .with_async_gate(Arc::clone(&gate));
    let first_fs = first_provider.async_file_system();
    let first_detector = Arc::clone(&detector);
    let mut first = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            first_detector
                .detect_async_provider_path_managed(&scope, &first_fs, &path("/guide/first.py"), 4096)
                .await
        })
        .unwrap();
    gate.wait_until_entered(Duration::from_secs(5));

    let first_wait = async {
        let outcome = first.wait_outcome().await.unwrap();
        let settlement = first.wait_settlement().await;
        (outcome, settlement)
    };
    let mut first_wait = Box::pin(first_wait);
    let heartbeat_reached = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let heartbeat_flag = Arc::clone(&heartbeat_reached);
    let mut heartbeat = Box::pin(async move {
        heartbeat_flag.store(true, Ordering::SeqCst);
    });
    let mut heartbeat_done = false;
    run_ready(std::future::poll_fn(|context| {
        let _ = first_wait.as_mut().poll(context);
        if !heartbeat_done && heartbeat.as_mut().poll(context).is_ready() {
            heartbeat_done = true;
        }
        if heartbeat_done { Poll::Ready(()) } else { Poll::Pending }
    }));
    assert!(heartbeat_reached.load(Ordering::SeqCst));
    gate.release();
    let (first_outcome, first_settlement) = run_ready(first_wait);
    assert!(matches!(
        first_outcome,
        qubit_operation::OperationOutcome::Succeeded(Some(ref mime)) if mime == "text/x-python"
    ));
    assert!(first_settlement.all_released());

    let second_provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access();
    let second_fs = second_provider.async_file_system();
    let second_detector = Arc::clone(&detector);
    let mut second = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            second_detector
                .detect_async_provider_path_managed(&scope, &second_fs, &path("/guide/second.py"), 4096)
                .await
        })
        .unwrap();
    let second_outcome = run_ready(second.wait_outcome()).unwrap();
    let second_settlement = run_ready(second.wait_settlement());
    assert!(matches!(
        second_outcome,
        qubit_operation::OperationOutcome::Succeeded(Some(ref mime)) if mime == "text/x-python"
    ));
    assert!(second_settlement.all_released());

    assert!(
        service
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5))
            .snapshot
            .active_operations
            == 0
    );
    assert!(
        detector
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30))
            .worker_stopped
    );
    assert!(
        supervisor
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5))
            .snapshot
            .active_operations
            == 0
    );
}

/// Releases a provider gate during unwinding so a failed assertion cannot
/// leave the managed runtime worker blocked in test I/O.
struct ReleaseGateOnDrop(Arc<AsyncProviderGate>);

impl Drop for ReleaseGateOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[test]
fn test_slow_provider_blocks_shared_worker_order() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 4 }).unwrap();
    let config = qubit_mime::MimeConfig::default();
    let runtime_budget = config.max_buffer_size();
    let runtime = Arc::new(
        ManagedInferenceRuntime::new(supervisor.clone(), config.max_buffer_size())
            .expect("managed inference runtime should initialize"),
    );
    let detector = Arc::new(
        qubit_magika::MagikaMimeDetector::builder()
            .mime_config(config)
            .managed_runtime(Arc::clone(&runtime))
            .build()
            .expect("bundled ONNX Runtime should initialize"),
    );

    let gate = AsyncProviderGate::new(AsyncProviderGatePoint::Read);
    let _release_gate = ReleaseGateOnDrop(Arc::clone(&gate));
    let first_provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .with_async_gate(Arc::clone(&gate));
    let first_fs = first_provider.async_file_system();
    let first_detector = Arc::clone(&detector);
    let mut first = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            first_detector
                .detect_async_provider_path_managed(&scope, &first_fs, &path("/hol/first.py"), 4096)
                .await
        })
        .unwrap();
    gate.wait_until_entered(Duration::from_secs(30));

    // Ensure this request fits alongside the provider's full reserved budget.
    // The managed owned-byte entry point always uses the shared worker.
    let second_bytes = PYTHON.to_vec();
    assert!(4096 + second_bytes.len() <= runtime_budget);
    let second_detector = Arc::clone(&detector);
    let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
    let mut second = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            let mut inference = Box::pin(second_detector.detect_bytes_managed(&scope, second_bytes));
            std::future::poll_fn(|context| match inference.as_mut().poll(context) {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => {
                    admitted_tx.send(()).expect("test receiver remains alive");
                    Poll::Pending
                }
            })
            .await
        })
        .unwrap();
    admitted_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("second owned-byte request should be admitted and queued");
    let snapshot = runtime.snapshot();
    assert_eq!(
        1, snapshot.worker_running,
        "the first worker owns the gated provider read"
    );
    assert_eq!(
        1, snapshot.admitted_not_started,
        "the second admitted request waits for the worker"
    );

    // The notification above is emitted only after detect_bytes_managed was
    // polled; that call reserves runtime capacity and dispatches before it
    // waits for the shared worker. Now explicitly observe Pending at the
    // operation result boundary while the first provider read is gated.
    let mut second_result = Box::pin(second.wait_outcome());
    let observed = run_ready(std::future::poll_fn(|context| {
        Poll::Ready(second_result.as_mut().poll(context))
    }));
    assert!(observed.is_pending());
    drop(second_result);

    gate.release();
    let first_outcome = run_ready(first.wait_outcome());
    let first_settlement = run_ready(first.wait_settlement());
    let second_outcome = run_ready(second.wait_outcome());
    let second_settlement = run_ready(second.wait_settlement());
    assert!(matches!(
        first_outcome,
        Ok(qubit_operation::OperationOutcome::Succeeded(Some(ref mime))) if mime == "text/x-python"
    ));
    assert!(matches!(
        second_outcome,
        Ok(qubit_operation::OperationOutcome::Succeeded(Some(ref mime))) if mime == "text/x-python"
    ));
    assert!(first_settlement.all_released());
    assert!(second_settlement.all_released());
    assert!(
        service
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5))
            .snapshot
            .active_operations
            == 0
    );
    assert!(
        detector
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30))
            .worker_stopped
    );
    assert!(
        supervisor
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5))
            .snapshot
            .active_operations
            == 0
    );
}

fn slow_provider_latency_sample(
    slow_detector: Arc<qubit_magika::MagikaMimeDetector>,
    second_detector: Arc<qubit_magika::MagikaMimeDetector>,
    service: &qubit_operation::OperationGroup,
    delay: Duration,
) -> (Duration, Duration) {
    let gate = AsyncProviderGate::new(AsyncProviderGatePoint::Read);
    let _release_gate = ReleaseGateOnDrop(Arc::clone(&gate));
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .with_async_gate(Arc::clone(&gate));
    let file_system = provider.async_file_system();
    let mut first = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            slow_detector
                .detect_async_provider_path_managed(&scope, &file_system, &path("/slow.py"), 4096)
                .await
        })
        .unwrap();
    gate.wait_until_entered(Duration::from_secs(30));

    let submitted_at = Instant::now();
    let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
    let mut second = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            let mut inference = Box::pin(second_detector.detect_bytes_managed(&scope, PYTHON.to_vec()));
            let mut signaled = false;
            std::future::poll_fn(|context| {
                let result = inference.as_mut().poll(context);
                if !signaled {
                    let _ = admitted_tx.send(Instant::now());
                    signaled = true;
                }
                result
            })
            .await
        })
        .unwrap();
    let admitted_at = admitted_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("second request should be admitted");
    let release_gate = Arc::clone(&gate);
    let release_thread = std::thread::spawn(move || {
        std::thread::sleep(delay);
        release_gate.release();
    });
    let start = Instant::now();
    let outcome = run_ready(second.wait_outcome()).unwrap();
    let elapsed = start.elapsed();
    assert!(matches!(outcome, qubit_operation::OperationOutcome::Succeeded(Some(_))));
    release_thread.join().unwrap();
    let first_outcome = run_ready(first.wait_outcome()).unwrap();
    assert!(matches!(
        first_outcome,
        qubit_operation::OperationOutcome::Succeeded(Some(_))
    ));
    assert!(run_ready(first.wait_settlement()).all_released());
    assert!(run_ready(second.wait_settlement()).all_released());
    (admitted_at.duration_since(submitted_at), elapsed)
}

#[test]
#[ignore = "measures real Magika latency with controlled async Provider delays"]
fn test_slow_provider_mixed_latency() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 4 }).unwrap();
    let config = qubit_mime::MimeConfig::default();
    let make_detector = || {
        let runtime = Arc::new(ManagedInferenceRuntime::new(supervisor.clone(), config.max_buffer_size()).unwrap());
        Arc::new(
            qubit_magika::MagikaMimeDetector::builder()
                .mime_config(config.clone())
                .managed_runtime(runtime)
                .build()
                .unwrap(),
        )
    };
    let shared = make_detector();
    let isolated = make_detector();
    // Warm both real Magika sessions before collecting samples.
    MimeDetector::detect_by_content(&*shared, PYTHON).unwrap();
    MimeDetector::detect_by_content(&*isolated, PYTHON).unwrap();

    for delay in [Duration::ZERO, Duration::from_millis(10), Duration::from_millis(100)] {
        for (label, second) in [("shared", Arc::clone(&shared)), ("independent", Arc::clone(&isolated))] {
            let mut admission_samples = Vec::with_capacity(30);
            let mut completion_samples = Vec::with_capacity(30);
            let mut admitted = 0;
            let refused = 0;
            for _ in 0..30 {
                let second_detector = if label == "shared" {
                    Arc::clone(&shared)
                } else {
                    second.clone()
                };
                let (admission, completion) =
                    slow_provider_latency_sample(Arc::clone(&shared), second_detector, &service, delay);
                admitted += 1;
                admission_samples.push(admission);
                completion_samples.push(completion);
            }
            let mut admission_micros: Vec<u128> = admission_samples.iter().map(|sample| sample.as_micros()).collect();
            admission_micros.sort_unstable();
            let mut micros: Vec<u128> = completion_samples.iter().map(|sample| sample.as_micros()).collect();
            micros.sort_unstable();
            let p50 = micros[micros.len() / 2];
            let p95 = micros[(micros.len() * 95).div_ceil(100).saturating_sub(1)];
            eprintln!(
                "scenario={label} provider_delay_ms={} n={} admitted={admitted} refused={refused} admission_us={admission_micros:?} completion_us={micros:?} p50_us={p50} p95_us={p95}",
                delay.as_millis(),
                micros.len(),
            );
        }
    }
    let _ = shared.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    let _ = isolated.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    let _ = service.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    let _ = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
}

#[test]
fn test_shared_runtime_provider_head_of_line_cancellation() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 4 }).unwrap();
    let detector = Arc::new(qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"));
    let gate = AsyncProviderGate::new(AsyncProviderGatePoint::Read);
    let _release_gate = ReleaseGateOnDrop(Arc::clone(&gate));
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .with_async_gate(Arc::clone(&gate));
    let file_system = provider.async_file_system();
    let body_detector = Arc::clone(&detector);
    let mut first = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            body_detector
                .detect_async_provider_path_managed(&scope, &file_system, &path("/hol/cancelled.py"), 4096)
                .await
        })
        .unwrap();
    gate.wait_until_entered(Duration::from_secs(30));
    let first_observer = first.observer();
    let _ = first.request_stop(StopReason::UserCancelled);
    let first_result = run_ready(first.wait_outcome());
    assert!(matches!(
        first_result,
        Ok(qubit_operation::OperationOutcome::Stopped(_))
    ));

    // Cancellation stops local polling, but the gated provider read has no
    // completion signal yet, so the first managed release remains Pending.
    let mut first_release = Box::pin(first_observer.wait_settlement());
    let release_state = run_ready(std::future::poll_fn(|context| {
        Poll::Ready(first_release.as_mut().poll(context))
    }));
    assert!(release_state.is_pending());

    let bytes = PYTHON.to_vec();
    assert!(4096 + bytes.len() <= qubit_mime::MimeConfig::default().max_buffer_size());
    let second_detector = Arc::clone(&detector);
    let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
    let mut second = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            let mut inference = Box::pin(second_detector.detect_bytes_managed(&scope, bytes));
            std::future::poll_fn(|context| match inference.as_mut().poll(context) {
                Poll::Ready(result) => {
                    let _ = admitted_tx.send(format!("completed: {result:?}"));
                    Poll::Ready(result)
                }
                Poll::Pending => {
                    let _ = admitted_tx.send("pending".to_owned());
                    Poll::Pending
                }
            })
            .await
        })
        .unwrap();
    let admission_state = admitted_rx.recv_timeout(Duration::from_secs(30));
    if let Ok(state) = admission_state.as_ref() {
        assert_eq!(state, "pending", "second owned-byte request ended before queueing");
    } else {
        panic!(
            "second owned-byte request body ended before its queue handshake: {admission_state:?}; outcome={:?}",
            run_ready(second.wait_outcome())
        );
    }
    // The second operation must still be waiting on the shared worker before
    // releasing the provider read.
    let mut second_result = Box::pin(second.wait_outcome());
    let result_state = run_ready(std::future::poll_fn(|context| {
        Poll::Ready(second_result.as_mut().poll(context))
    }));
    assert!(result_state.is_pending());
    drop(second_result);

    // Releasing the read lets the second owned-byte request progress.
    gate.release();
    let second_result = run_ready(second.wait_outcome());
    let second_settlement = run_ready(second.wait_settlement());
    assert!(matches!(
        second_result,
        Ok(qubit_operation::OperationOutcome::Succeeded(Some(ref mime))) if mime == "text/x-python"
    ));
    assert!(second_settlement.all_released());
    // Even though the gate is released and the second request completes, the
    // first provider future was dropped while pending. Its native completion
    // cannot be observed through this fixture, so managed release stays
    // Pending and shutdown admission remains incomplete by contract.
    let release_state = run_ready(std::future::poll_fn(|context| {
        Poll::Ready(first_release.as_mut().poll(context))
    }));
    assert!(release_state.is_pending());
    let shutdown = detector.shutdown_blocking(ShutdownMode::Cancel, Duration::from_millis(50));
    assert!(!shutdown.worker_stopped);
}

#[test]
fn test_managed_provider_session_api_classifies_and_settles_worker() {
    let detector = Arc::new(qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"));
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 4 }).unwrap();
    let file_system = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .async_file_system();
    let fallback_file_system = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .without_stat()
        .random_open_error(qubit_fs::error::FsErrorKind::UnsupportedOperation)
        .async_file_system();
    let body_detector = Arc::clone(&detector);
    let mut handle = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
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
                .detect_async_provider_path_managed(&scope, &file_system, &path("/managed/python.py"), 4096)
                .await
                .map(|mime| mime.expect("Python sample should be classified"))?;
            let fallback_result = body_detector
                .detect_async_provider_path_managed(&scope, &fallback_file_system, &path("/managed/fallback.py"), 4096)
                .await
                .map(|mime| mime.expect("fallback Python sample should be classified"))?;
            Ok::<_, MimeError>((session_result, fallback_result))
        })
        .unwrap();
    assert!(matches!(
        run_ready(handle.wait_outcome()).unwrap(),
        qubit_operation::OperationOutcome::Succeeded((session_mime, fallback_mime))
            if session_mime == "text/x-python" && fallback_mime == "text/x-python"
    ));
    assert!(run_ready(handle.wait_settlement()).all_released());
    let report = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    assert!(report.worker_stopped);
}

#[test]
fn test_managed_provider_admission_rejects_cancelled_scope_before_provider_io() {
    let detector = Arc::new(qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"));
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 1 }).unwrap();
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access();
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let body_detector = Arc::clone(&detector);
    let mut handle = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            let _ = scope.request_stop(qubit_operation::StopReason::UserCancelled);
            let result = body_detector
                .detect_async_provider_path_managed(&scope, &file_system, &path("/cancelled.py"), 4096)
                .await;
            assert!(result.is_err());
            Ok::<_, MimeError>(())
        })
        .unwrap();
    let _ = run_ready(handle.wait_outcome());
    let snapshot = detector.runtime_snapshot();
    assert_eq!(
        0, snapshot.unconfirmed_provider_io,
        "cancel before Provider I/O is confirmed"
    );
    assert_eq!(0, observations.opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.stats.load(Ordering::SeqCst));
    assert_eq!(0, observations.random_opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.random_reads.load(Ordering::SeqCst));
    let _ = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
}

#[test]
fn test_managed_provider_admission_rejects_closed_runtime_before_provider_io() {
    let detector = Arc::new(qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"));
    let report = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    assert!(report.worker_stopped);
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 1 }).unwrap();
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access();
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let body_detector = Arc::clone(&detector);
    let mut handle = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            let result = body_detector
                .detect_async_provider_path_managed(&scope, &file_system, &path("/closed.py"), 4096)
                .await;
            assert!(result.is_err());
            Ok::<_, MimeError>(())
        })
        .unwrap();
    let _ = run_ready(handle.wait_outcome());
    assert_eq!(0, observations.opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.stats.load(Ordering::SeqCst));
    assert_eq!(0, observations.random_opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.random_reads.load(Ordering::SeqCst));
}

#[test]
fn test_managed_provider_admission_rejects_full_capacity_before_provider_io() {
    let detector = Arc::new(qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"));
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 6 }).unwrap();
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
                .as_async()
                .try_submit(OperationOptions::default(), move |scope| async move {
                    body_detector
                        .detect_async_provider_path_managed(&scope, &file_system, &path("/full.py"), 4096)
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
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            let result = body_detector
                .detect_async_provider_path_managed(&scope, &file_system, &path("/over-capacity.py"), 4096)
                .await;
            assert!(result.is_err());
            Ok::<_, MimeError>(())
        })
        .unwrap();
    let _ = run_ready(rejected_handle.wait_outcome());
    assert_eq!(0, observations.opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.stats.load(Ordering::SeqCst));
    assert_eq!(0, observations.random_opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.random_reads.load(Ordering::SeqCst));
    for gate in &gates {
        gate.release();
    }
    for handle in &mut handles {
        let _ = run_ready(handle.wait_outcome());
    }
    let _ = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
}

#[test]
fn test_cancel_after_unsupported_open_prevents_later_stat_and_fallback_io() {
    let detector = Arc::new(qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"));
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 2 }).unwrap();
    let gate = AsyncProviderGate::new(AsyncProviderGatePoint::Open);
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .random_open_error(qubit_fs::error::FsErrorKind::UnsupportedOperation)
        .with_async_gate(Arc::clone(&gate));
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let body_detector = Arc::clone(&detector);
    let handle = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            body_detector
                .detect_async_provider_path_managed(&scope, &file_system, &path("/cancel-between-ops.py"), 4096)
                .await
        })
        .unwrap();
    gate.wait_until_entered(Duration::from_secs(5));
    let _ = handle.request_stop(StopReason::UserCancelled);
    gate.release();
    let mut handle = handle;
    let _ = run_ready(handle.wait_outcome());
    let snapshot = detector.runtime_snapshot();
    assert_eq!(
        0, snapshot.unconfirmed_provider_io,
        "the gated open returned Ready before cancellation settled"
    );
    assert_eq!(0, observations.random_opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.stats.load(Ordering::SeqCst));
    assert_eq!(0, observations.opens.load(Ordering::SeqCst));
    assert_eq!(0, observations.read_bytes.load(Ordering::SeqCst));
    let _ = detector.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));
}

#[test]
fn test_pending_provider_read_keeps_managed_release_pending_after_bridge_close() {
    let detector = Arc::new(qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize"));
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 4 }).unwrap();
    let gate = AsyncProviderGate::new(AsyncProviderGatePoint::Read);
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .with_async_gate(Arc::clone(&gate));
    let file_system = provider.async_file_system();
    let body_detector = Arc::clone(&detector);
    let handle = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            body_detector
                .detect_async_provider_path_managed(&scope, &file_system, &path("/pending.py"), 4096)
                .await
        })
        .unwrap();
    gate.wait_until_entered(Duration::from_secs(30));
    let observer = handle.observer();
    let _ = handle.request_stop(StopReason::UserCancelled);
    let mut handle = handle;
    let _ = run_ready(handle.wait_outcome());
    let (released_tx, released_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = released_tx.send(run_ready(observer.wait_settlement()));
    });
    assert!(released_rx.recv_timeout(Duration::from_millis(50)).is_err());
    let report = detector.shutdown_blocking(ShutdownMode::Cancel, Duration::from_millis(50));
    assert!(report.admission.snapshot.service_resources != 0);
    // The async fs contract does not let us certify provider-native completion.
    gate.release();
}

#[test]
fn test_scope_less_dropped_provider_future_retains_pending_capacity() {
    struct ThreadWake(std::thread::Thread);
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    for (point, range) in [
        (AsyncProviderGatePoint::Open, true),
        (AsyncProviderGatePoint::Read, true),
        (AsyncProviderGatePoint::Read, false),
    ] {
        let detector = qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize");
        let gate = AsyncProviderGate::new(point);
        let mut first_provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_async_gate(Arc::clone(&gate));
        if range {
            first_provider = first_provider.with_random_access();
        } else {
            first_provider = first_provider.without_range();
        }
        let first_fs = first_provider.async_file_system();
        let max_bytes = detector.max_buffer_size();
        let first_path = path("/dropped-scope-less.py");
        let mut future = Box::pin(MimeDetectorBackend::guess_from_async_provider_path(
            &detector,
            &first_fs,
            &first_path,
            max_bytes,
        ));
        let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
        let mut context = Context::from_waker(&waker);
        let deadline = Instant::now() + Duration::from_secs(5);
        while !gate.is_entered() {
            assert!(Instant::now() < deadline, "{point:?} provider operation did not start");
            assert!(future.as_mut().poll(&mut context).is_pending());
            std::thread::park_timeout(Duration::from_millis(10));
        }
        drop(future);
        let second_provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access();
        let second_observations = second_provider.observations();
        let second_fs = second_provider.async_file_system();
        let second_path = path("/after-drop.py");
        let result = crate::support::run_ready(MimeDetectorBackend::guess_from_async_provider_path(
            &detector,
            &second_fs,
            &second_path,
            1024,
        ));
        assert!(
            result.is_err(),
            "{point:?} drop released scope-less admission too early"
        );
        assert_eq!(0, second_observations.opens.load(Ordering::SeqCst));
        assert_eq!(0, second_observations.random_opens.load(Ordering::SeqCst));
        let _ = detector.shutdown_blocking(ShutdownMode::Cancel, Duration::from_millis(20));
    }
}

#[test]
fn test_sync_provider_backend_uses_session_range_and_full_read_routes() {
    let detector = qubit_magika::MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize");
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
        let (candidates, returned_bytes) =
            MimeDetectorBackend::guess_from_provider_path(&detector, &file_system, &path("/managed/sync.py"), 4096)
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
    assert!(result.is_err(), "a Pending future without a wake must time out");
    assert_eq!(1, polls.load(Ordering::SeqCst), "the driver must not actively repoll");

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
    assert!(future.as_mut().poll(&mut Context::from_waker(&old_waker)).is_pending());
    assert!(future.as_mut().poll(&mut Context::from_waker(&new_waker)).is_pending());
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
            AsyncProviderGatePoint::Stat => Box::pin(async move { file_system.stat(&request_path).await.map(|_| ()) }),
            AsyncProviderGatePoint::Open => Box::pin(async move {
                file_system
                    .open_reader(&request_path, qubit_fs::read::ReadOptions::default())
                    .await
                    .map(drop)
            }),
            AsyncProviderGatePoint::Read => Box::pin(async move {
                let mut reader = file_system
                    .open_random_access_reader(&request_path, qubit_fs::read::ReadOptions::default())
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
            assert_eq!(1, provider.observations().session_drops.load(Ordering::SeqCst));
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
    let observer = std::thread::spawn(move || worker_gate.wait_until_entered(Duration::from_secs(1)));
    let file_system = provider.async_file_system();
    let request_path = path("/script.py");
    let mut reader =
        run_ready(file_system.open_random_access_reader(&request_path, qubit_fs::read::ReadOptions::default()))
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
    let mut reader =
        run_ready(file_system.open_random_access_reader(&request_path, qubit_fs::read::ReadOptions::default()))
            .expect("session should open before the read is gated");
    let result = run_ready(reader.read_at(0, &mut [0_u8; 1]));
    releaser.join().unwrap();
    assert!(result.is_err(), "the provider read error should propagate after wake");
    drop(reader);
    assert_eq!(1, provider.observations().session_drops.load(Ordering::SeqCst));
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
    assert_eq!(observations.stats.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(observations.opens.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(
        observations
            .options
            .lock()
            .unwrap()
            .iter()
            .all(|options| options.offset().is_some() && options.length().is_some())
    );
    assert!(observations.read_bytes.load(std::sync::atomic::Ordering::SeqCst) <= 8192);
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
    assert!(observations.read_bytes.load(std::sync::atomic::Ordering::SeqCst) <= 8192);
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
    assert_eq!(1, observations.random_opens.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(0, observations.stats.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(0, observations.opens.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(1, observations.session_drops.load(std::sync::atomic::Ordering::SeqCst));
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
    assert_eq!(1, observations.random_opens.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(0, observations.stats.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(0, observations.opens.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(1, observations.session_drops.load(std::sync::atomic::Ordering::SeqCst));
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
    assert_eq!(1, observations.random_opens.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(1, observations.stats.load(std::sync::atomic::Ordering::SeqCst));
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
    assert_eq!(1, observations.random_opens.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(1, observations.stats.load(std::sync::atomic::Ordering::SeqCst));
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
    assert_eq!(0, observations.stats.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(0, observations.opens.load(std::sync::atomic::Ordering::SeqCst));
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
    assert_eq!(0, observations.stats.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(0, observations.opens.load(std::sync::atomic::Ordering::SeqCst));
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
    assert_eq!(1, observations.random_opens.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(0, observations.stats.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(0, observations.opens.load(std::sync::atomic::Ordering::SeqCst));
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
    assert_eq!(1, observations.random_opens.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(0, observations.stats.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(0, observations.opens.load(std::sync::atomic::Ordering::SeqCst));
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
    assert_eq!(observations.stats.load(std::sync::atomic::Ordering::SeqCst), 1);
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
    assert_eq!(observations.stats.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(observations.opens.load(std::sync::atomic::Ordering::SeqCst), 0);
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
    let provider = large_provider(true).with_conditional().with_max_read_range_bytes(2);
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
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_range().short_reads();
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
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_range().short_reads();
    let error = run_ready(detector().detect_async_path(
        &provider.async_file_system(),
        &path("/script.py"),
        8192,
        MimeDetectionPolicy::VerifyContent,
    ))
    .expect_err("short async provider ranges must fail");
    assert!(matches!(error, MimeError::Io(_)));
}

/// A runtime-only Cancel must wake either async Provider entrance while the
/// Provider future is Pending. Always release the fixture gate before joining
/// the shutdown thread so a regression cannot strand the test process.
fn assert_runtime_cancel_returns_while_provider_is_pending(
    managed: bool,
    point: AsyncProviderGatePoint,
    session: bool,
    range: bool,
) -> Option<String> {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let config = qubit_mime::MimeConfig::default();
    let runtime = Arc::new(
        ManagedInferenceRuntime::new(supervisor.clone(), config.max_buffer_size())
            .expect("managed inference runtime should initialize"),
    );
    let detector = Arc::new(
        qubit_magika::MagikaMimeDetector::builder()
            .mime_config(config)
            .managed_runtime(runtime)
            .build()
            .expect("bundled ONNX Runtime should initialize"),
    );
    let gate = AsyncProviderGate::new(point);
    let _release_gate = ReleaseProviderGateOnDrop(Arc::clone(&gate));
    let mut provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_async_gate(Arc::clone(&gate));
    if session {
        provider = provider.with_random_access();
    } else if range {
        provider = provider.with_range();
    } else {
        provider = provider.without_range();
    }
    let file_system = provider.async_file_system();
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 2 }).unwrap();
    let request_detector = Arc::clone(&detector);
    let mut operation = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            if managed {
                request_detector
                    .detect_async_provider_path_managed(
                        &scope,
                        &file_system,
                        &path("/runtime-cancel.py"),
                        4096,
                    )
                    .await
                    .map(|_| ())
            } else {
                MimeDetectorBackend::guess_from_async_provider_path(
                    &*request_detector,
                    &file_system,
                    &path("/runtime-cancel.py"),
                    4096,
                )
                .await
                .map(|_| ())
            }
        })
        .unwrap();
    let gate_deadline = Instant::now() + Duration::from_secs(5);
    while !gate.is_entered() && Instant::now() < gate_deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let gate_was_entered = gate.is_entered();
    let observer = operation.observer();
    let shutdown_detector = Arc::clone(&detector);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let shutdown = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        let report = shutdown_detector.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));
        finished_tx.send((Instant::now(), report)).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let before_release = finished_rx.recv_timeout(Duration::from_millis(250));
    let returned_before_release = before_release.is_ok();
    let mut settlement_before = Box::pin(observer.wait_settlement());
    let settlement_before_release = run_ready(std::future::poll_fn(|context| {
        Poll::Ready(settlement_before.as_mut().poll(context))
    }));
    drop(settlement_before);
    gate.release();
    let completed = match before_release {
        Ok(completed) => Ok(completed),
        Err(_) => finished_rx.recv_timeout(Duration::from_secs(3)),
    };
    shutdown.join().unwrap();
    let (finished_at, report) = completed.expect("runtime Cancel should finish after the gate is released");
    let mut failures = Vec::new();
    if !gate_was_entered || !returned_before_release || !report.worker_stopped {
        failures.push(format!(
            "managed={managed}, point={point:?}, session={session}, range={range}, gate_entered={gate_was_entered}, returned_before_release={returned_before_release}, report={report:?}, completed_at={finished_at:?}"
        ));
    }
    let _ = run_ready(operation.wait_outcome());
    let mut settlement_after = Box::pin(observer.wait_settlement());
    let settlement_poll = run_ready(std::future::poll_fn(|context| {
        Poll::Ready(settlement_after.as_mut().poll(context))
    }));
    if managed
        && gate_was_entered
        && matches!(settlement_before_release, Poll::Ready(report) if report.all_released())
    {
        failures.push(format!(
            "managed scope reported Released while Provider future remained Pending; point={point:?}, session={session}, range={range}"
        ));
    }
    let _settlement_after_release = settlement_poll;
    let _ = service.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    let _ = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    (!failures.is_empty()).then(|| failures.join("; "))
}

#[test]
fn test_runtime_only_cancel_wakes_provider_pending_at_each_entry_boundary() {
    let mut failures = Vec::new();
    for managed in [false, true] {
        for (point, session, range) in [
            (AsyncProviderGatePoint::Stat, false, false),
            (AsyncProviderGatePoint::Open, false, false),
            (AsyncProviderGatePoint::Read, false, false),
            (AsyncProviderGatePoint::Read, false, true),
            (AsyncProviderGatePoint::Open, true, false),
            (AsyncProviderGatePoint::Read, true, false),
        ] {
            if let Some(failure) =
                assert_runtime_cancel_returns_while_provider_is_pending(managed, point, session, range)
            {
                failures.push(failure);
            }
        }
    }
    assert!(failures.is_empty(), "runtime-only Cancel failures: {failures:#?}");
}

#[test]
fn test_managed_provider_routes_fit_single_resource_scope_quota() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let config = qubit_mime::MimeConfig::default();
    let runtime = Arc::new(
        ManagedInferenceRuntime::new(supervisor.clone(), config.max_buffer_size())
            .expect("managed inference runtime should initialize"),
    );
    let detector = Arc::new(
        qubit_magika::MagikaMimeDetector::builder()
            .mime_config(config)
            .managed_runtime(runtime)
            .build()
            .expect("bundled ONNX Runtime should initialize"),
    );
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 2 }).unwrap();
    let mut failures = Vec::new();
    for provider in [
        ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access(),
        ProviderFileSystemSpi::new(PYTHON.to_vec()).with_range(),
        ProviderFileSystemSpi::new(PYTHON.to_vec()).without_range(),
    ] {
        let request_detector = Arc::clone(&detector);
        let mut operation = service
            .as_async()
            .try_submit(
                OperationOptions {
                    limits: ScopeLimits {
                        resources: 1,
                        ..ScopeLimits::default()
                    },
                    ..OperationOptions::default()
                },
                move |scope| async move {
                    let mime = request_detector
                        .detect_async_provider_path_managed(
                            &scope,
                            &provider.async_file_system(),
                            &path("/quota.py"),
                            4096,
                        )
                        .await?;
                    assert_eq!(Some("text/x-python".to_owned()), mime);
                    Ok::<(), MimeError>(())
                },
            )
            .unwrap();
        let outcome = run_ready(operation.wait_outcome()).unwrap();
        let settlement = run_ready(operation.wait_settlement());
        if !matches!(outcome, qubit_operation::OperationOutcome::Succeeded(())) {
            failures.push(format!("single-resource provider route failed: {outcome:?}"));
        }
        if !settlement.all_released() {
            failures.push("single-resource route did not settle all resources".to_owned());
        }
    }
    let shutdown = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    let _ = service.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    let _ = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    if shutdown.admission.snapshot.service_resources != 0 {
        failures.push(format!("runtime still owns service permits at shutdown: {shutdown:?}"));
    }
    assert!(failures.is_empty(), "single-resource provider quota failures: {failures:#?}");
}

#[test]
fn test_managed_provider_returns_runtime_permits_while_scope_remains_open() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let config = qubit_mime::MimeConfig::default();
    let runtime = Arc::new(
        ManagedInferenceRuntime::new(supervisor.clone(), config.max_buffer_size())
            .expect("managed inference runtime should initialize"),
    );
    let detector = Arc::new(
        qubit_magika::MagikaMimeDetector::builder()
            .mime_config(config)
            .managed_runtime(runtime)
            .build()
            .expect("bundled ONNX Runtime should initialize"),
    );
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 2 }).unwrap();
    let request_detector = Arc::clone(&detector);
    let mut operation = service
        .as_async()
        .try_submit(
            OperationOptions {
                limits: ScopeLimits {
                    resources: 8,
                    ..ScopeLimits::default()
                },
                ..OperationOptions::default()
            },
            move |scope| async move {
                for provider in [
                    ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access(),
                    ProviderFileSystemSpi::new(PYTHON.to_vec()).with_range(),
                    ProviderFileSystemSpi::new(PYTHON.to_vec()).without_range(),
                    ProviderFileSystemSpi::new(PYTHON.to_vec()).with_random_access(),
                ] {
                    let mime = request_detector
                        .detect_async_provider_path_managed(
                            &scope,
                            &provider.async_file_system(),
                            &path("/long-lived-scope.py"),
                            4096,
                        )
                        .await?;
                    assert_eq!(Some("text/x-python".to_owned()), mime);
                }
                Ok::<(), MimeError>(())
            },
        )
        .unwrap();
    let outcome = run_ready(operation.wait_outcome()).unwrap();
    let settled = run_ready(operation.wait_settlement());
    let report = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    let _ = service.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    let _ = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    assert!(
        matches!(outcome, qubit_operation::OperationOutcome::Succeeded(())),
        "sequential routes in a long-lived scope should not exhaust two runtime permits: {outcome:?}"
    );
    assert!(settled.all_released());
    assert_eq!(0, report.admission.snapshot.service_resources);
}

#[test]
fn test_pending_provider_io_is_not_released_after_last_runtime_drop() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let config = qubit_mime::MimeConfig::default();
    let runtime = Arc::new(
        ManagedInferenceRuntime::new(supervisor.clone(), config.max_buffer_size())
            .expect("managed inference runtime should initialize"),
    );
    let detector = Arc::new(
        qubit_magika::MagikaMimeDetector::builder()
            .mime_config(config)
            .managed_runtime(Arc::clone(&runtime))
            .build()
            .expect("bundled ONNX Runtime should initialize"),
    );
    drop(runtime);
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 2 }).unwrap();
    let gate = AsyncProviderGate::new(AsyncProviderGatePoint::Read);
    let _release_gate = ReleaseProviderGateOnDrop(Arc::clone(&gate));
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_random_access()
        .with_async_gate(Arc::clone(&gate));
    let request_detector = Arc::clone(&detector);
    let mut operation = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            request_detector
                .detect_async_provider_path_managed(
                    &scope,
                    &provider.async_file_system(),
                    &path("/unconfirmed.py"),
                    4096,
                )
                .await
        })
        .unwrap();
    let gate_deadline = Instant::now() + Duration::from_secs(5);
    while !gate.is_entered() && Instant::now() < gate_deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    let gate_was_entered = gate.is_entered();
    let observer = operation.observer();
    let mut before_bridge = Box::pin(observer.wait_settlement());
    let before_bridge_state = run_ready(std::future::poll_fn(|context| {
        Poll::Ready(before_bridge.as_mut().poll(context))
    }));
    drop(before_bridge);
    let observer_was_pending_before_bridge = matches!(before_bridge_state, Poll::Pending);

    let shutdown_detector = Arc::clone(&detector);
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel();
    let shutdown_thread = std::thread::spawn(move || {
        let report = shutdown_detector.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));
        let _ = shutdown_tx.send(report);
    });
    let shutdown_before_gate_release = shutdown_rx.recv_timeout(Duration::from_millis(250)).ok();
    let bridge_outcome_before_gate_release = run_until(operation.wait_outcome(), Duration::from_millis(250));
    let shutdown_returned_before_gate_release = shutdown_before_gate_release.is_some();
    let bridge_returned_before_gate_release = bridge_outcome_before_gate_release.is_ok();
    let cancelled_bridge_exited_while_pending =
        shutdown_returned_before_gate_release && bridge_returned_before_gate_release;
    if !cancelled_bridge_exited_while_pending {
        gate.release();
    }
    let shutdown_report = match shutdown_before_gate_release {
        Some(report) => Some(report),
        None => shutdown_rx.recv_timeout(Duration::from_secs(3)).ok(),
    };
    let operation_outcome = match bridge_outcome_before_gate_release {
        Ok(outcome) => Ok(outcome),
        Err(()) => run_until(operation.wait_outcome(), Duration::from_secs(3)),
    };
    shutdown_thread.join().unwrap();
    drop(detector);
    let settlement_after_last_runtime_drop = if cancelled_bridge_exited_while_pending {
        run_until(observer.wait_settlement(), Duration::from_secs(2))
    } else {
        Err(())
    };
    gate.release();
    let settlement_after_gate_release = run_until(observer.wait_settlement(), Duration::from_secs(2));
    let _ = service.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    let _ = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));

    let mut failures = Vec::new();
    if !gate_was_entered {
        failures.push("Provider read did not reach Pending before runtime Cancel".to_owned());
    }
    if !observer_was_pending_before_bridge {
        failures.push("pre-bridge observer was Ready while Provider I/O was Pending".to_owned());
    }
    if !cancelled_bridge_exited_while_pending {
        failures.push(format!(
            "runtime-only Cancel did not finish before gate release; shutdown_returned={}, bridge_returned={}",
            shutdown_returned_before_gate_release,
            bridge_returned_before_gate_release
        ));
    }
    if !shutdown_report.as_ref().is_some_and(|report| report.worker_stopped) {
        failures.push(format!("runtime worker did not stop before gate release: {shutdown_report:?}"));
    }
    if operation_outcome.is_err() {
        failures.push("bridge operation did not finish after gate cleanup".to_owned());
    }
    match settlement_after_last_runtime_drop {
        Ok(report) if report.all_released() => {
            failures.push("post-drop settlement report incorrectly had all_released=true".to_owned());
        }
        Ok(_) => {}
        Err(()) => failures.push(format!(
            "post-drop observer timed out Pending; after gate release report state was {:?}",
            settlement_after_gate_release.as_ref().map(|report| report.all_released())
        )),
    }
    assert!(failures.is_empty(), "last-runtime release evidence failures: {failures:#?}");
}

#[test]
fn test_provider_path_sync_read_failure_is_an_io_error() {
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_range().fail_reads();
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
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_range().fail_reads();
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

#[test]
fn test_managed_owned_bytes_preserve_public_result_and_release_contract() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let zero_budget = qubit_magika::ManagedInferenceRuntime::new(supervisor.clone(), 0)
        .expect_err("a managed runtime must reject a zero byte budget");
    assert!(zero_budget.to_string().contains("runtime byte budget must be nonzero"));
    let builder = qubit_magika::MagikaMimeDetectorBuilder::default().operation_manager(supervisor.clone());
    assert!(format!("{builder:?}").contains("manager_configured"));
    let builder_detector = builder.build().unwrap();
    assert!(
        builder_detector
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30))
            .worker_stopped
    );
    let runtime = Arc::new(
        qubit_magika::ManagedInferenceRuntime::new(
            supervisor.clone(),
            qubit_mime::MimeConfig::default().max_buffer_size(),
        )
        .unwrap(),
    );
    assert!(format!("{runtime:?}").contains("ManagedInferenceRuntime"));
    let detector = Arc::new(
        qubit_magika::MagikaMimeDetector::builder()
            .managed_runtime(runtime)
            .build()
            .unwrap(),
    );
    let request_detector = Arc::clone(&detector);
    let service = supervisor.group(qubit_operation::GroupOptions { capacity: 2 }).unwrap();
    let mut request = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            request_detector.detect_bytes_managed(&scope, PYTHON.to_vec()).await
        })
        .unwrap();

    let outcome = run_ready(request.wait_outcome()).unwrap();
    assert!(matches!(
        outcome,
        qubit_operation::OperationOutcome::Succeeded(Some(value)) if value == "text/x-python"
    ));
    assert!(run_ready(request.wait_settlement()).all_released());

    let bytes = (0..512)
        .map(|index| ((index * 37 + 11) % 256) as u8)
        .collect::<Vec<_>>();
    assert!(matches!(
        magika::FeaturesOrRuled::extract_sync(&bytes[..]).unwrap(),
        magika::FeaturesOrRuled::Features(_)
    ));
    let mut reader = Cursor::new(bytes);
    reader.set_position(7);
    assert!(
        qubit_mime::MimeDetector::detect_reader(
            detector.as_ref(),
            &mut reader,
            None,
            qubit_mime::MimeDetectionPolicy::VerifyContent,
        )
        .is_ok()
    );
    assert_eq!(reader.position(), 7);

    let rejected_detector = Arc::clone(&detector);
    let mut rejected_options = OperationOptions::default();
    rejected_options.limits.child_operations = 0;
    let mut rejected_request = service
        .as_async()
        .try_submit(rejected_options, move |scope| async move {
            assert!(
                rejected_detector
                    .detect_bytes_managed(&scope, Vec::new())
                    .await
                    .is_err()
            );
            Ok::<(), ()>(())
        })
        .unwrap();
    let _ = run_ready(rejected_request.wait_outcome()).unwrap();
    assert!(run_ready(rejected_request.wait_settlement()).all_released());

    let report = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    assert!(report.worker_stopped);
    assert!(report.admission.snapshot.active_operations == 0);
}
