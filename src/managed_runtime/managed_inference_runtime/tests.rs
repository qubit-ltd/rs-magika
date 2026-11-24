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
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use qubit_mime::MimeError;
use qubit_operation::ChildOutcomeKind;
use qubit_operation::GroupOptions;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ScopeLimits;
use qubit_operation::ShutdownMode;
use qubit_operation::StopReason;

use super::super::worker_input::WorkerBytes;
use super::ManagedInferenceRuntime;
use super::WorkerInput;
use super::worker_loop::execute_classified;

struct ThreadWake(std::thread::Thread);

impl std::task::Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn map_provider_test_error(_: magika::Error) -> MimeError {
    MimeError::detector_backend("magika", "test input failed")
}

fn borrowed_test_runtime(max_bytes: usize) -> Arc<ManagedInferenceRuntime> {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        max_bytes,
        Box::new(|_| Ok(None)),
        Box::new(|| {}),
    ))
}

#[test]
fn test_snapshot_idle() {
    let runtime = borrowed_test_runtime(17);
    let snapshot = runtime.snapshot();
    assert!(!snapshot.admission_closed);
    assert!(!snapshot.cancelling);
    assert!(!snapshot.faulted);
    assert_eq!(0, snapshot.admitted_not_started);
    assert_eq!(0, snapshot.worker_running);
    assert_eq!(0, snapshot.awaiting_release);
    assert_eq!(0, snapshot.unconfirmed_provider_io);
    assert_eq!(0, snapshot.reserved_bytes);
    assert_eq!(17, snapshot.max_in_flight_bytes);
}

#[test]
fn test_snapshot_and_shutdown_recover_poisoned_runtime_locks() {
    let runtime = borrowed_test_runtime(17);

    let poisoned_tasks = Arc::clone(&runtime);
    assert!(
        std::thread::spawn(move || {
            let _guard = poisoned_tasks.tasks.lock().unwrap();
            panic!("poison task registry before snapshot");
        })
        .join()
        .is_err()
    );
    let poisoned_gate = Arc::clone(&runtime);
    assert!(
        std::thread::spawn(move || {
            let _guard = poisoned_gate.admission_gate.lock().unwrap();
            panic!("poison admission gate before snapshot");
        })
        .join()
        .is_err()
    );
    let poisoned_sender = Arc::clone(&runtime);
    assert!(
        std::thread::spawn(move || {
            let _guard = poisoned_sender.sender.lock().unwrap();
            panic!("poison request sender before shutdown");
        })
        .join()
        .is_err()
    );
    let poisoned_worker = Arc::clone(&runtime);
    assert!(
        std::thread::spawn(move || {
            let _guard = poisoned_worker.worker.lock().unwrap();
            panic!("poison worker slot before shutdown");
        })
        .join()
        .is_err()
    );

    let snapshot = runtime.snapshot();
    assert_eq!(0, snapshot.admitted_not_started);
    assert_eq!(0, snapshot.awaiting_release);
    assert!(!snapshot.admission_closed);

    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
    assert!(!report.worker_panicked);
}

#[test]
fn test_snapshot_classifies_admitted_running_and_awaiting_release() {
    let runtime = borrowed_test_runtime(17);
    let admitted = Arc::new(super::super::task_state::TaskState::new(1, None));
    let running = Arc::new(super::super::task_state::TaskState::new(2, None));
    let awaiting = Arc::new(super::super::task_state::TaskState::new(3, None));
    {
        let mut inner = running.inner.lock().unwrap();
        inner.started = true;
    }
    {
        let mut inner = awaiting.inner.lock().unwrap();
        inner.started = true;
        inner.worker_done = true;
    }
    runtime.tasks.lock().unwrap().extend([admitted, running, awaiting]);
    let snapshot = runtime.snapshot();
    assert_eq!(1, snapshot.admitted_not_started);
    assert_eq!(1, snapshot.worker_running);
    assert_eq!(1, snapshot.awaiting_release);
}

#[test]
fn test_snapshot_omits_released_tasks_and_counts_unconfirmed_marker() {
    let runtime = borrowed_test_runtime(17);
    let released = Arc::new(super::super::task_state::TaskState::new(1, None));
    let mut inner = released.inner.lock().unwrap();
    inner.started = true;
    inner.worker_done = true;
    inner.released = true;
    drop(inner);
    let pending = Arc::new(super::super::task_state::TaskState::new(2, None));
    pending.mark_unconfirmed_provider_io();
    runtime.tasks.lock().unwrap().extend([released, pending]);
    let snapshot = runtime.snapshot();
    assert_eq!(1, snapshot.admitted_not_started);
    assert_eq!(0, snapshot.awaiting_release);
    assert_eq!(1, snapshot.unconfirmed_provider_io);
}

#[test]
fn test_shutdown_stops_worker_after_local_access_without_releasing_unknown_io() {
    let runtime = borrowed_test_runtime(17);
    let state = Arc::new(super::super::task_state::TaskState::new(4, None));
    state.mark_unconfirmed_provider_io();
    state.mark_access_complete();
    runtime.tasks.lock().unwrap().push(Arc::clone(&state));

    let report = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));

    assert!(report.worker_stopped);
    assert!(state.access_complete());
    assert!(!state.inner.lock().unwrap().released);
    assert_eq!(1, runtime.snapshot().unconfirmed_provider_io);
    assert_eq!(1, runtime.tasks.lock().unwrap().len());
}

#[test]
fn test_snapshot_concurrent_with_shutdown_finishes_within_deadline() {
    let runtime = borrowed_test_runtime(17);
    let (started_tx, started_rx) = mpsc::channel();
    let observer_runtime = Arc::clone(&runtime);
    let observer = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        for _ in 0..100 {
            let snapshot = observer_runtime.snapshot();
            assert_eq!(0, snapshot.reserved_bytes);
        }
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
    observer.join().expect("concurrent snapshot observer should finish");
}

struct ReleaseGate(Option<mpsc::SyncSender<()>>);

impl ReleaseGate {
    fn release(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.release();
    }
}

fn wait_for_snapshot(
    runtime: &ManagedInferenceRuntime,
    stage: &str,
    condition: impl Fn(&super::super::ManagedInferenceSnapshot) -> bool,
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let snapshot = runtime.snapshot();
        if condition(&snapshot) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for {stage}: {snapshot:?}"
        );
        std::thread::yield_now();
    }
}

#[test]
fn test_snapshot_active_request_release_and_shutdown_are_bounded() {
    let (entered_tx, entered_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let release_wait = Arc::new(Mutex::new(release_rx));
    let engine_release = Arc::clone(&release_wait);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        OperationManager::new(ManagerOptions::default()).unwrap(),
        8,
        Box::new(move |_| {
            entered_tx.send(()).unwrap();
            engine_release.lock().unwrap().recv().unwrap();
            Ok(Some("text/plain".to_owned()))
        }),
        Box::new(|| {}),
    ));
    let mut release_on_failure = ReleaseGate(Some(release_tx));
    let inference = runtime.submit(None, WorkerInput::Bytes(vec![1, 2, 3].into())).unwrap();
    entered_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("engine must enter before the active-request snapshot");

    let states = runtime.tasks.lock().unwrap().clone();
    assert_eq!(states.len(), 1, "real submitted request must be registered");
    let active = runtime.snapshot();
    assert_eq!(1, active.worker_running);
    assert_eq!(3, active.reserved_bytes);

    let (observer_ready_tx, observer_ready_rx) = mpsc::sync_channel(1);
    let (observer_done_tx, observer_done_rx) = mpsc::sync_channel(1);
    let observer_runtime = Arc::clone(&runtime);
    let observer = std::thread::spawn(move || {
        let first = observer_runtime.snapshot();
        observer_ready_tx.send(first.worker_running == 1).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let snapshot = observer_runtime.snapshot();
            if snapshot.worker_running == 0 && snapshot.awaiting_release == 0 && snapshot.reserved_bytes == 0 {
                observer_done_tx.send(()).unwrap();
                return;
            }
            if std::time::Instant::now() >= deadline {
                observer_done_tx.send(()).unwrap();
                panic!("timed out observing request release: {snapshot:?}");
            }
            std::thread::yield_now();
        }
    });
    assert!(
        observer_ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("snapshot observer must start"),
        "observer must first see the real active request"
    );

    let (shutdown_started_tx, shutdown_started_rx) = mpsc::sync_channel(1);
    let (shutdown_done_tx, shutdown_done_rx) = mpsc::sync_channel(1);
    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown = std::thread::spawn(move || {
        shutdown_started_tx.send(()).unwrap();
        let report = shutdown_runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
        shutdown_done_tx.send(report).unwrap();
    });
    shutdown_started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("shutdown thread must start");
    wait_for_snapshot(&runtime, "shutdown admission closure", |snapshot| {
        snapshot.admission_closed
    });

    release_on_failure.release();
    assert_eq!(
        block_on(inference).unwrap().as_deref(),
        Some("text/plain"),
        "accepted inference should return its real worker result"
    );
    let report = shutdown_done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("shutdown must finish after request release");
    assert!(report.worker_stopped);
    observer_done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("snapshot observer must finish after registry cleanup");
    shutdown.join().expect("completed shutdown thread must be reclaimable");
    observer.join().expect("completed snapshot thread must be reclaimable");

    let final_snapshot = runtime.snapshot();
    assert!(final_snapshot.admission_closed);
    assert_eq!(0, final_snapshot.worker_running);
    assert_eq!(0, final_snapshot.awaiting_release);
    assert_eq!(0, final_snapshot.reserved_bytes);
    assert!(
        runtime.tasks.lock().unwrap().is_empty(),
        "released task must leave the registry"
    );
    assert_eq!(0, runtime.active_requests.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn test_snapshot_driver_release_and_registry_cleanup_are_bounded() {
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        OperationManager::new(ManagerOptions::default()).unwrap(),
        4,
        Box::new(|bytes| Ok(Some(format!("application/x-bytes-{}", bytes.len())))),
        Box::new(|| {}),
    ));
    let bridge = crate::internal::async_sample_bridge::AsyncSampleBridge::new(4);
    let driver = bridge.driver();
    let inference = runtime
        .submit(
            None,
            WorkerInput::Bridge(Arc::clone(&bridge), 4, map_provider_test_error),
        )
        .unwrap();
    let state = runtime.tasks.lock().unwrap()[0].clone();

    let (request_tx, request_rx) = mpsc::sync_channel(1);
    let request_bridge = Arc::clone(&bridge);
    let request_thread = std::thread::spawn(move || {
        request_tx.send(block_on(request_bridge.next_request())).unwrap();
    });
    let request = request_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("worker sampling request must arrive");
    let request = request.expect("bridge remains open while the retained driver owns it");
    assert_eq!(0, request.offset());
    assert_eq!(4, request.len());
    request.respond(Ok(vec![1, 2, 3, 4]));
    request_thread
        .join()
        .expect("completed sampling request thread must be reclaimable");
    assert!(
        state.wait_worker_done_blocking(Duration::from_secs(5)),
        "worker must finish sampling"
    );
    let waiting = runtime.snapshot();
    assert_eq!(1, waiting.awaiting_release);
    assert_eq!(4, waiting.reserved_bytes);
    assert!(!state.inner.lock().unwrap().released);

    let (observer_ready_tx, observer_ready_rx) = mpsc::sync_channel(1);
    let (observer_done_tx, observer_done_rx) = mpsc::sync_channel(1);
    let observer_runtime = Arc::clone(&runtime);
    let observer = std::thread::spawn(move || {
        let first = observer_runtime.snapshot();
        observer_ready_tx
            .send(first.awaiting_release == 1 && first.reserved_bytes == 4)
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let snapshot = observer_runtime.snapshot();
            if snapshot.awaiting_release == 0 && snapshot.reserved_bytes == 0 {
                observer_done_tx.send(()).unwrap();
                return;
            }
            if std::time::Instant::now() >= deadline {
                observer_done_tx.send(()).unwrap();
                panic!("timed out observing driver release: {snapshot:?}");
            }
            std::thread::yield_now();
        }
    });
    assert!(
        observer_ready_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("snapshot observer must start"),
        "observer must see worker-done request held by the real driver"
    );

    let (shutdown_started_tx, shutdown_started_rx) = mpsc::sync_channel(1);
    let (shutdown_done_tx, shutdown_done_rx) = mpsc::sync_channel(1);
    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown = std::thread::spawn(move || {
        shutdown_started_tx.send(()).unwrap();
        let report = shutdown_runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
        shutdown_done_tx.send(report).unwrap();
    });
    shutdown_started_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("shutdown thread must start");
    wait_for_snapshot(&runtime, "shutdown admission closure", |snapshot| {
        snapshot.admission_closed
    });

    drop(driver);
    state.mark_driver_done();
    assert_eq!(block_on(inference).unwrap().as_deref(), Some("application/x-bytes-4"));
    let report = shutdown_done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("shutdown must finish after the known-safe driver completes");
    assert!(report.worker_stopped);
    observer_done_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("snapshot observer must finish after registry cleanup");
    shutdown.join().expect("completed shutdown thread must be reclaimable");
    observer.join().expect("completed snapshot thread must be reclaimable");

    let final_snapshot = runtime.snapshot();
    assert!(final_snapshot.admission_closed);
    assert_eq!(0, final_snapshot.worker_running);
    assert_eq!(0, final_snapshot.awaiting_release);
    assert_eq!(0, final_snapshot.reserved_bytes);
    assert!(
        runtime.tasks.lock().unwrap().is_empty(),
        "released task must leave the registry"
    );
    assert_eq!(0, runtime.active_requests.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn test_snapshot_unconfirmed_admission_drop_and_confirmed_or_unstarted_controls() {
    let runtime = borrowed_test_runtime(17);
    let request_runtime = Arc::clone(&runtime);
    let mut pending_provider = Box::pin(async move {
        let mut admission = request_runtime.reserve_for_provider(None, 2).unwrap();
        admission.begin_provider_operation().unwrap();
        std::future::pending::<()>().await;
        drop(admission);
    });
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    assert!(matches!(pending_provider.as_mut().poll(&mut context), Poll::Pending));
    drop(pending_provider);
    assert_eq!(1, runtime.snapshot().unconfirmed_provider_io);
    assert_eq!(2, runtime.snapshot().reserved_bytes);
    let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));

    let runtime = borrowed_test_runtime(17);
    let mut confirmed = runtime.reserve_for_provider(None, 4).unwrap();
    confirmed.begin_provider_operation().unwrap();
    confirmed.provider_operation_completed();
    drop(confirmed);
    assert_eq!(0, runtime.snapshot().unconfirmed_provider_io);

    let unstarted = runtime.reserve_for_provider(None, 2).unwrap();
    drop(unstarted);
    let after_unstarted = runtime.snapshot();
    assert_eq!(0, after_unstarted.unconfirmed_provider_io);
    assert_eq!(0, after_unstarted.reserved_bytes);
    let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));
}

fn assert_scope_admission_rollback(stopped: bool) {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let runtime = borrowed_test_runtime(8);
    let request_runtime = Arc::clone(&runtime);
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
    let (facts_tx, facts_rx) = mpsc::channel();
    let mut operation = manager
        .as_async()
        .try_submit(options, move |scope| async move {
            let _held = if stopped { None } else { Some(scope.manage(()).unwrap()) };
            if stopped {
                let status = scope.request_stop(StopReason::UserCancelled);
                assert_eq!(status, qubit_operation::StopRequestStatus::Accepted);
                scope.context().stop_requested().await;
            }
            let error = request_runtime
                .reserve_for_provider(Some(&scope), 8)
                .err()
                .expect("scope must reject registration");
            let message = format!("{error:?}");
            let no_tasks = request_runtime.tasks.lock().unwrap().is_empty();
            let active = request_runtime
                .active_requests
                .load(std::sync::atomic::Ordering::SeqCst);
            let full_budget_returned = request_runtime
                .reserve_for_provider(None, 8)
                .map(|permit| {
                    drop(permit);
                    true
                })
                .unwrap_or(false);
            let slots_returned = request_runtime
                .reserve_for_provider(None, 0)
                .and_then(|first| {
                    request_runtime
                        .reserve_for_provider(None, 0)
                        .map(|second| drop((first, second)))
                })
                .is_ok();
            facts_tx
                .send((message, no_tasks, active, full_budget_returned, slots_returned))
                .unwrap();
            Ok::<_, MimeError>(())
        })
        .unwrap();
    let outcome = block_on(operation.wait_outcome()).expect("operation outcome must be available");
    if stopped {
        assert!(
            matches!(outcome, OperationOutcome::Succeeded(()))
                || matches!(outcome, OperationOutcome::Stopped(ref report) if report.reason == StopReason::UserCancelled),
            "unexpected outcome: {outcome:?}"
        );
    } else {
        assert!(
            matches!(outcome, OperationOutcome::Succeeded(())),
            "unexpected outcome: {outcome:?}"
        );
    }
    let (message, no_tasks, active, full_budget_returned, slots_returned) = facts_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("scope admission facts must arrive");
    assert!(
        message.contains("operation scope rejected inference"),
        "unexpected error: {message}"
    );
    assert!(no_tasks);
    assert_eq!(active, 0);
    assert!(full_budget_returned);
    assert!(slots_returned);
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
    assert!(runtime.tasks.lock().unwrap().is_empty());
}

#[test]
fn test_scope_admission_stopped_rolls_back() {
    assert_scope_admission_rollback(true);
}

#[test]
fn test_scope_admission_capacity_rolls_back() {
    assert_scope_admission_rollback(false);
}

#[test]
fn test_scope_admission_repeated_failures_do_not_accumulate() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let runtime = borrowed_test_runtime(8);
    let request_runtime = Arc::clone(&runtime);
    let options = OperationOptions {
        limits: ScopeLimits {
            resources: 1,
            ..ScopeLimits::default()
        },
        ..OperationOptions::default()
    };
    let mut operation = manager
        .as_async()
        .try_submit(options, move |scope| async move {
            let _held = scope.manage(()).unwrap();
            for _ in 0..32 {
                assert!(request_runtime.reserve_for_provider(Some(&scope), 8).is_err());
                assert!(request_runtime.tasks.lock().unwrap().is_empty());
                assert_eq!(
                    request_runtime
                        .active_requests
                        .load(std::sync::atomic::Ordering::SeqCst),
                    0
                );
                drop(
                    request_runtime
                        .byte_budget
                        .try_acquire(8)
                        .expect("byte capacity must be returned"),
                );
            }
            Ok::<_, MimeError>(())
        })
        .unwrap();
    let outcome = block_on(operation.wait_outcome()).expect("operation outcome must be available");
    assert!(
        matches!(outcome, OperationOutcome::Succeeded(())),
        "unexpected outcome: {outcome:?}"
    );
    let report = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(2));
    assert!(report.worker_stopped);
}

#[test]
fn test_scope_admission_failure_preserves_other_request() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let runtime = borrowed_test_runtime(8);
    let independent = runtime.reserve_for_provider(None, 4).unwrap();
    let original_state = runtime.tasks.lock().unwrap()[0].clone();
    let request_runtime = Arc::clone(&runtime);
    let (facts_tx, facts_rx) = mpsc::channel();
    let options = OperationOptions {
        limits: ScopeLimits {
            resources: 1,
            ..ScopeLimits::default()
        },
        ..OperationOptions::default()
    };
    let mut operation = manager
        .as_async()
        .try_submit(options, move |scope| async move {
            let _held = scope.manage(()).unwrap();
            let error = request_runtime
                .reserve_for_provider(Some(&scope), 1)
                .err()
                .expect("scope must reject second request");
            facts_tx.send(format!("{error:?}")).unwrap();
            Ok::<_, MimeError>(())
        })
        .unwrap();
    let outcome = block_on(operation.wait_outcome()).expect("operation outcome must be available");
    assert!(
        matches!(outcome, OperationOutcome::Succeeded(())),
        "unexpected outcome: {outcome:?}"
    );
    let error = facts_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("scope rejection fact must arrive");
    assert!(
        error.contains("operation scope rejected inference"),
        "unexpected error: {error}"
    );
    let tasks = runtime.tasks.lock().unwrap();
    assert_eq!(tasks.len(), 1);
    assert!(Arc::ptr_eq(&tasks[0], &original_state));
    drop(tasks);
    assert_eq!(runtime.active_requests.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(runtime.byte_budget.try_acquire(5).is_none());
    drop(
        runtime
            .byte_budget
            .try_acquire(4)
            .expect("failed request must return byte permit"),
    );
    drop(
        runtime
            .reserve_for_provider(None, 0)
            .expect("failed request must return request slot"),
    );
    assert!(Arc::ptr_eq(&runtime.tasks.lock().unwrap()[0], &original_state));
    drop(independent);
    assert!(runtime.tasks.lock().unwrap().is_empty());
    assert_eq!(runtime.active_requests.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(
        runtime
            .byte_budget
            .try_acquire(8)
            .expect("full budget must be restored"),
    );
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
}

#[test]
fn test_borrowed_content_closed_does_not_copy() {
    let runtime = borrowed_test_runtime(8);
    let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(2));
    let copies = std::sync::atomic::AtomicUsize::new(0);
    let result = block_on(runtime.detect_content_unmanaged_with_copy(b"x", |_| {
        copies.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::from(b"x".as_slice())
    }));
    assert!(result.is_err());
    assert_eq!(copies.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn test_borrowed_content_full_does_not_copy() {
    let runtime = borrowed_test_runtime(8);
    let first = runtime.reserve_for_provider(None, 0).unwrap();
    let second = runtime.reserve_for_provider(None, 0).unwrap();
    let copies = std::sync::atomic::AtomicUsize::new(0);
    let result = block_on(runtime.detect_content_unmanaged_with_copy(b"x", |_| {
        copies.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::from(b"x".as_slice())
    }));
    assert!(result.is_err());
    assert_eq!(copies.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop((first, second));
    let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(2));
}

#[test]
fn test_borrowed_content_budget_rejection_does_not_copy() {
    let runtime = borrowed_test_runtime(8);
    let reservation = runtime.reserve_for_provider(None, 8).unwrap();
    let copies = std::sync::atomic::AtomicUsize::new(0);
    let result = block_on(runtime.detect_content_unmanaged_with_copy(b"x", |_| {
        copies.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Box::from(b"x".as_slice())
    }));
    assert!(result.is_err());
    assert_eq!(copies.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(reservation);
    let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(2));
}

#[test]
fn test_borrowed_content_accepted_copies_once() {
    let runtime = borrowed_test_runtime(8);
    let copies = std::sync::atomic::AtomicUsize::new(0);
    let result = block_on(runtime.detect_content_unmanaged_with_copy(b"x", |content| {
        copies.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert!(runtime.byte_budget.try_acquire(8).is_none());
        content.to_vec().into_boxed_slice()
    }));
    assert_eq!(result.unwrap(), None);
    assert_eq!(copies.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(runtime.byte_budget.try_acquire(8).is_some());
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
}

#[test]
fn test_admitted_bytes_cancel_returns_before_worker_releases_capacity() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let worker_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let engine_gate = Arc::clone(&worker_gate);
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(move |_| {
            started_tx.send(()).unwrap();
            let (lock, changed) = &*engine_gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
            Ok(Some("application/x-held".to_owned()))
        }),
        Box::new(|| {}),
    ));
    let gate_cleanup = ReleaseWorkerGate(Arc::clone(&worker_gate));
    let admission = runtime.reserve_for_provider(None, 4).unwrap();
    let request_runtime = Arc::clone(&runtime);
    let mut request = Box::pin(async move { request_runtime.detect_bytes_admitted(admission, b"data".to_vec()).await });
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    assert!(request.as_mut().poll(&mut context).is_pending());
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown =
        std::thread::spawn(move || shutdown_runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(5)));
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !runtime.snapshot().cancelling {
        assert!(std::time::Instant::now() < deadline, "runtime Cancel was not requested");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let stop_deadline = std::time::Instant::now() + Duration::from_secs(2);
    let cancelled_result = loop {
        if let Poll::Ready(result) = request.as_mut().poll(&mut context) {
            break result;
        }
        assert!(
            std::time::Instant::now() < stop_deadline,
            "runtime Cancel did not end business waiting while worker was gated"
        );
        std::thread::park_timeout(stop_deadline.saturating_duration_since(std::time::Instant::now()));
    };
    let cancelled_before_release = cancelled_result.is_err();
    let capacity_still_held = runtime.byte_budget.try_acquire(8).is_none();
    let worker_still_running = runtime.snapshot().worker_running == 1;

    gate_cleanup.release();
    let report = shutdown.join().unwrap();
    assert!(
        cancelled_before_release,
        "Cancel must end business waiting while worker is gated"
    );
    assert!(capacity_still_held, "worker-owned bytes must retain aggregate capacity");
    assert!(
        worker_still_running,
        "worker must still be executing before the gate opens"
    );
    let error = cancelled_result.unwrap_err();
    assert!(format!("{error:?}").contains("provider operation stopped after cancellation"));
    assert!(report.worker_stopped);
}

#[test]
fn test_unmanaged_content_cancel_returns_before_worker_releases_capacity() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let worker_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let engine_gate = Arc::clone(&worker_gate);
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(move |_| {
            started_tx.send(()).unwrap();
            let (lock, changed) = &*engine_gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
            Ok(Some("application/x-held".to_owned()))
        }),
        Box::new(|| {}),
    ));
    let gate_cleanup = ReleaseWorkerGate(Arc::clone(&worker_gate));
    let request_runtime = Arc::clone(&runtime);
    let mut request = Box::pin(async move { request_runtime.detect_content_unmanaged(b"data").await });
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    assert!(request.as_mut().poll(&mut context).is_pending());
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown =
        std::thread::spawn(move || shutdown_runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(5)));
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !runtime.snapshot().cancelling {
        assert!(std::time::Instant::now() < deadline, "runtime Cancel was not requested");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let stop_deadline = std::time::Instant::now() + Duration::from_secs(2);
    let cancelled_result = loop {
        if let Poll::Ready(result) = request.as_mut().poll(&mut context) {
            break result;
        }
        assert!(
            std::time::Instant::now() < stop_deadline,
            "runtime Cancel did not end business waiting while worker was gated"
        );
        std::thread::park_timeout(stop_deadline.saturating_duration_since(std::time::Instant::now()));
    };
    let cancelled_before_release = cancelled_result.is_err();
    let capacity_still_held = runtime.byte_budget.try_acquire(8).is_none();
    let worker_still_running = runtime.snapshot().worker_running == 1;

    gate_cleanup.release();
    let report = shutdown.join().unwrap();
    assert!(
        cancelled_before_release,
        "Cancel must end business waiting while worker is gated"
    );
    assert!(capacity_still_held, "worker-owned bytes must retain aggregate capacity");
    assert!(
        worker_still_running,
        "worker must still be executing before the gate opens"
    );
    let error = cancelled_result.unwrap_err();
    assert!(format!("{error:?}").contains("provider operation stopped after cancellation"));
    assert!(report.worker_stopped);
}

#[test]
fn test_admitted_bytes_preserve_published_result_when_cancel_follows() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let release_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let callback_gate = Arc::clone(&release_gate);
    let (released_result_tx, released_result_rx) = mpsc::sync_channel(1);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(|_| Ok(Some("application/x-published".to_owned()))),
        Box::new(move || {
            released_result_tx.send(()).unwrap();
            let (lock, changed) = &*callback_gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
        }),
    ));
    let gate_cleanup = ReleaseWorkerGate(Arc::clone(&release_gate));
    let admission = runtime.reserve_for_provider(None, 4).unwrap();
    let request_runtime = Arc::clone(&runtime);
    let mut request = Box::pin(async move { request_runtime.detect_bytes_admitted(admission, b"data".to_vec()).await });
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    assert!(request.as_mut().poll(&mut context).is_pending());
    released_result_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown =
        std::thread::spawn(move || shutdown_runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(5)));
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !runtime.snapshot().cancelling {
        assert!(std::time::Instant::now() < deadline, "runtime Cancel was not requested");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    let result = loop {
        if let Poll::Ready(result) = request.as_mut().poll(&mut context) {
            break result;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "published result was not observed"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    };
    let capacity_still_held = runtime.byte_budget.try_acquire(8).is_none();

    gate_cleanup.release();
    let report = shutdown.join().unwrap();
    assert_eq!(result.unwrap().as_deref(), Some("application/x-published"));
    assert!(
        capacity_still_held,
        "published business result must not release worker-owned bytes"
    );
    assert!(report.worker_stopped);
}

#[test]
fn test_unmanaged_content_waits_for_worker_settlement_after_business_result() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let release_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let callback_gate = Arc::clone(&release_gate);
    let (business_tx, business_rx) = mpsc::sync_channel(1);
    let (result_tx, result_rx) = mpsc::sync_channel(1);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(|_| Ok(Some("application/x-settled".to_owned()))),
        Box::new(move || {
            business_tx.send(()).unwrap();
            let (lock, changed) = &*callback_gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
        }),
    ));
    let gate_cleanup = ReleaseWorkerGate(Arc::clone(&release_gate));
    let request_runtime = Arc::clone(&runtime);
    let request = std::thread::spawn(move || {
        result_tx
            .send(block_on(request_runtime.detect_content_unmanaged(b"data")))
            .unwrap();
    });
    business_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(matches!(result_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));
    assert!(runtime.byte_budget.try_acquire(8).is_none());

    gate_cleanup.release();
    assert_eq!(
        Some("application/x-settled".to_owned()),
        result_rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap()
    );
    request.join().unwrap();
    assert!(
        runtime
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2))
            .worker_stopped
    );
}

struct ReleaseWorkerGate(Arc<(Mutex<bool>, Condvar)>);

impl ReleaseWorkerGate {
    fn release(&self) {
        let (lock, changed) = &*self.0;
        *lock.lock().unwrap_or_else(|error| error.into_inner()) = true;
        changed.notify_all();
    }
}

impl Drop for ReleaseWorkerGate {
    fn drop(&mut self) {
        self.release();
    }
}

struct PausedDropProbe {
    entered: mpsc::SyncSender<()>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}

impl Drop for PausedDropProbe {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let (lock, changed) = &*self.gate;
        let mut released = lock.lock().unwrap_or_else(|error| error.into_inner());
        while !*released {
            released = changed.wait(released).unwrap_or_else(|error| error.into_inner());
        }
    }
}

#[test]
fn test_cancelled_input_drops_before_worker_release() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let first_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let first_gate_worker = Arc::clone(&first_gate);
    let (first_started_tx, first_started_rx) = mpsc::sync_channel(1);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(move |_| {
            let _ = first_started_tx.send(());
            let (lock, changed) = &*first_gate_worker;
            let mut released = lock.lock().unwrap_or_else(|error| error.into_inner());
            while !*released {
                released = changed.wait(released).unwrap_or_else(|error| error.into_inner());
            }
            Ok(None)
        }),
        Box::new(|| {}),
    ));
    let first_gate_cleanup = ReleaseWorkerGate(Arc::clone(&first_gate));

    let _first = runtime.submit(None, WorkerInput::Bytes(vec![b'a'].into())).unwrap();
    first_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (probe_entered_tx, probe_entered_rx) = mpsc::sync_channel(1);
    let probe_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let probe_gate_cleanup = ReleaseWorkerGate(Arc::clone(&probe_gate));
    let _probe = runtime
        .submit(
            None,
            WorkerInput::Bytes(WorkerBytes::with_drop_probe(
                Box::new([]),
                Box::new(PausedDropProbe {
                    entered: probe_entered_tx,
                    gate: Arc::clone(&probe_gate),
                }),
            )),
        )
        .unwrap();
    let state = runtime.tasks.lock().unwrap()[1].clone();
    state.request_stop();
    first_gate_cleanup.release();

    let entered = probe_entered_rx.recv_timeout(Duration::from_secs(2));
    if entered.is_err() {
        probe_gate_cleanup.release();
        first_gate_cleanup.release();
        panic!("cancelled input destructor did not enter");
    }
    assert!(!state.wait_worker_done_blocking(Duration::ZERO));
    assert!(!state.wait_released_blocking(Duration::ZERO));
    probe_gate_cleanup.release();
    assert!(state.wait_worker_done_blocking(Duration::from_secs(2)));
    assert!(state.wait_released_blocking(Duration::from_secs(2)));
    assert!(runtime.byte_budget.try_acquire(8).is_some());
    let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(2));
}

#[test]
fn test_cancelled_queued_bytes_restore_byte_budget() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let worker_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let worker_gate_copy = Arc::clone(&worker_gate);
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(move |_| {
            let _ = started_tx.send(());
            let (lock, changed) = &*worker_gate_copy;
            let mut released = lock.lock().unwrap_or_else(|error| error.into_inner());
            while !*released {
                released = changed.wait(released).unwrap_or_else(|error| error.into_inner());
            }
            Ok(None)
        }),
        Box::new(|| {}),
    ));
    let gate_cleanup = ReleaseWorkerGate(Arc::clone(&worker_gate));

    let _first = runtime.submit(None, WorkerInput::Bytes(vec![0; 4].into())).unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let first_state = runtime.tasks.lock().unwrap()[0].clone();
    let queued = runtime.submit(None, WorkerInput::Bytes(vec![0; 4].into())).unwrap();
    let queued_state = runtime.tasks.lock().unwrap()[1].clone();
    drop(queued);
    // Dropping the result waiter requests stop, but the queued worker still
    // owns the bytes until it dequeues and confirms the input was discarded.
    assert!(runtime.reserve_for_provider(None, 1).is_err());

    gate_cleanup.release();
    assert!(first_state.wait_worker_done_blocking(Duration::from_secs(2)));
    assert!(first_state.wait_released_blocking(Duration::from_secs(2)));
    assert!(queued_state.wait_worker_done_blocking(Duration::from_secs(2)));
    assert!(queued_state.wait_released_blocking(Duration::from_secs(2)));
    assert!(runtime.byte_budget.try_acquire(8).is_some());
    let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(2));
}

#[test]
fn test_provider_read_error_is_mapped_and_settled() {
    struct FailedLengthInput;

    impl magika::AsyncInput for FailedLengthInput {
        fn length(&self) -> impl Future<Output = Result<u64, magika::Error>> {
            std::future::ready(Err(magika::Error::IOError(std::io::Error::other("test length failed"))))
        }

        fn read_at(&mut self, _buffer: &mut [u8], _offset: u64) -> impl Future<Output = Result<(), magika::Error>> {
            std::future::ready(Ok(()))
        }
    }

    struct FailedReadInput;

    impl magika::AsyncInput for FailedReadInput {
        fn length(&self) -> impl Future<Output = Result<u64, magika::Error>> {
            std::future::ready(Ok(8192))
        }

        fn read_at(&mut self, _buffer: &mut [u8], _offset: u64) -> impl Future<Output = Result<(), magika::Error>> {
            std::future::ready(Err(magika::Error::IOError(std::io::Error::other("test read failed"))))
        }
    }

    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8192,
        Box::new(|_| Ok(None)),
        Box::new(|| {}),
    ));
    let mapped_error = map_provider_test_error(magika::Error::IOError(std::io::Error::other("test mapping")));
    assert!(matches!(mapped_error, MimeError::DetectorBackend { .. }));
    let length_admission = runtime.reserve_for_provider(None, 8192).unwrap();
    let length_result = block_on(runtime.detect_async_input_admitted_test(
        length_admission,
        FailedLengthInput,
        8192,
        map_provider_test_error,
    ));
    assert!(matches!(length_result, Err(MimeError::DetectorBackend { .. })));

    let admission = runtime.reserve_for_provider(None, 8192).unwrap();
    let result =
        block_on(runtime.detect_async_input_admitted_test(admission, FailedReadInput, 8192, map_provider_test_error));

    assert!(matches!(result, Err(MimeError::Io(_))));
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.admission.snapshot.active_operations == 0);
    assert!(report.worker_stopped);
}

#[test]
fn test_shutdown_scan_cannot_miss_a_request_between_service_and_task_registration() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(|_| Ok(None)),
        Box::new(|| {}),
    ));
    let reservation_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let reservation_wait = Arc::clone(&reservation_gate);
    let (reserved_tx, reserved_rx) = mpsc::channel();
    let (admission_tx, admission_rx) = mpsc::channel();
    let reserve_runtime = Arc::clone(&runtime);
    let reserver = std::thread::spawn(move || {
        let admission = reserve_runtime
            .reserve_request_with_hook(None, 8, move || {
                reserved_tx.send(()).unwrap();
                let (lock, changed) = &*reservation_wait;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = changed.wait(released).unwrap();
                }
            })
            .unwrap();
        admission_tx.send(admission).unwrap();
    });
    reserved_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let (shutdown_started_tx, shutdown_started_rx) = mpsc::channel();
    let (scan_tx, scan_rx) = mpsc::channel();
    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown = std::thread::spawn(move || {
        shutdown_started_tx.send(()).unwrap();
        shutdown_runtime.shutdown_blocking_with_hook(ShutdownMode::Cancel, Duration::from_millis(250), move || {
            scan_tx.send(()).unwrap()
        })
    });
    shutdown_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let scanned_before_registration = scan_rx.recv_timeout(Duration::from_millis(50)).is_ok();

    let (lock, changed) = &*reservation_gate;
    *lock.lock().unwrap() = true;
    changed.notify_one();
    let admission = admission_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    if !scanned_before_registration {
        scan_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    let _report = shutdown.join().unwrap();
    reserver.join().unwrap();

    assert!(
        admission.state.stop_requested(),
        "shutdown must stop every request admitted before its task scan"
    );
    drop(admission);
    let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));
}

#[test]
fn test_committed_provider_admission_checks_stop_before_first_and_later_io() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(GroupOptions { capacity: 2 }).unwrap();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(|_| Ok(None)),
        Box::new(|| {}),
    ));

    let first_runtime = Arc::clone(&runtime);
    let mut first = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            let mut admission = first_runtime.reserve_for_provider(Some(&scope), 8)?;
            let _ = scope.request_stop(StopReason::UserCancelled);
            assert!(admission.begin_provider_operation().is_err());
            Ok::<_, MimeError>(())
        })
        .unwrap();
    let _ = block_on(first.wait_outcome());

    let later_runtime = Arc::clone(&runtime);
    let mut later = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            let mut admission = later_runtime.reserve_for_provider(Some(&scope), 8)?;
            admission.begin_provider_operation()?;
            admission.provider_operation_completed();
            let _ = scope.request_stop(StopReason::UserCancelled);
            assert!(admission.begin_provider_operation().is_err());
            Ok::<_, MimeError>(())
        })
        .unwrap();
    let _ = block_on(later.wait_outcome());

    let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));
}

#[test]
fn test_scope_less_pending_read_keeps_budget_after_worker_done() {
    struct ThreadWake(std::thread::Thread);
    impl std::task::Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    struct PendingReadInput(std::sync::mpsc::SyncSender<()>);
    impl magika::AsyncInput for PendingReadInput {
        fn length(&self) -> impl Future<Output = Result<u64, magika::Error>> {
            std::future::ready(Ok(1))
        }
        fn read_at(&mut self, _buffer: &mut [u8], _offset: u64) -> impl Future<Output = Result<(), magika::Error>> {
            let started = self.0.clone();
            async move {
                started.send(()).unwrap();
                std::future::pending::<()>().await;
                Ok(())
            }
        }
    }
    fn map_error(_: magika::Error) -> MimeError {
        MimeError::detector_backend("magika", "test input failed")
    }

    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(|_| Ok(None)),
        Box::new(|| {}),
    ));
    let admission = runtime.reserve_for_provider(None, 8).unwrap();
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let input = PendingReadInput(started_tx);
    let body_runtime = Arc::clone(&runtime);
    let mut future = Box::pin(async move {
        body_runtime
            .detect_async_input_admitted_test(admission, input, 8, map_error)
            .await
    });
    let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        match started_rx.try_recv() {
            Ok(()) => break,
            Err(mpsc::TryRecvError::Disconnected) => panic!("provider input dropped before read"),
            Err(mpsc::TryRecvError::Empty) => {}
        }
        assert!(future.as_mut().poll(&mut context).is_pending());
        std::thread::park_timeout(Duration::from_millis(10));
    }
    let state = runtime.tasks.lock().unwrap().first().unwrap().clone();
    drop(future);
    assert!(state.wait_worker_done_blocking(Duration::from_secs(2)));
    assert!(!state.inner.lock().unwrap().released);
    let snapshot = runtime.snapshot();
    assert_eq!(1, snapshot.unconfirmed_provider_io);
    assert_eq!(8, snapshot.reserved_bytes);
    assert!(runtime.reserve_for_provider(None, 1).is_err());
    let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_millis(10));
}

#[test]
fn test_provider_driver_safe_drop_after_read_ready_releases_capacity() {
    struct ThreadWake(std::thread::Thread);
    impl std::task::Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    struct ReadyInput;
    impl magika::AsyncInput for ReadyInput {
        fn length(&self) -> impl Future<Output = Result<u64, magika::Error>> {
            std::future::ready(Ok(1))
        }
        fn read_at(&mut self, buffer: &mut [u8], _offset: u64) -> impl Future<Output = Result<(), magika::Error>> {
            let _ = buffer;
            std::future::ready(Err(magika::Error::IOError(std::io::Error::other("test read failed"))))
        }
    }

    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let (worker_tx, worker_rx) = mpsc::sync_channel(1);
    let worker_gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let callback_gate = Arc::clone(&worker_gate);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(|_| Ok(None)),
        Box::new(move || {
            worker_tx.send(()).unwrap();
            let (lock, changed) = &*callback_gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
        }),
    ));
    // Declared after the runtime so an assertion unwind releases the
    // worker before runtime teardown attempts to join it.
    let gate_cleanup = ReleaseWorkerGate(Arc::clone(&worker_gate));
    let admission = runtime.reserve_for_provider(None, 8).unwrap();
    let body_runtime = Arc::clone(&runtime);
    let mut future = Box::pin(async move {
        body_runtime
            .detect_async_input_admitted_test(admission, ReadyInput, 8, map_provider_test_error)
            .await
    });
    let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let mut worker_gate_entered = false;
    loop {
        match worker_rx.try_recv() {
            Ok(()) => {
                worker_gate_entered = true;
                break;
            }
            Err(mpsc::TryRecvError::Disconnected) => panic!("worker stopped before reaching its gate"),
            Err(mpsc::TryRecvError::Empty) => {}
        }
        assert!(std::time::Instant::now() < deadline, "worker did not reach its gate");
        if matches!(future.as_mut().poll(&mut context), Poll::Ready(_)) {
            // The provider read error may publish the business result before
            // the worker's after-business gate sends its handshake.
            break;
        }
        std::thread::park_timeout(Duration::from_millis(1));
    }
    if !worker_gate_entered {
        worker_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("worker should be waiting at the release gate");
    }
    let state = runtime.tasks.lock().unwrap().first().unwrap().clone();
    assert!(!state.wait_worker_done_blocking(Duration::ZERO));
    drop(future);
    gate_cleanup.release();
    assert!(state.wait_worker_done_blocking(Duration::from_secs(2)));
    assert!(state.wait_released_blocking(Duration::from_secs(2)));
    assert!(runtime.byte_budget.try_acquire(8).is_some());
    let _ = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
}

#[test]
fn test_provider_driver_safe_drop_before_range_read_releases_queued_capacity() {
    struct ThreadWake(std::thread::Thread);
    impl std::task::Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    struct CountedInput(Arc<std::sync::atomic::AtomicUsize>);
    impl magika::AsyncInput for CountedInput {
        fn length(&self) -> impl Future<Output = Result<u64, magika::Error>> {
            std::future::ready(Ok(1))
        }
        fn read_at(&mut self, buffer: &mut [u8], _offset: u64) -> impl Future<Output = Result<(), magika::Error>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            buffer[0] = b'x';
            std::future::ready(Ok(()))
        }
    }
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let inference_gate = Arc::new((Mutex::new(false), Condvar::new()));
    let inference_gate_worker = Arc::clone(&inference_gate);
    let (inference_started_tx, inference_started_rx) = mpsc::sync_channel(1);
    let (worker_tx, worker_rx) = mpsc::sync_channel(1);
    let worker_signaled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_signaled = Arc::clone(&worker_signaled);
    let worker_gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let callback_gate = Arc::clone(&worker_gate);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        16,
        Box::new(move |_| {
            inference_started_tx.send(()).unwrap();
            let (lock, changed) = &*inference_gate_worker;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = changed.wait(released).unwrap();
            }
            Ok(None)
        }),
        Box::new(move || {
            if !callback_signaled.swap(true, std::sync::atomic::Ordering::SeqCst) {
                worker_tx.send(()).unwrap();
                let (lock, changed) = &*callback_gate;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = changed.wait(released).unwrap();
                }
            }
        }),
    ));
    // Declared after the runtime so an assertion unwind releases the
    // worker before runtime teardown attempts to join it.
    let gate_cleanup = ReleaseWorkerGate(Arc::clone(&worker_gate));
    let inference_gate_cleanup = ReleaseWorkerGate(Arc::clone(&inference_gate));
    let first_admission = runtime.reserve_for_provider(None, 8).unwrap();
    let first_runtime = Arc::clone(&runtime);
    let mut first = Box::pin(async move { first_runtime.detect_bytes_admitted(first_admission, vec![b'a']).await });
    let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while inference_started_rx.try_recv().is_err() {
        assert!(
            std::time::Instant::now() < deadline,
            "first inference did not reach its gate"
        );
        assert!(first.as_mut().poll(&mut context).is_pending());
        std::thread::park_timeout(Duration::from_millis(1));
    }

    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let second_admission = runtime.reserve_for_provider(None, 8).unwrap();
    let second_runtime = Arc::clone(&runtime);
    let second_reads = Arc::clone(&reads);
    let mut second = Box::pin(async move {
        second_runtime
            .detect_async_input_admitted_test(second_admission, CountedInput(second_reads), 8, map_provider_test_error)
            .await
    });
    assert!(second.as_mut().poll(&mut context).is_pending());
    assert_eq!(0, reads.load(std::sync::atomic::Ordering::SeqCst));
    let states = runtime.tasks.lock().unwrap().clone();
    assert_eq!(2, states.len());
    drop(second);
    assert!(runtime.reserve_for_provider(None, 1).is_err());

    inference_gate_cleanup.release();
    worker_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    gate_cleanup.release();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Poll::Ready(result) = first.as_mut().poll(&mut context) {
            result.unwrap();
            break;
        }
        assert!(std::time::Instant::now() < deadline, "first request did not finish");
        std::thread::park_timeout(Duration::from_millis(1));
    }
    for state in states {
        assert!(state.wait_released_blocking(Duration::from_secs(2)));
    }
    assert_eq!(0, reads.load(std::sync::atomic::Ordering::SeqCst));
    assert!(runtime.byte_budget.try_acquire(8).is_some());
    let success_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let success_admission = runtime.reserve_for_provider(None, 8).unwrap();
    let success_runtime = Arc::clone(&runtime);
    let success_input = Arc::clone(&success_reads);
    let mut success = Box::pin(async move {
        success_runtime
            .detect_async_input_admitted_test(
                success_admission,
                CountedInput(success_input),
                8,
                map_provider_test_error,
            )
            .await
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        if let Poll::Ready(result) = success.as_mut().poll(&mut context) {
            assert!(result.is_ok());
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "successful provider request timed out"
        );
        std::thread::park_timeout(Duration::from_millis(1));
    }
    assert_eq!(1, success_reads.load(std::sync::atomic::Ordering::SeqCst));
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
    assert!(report.admission.snapshot.active_operations == 0);
}

#[test]
fn test_admitted_owned_bytes_return_worker_result() {
    struct ThreadWake(std::thread::Thread);
    impl std::task::Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(|bytes| Ok(Some(format!("application/x-bytes-{}", bytes.len())))),
        Box::new(|| {}),
    ));
    let admission = runtime.reserve_for_provider(None, 3).unwrap();
    let mut future = Box::pin(runtime.detect_bytes_admitted(admission, vec![1, 2, 3]));
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let result = loop {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            break result;
        }
        std::thread::park_timeout(Duration::from_secs(1));
    };
    assert_eq!(result.unwrap().as_deref(), Some("application/x-bytes-3"));
    assert!(
        runtime
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1))
            .worker_stopped
    );
}

#[test]
fn test_shared_worker_classification_covers_success_and_panic() {
    let (result, outcome) = execute_classified(|| Ok::<_, MimeError>(Some("text/plain".to_owned())));
    assert!(matches!(result, Ok(Some(value)) if value == "text/plain"));
    assert_eq!(outcome, ChildOutcomeKind::Succeeded);

    let (result, outcome): (qubit_mime::MimeResult<()>, _) = execute_classified(|| panic!("engine panic"));
    assert!(result.is_err());
    assert_eq!(outcome, ChildOutcomeKind::Panicked);
}

fn assert_current_bridge_closes_for_engine(
    engine: crate::managed_runtime::managed_inference_runtime::test_support::ManagedInferenceEngine,
    expected: ChildOutcomeKind,
) {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let (business_tx, business_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let release_rx = Arc::new(Mutex::new(release_rx));
    let release_wait = Arc::clone(&release_rx);
    let runtime = ManagedInferenceRuntime::with_engine(
        supervisor,
        4,
        engine,
        Box::new(move || {
            business_tx.send(()).unwrap();
            release_wait.lock().unwrap().recv().unwrap();
        }),
    );
    let bridge = crate::internal::async_sample_bridge::AsyncSampleBridge::new(0);
    let driver = bridge.driver();
    let _inference = runtime
        .submit(
            None,
            WorkerInput::Bridge(Arc::clone(&bridge), 4, map_provider_test_error),
        )
        .unwrap();
    let state = runtime.tasks.lock().unwrap()[0].clone();

    business_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(state.inner.lock().unwrap().business, Some(expected));
    assert!(!state.inner.lock().unwrap().worker_done);
    assert!(!state.inner.lock().unwrap().released);
    assert!(runtime.byte_budget.try_acquire(4).is_none());

    release_tx.send(()).unwrap();
    let (closed_tx, closed_rx) = mpsc::sync_channel(1);
    let waiting_bridge = Arc::clone(&bridge);
    let closure_observer = std::thread::spawn(move || {
        block_on(waiting_bridge.closed());
        closed_tx.send(()).unwrap();
    });
    if closed_rx.recv_timeout(Duration::from_secs(2)).is_err() {
        drop(driver);
        closed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        closure_observer.join().unwrap();
        panic!("worker did not close the current bridge");
    }
    closure_observer.join().unwrap();
    assert!(state.wait_worker_done_blocking(Duration::from_secs(2)));
    assert!(!state.inner.lock().unwrap().released);
    assert!(runtime.byte_budget.try_acquire(4).is_none());

    drop(driver);
    state.mark_driver_done();
    assert!(state.wait_released_blocking(Duration::from_secs(2)));
    assert!(runtime.byte_budget.try_acquire(4).is_some());
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
}

#[test]
fn test_worker_current_bridge_closed_on_success() {
    assert_current_bridge_closes_for_engine(
        Box::new(|_| Ok(Some("text/plain".to_owned()))),
        ChildOutcomeKind::Succeeded,
    );
}

#[test]
fn test_worker_current_bridge_closed_on_error() {
    assert_current_bridge_closes_for_engine(
        Box::new(|_| Err(MimeError::detector_backend("magika", "injected inference error"))),
        ChildOutcomeKind::Failed,
    );
}

#[test]
fn test_worker_current_bridge_closed_on_panic() {
    assert_current_bridge_closes_for_engine(
        Box::new(|_| panic!("injected inference panic")),
        ChildOutcomeKind::Panicked,
    );
}

#[test]
fn test_drain_admitted_provider_completes_after_gate_closes() {
    struct GatedLengthInput(mpsc::SyncSender<()>, Arc<Mutex<mpsc::Receiver<()>>>);
    impl magika::AsyncInput for GatedLengthInput {
        fn length(&self) -> impl Future<Output = Result<u64, magika::Error>> {
            let started = self.0.clone();
            let release = Arc::clone(&self.1);
            async move {
                started.send(()).unwrap();
                release.lock().unwrap().recv().unwrap();
                Ok(1)
            }
        }
        fn read_at(&mut self, buffer: &mut [u8], _offset: u64) -> impl Future<Output = Result<(), magika::Error>> {
            buffer[0] = b'x';
            std::future::ready(Ok(()))
        }
    }

    let runtime = borrowed_test_runtime(8);
    let admission = runtime.reserve_for_provider(None, 8).unwrap();
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let release_rx = Arc::new(Mutex::new(release_rx));
    let detect_runtime = Arc::clone(&runtime);
    let detect = std::thread::spawn(move || {
        block_on(detect_runtime.detect_async_input_admitted_test(
            admission,
            GatedLengthInput(started_tx, release_rx),
            8,
            map_provider_test_error,
        ))
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let (closed_tx, closed_rx) = mpsc::sync_channel(1);
    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown = std::thread::spawn(move || {
        shutdown_runtime.shutdown_blocking_with_hook(ShutdownMode::Drain, Duration::from_secs(2), move || {
            closed_tx.send(()).unwrap();
        })
    });
    closed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(runtime.reserve_for_provider(None, 1).is_err());
    release_tx.send(()).unwrap();
    assert_eq!(detect.join().unwrap().unwrap(), None);
    let report = shutdown.join().unwrap();
    assert_eq!(0, report.admission.snapshot.active_operations);
    assert!(report.worker_stopped);
}

#[test]
fn test_drain_real_managed_provider_route_completes_gated_read() {
    use crate::tests::internal::random_access_session_tests::Fixture;
    use crate::tests::internal::random_access_session_tests::TestAsyncReadGate;

    struct ReleaseOnDrop(Arc<TestAsyncReadGate>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(GroupOptions { capacity: 2 }).unwrap();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor.clone(),
        qubit_mime::MimeConfig::default().max_buffer_size(),
        Box::new(|bytes| {
            assert_eq!(bytes, b"sample");
            Ok(Some("application/x-fixture".to_owned()))
        }),
        Box::new(|| {}),
    ));
    let detector = Arc::new(
        crate::MagikaMimeDetector::builder()
            .managed_runtime(Arc::clone(&runtime))
            .build()
            .unwrap(),
    );
    let gate = Arc::new(TestAsyncReadGate::new());
    let _release_gate = ReleaseOnDrop(Arc::clone(&gate));
    let fixture = Fixture::new(b"sample", qubit_fs::metadata::FileSystemLimit::Maximum(2))
        .with_async_read_gate(Arc::clone(&gate));
    let file_system = qubit_fs::AsyncFileSystem::from_spi(fixture).unwrap();
    let request_detector = Arc::clone(&detector);
    let mut operation = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            request_detector
                .detect_async_provider_path_managed(
                    &scope,
                    &file_system,
                    &qubit_fs::Path::parse("/drain/gated-read").unwrap(),
                    6,
                )
                .await
        })
        .unwrap();

    gate.wait_until_started(Duration::from_secs(2));
    assert_eq!(1, runtime.tasks.lock().unwrap().len());
    let (closed_tx, closed_rx) = mpsc::sync_channel(1);
    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown = std::thread::spawn(move || {
        shutdown_runtime.shutdown_blocking_with_hook(ShutdownMode::Drain, Duration::from_secs(2), move || {
            closed_tx.send(()).unwrap();
        })
    });
    closed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(runtime.reserve_for_provider(None, 1).is_err());
    gate.release();
    assert!(matches!(
        block_on(operation.wait_outcome()).unwrap(),
        OperationOutcome::Succeeded(Some(ref mime)) if mime == "application/x-fixture"
    ));
    assert!(block_on(operation.wait_settlement()).all_released());
    let report = shutdown.join().unwrap();
    assert_eq!(0, report.admission.snapshot.active_operations);
    assert!(report.worker_stopped);
    service.close_admission();
    assert_eq!(
        0,
        supervisor
            .request_shutdown(ShutdownMode::Drain)
            .wait()
            .unwrap()
            .snapshot
            .active_operations
    );
}

#[test]
fn test_drain_reserved_content_can_submit_operation_after_gate_closes() {
    let runtime = borrowed_test_runtime(8);
    let admission = runtime.reserve_for_provider(None, 1).unwrap();
    let (submit_tx, submit_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let release_rx = Arc::new(Mutex::new(release_rx));
    let submit_runtime = Arc::clone(&runtime);
    let submit = std::thread::spawn(move || {
        block_on(submit_runtime.submit_operation_admitted_with_hook(
            None,
            admission,
            WorkerInput::Bytes(vec![b'x'].into()),
            move || {
                submit_tx.send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            },
        ))
    });
    submit_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let (closed_tx, closed_rx) = mpsc::sync_channel(1);
    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown = std::thread::spawn(move || {
        shutdown_runtime.shutdown_blocking_with_hook(ShutdownMode::Drain, Duration::from_secs(2), move || {
            closed_tx.send(()).unwrap();
        })
    });
    closed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(runtime.reserve_for_provider(None, 1).is_err());
    release_tx.send(()).unwrap();
    assert_eq!(submit.join().unwrap().unwrap(), None);
    let report = shutdown.join().unwrap();
    assert_eq!(0, report.admission.snapshot.active_operations);
    assert!(report.worker_stopped);
}

#[test]
fn test_cancel_admitted_provider_does_not_submit_model_job() {
    struct GatedLengthInput(mpsc::SyncSender<()>, Arc<Mutex<mpsc::Receiver<()>>>);
    impl magika::AsyncInput for GatedLengthInput {
        fn length(&self) -> impl Future<Output = Result<u64, magika::Error>> {
            let started = self.0.clone();
            let release = Arc::clone(&self.1);
            async move {
                started.send(()).unwrap();
                release.lock().unwrap().recv().unwrap();
                Ok(1)
            }
        }
        fn read_at(&mut self, _buffer: &mut [u8], _offset: u64) -> impl Future<Output = Result<(), magika::Error>> {
            std::future::ready(Ok(()))
        }
    }

    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let engine_calls = Arc::clone(&calls);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        OperationManager::new(ManagerOptions::default()).unwrap(),
        8,
        Box::new(move |_| {
            engine_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(None)
        }),
        Box::new(|| {}),
    ));
    let admission = runtime.reserve_for_provider(None, 8).unwrap();
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let release_rx = Arc::new(Mutex::new(release_rx));
    let detect_runtime = Arc::clone(&runtime);
    let detect = std::thread::spawn(move || {
        block_on(detect_runtime.detect_async_input_admitted_test(
            admission,
            GatedLengthInput(started_tx, release_rx),
            8,
            map_provider_test_error,
        ))
    });
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (closed_tx, closed_rx) = mpsc::sync_channel(1);
    let shutdown_runtime = Arc::clone(&runtime);
    let shutdown = std::thread::spawn(move || {
        shutdown_runtime.shutdown_blocking_with_hook(ShutdownMode::Cancel, Duration::from_secs(2), move || {
            closed_tx.send(()).unwrap();
        })
    });
    closed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    release_tx.send(()).unwrap();
    assert!(detect.join().unwrap().is_err());
    let report = shutdown.join().unwrap();
    assert!(report.worker_stopped);
    assert_eq!(0, calls.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn test_worker_panic_rejects_previously_admitted_dispatch() {
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        OperationManager::new(ManagerOptions::default()).unwrap(),
        8,
        Box::new(|_| panic!("injected worker panic")),
        Box::new(|| {}),
    ));
    let second_admission = runtime.reserve_for_provider(None, 1).unwrap();
    let first_admission = runtime.reserve_for_provider(None, 1).unwrap();
    let first = runtime
        .dispatch(first_admission, WorkerInput::Bytes(vec![b'x'].into()))
        .unwrap();
    assert!(first.wait_blocking().is_err());
    let error = match runtime.dispatch(second_admission, WorkerInput::Bytes(vec![b'y'].into())) {
        Ok(_) => panic!("dispatch after worker fault unexpectedly succeeded"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("after worker fault"));
    assert!(runtime.admission_gate.lock().unwrap().faulted);
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
}

#[test]
fn test_worker_panic_wakes_pending_provider_io_before_delivery() {
    for phase in ["open", "stat", "read_all"] {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let (engine_started_tx, engine_started_rx) = mpsc::sync_channel(1);
        let (panic_tx, panic_rx) = mpsc::sync_channel(1);
        let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
            manager.clone(),
            16,
            Box::new(move |_| {
                engine_started_tx.send(()).unwrap();
                panic_rx.recv().unwrap();
                panic!("injected inference worker panic");
            }),
            Box::new(|| {}),
        ));

        let first = runtime.submit(None, WorkerInput::Bytes(vec![b'a'].into())).unwrap();
        engine_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let (provider_started_tx, provider_started_rx) = mpsc::sync_channel(1);
        let (provider_result_tx, provider_result_rx) = mpsc::sync_channel(1);
        let (provider_state_tx, provider_state_rx) = mpsc::sync_channel(1);
        let provider_runtime = Arc::clone(&runtime);
        let provider = manager
            .as_async()
            .try_submit(OperationOptions::default(), move |scope| async move {
                let mut admission = provider_runtime.reserve_for_provider(Some(&scope), 1)?;
                provider_state_tx.send(admission.task_state()).unwrap();
                let io_in_flight = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let io_for_future = Arc::clone(&io_in_flight);
                let pending_io = std::future::poll_fn(move |_| {
                    io_for_future.store(true, std::sync::atomic::Ordering::Release);
                    let _ = provider_started_tx.try_send(());
                    Poll::<()>::Pending
                });
                let result = if phase == "read_all" {
                    admission
                        .run_provider_operation_with_status(&scope, pending_io, io_in_flight)
                        .await
                } else {
                    admission.run_provider_operation(&scope, pending_io).await
                };
                provider_result_tx
                    .send(result.map(|_| ()).map_err(|error| error.to_string()))
                    .unwrap();
                Ok::<_, MimeError>(())
            })
            .unwrap();
        provider_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let provider_state = provider_state_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        panic_tx.send(()).unwrap();
        let provider_result = provider_result_rx.recv_timeout(Duration::from_secs(2));
        let promptly_notified = provider_result.is_ok();
        if !promptly_notified {
            // Keep the regression failure bounded while allowing the old
            // implementation to release its pending request during unwind.
            provider_state.request_stop();
        }
        let provider_result = match provider_result {
            Ok(result) => result,
            Err(_) => provider_result_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        };
        let provider_error = provider_result.unwrap_err();

        assert!(
            promptly_notified,
            "pending {phase} operation was not notified of worker fault"
        );
        assert!(
            provider_error.contains("after worker fault"),
            "pending {phase} operation should report the worker fault, got: {provider_error}"
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !provider_state.inner.lock().unwrap().unconfirmed_provider_io {
            assert!(
                std::time::Instant::now() < deadline,
                "pending {phase} operation did not retain its I/O uncertainty"
            );
            std::thread::yield_now();
        }
        assert!(
            provider_state.inner.lock().unwrap().unconfirmed_provider_io,
            "pending {phase} operation must retain its unconfirmed I/O fact"
        );
        assert_eq!(
            block_on(first).unwrap_err().to_string(),
            "MIME detector backend 'magika' failed: inference worker panicked",
            "the request that triggered the panic must retain its original MIME failure"
        );
        let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
        assert!(report.worker_stopped);
        assert!(!report.worker_panicked);
        let _ = block_on(provider.wait_settlement());
    }
}

#[test]
fn test_worker_panic_wakes_real_pending_provider_path_stages() {
    use crate::tests::internal::random_access_session_tests::Fixture;
    use crate::tests::internal::random_access_session_tests::TestAsyncReadGate;

    struct ReleaseOnDrop(Arc<TestAsyncReadGate>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    for phase in ["open", "stat", "read_all"] {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let service = manager.group(GroupOptions { capacity: 2 }).unwrap();
        let (engine_started_tx, engine_started_rx) = mpsc::sync_channel(1);
        let (panic_tx, panic_rx) = mpsc::sync_channel(1);
        let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
            manager.clone(),
            qubit_mime::MimeConfig::default().max_buffer_size(),
            Box::new(move |_| {
                engine_started_tx.send(()).unwrap();
                panic_rx.recv().unwrap();
                panic!("injected inference worker panic");
            }),
            Box::new(|| {}),
        ));
        let first = runtime.submit(None, WorkerInput::Bytes(vec![b'a'].into())).unwrap();
        engine_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();

        let gate = Arc::new(TestAsyncReadGate::new());
        let _release_gate = ReleaseOnDrop(Arc::clone(&gate));
        let fixture = match phase {
            "open" => Fixture::new(b"sample", qubit_fs::metadata::FileSystemLimit::Maximum(6))
                .with_async_open_gate(Arc::clone(&gate)),
            "stat" => Fixture::new(b"sample", qubit_fs::metadata::FileSystemLimit::Maximum(6))
                .without_async_random_access()
                .with_async_stat_gate(Arc::clone(&gate)),
            "read_all" => Fixture::new(b"sample", qubit_fs::metadata::FileSystemLimit::Maximum(6))
                .without_async_random_access()
                .with_async_stream_read_gate(Arc::clone(&gate)),
            _ => unreachable!("test stage is enumerated above"),
        };
        let file_system = qubit_fs::AsyncFileSystem::from_spi(fixture).unwrap();
        let detector = Arc::new(
            crate::MagikaMimeDetector::builder()
                .managed_runtime(Arc::clone(&runtime))
                .build()
                .unwrap(),
        );
        let (provider_result_tx, provider_result_rx) = mpsc::sync_channel(1);
        let provider_detector = Arc::clone(&detector);
        let mut provider = service
            .as_async()
            .try_submit(OperationOptions::default(), move |scope| async move {
                let result = provider_detector
                    .detect_async_provider_path_managed(
                        &scope,
                        &file_system,
                        &qubit_fs::Path::parse("/worker-fault/pending-stage").unwrap(),
                        6,
                    )
                    .await;
                provider_result_tx
                    .send(result.map(|_| ()).map_err(|error| error.to_string()))
                    .unwrap();
                Ok::<_, MimeError>(())
            })
            .unwrap();

        gate.wait_until_started(Duration::from_secs(2));
        let provider_state = runtime
            .tasks
            .lock()
            .unwrap()
            .iter()
            .find(|state| !state.was_dispatched())
            .cloned()
            .expect("pending provider request is registered before dispatch");
        panic_tx.send(()).unwrap();
        let provider_error = provider_result_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("worker fault must promptly wake provider I/O")
            .unwrap_err();
        assert!(
            provider_error.contains("after worker fault"),
            "pending {phase} path should expose the worker fault, got: {provider_error}"
        );
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !provider_state.inner.lock().unwrap().unconfirmed_provider_io {
            assert!(
                std::time::Instant::now() < deadline,
                "pending {phase} provider I/O did not retain its uncertain state"
            );
            std::thread::yield_now();
        }
        assert_eq!(
            block_on(first).unwrap_err().to_string(),
            "MIME detector backend 'magika' failed: inference worker panicked",
            "the worker panic request keeps its original MIME error"
        );
        let outcome = block_on(provider.wait_outcome()).unwrap();
        assert!(matches!(outcome, OperationOutcome::Succeeded(())));
        let settlement = block_on(provider.wait_settlement());
        assert!(
            !settlement.all_released(),
            "pending {phase} I/O must remain visible as a negative settlement"
        );
        assert!(runtime.snapshot().faulted);
    }
}

#[test]
fn test_drain_timeout_can_be_observed_again_by_multiple_shutdown_callers() {
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    let release_rx = Arc::new(Mutex::new(release_rx));
    let engine_release = Arc::clone(&release_rx);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        OperationManager::new(ManagerOptions::default()).unwrap(),
        8,
        Box::new(move |_| {
            started_tx.send(()).unwrap();
            engine_release.lock().unwrap().recv().unwrap();
            Ok(None)
        }),
        Box::new(|| {}),
    ));
    let inference = runtime.submit(None, WorkerInput::Bytes(vec![b'x'].into())).unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let first = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_millis(1));
    assert!(!first.worker_stopped);
    assert_eq!(0, first.admission.snapshot.active_operations);
    assert_eq!(1, first.admission.snapshot.service_resources);

    release_tx.send(()).unwrap();
    assert_eq!(block_on(inference).unwrap(), None);
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let first_runtime = Arc::clone(&runtime);
    let first_barrier = Arc::clone(&barrier);
    let first_shutdown = std::thread::spawn(move || {
        first_barrier.wait();
        first_runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2))
    });
    let second_runtime = Arc::clone(&runtime);
    let second_barrier = Arc::clone(&barrier);
    let second_shutdown = std::thread::spawn(move || {
        second_barrier.wait();
        second_runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2))
    });
    barrier.wait();
    assert!(first_shutdown.join().unwrap().worker_stopped);
    assert!(second_shutdown.join().unwrap().worker_stopped);
}

#[test]
fn test_worker_panic_faults_runtime_and_settles_queued_bridge() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (panic_tx, panic_rx) = mpsc::channel();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(move |_| {
            entered_tx.send(()).unwrap();
            panic_rx.recv().unwrap();
            panic!("injected worker panic")
        }),
        Box::new(|| {}),
    ));

    let _current = runtime.submit(None, WorkerInput::Bytes(vec![1].into())).unwrap();
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let bridge = crate::internal::async_sample_bridge::AsyncSampleBridge::new(4);
    let _queued = runtime
        .submit(
            None,
            WorkerInput::Bridge(Arc::clone(&bridge), 4, map_provider_test_error),
        )
        .unwrap();
    let (read_started_tx, read_started_rx) = mpsc::channel();
    let waiting_bridge = Arc::clone(&bridge);
    let bridge_waiter = std::thread::spawn(move || {
        read_started_tx.send(()).unwrap();
        waiting_bridge.read_at(0, &mut [0; 1])
    });
    read_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let states = runtime.tasks.lock().unwrap().clone();
    assert_eq!(states.len(), 2);

    panic_tx.send(()).unwrap();
    assert!(states[0].wait_worker_done_blocking(Duration::from_secs(2)));
    assert!(states[1].wait_worker_done_blocking(Duration::from_secs(2)));
    assert!(states[0].wait_released_blocking(Duration::from_secs(2)));
    assert!(
        !states[1].inner.lock().unwrap().released,
        "pending bridge driver still owns resources"
    );
    assert_eq!(
        states[0].inner.lock().unwrap().business,
        Some(ChildOutcomeKind::Panicked)
    );
    assert_eq!(states[1].inner.lock().unwrap().business, Some(ChildOutcomeKind::Failed));
    assert_eq!(
        states[1]
            .inner
            .lock()
            .unwrap()
            .result
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .to_string(),
        "MIME detector backend 'magika' failed: inference session discarded after worker panic"
    );
    assert!(
        bridge_waiter.join().unwrap().is_err(),
        "queued bridge waiter is woken by closure"
    );
    assert!(runtime.reserve_for_provider(None, 1).is_err());
    assert!(runtime.byte_budget.try_acquire(5).is_none());
    assert!(runtime.snapshot().faulted);

    // Only actual provider-driver completion releases a queued bridge's resources.
    states[1].mark_driver_done();
    assert!(states[1].wait_released_blocking(Duration::from_secs(2)));
    assert!(runtime.byte_budget.try_acquire(8).is_some());
    let first = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    let second = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(2));
    assert!(first.worker_stopped);
    assert!(second.worker_stopped);
    let after_shutdown = runtime.snapshot();
    assert!(after_shutdown.faulted);
    assert!(runtime.snapshot().faulted, "fault diagnosis remains sticky");
    assert!(first.worker_stopped);
    assert!(!first.worker_panicked);
}

#[test]
fn test_unmanaged_detection_enforces_limit_and_shutdown_joins_worker() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    assert!(ManagedInferenceRuntime::new(supervisor.clone(), 0).is_err());

    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(|bytes| Ok(Some(format!("application/x-{}", bytes.len())))),
        Box::new(|| {}),
    ));
    assert!(format!("{runtime:?}").contains("ManagedInferenceRuntime"));
    assert_eq!(runtime.max_in_flight_bytes(), 8);
    assert!(matches!(
        block_on(runtime.detect_bytes_unmanaged(vec![0; 9])),
        Err(MimeError::BufferLimitExceeded { .. })
    ));
    assert_eq!(
        Some("application/x-3".to_owned()),
        block_on(runtime.detect_bytes_unmanaged(vec![1, 2, 3])).unwrap()
    );

    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    assert!(report.admission.snapshot.active_operations == 0);
    assert!(report.worker_stopped);
    assert!(!report.worker_panicked);
}

#[test]
fn test_runtime_submit_rejects_aggregate_byte_budget_overflow() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let engine_gate = Arc::clone(&gate);
    let (started_tx, started_rx) = mpsc::channel();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(move |_| {
            started_tx.send(()).unwrap();
            let (lock, ready) = &*engine_gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            Ok(None)
        }),
        Box::new(|| {}),
    ));
    let poisoned_tasks = Arc::clone(&runtime);
    let _ = std::thread::spawn(move || {
        let _guard = poisoned_tasks.tasks.lock().unwrap();
        panic!("poison runtime task list before submission");
    })
    .join();
    let poisoned_sender = Arc::clone(&runtime);
    let _ = std::thread::spawn(move || {
        let _guard = poisoned_sender.sender.lock().unwrap();
        panic!("poison runtime sender before submission");
    })
    .join();
    let first = runtime
        .submit(None, WorkerInput::Bytes(vec![0; 5].into()))
        .expect("first job should fit the byte budget");
    started_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(runtime.submit(None, WorkerInput::Bytes(vec![0; 5].into())).is_err());

    let (lock, ready) = &*gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    assert!(matches!(first.wait_blocking(), Ok(None)));
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(30));
    assert!(report.worker_stopped);
}

#[test]
fn test_shutdown_recovers_poisoned_runtime_mutexes() {
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        OperationManager::new(ManagerOptions::default()).unwrap(),
        1,
        Box::new(|_| Ok(None)),
        Box::new(|| {}),
    ));
    let poisoned_tasks = Arc::clone(&runtime);
    let _ = std::thread::spawn(move || {
        let _guard = poisoned_tasks.tasks.lock().unwrap();
        panic!("poison runtime task list for recovery test");
    })
    .join();
    let poisoned_sender = Arc::clone(&runtime);
    let _ = std::thread::spawn(move || {
        let _guard = poisoned_sender.sender.lock().unwrap();
        panic!("poison runtime sender for recovery test");
    })
    .join();
    let poisoned_worker = Arc::clone(&runtime);
    let _ = std::thread::spawn(move || {
        let _guard = poisoned_worker.worker.lock().unwrap();
        panic!("poison runtime worker for recovery test");
    })
    .join();

    let report = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(1));
    assert!(report.admission.snapshot.active_operations == 0);
    assert!(report.worker_stopped);
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    loop {
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[test]
fn test_first_receive_gate_delivers_two_admitted_jobs_and_rejects_third() {
    struct WorkerGateRelease(Arc<(Mutex<bool>, Condvar)>);
    impl WorkerGateRelease {
        fn open(&self) {
            let (lock, ready) = &*self.0;
            *lock.lock().unwrap() = true;
            ready.notify_all();
        }
    }
    impl Drop for WorkerGateRelease {
        fn drop(&mut self) {
            self.open();
        }
    }

    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let release_worker = WorkerGateRelease(Arc::clone(&gate));
    let worker_gate = Arc::clone(&gate);
    let (entered_tx, entered_rx) = mpsc::channel();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine_before_first_recv(
        supervisor,
        16,
        Box::new(|_| Ok(Some("application/x-gated".to_owned()))),
        Box::new(move || {
            entered_tx.send(()).unwrap();
            let (lock, ready) = &*worker_gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = ready.wait(open).unwrap();
            }
        }),
    ));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let first = runtime.reserve_request(None, 3).unwrap();
    let first_state = Arc::clone(&first.state);
    let first = runtime
        .dispatch(first, WorkerInput::Bytes(vec![b'a'; 3].into()))
        .unwrap();
    let second = runtime.reserve_request(None, 4).unwrap();
    let second_state = Arc::clone(&second.state);
    let second = runtime
        .dispatch(second, WorkerInput::Bytes(vec![b'b'; 4].into()))
        .unwrap();
    let third_admission = runtime.reserve_request(None, 1);
    let reserved_while_worker_is_gated = runtime.snapshot().reserved_bytes;
    release_worker.open();
    assert!(third_admission.is_err());
    assert_eq!(7, reserved_while_worker_is_gated);

    assert_eq!(Some("application/x-gated".to_owned()), block_on(first).unwrap());
    assert_eq!(Some("application/x-gated".to_owned()), block_on(second).unwrap());
    assert!(first_state.wait_released_blocking(Duration::from_secs(2)));
    assert!(second_state.wait_released_blocking(Duration::from_secs(2)));
    assert_eq!(
        Some(qubit_operation::ChildOutcomeKind::Succeeded),
        first_state.inner.lock().unwrap().business
    );
    assert_eq!(
        Some(qubit_operation::ChildOutcomeKind::Succeeded),
        second_state.inner.lock().unwrap().business
    );
    assert_eq!(0, runtime.snapshot().reserved_bytes);
    assert_eq!(0, runtime.active_requests.load(std::sync::atomic::Ordering::Acquire));
    assert!(
        runtime
            .shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2))
            .worker_stopped
    );
}

#[test]
fn test_receiver_disconnect_settles_undelivered_dispatch() {
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine_before_first_recv(
        OperationManager::new(ManagerOptions::default()).unwrap(),
        16,
        Box::new(|_| Ok(None)),
        Box::new(|| panic!("test worker exits before receiving")),
    ));
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !runtime.worker.lock().unwrap().handle.as_ref().unwrap().is_finished() {
        assert!(std::time::Instant::now() < deadline, "test worker did not exit");
        std::thread::yield_now();
    }
    let admission = runtime.reserve_request(None, 4).unwrap();
    let state = Arc::clone(&admission.state);
    let bridge = crate::internal::async_sample_bridge::AsyncSampleBridge::new(4);
    let future = runtime
        .dispatch(
            admission,
            WorkerInput::Bridge(Arc::clone(&bridge), 4, map_provider_test_error),
        )
        .unwrap();

    assert!(block_on(bridge.next_request()).is_none());
    let error = block_on(future).unwrap_err();
    assert!(error.to_string().contains("disconnected"));
    let inner = state.inner.lock().unwrap();
    assert_eq!(Some(qubit_operation::ChildOutcomeKind::Failed), inner.business);
    assert!(inner.released);
    drop(inner);
    assert_eq!(0, runtime.snapshot().reserved_bytes);
    assert_eq!(0, runtime.snapshot().admitted_not_started);
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
    assert!(report.worker_panicked);
}

#[test]
fn test_shutdown_timeout_observes_joinable_worker_while_admission_is_held() {
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine_before_first_recv(
        OperationManager::new(ManagerOptions::default()).unwrap(),
        8,
        Box::new(|_| Ok(None)),
        Box::new(|| panic!("injected worker exit before first receive")),
    ));
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !runtime.worker.lock().unwrap().handle.as_ref().unwrap().is_finished() {
        assert!(std::time::Instant::now() < deadline, "worker did not exit");
        std::thread::yield_now();
    }

    let admission = runtime.reserve_request(None, 4).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let first_runtime = Arc::clone(&runtime);
    let first_barrier = Arc::clone(&barrier);
    let first_shutdown = std::thread::spawn(move || {
        first_barrier.wait();
        first_runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_millis(50))
    });
    let second_runtime = Arc::clone(&runtime);
    let second_barrier = Arc::clone(&barrier);
    let second_shutdown = std::thread::spawn(move || {
        second_barrier.wait();
        second_runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_millis(50))
    });
    barrier.wait();
    let first = first_shutdown.join().unwrap();
    let second = second_shutdown.join().unwrap();

    assert_eq!(1, first.admission.snapshot.service_resources);
    assert_eq!(1, second.admission.snapshot.service_resources);
    assert_eq!(4, runtime.snapshot().reserved_bytes);
    assert!(runtime.byte_budget.try_acquire(8).is_none());
    assert!(
        first.worker_stopped,
        "first timed-out shutdown must reap the exited worker"
    );
    assert!(first.worker_panicked, "join must preserve the escaped panic fact");
    assert!(
        second.worker_stopped,
        "concurrent shutdown must observe the completed join"
    );
    assert!(second.worker_panicked, "concurrent shutdown must retain the panic fact");

    drop(admission);
    let resumed = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    assert!(resumed.worker_stopped);
    assert!(resumed.worker_panicked);
    assert_eq!(0, resumed.admission.snapshot.service_resources);
    assert_eq!(0, runtime.snapshot().reserved_bytes);
}
