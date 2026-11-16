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
use qubit_operation::ShutdownMode;
use qubit_operation::StopReason;

use super::super::worker_input::WorkerBytes;
use super::ManagedInferenceRuntime;
use super::WorkerInput;
use super::worker_loop::execute_classified;

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

    // Only actual provider-driver completion releases a queued bridge's resources.
    states[1].mark_driver_done();
    assert!(states[1].wait_released_blocking(Duration::from_secs(2)));
    assert!(runtime.byte_budget.try_acquire(8).is_some());
    let first = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    let second = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(2));
    assert!(first.worker_stopped);
    assert!(second.worker_stopped);
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
