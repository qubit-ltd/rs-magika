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
use qubit_operation::report::ChildBusinessOutcome;
use qubit_operation::report::StopReason;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ShutdownMode;

use super::super::worker_input::WorkerBytes;
use super::ManagedInferenceRuntime;
use super::WorkerInput;
use super::execute_classified;

fn map_provider_test_error(_: magika::Error) -> MimeError {
    MimeError::detector_backend("magika", "test input failed")
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
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
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
    assert!(runtime.reserve_for_provider(None, 0).is_ok());
    let _ = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(2));
}

#[test]
fn test_cancelled_queued_bytes_restore_byte_budget() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
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
    let queued = runtime.submit(None, WorkerInput::Bytes(vec![0; 4].into())).unwrap();
    let state = runtime.tasks.lock().unwrap()[1].clone();
    drop(queued);
    assert!(runtime.reserve_for_provider(None, 1).is_err());

    gate_cleanup.release();
    assert!(state.wait_worker_done_blocking(Duration::from_secs(2)));
    assert!(state.wait_released_blocking(Duration::from_secs(2)));
    assert!(runtime.reserve_for_provider(None, 8).is_ok());
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

    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8192,
        Box::new(|_| Ok(None)),
        Box::new(|| {}),
    ));
    let mapped_error = map_provider_test_error(magika::Error::IOError(std::io::Error::other("test mapping")));
    assert!(matches!(mapped_error, MimeError::DetectorBackend { .. }));
    let length_admission = runtime.reserve_for_provider(None, 8192).unwrap();
    let length_result = block_on(runtime.detect_async_input_admitted(
        length_admission,
        FailedLengthInput,
        8192,
        map_provider_test_error,
    ));
    assert!(matches!(length_result, Err(MimeError::DetectorBackend { .. })));

    let admission = runtime.reserve_for_provider(None, 8192).unwrap();
    let result =
        block_on(runtime.detect_async_input_admitted(admission, FailedReadInput, 8192, map_provider_test_error));

    assert!(matches!(result, Err(MimeError::Io(_))));
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.admission.is_complete());
    assert!(report.worker_stopped);
}

#[test]
fn test_shutdown_scan_cannot_miss_a_request_between_service_and_task_registration() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
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
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let service = supervisor.service_group(2).unwrap();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(|_| Ok(None)),
        Box::new(|| {}),
    ));

    let first_runtime = Arc::clone(&runtime);
    let mut first = service
        .start(
            qubit_operation::scope::ScopeOptions::default(),
            move |scope| async move {
                let mut admission = first_runtime.reserve_for_provider(Some(&scope), 8)?;
                scope.request_cancel(StopReason::UserCancelled);
                assert!(admission.begin_provider_operation().is_err());
                Ok::<_, MimeError>(())
            },
        )
        .unwrap();
    let _ = first.result_blocking();

    let later_runtime = Arc::clone(&runtime);
    let mut later = service
        .start(
            qubit_operation::scope::ScopeOptions::default(),
            move |scope| async move {
                let mut admission = later_runtime.reserve_for_provider(Some(&scope), 8)?;
                admission.begin_provider_operation()?;
                admission.provider_operation_completed();
                scope.request_cancel(StopReason::UserCancelled);
                assert!(admission.begin_provider_operation().is_err());
                Ok::<_, MimeError>(())
            },
        )
        .unwrap();
    let _ = later.result_blocking();

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

    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
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
            .detect_async_input_admitted(admission, input, 8, map_error)
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

    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
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
            .detect_async_input_admitted(admission, ReadyInput, 8, map_provider_test_error)
            .await
    });
    let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while worker_rx.try_recv().is_err() {
        assert!(std::time::Instant::now() < deadline, "worker did not reach its gate");
        assert!(future.as_mut().poll(&mut context).is_pending());
        std::thread::park_timeout(Duration::from_millis(1));
    }
    drop(future);
    gate_cleanup.release();
    let state = runtime.tasks.lock().unwrap().first().unwrap().clone();
    assert!(state.wait_worker_done_blocking(Duration::from_secs(2)));
    assert!(state.wait_released_blocking(Duration::from_secs(2)));
    assert!(runtime.reserve_for_provider(None, 8).is_ok());
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
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let (worker_tx, worker_rx) = mpsc::sync_channel(1);
    let worker_signaled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let callback_signaled = Arc::clone(&worker_signaled);
    let worker_gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let callback_gate = Arc::clone(&worker_gate);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        supervisor,
        16,
        Box::new(|_| Ok(None)),
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
    let first_admission = runtime.reserve_for_provider(None, 8).unwrap();
    let first_runtime = Arc::clone(&runtime);
    let mut first = Box::pin(async move { first_runtime.detect_bytes_admitted(first_admission, vec![b'a']).await });
    let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while worker_rx.try_recv().is_err() {
        assert!(
            std::time::Instant::now() < deadline,
            "first worker did not reach its gate"
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
            .detect_async_input_admitted(second_admission, CountedInput(second_reads), 8, map_provider_test_error)
            .await
    });
    assert!(second.as_mut().poll(&mut context).is_pending());
    assert_eq!(0, reads.load(std::sync::atomic::Ordering::SeqCst));
    let states = runtime.tasks.lock().unwrap().clone();
    assert_eq!(2, states.len());
    drop(second);
    assert!(runtime.reserve_for_provider(None, 1).is_err());

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
    assert!(runtime.reserve_for_provider(None, 8).is_ok());
    let success_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let success_admission = runtime.reserve_for_provider(None, 8).unwrap();
    let success_runtime = Arc::clone(&runtime);
    let success_input = Arc::clone(&success_reads);
    let mut success = Box::pin(async move {
        success_runtime
            .detect_async_input_admitted(
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
    assert!(report.admission.is_complete());
}

#[test]
fn test_admitted_owned_bytes_return_worker_result() {
    struct ThreadWake(std::thread::Thread);
    impl std::task::Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }

    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let runtime = ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(|bytes| Ok(Some(format!("application/x-bytes-{}", bytes.len())))),
        Box::new(|| {}),
    );
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
    assert_eq!(outcome, ChildBusinessOutcome::Succeeded);

    let (result, outcome): (qubit_mime::MimeResult<()>, _) = execute_classified(|| panic!("engine panic"));
    assert!(result.is_err());
    assert_eq!(outcome, ChildBusinessOutcome::Panicked);
}

#[test]
fn test_unmanaged_detection_enforces_limit_and_shutdown_joins_worker() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    assert!(ManagedInferenceRuntime::new(supervisor.clone(), 0).is_err());

    let runtime = ManagedInferenceRuntime::with_engine(
        supervisor,
        8,
        Box::new(|bytes| Ok(Some(format!("application/x-{}", bytes.len())))),
        Box::new(|| {}),
    );
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
    assert!(report.admission.is_complete());
    assert!(report.worker_stopped);
    assert!(!report.worker_panicked);
}

#[test]
fn test_runtime_submit_rejects_aggregate_byte_budget_overflow() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
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
        OperationSupervisor::new(SupervisorLimits::default()),
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
    assert!(report.admission.is_complete());
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
