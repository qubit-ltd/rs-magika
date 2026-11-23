// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public operation integration for the session-owning Magika runtime.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use magika::SyncInput;
use qubit_mime::MimeError;
use qubit_operation::GroupOptions;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ShutdownMode;
use qubit_operation::StopReason;

use crate::managed_runtime::ManagedInferenceRuntime;

struct ThreadWaker(std::thread::Thread);
impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = std::task::Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

#[test]
fn test_public_child_result_precedes_worker_resource_release() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let worker_gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let worker_gate_task = Arc::clone(&worker_gate);
    let started = std::sync::mpsc::channel();
    let started_tx = started.0;
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        manager.clone(),
        16,
        Box::new(move |_| {
            started_tx.send(()).unwrap();
            Ok(Some("application/x-lifecycle".to_owned()))
        }),
        Box::new(move || {
            let (lock, ready) = &*worker_gate_task;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
        }),
    ));
    let request_runtime = Arc::clone(&runtime);
    let mut operation = group
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            request_runtime.detect_bytes(&scope, b"sample".to_vec()).await
        })
        .unwrap();
    started.1.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(matches!(
        block_on(operation.wait_outcome()).unwrap(),
        OperationOutcome::Succeeded(Some(ref mime)) if mime == "application/x-lifecycle"
    ));
    assert!(group.snapshot().active_operations > 0);

    let (lock, ready) = &*worker_gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    let _settlement = block_on(operation.wait_settlement());
    assert_eq!(0, group.snapshot().active_operations);
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
}

#[test]
fn test_public_stop_returns_outcome_while_native_worker_retains_resources() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let entered = std::sync::mpsc::channel();
    let entered_tx = entered.0;
    let worker_gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let worker_gate_task = Arc::clone(&worker_gate);
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        manager.clone(),
        16,
        Box::new(move |_| {
            entered_tx.send(()).unwrap();
            let (lock, ready) = &*worker_gate_task;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            Ok(None)
        }),
        Box::new(|| {}),
    ));
    let request_runtime = Arc::clone(&runtime);
    let mut operation = group
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            request_runtime.detect_bytes(&scope, b"sample".to_vec()).await
        })
        .unwrap();
    entered.1.recv_timeout(Duration::from_secs(2)).unwrap();
    let _ = operation.request_stop(StopReason::UserCancelled);
    assert!(matches!(
        block_on(operation.wait_outcome()).unwrap(),
        OperationOutcome::Stopped(_)
    ));
    assert!(group.snapshot().active_operations > 0);

    let report = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_millis(20));
    assert!(!report.worker_stopped);
    let (lock, ready) = &*worker_gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    let _settlement = block_on(operation.wait_settlement());
    let report = runtime.shutdown_blocking(ShutdownMode::Cancel, Duration::from_secs(2));
    assert!(report.worker_stopped);
}

#[test]
fn test_runtime_group_shutdown_does_not_close_the_application_manager() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let runtime = Arc::new(ManagedInferenceRuntime::new(manager.clone(), 16).unwrap());
    let independent_group = manager.group(GroupOptions { capacity: 1 }).unwrap();

    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
    let mut handle = independent_group
        .try_submit(OperationOptions::default(), |_| Ok::<_, MimeError>(()))
        .unwrap();
    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
}

struct ProviderBytes(Vec<u8>);

impl SyncInput for ProviderBytes {
    fn length(&self) -> magika::Result<u64> {
        Ok(self.0.len() as u64)
    }

    fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> magika::Result<()> {
        let start = usize::try_from(offset).unwrap();
        let length = buffer.len();
        buffer.copy_from_slice(&self.0[start..start + length]);
        Ok(())
    }
}

fn map_provider_error(_: magika::Error) -> MimeError {
    MimeError::detector_backend("magika", "provider failed")
}

#[test]
fn test_public_managed_provider_worker_panic_reports_failure_and_settlement() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let panic_gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let engine_gate = Arc::clone(&panic_gate);
    let entered = std::sync::mpsc::channel();
    let entered_tx = entered.0;
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        manager,
        128,
        Box::new(move |_| {
            entered_tx.send(()).unwrap();
            let (lock, ready) = &*engine_gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = ready.wait(open).unwrap();
            }
            panic!("inference engine panic")
        }),
        Box::new(|| {}),
    ));
    let request_runtime = Arc::clone(&runtime);
    let mut operation = group
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            let admission = request_runtime.reserve_for_provider(Some(&scope), 64)?;
            request_runtime
                .detect_async_input_admitted_test(
                    admission,
                    ProviderBytes(b"provider content".to_vec()),
                    64,
                    map_provider_error,
                )
                .await
        })
        .unwrap();

    entered
        .1
        .recv_timeout(Duration::from_secs(2))
        .expect("provider request should reach inference");
    let queued_admission = runtime.reserve_for_provider(None, 6).unwrap();
    let mut queued = pin!(runtime.detect_bytes_admitted(queued_admission, b"queued".to_vec()));
    let mut context = Context::from_waker(Waker::noop());
    assert!(queued.as_mut().poll(&mut context).is_pending());
    assert_eq!(1, runtime.snapshot().admitted_not_started);
    let (lock, ready) = &*panic_gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();

    assert!(matches!(
        block_on(operation.wait_outcome()).unwrap(),
        OperationOutcome::Failed(_)
    ));
    let queued_error = block_on(queued).unwrap_err();
    assert!(queued_error.to_string().contains("worker panic"));
    let settlement = block_on(operation.wait_settlement());
    assert!(settlement.all_released());
    let snapshot = runtime.snapshot();
    assert!(snapshot.faulted);
    assert_eq!(0, snapshot.reserved_bytes);
    assert_eq!(0, snapshot.unconfirmed_provider_io);
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
    assert!(!report.worker_panicked);
}

#[test]
fn test_public_managed_bytes_worker_panic_is_mime_failure() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let runtime = Arc::new(ManagedInferenceRuntime::with_engine(
        manager,
        64,
        Box::new(|_| panic!("inference engine panic")),
        Box::new(|| {}),
    ));
    let request_runtime = Arc::clone(&runtime);
    let mut operation = group
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            request_runtime
                .detect_bytes(&scope, b"sample".to_vec())
                .await
        })
        .unwrap();

    assert!(matches!(
        block_on(operation.wait_outcome()).unwrap(),
        OperationOutcome::Failed(_)
    ));
    assert!(block_on(operation.wait_settlement()).all_released());
    assert!(runtime.snapshot().faulted);
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(2));
    assert!(report.worker_stopped);
    assert!(!report.worker_panicked);
}
