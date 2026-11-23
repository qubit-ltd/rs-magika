// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::io::Cursor;
use std::path::Path as StdPath;
use std::sync::Arc;

use qubit_fs::Path;
use qubit_mime::MimeContentBackend;
use qubit_mime::MimeDetectorBackend;
use qubit_mime::MimeError;
use qubit_operation::GroupOptions;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ShutdownMode;

use crate::MagikaMimeDetector;
use crate::tests::internal::provider_input_fixture::ProviderFileSystemSpi;
use crate::tests::managed_operation_tests::block_on;

const PYTHON: &[u8] = include_bytes!("../../tests/fixtures/real_files/script.py");

#[test]
fn test_public_async_content_and_runtime_snapshot() {
    let detector = MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize Magika");
    let before = detector.runtime_snapshot();
    assert_eq!(0, before.admitted_not_started);
    assert_eq!(0, before.worker_running);

    let candidates = block_on(MimeDetectorBackend::guess_from_content_async(&detector, PYTHON))
        .expect("async content hook should detect the fixture");

    assert_eq!(vec!["text/x-python"], candidates);
    assert_eq!(
        vec!["text/x-python"],
        MimeDetectorBackend::guess_from_content(&detector, PYTHON).expect("sync content hook should detect fixture")
    );
    let mut reader = Cursor::new(PYTHON);
    assert_eq!(
        vec!["text/x-python"],
        MimeDetectorBackend::guess_from_reader(&detector, &mut reader)
            .expect("reader hook should detect fixture")
            .0
    );
    assert_eq!(0, reader.position(), "reader hook should restore its position");
    assert_eq!(
        vec!["text/x-python"],
        MimeDetectorBackend::guess_from_file(&detector, StdPath::new("tests/fixtures/real_files/script.py"))
            .expect("file hook should detect fixture")
            .0
    );
    let after = detector.runtime_snapshot();
    assert_eq!(0, after.admitted_not_started);
    assert_eq!(0, after.worker_running);
    let report = detector.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(5));
    assert!(report.worker_stopped);
}

#[test]
fn test_public_sync_provider_path_hook_uses_random_access_session() {
    let detector = MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize Magika");
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_range();
    let observations = provider.observations();
    let file_system = provider.file_system();
    let path = Path::parse("/fixture.py").expect("fixture path should parse");

    let (candidates, additional_content) =
        MimeDetectorBackend::guess_from_provider_path(&detector, &file_system, &path, 4096)
            .expect("sync provider path hook should detect the fixture");

    assert_eq!(vec!["text/x-python"], candidates);
    assert!(additional_content.is_none());
    assert!(observations.opens.load(std::sync::atomic::Ordering::SeqCst) > 0);
    let report = detector.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(5));
    assert!(report.worker_stopped);
}

#[test]
fn test_public_async_provider_path_hook_completes_operation_child() {
    let detector = MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize Magika");
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_range();
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let path = Path::parse("/fixture.py").expect("fixture path should parse");

    let (candidates, additional_content) = block_on(MimeDetectorBackend::guess_from_async_provider_path(
        &detector,
        &file_system,
        &path,
        4096,
    ))
    .expect("async provider path hook should detect the fixture");

    assert_eq!(vec!["text/x-python"], candidates);
    assert!(additional_content.is_none());
    assert!(observations.opens.load(std::sync::atomic::Ordering::SeqCst) > 0);
    let report = detector.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(5));
    assert!(report.worker_stopped);
}

#[test]
fn test_public_sync_provider_path_reports_conditional_open_failure() {
    let detector = MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize Magika");
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_conditional()
        .fail_conditional_on_open(0);
    let observations = provider.observations();
    let file_system = provider.file_system();
    let path = Path::parse("/fixture.py").expect("fixture path should parse");

    let error = MimeDetectorBackend::guess_from_provider_path(&detector, &file_system, &path, 4096)
        .expect_err("a failed conditional open should be reported");
    assert!(error.to_string().contains("configured conditional read failure"));
    assert!(observations.opens.load(std::sync::atomic::Ordering::SeqCst) > 0);
    assert!(
        detector
            .shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(5))
            .worker_stopped
    );
}

#[test]
fn test_public_async_provider_path_reports_conditional_open_failure() {
    let detector = MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize Magika");
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec())
        .with_conditional()
        .fail_conditional_on_open(0);
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let path = Path::parse("/fixture.py").expect("fixture path should parse");

    let error = block_on(MimeDetectorBackend::guess_from_async_provider_path(
        &detector,
        &file_system,
        &path,
        4096,
    ))
    .expect_err("a failed conditional open should be reported");
    assert!(error.to_string().contains("configured conditional read failure"));
    assert!(observations.opens.load(std::sync::atomic::Ordering::SeqCst) > 0);
    assert!(
        detector
            .shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(5))
            .worker_stopped
    );
}

#[test]
fn test_public_managed_content_hook_runs_as_operation_child() {
    let detector = Arc::new(MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize Magika"));
    let manager = OperationManager::new(ManagerOptions::default()).expect("operation manager should construct");
    let group = manager
        .group(GroupOptions { capacity: 2 })
        .expect("operation group should construct");
    let request_detector = Arc::clone(&detector);
    let content = PYTHON.to_vec();
    let mut operation = group
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            request_detector.detect_bytes_managed(&scope, content).await
        })
        .expect("managed child should be admitted");

    let outcome = block_on(operation.wait_outcome()).expect("operation outcome should be delivered");
    assert!(
        matches!(outcome, OperationOutcome::Succeeded(Some(ref mime)) if mime == "text/x-python"),
        "managed content hook returned {outcome:?}"
    );
    let settlement = block_on(operation.wait_settlement());
    assert!(settlement.all_released());
    let report = detector.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(5));
    assert!(report.worker_stopped);
}

#[test]
fn test_public_managed_provider_path_hook_settles_operation_child() {
    let detector = Arc::new(MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize Magika"));
    let manager = OperationManager::new(ManagerOptions::default()).expect("operation manager should construct");
    let group = manager
        .group(GroupOptions { capacity: 2 })
        .expect("operation group should construct");
    let provider = ProviderFileSystemSpi::new(PYTHON.to_vec()).with_range();
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let path = Path::parse("/fixture.py").expect("fixture path should parse");
    let request_detector = Arc::clone(&detector);
    let mut operation = group
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            request_detector
                .detect_async_provider_path_managed(&scope, &file_system, &path, 4096)
                .await
        })
        .expect("managed provider child should be admitted");

    let outcome = block_on(operation.wait_outcome()).expect("operation outcome should be delivered");
    assert!(
        matches!(outcome, OperationOutcome::Succeeded(Some(ref mime)) if mime == "text/x-python"),
        "managed provider hook returned {outcome:?}"
    );
    assert!(observations.opens.load(std::sync::atomic::Ordering::SeqCst) > 0);
    assert!(block_on(operation.wait_settlement()).all_released());
    let report = detector.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(5));
    assert!(report.worker_stopped);
}

#[test]
fn test_public_builder_accepts_application_operation_manager() {
    let manager = OperationManager::new(ManagerOptions::default()).expect("operation manager should construct");
    let detector = MagikaMimeDetector::builder()
        .operation_manager(manager)
        .build()
        .expect("builder should construct a detector using the supplied manager");

    assert_eq!(
        vec!["text/x-python"],
        MimeDetectorBackend::guess_from_content(&detector, PYTHON).expect("fixture should classify")
    );
    assert!(
        detector
            .shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(5))
            .worker_stopped
    );
}

#[test]
fn test_public_remaining_reader_reports_window_before_start() {
    let detector = MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize Magika");
    let mut reader = Cursor::new(PYTHON);
    reader.set_position((PYTHON.len() + 1) as u64);

    let error = MimeContentBackend::detect_reader(&detector, &mut reader)
        .expect_err("a reader position beyond its end should be rejected");
    assert!(matches!(error, MimeError::Io(ref cause) if cause.kind() == std::io::ErrorKind::InvalidInput));
    assert_eq!(
        (PYTHON.len() + 1) as u64,
        reader.position(),
        "reader position should be restored"
    );
    assert!(
        detector
            .shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(5))
            .worker_stopped
    );
}
