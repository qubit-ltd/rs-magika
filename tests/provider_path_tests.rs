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
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;

use qubit_fs::Path;
use qubit_fs_local::LocalFileSystems;
use qubit_fs_local::LocalResourcePolicy;
use qubit_mime::MimeDetectionPolicy;
use qubit_mime::MimeDetector;
use qubit_mime::MimeError;

use crate::support::ProviderFileSystemSpi;
use crate::support::detector;

const PYTHON: &[u8] = b"#!/usr/bin/env python3\nprint('hello')\n";

fn path(value: &str) -> Path {
    Path::parse(value).expect("test path should be valid")
}

/// Polls a future until it completes, yielding the thread between pending
/// polls.
fn run_ready<F: Future>(future: F) -> F::Output {
    fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    fn wake(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake, |_: *const ()| {});
    let waker = unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) };
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match Future::poll(Pin::as_mut(&mut future), &mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
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
