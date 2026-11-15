// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fs;
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::ExitStatus;
use std::time::Duration;
use std::time::Instant;

use qubit_magika::MagikaMimeDetector;
use qubit_mime::MimeDetectionPolicy;
use qubit_mime::MimeDetector;
use qubit_mime::MimeDetectorBackend;
use qubit_mime::MimeError;
use tempfile::tempdir;

const FIFO_PATH: &str = "QUBIT_MAGIKA_FIFO_CHILD_PATH";
const READY_PATH: &str = "QUBIT_MAGIKA_FIFO_CHILD_READY";
const MODE: &str = "QUBIT_MAGIKA_FIFO_CHILD_MODE";

#[test]
fn test_fifo_content_child() {
    let (Some(path), Some(ready), Some(mode)) = (
        std::env::var_os(FIFO_PATH),
        std::env::var_os(READY_PATH),
        std::env::var_os(MODE),
    ) else {
        return;
    };
    let detector = MagikaMimeDetector::new().expect("detector should initialize");
    let ready = PathBuf::from(ready);
    let mut pending_ready = ready.clone();
    pending_ready.set_extension("ready.tmp");
    fs::write(&pending_ready, b"ready").expect("pending ready marker should be written");
    fs::rename(&pending_ready, &ready).expect("ready marker should be published atomically");
    let path = PathBuf::from(path);
    let error = match mode.to_str().expect("mode should be UTF-8") {
        "backend" => MimeDetectorBackend::guess_from_file(&detector, &path)
            .expect_err("FIFO content detection should be rejected"),
        "high_level" => detector
            .detect_file(&path, MimeDetectionPolicy::VerifyContent)
            .expect_err("FIFO content detection should be rejected"),
        other => panic!("unknown child mode: {other}"),
    };
    assert!(matches!(error, MimeError::Io(ref error) if error.kind() == std::io::ErrorKind::Unsupported));
}

struct ChildGuard(Child);

impl ChildGuard {
    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.0.try_wait()
    }

    fn kill_and_wait(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.kill_and_wait();
    }
}

#[test]
fn test_fifo_content_rejected_without_writer() {
    let directory = tempdir().expect("temporary directory should be created");
    let fifo = directory.path().join("content.fifo");
    let status = Command::new("mkfifo").arg(&fifo).status().expect("mkfifo should run");
    assert!(status.success(), "mkfifo failed with {status}");

    for mode in ["backend", "high_level"] {
        let ready = directory.path().join(format!("{mode}.ready"));
        let child = Command::new(std::env::current_exe().expect("test executable path"))
            .args([
                "--exact",
                "local_file_type_tests::test_fifo_content_child",
                "--nocapture",
            ])
            .env(FIFO_PATH, &fifo)
            .env(READY_PATH, &ready)
            .env(MODE, mode)
            .spawn()
            .expect("child process should start");
        let mut child = ChildGuard(child);
        let init_deadline = Instant::now() + Duration::from_secs(60);
        let mut ready_seen = false;
        let mut init_exit = None;
        while Instant::now() < init_deadline {
            if ready.exists() {
                assert_eq!(fs::read(&ready).expect("readiness marker should be readable"), b"ready");
                ready_seen = true;
                break;
            }
            init_exit = child.try_wait().expect("child status should be available");
            if init_exit.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            ready_seen,
            "child did not initialize and write readiness marker; exit: {init_exit:?}"
        );

        let content_deadline = Instant::now() + Duration::from_secs(5);
        let mut timed_out = false;
        let status = loop {
            if let Some(status) = child.try_wait().expect("child status should be available") {
                break Some(status);
            }
            if Instant::now() >= content_deadline {
                timed_out = true;
                child.kill_and_wait();
                break None;
            }
            std::thread::sleep(Duration::from_millis(20));
        };
        assert!(!timed_out, "{mode} FIFO detection hung after readiness");
        assert!(status.expect("child should exit").success(), "{mode} child should pass");
    }
}

#[test]
fn test_socket_content_is_rejected() {
    let directory = tempdir().expect("temporary directory should be created");
    let socket = directory.path().join("content.sock");
    let _listener = UnixListener::bind(&socket).expect("Unix socket should bind");
    assert_unsupported(MimeDetectorBackend::guess_from_file(crate::support::detector(), &socket).unwrap_err());
}

#[test]
fn test_symlink_to_fifo_is_not_followed() {
    let directory = tempdir().expect("temporary directory should be created");
    let fifo = directory.path().join("content.fifo");
    let status = Command::new("mkfifo").arg(&fifo).status().expect("mkfifo should run");
    assert!(status.success(), "mkfifo failed with {status}");
    let link = directory.path().join("content-link");
    symlink(&fifo, &link).expect("symlink should be created");
    let candidates = MimeDetectorBackend::guess_from_file(crate::support::detector(), &link)
        .expect("symlink metadata should be handled without following target");
    assert!(candidates.0.is_empty());
}

fn assert_unsupported(error: MimeError) {
    assert!(matches!(error, MimeError::Io(ref error) if error.kind() == std::io::ErrorKind::Unsupported));
}
