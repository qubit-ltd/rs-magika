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
use std::os::unix::process::ExitStatusExt;
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

fn observe_ready_or_exit(
    ready: &mut impl FnMut() -> bool,
    child_exit: &mut impl FnMut() -> Option<ExitStatus>,
) -> (bool, Option<ExitStatus>) {
    if ready() {
        return (true, None);
    }
    let status = child_exit();
    if status.is_some() {
        (ready(), status)
    } else {
        (false, None)
    }
}

#[test]
fn test_readiness_observation_exit_after_marker_check() {
    let ready_results = [false, true];
    let mut ready_calls = 0;
    let mut ready = || {
        let result = ready_results[ready_calls];
        ready_calls += 1;
        result
    };
    let mut exit_calls = 0;
    let mut child_exit = || {
        exit_calls += 1;
        Some(ExitStatus::from_raw(0))
    };

    let (ready_seen, status) = observe_ready_or_exit(&mut ready, &mut child_exit);

    assert!(ready_seen);
    assert!(status.expect("exit status should be preserved").success());
    assert_eq!(ready_calls, 2);
    assert_eq!(exit_calls, 1);
}

#[test]
fn test_readiness_observation_ready_without_exit_check() {
    let mut ready_calls = 0;
    let mut ready = || {
        ready_calls += 1;
        true
    };
    let mut child_exit = || panic!("exit should not be checked after readiness");

    let (ready_seen, status) = observe_ready_or_exit(&mut ready, &mut child_exit);

    assert!(ready_seen);
    assert!(status.is_none());
    assert_eq!(ready_calls, 1);
}

#[test]
fn test_readiness_observation_child_still_running() {
    let ready_results = [false];
    let mut ready_calls = 0;
    let mut ready = || {
        let result = ready_results[ready_calls];
        ready_calls += 1;
        result
    };
    let mut exit_calls = 0;
    let mut child_exit = || {
        exit_calls += 1;
        None
    };

    let (ready_seen, status) = observe_ready_or_exit(&mut ready, &mut child_exit);

    assert!(!ready_seen);
    assert!(status.is_none());
    assert_eq!(ready_calls, 1);
    assert_eq!(exit_calls, 1);
}

#[test]
fn test_readiness_observation_exit_without_marker() {
    let ready_results = [false, false];
    let mut ready_calls = 0;
    let mut ready = || {
        let result = ready_results[ready_calls];
        ready_calls += 1;
        result
    };
    let mut child_exit = || Some(ExitStatus::from_raw(0));

    let (ready_seen, status) = observe_ready_or_exit(&mut ready, &mut child_exit);

    assert!(!ready_seen);
    assert!(status.expect("exit status should be preserved").success());
    assert_eq!(ready_calls, 2);
}

#[test]
fn test_readiness_observation_nonzero_exit_after_marker() {
    let ready_results = [false, true];
    let mut ready_calls = 0;
    let mut ready = || {
        let result = ready_results[ready_calls];
        ready_calls += 1;
        result
    };
    let mut child_exit = || Some(ExitStatus::from_raw(1 << 8));

    let (ready_seen, status) = observe_ready_or_exit(&mut ready, &mut child_exit);

    assert!(ready_seen);
    assert!(!status.expect("exit status should be preserved").success());
    assert_eq!(ready_calls, 2);
}

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
            let (ready, exit) = observe_ready_or_exit(
                &mut || {
                    if ready.exists() {
                        assert_eq!(fs::read(&ready).expect("readiness marker should be readable"), b"ready");
                        true
                    } else {
                        false
                    }
                },
                &mut || child.try_wait().expect("child status should be available"),
            );
            ready_seen = ready;
            init_exit = exit;
            if ready_seen || init_exit.is_some() {
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
        let status = if init_exit.is_some() {
            init_exit
        } else {
            loop {
                if let Some(status) = child.try_wait().expect("child status should be available") {
                    break Some(status);
                }
                if Instant::now() >= content_deadline {
                    timed_out = true;
                    child.kill_and_wait();
                    break None;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
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
