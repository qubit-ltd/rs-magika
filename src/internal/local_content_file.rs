// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;

/// Opens a local content source and verifies the opened handle is a regular
/// file.
///
/// Unix opens do not follow the final symbolic link and do not wait for a FIFO
/// writer. Other platforms use the standard file open behavior and still
/// verify the type reported by the opened handle.
///
/// # Parameters
///
/// - `path`: Path to the local content source to open.
///
/// # Returns
///
/// The opened file handle when it refers to a regular file.
///
/// # Errors
///
/// Returns the operating-system error if opening or inspecting the path fails,
/// or [`io::ErrorKind::Unsupported`] when the opened handle is not a regular
/// file.
pub(crate) fn open_regular_file(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    validate_regular_file(options.open(path)?)
}

/// Confirms that an already-open handle refers to a regular file.
///
/// # Parameters
///
/// - `file`: Open handle whose metadata determines the object type.
///
/// # Returns
///
/// The same handle when its metadata reports a regular file.
///
/// # Errors
///
/// Returns the metadata I/O error, or [`io::ErrorKind::Unsupported`] when the
/// handle is not a regular file.
fn validate_regular_file(file: File) -> io::Result<File> {
    if file.metadata()?.is_file() {
        Ok(file)
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "local MIME content detection requires a regular file",
        ))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::env;
    use std::fs::File;
    #[cfg(unix)]
    use std::io::ErrorKind;
    use std::io::Read;
    use std::io::Write;
    #[cfg(unix)]
    use std::process::Child;
    #[cfg(unix)]
    use std::process::Command;
    #[cfg(unix)]
    use std::time::Duration;
    #[cfg(unix)]
    use std::time::Instant;

    #[cfg(unix)]
    const FIFO_PATH: &str = "QUBIT_MAGIKA_PRIVATE_FIFO_PATH";
    #[cfg(unix)]
    const COMPLETION_PATH: &str = "QUBIT_MAGIKA_PRIVATE_FIFO_COMPLETION_PATH";

    #[cfg(unix)]
    #[test]
    fn test_rejects_non_regular_open_handle() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let file = File::open(directory.path()).expect("directory handle should open");

        let error = super::validate_regular_file(file).expect_err("directory handle should be rejected");

        assert_eq!(error.kind(), ErrorKind::Unsupported);
    }

    #[test]
    fn test_keeps_regular_open_handle_readable() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let path = directory.path().join("sample.txt");
        let mut created = File::create(&path).expect("sample file should be created");
        created.write_all(b"sample").expect("sample should be written");
        drop(created);

        let file = super::open_regular_file(&path).expect("regular file should open");
        let mut contents = Vec::new();
        file.take(6)
            .read_to_end(&mut contents)
            .expect("opened handle should remain readable");

        assert_eq!(contents, b"sample");
    }

    #[cfg(unix)]
    #[test]
    fn test_rejects_static_final_symlink_at_open() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let target = directory.path().join("sample.txt");
        File::create(&target).expect("regular target should be created");
        let link = directory.path().join("sample-link.txt");
        symlink(&target, &link).expect("static symlink should be created");

        let _error =
            super::open_regular_file(&link).expect_err("final symlink should be rejected by the Unix open flags");
    }

    #[cfg(unix)]
    #[test]
    fn test_fifo_open_returns_unsupported_before_deadline() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let fifo = directory.path().join("content.fifo");
        let completion = directory.path().join("fifo-test.completed");
        let status = Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .expect("mkfifo should run");
        assert!(status.success(), "mkfifo failed with {status}");

        let child = Command::new(env::current_exe().expect("test executable path"))
            .args([
                "--exact",
                "internal::local_content_file::tests::test_fifo_open_child",
                "--nocapture",
            ])
            .env(FIFO_PATH, &fifo)
            .env(COMPLETION_PATH, &completion)
            .spawn()
            .expect("FIFO test child should start");
        let mut child = ChildGuard(child);
        let status = wait_for_success(&mut child);
        assert!(status.success(), "FIFO test child failed with {status}");
        assert!(
            completion.is_file(),
            "FIFO child did not write its completion marker"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_fifo_open_child() {
        let Some(path) = env::var_os(FIFO_PATH) else {
            return;
        };
        let completion =
            env::var_os(COMPLETION_PATH).expect("FIFO completion path should be configured");
        let path = std::path::PathBuf::from(path);
        let error = super::open_regular_file(&path).expect_err("FIFO handle should be rejected");
        assert_eq!(error.kind(), ErrorKind::Unsupported);
        std::fs::write(completion, b"completed").expect("FIFO completion marker should be written");
    }

    #[cfg(unix)]
    fn wait_for_success(child: &mut ChildGuard) -> std::process::ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().expect("child status should be available") {
                return status;
            }
            if Instant::now() >= deadline {
                panic!("private FIFO open did not finish before its deadline");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    #[cfg(unix)]
    struct ChildGuard(Child);

    #[cfg(unix)]
    impl ChildGuard {
        fn try_wait(&mut self) -> std::io::Result<Option<std::process::ExitStatus>> {
            self.0.try_wait()
        }
    }

    #[cfg(unix)]
    impl Drop for ChildGuard {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
