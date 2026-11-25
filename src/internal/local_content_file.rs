use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::path::Path;

/// Opens a local content source and verifies the opened handle is a regular file.
///
/// Unix opens do not follow the final symbolic link and do not wait for a FIFO
/// writer. Other platforms use the standard file open behavior and still
/// verify the type reported by the opened handle.
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
    use std::env;
    use std::fs::File;
    use std::io::ErrorKind;
    use std::io::Read;
    use std::io::Write;
    use std::process::Child;
    use std::process::Command;
    use std::time::Duration;
    use std::time::Instant;

    #[cfg(unix)]
    const FIFO_PATH: &str = "QUBIT_MAGIKA_PRIVATE_FIFO_PATH";

    #[cfg(unix)]
    #[test]
    fn rejects_non_regular_open_handle() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let file = File::open(directory.path()).expect("directory handle should open");

        let error =
            super::validate_regular_file(file).expect_err("directory handle should be rejected");

        assert_eq!(error.kind(), ErrorKind::Unsupported);
    }

    #[test]
    fn keeps_regular_open_handle_readable() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let path = directory.path().join("sample.txt");
        let mut created = File::create(&path).expect("sample file should be created");
        created
            .write_all(b"sample")
            .expect("sample should be written");
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
    fn fifo_open_returns_unsupported_before_deadline() {
        let directory = tempfile::tempdir().expect("temporary directory should be created");
        let fifo = directory.path().join("content.fifo");
        let status = Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .expect("mkfifo should run");
        assert!(status.success(), "mkfifo failed with {status}");

        let mut child = Command::new(env::current_exe().expect("test executable path"))
            .args([
                "--exact",
                "internal::local_content_file::tests::fifo_open_child",
                "--nocapture",
            ])
            .env(FIFO_PATH, &fifo)
            .spawn()
            .expect("FIFO test child should start");
        wait_for_success(&mut child);
    }

    #[cfg(unix)]
    #[test]
    fn fifo_open_child() {
        let Some(path) = env::var_os(FIFO_PATH) else {
            return;
        };
        let path = std::path::PathBuf::from(path);
        let error = super::open_regular_file(&path).expect_err("FIFO handle should be rejected");
        assert_eq!(error.kind(), ErrorKind::Unsupported);
    }

    #[cfg(unix)]
    fn wait_for_success(child: &mut Child) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = child.try_wait().expect("child status should be available") {
                assert!(status.success(), "FIFO test child failed with {status}");
                return;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("private FIFO open did not finish before its deadline");
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
