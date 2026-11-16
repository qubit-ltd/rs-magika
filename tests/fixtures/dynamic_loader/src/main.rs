use std::{env, path::PathBuf, time::Duration};

use qubit_magika::MagikaMimeDetector;
use qubit_mime::MimeDetectorBackend;
use qubit_operation::ShutdownMode;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let library = env::var_os("ORT_LIBRARY_PATH")
        .map(PathBuf::from)
        .ok_or("ORT_LIBRARY_PATH must name the ONNX Runtime shared library")?;
    ort::init_from(library)?.commit();

    let detector = MagikaMimeDetector::new()?;
    let detected = detector.guess_from_content(b"#!/usr/bin/env python3\nprint('hello')\n")?;
    assert!(detected.iter().any(|mime| mime == "text/x-python"), "unexpected MIME candidates: {detected:?}");

    let report = detector.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(5));
    assert!(report.worker_stopped, "inference worker did not stop: {report:?}");
    Ok(())
}
