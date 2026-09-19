use std::error::Error;
use std::path::PathBuf;

use qubit_magika::MagikaMimeDetector;
use qubit_mime::MimeDetector;

fn main() -> Result<(), Box<dyn Error>> {
    let runtime = PathBuf::from(std::env::var("ORT_LIBRARY_PATH")?);
    if !ort::init_from(runtime)?.commit() {
        return Err("ONNX Runtime environment was already initialized".into());
    }
    let detector = MagikaMimeDetector::new()?;
    let actual = detector.detect_by_content(b"#!/usr/bin/env python3\nprint('hello')\n")?;
    if actual.as_deref() != Some("text/x-python") {
        return Err(format!("dynamic ONNX Runtime returned unexpected MIME type: {actual:?}").into());
    }
    Ok(())
}
