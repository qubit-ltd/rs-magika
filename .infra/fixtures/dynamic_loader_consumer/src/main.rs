// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::env::var;
use std::error::Error;
use std::path::PathBuf;

use ort::init_from;
use qubit_magika::MagikaMimeDetector;
use qubit_mime::MimeDetector;

/// Runs the dynamic loader consumer against the configured ONNX Runtime.
fn main() -> Result<(), Box<dyn Error>> {
    let runtime = PathBuf::from(var("ORT_LIBRARY_PATH")?);
    if !init_from(runtime)?.commit() {
        return Err("ONNX Runtime environment was already initialized".into());
    }
    let detector = MagikaMimeDetector::new()?;
    let actual = detector.detect_by_content(b"#!/usr/bin/env python3\nprint('hello')\n")?;
    if actual.as_deref() != Some("text/x-python") {
        return Err(
            format!("dynamic ONNX Runtime returned unexpected MIME type: {actual:?}").into(),
        );
    }
    Ok(())
}
