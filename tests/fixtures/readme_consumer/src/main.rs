// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::sync::Arc;

use qubit_magika::{MagikaMimeDetectorProvider, ManagedInferenceRuntime};
use qubit_mime::{MimeConfig, MimeDetectionPolicy, MimeDetector, MimeDetectorRegistry};
use qubit_operation::{ManagerOptions, OperationManager, ShutdownMode};
use qubit_spi::ProviderSelection;

fn create_detector() -> Result<
    (
        MimeDetectorRegistry,
        Arc<ManagedInferenceRuntime>,
        Arc<dyn MimeDetector>,
    ),
    Box<dyn Error>,
> {
    let config = MimeConfig::default();
    let manager = OperationManager::new(ManagerOptions::default())?;
    let runtime = Arc::new(ManagedInferenceRuntime::new(
        manager,
        config.max_buffer_size(),
    )?);
    let registry = MimeDetectorRegistry::default();
    registry.register(MagikaMimeDetectorProvider::new().managed_runtime(Arc::clone(&runtime)))?;
    let selection = ProviderSelection::named("magika")?;
    let provider = registry.resolve_selected(&selection)?;
    let detector = provider.create_configured(&config)?;
    Ok((registry, runtime, detector))
}

fn main() -> Result<(), Box<dyn Error>> {
    let library = std::env::var_os("ORT_LIBRARY_PATH")
        .map(std::path::PathBuf::from)
        .ok_or("ORT_LIBRARY_PATH must name the ONNX Runtime shared library")?;
    ort::init_from(library)?.commit();
    let (_registry, runtime, detector) = create_detector()?;
    assert_eq!(
        Some("text/x-python".to_owned()),
        detector.detect(
            b"#!/usr/bin/env python3\nprint('hello')\n",
            Some("upload.txt"),
            MimeDetectionPolicy::VerifyContent,
        )?,
    );
    assert_eq!(
        Some("application/pdf".to_owned()),
        detector.detect_by_filename("document.pdf")?,
    );
    let report = runtime.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(5));
    assert!(report.worker_stopped);
    Ok(())
}
