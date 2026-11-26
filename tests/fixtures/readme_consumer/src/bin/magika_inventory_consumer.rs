// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::sync::Arc;

#[cfg(not(feature = "inventory"))]
use qubit_magika::MagikaMimeDetectorProvider;
use qubit_magika::ManagedInferenceRuntime;
use qubit_mime::MimeConfig;
use qubit_mime::MimeDetectionPolicy;
use qubit_mime::MimeDetector;
use qubit_mime::MimeDetectorRegistry;
use qubit_spi::ProviderSelection;
use qubit_spi::ProviderSelectionTargetRef;

fn create_detector() -> Result<
    (
        MimeDetectorRegistry,
        Arc<dyn MimeDetector>,
        Option<Arc<ManagedInferenceRuntime>>,
    ),
    Box<dyn Error>,
> {
    let config = MimeConfig::default();
    let registry = MimeDetectorRegistry::builtin();
    assert!(matches!(
        registry.default_selection().target(),
        ProviderSelectionTargetRef::Named(selector) if selector.as_str() == "repository"
    ));

    #[cfg(feature = "inventory")]
    {
        eprintln!("Magika consumer mode: inventory auto-discovery");
        assert!(registry.provider_ids().iter().any(|id| id.as_str() == "magika"));
        let selection = ProviderSelection::named("magika")?;
        let detector = registry.resolve_selected(&selection)?.create_configured(&config)?;
        Ok((registry, detector, None))
    }

    #[cfg(not(feature = "inventory"))]
    {
        eprintln!("Magika consumer mode: explicit registration");
        let manager = qubit_operation::OperationManager::new(
            qubit_operation::ManagerOptions::default(),
        )?;
        let runtime = Arc::new(ManagedInferenceRuntime::new(manager, config.max_buffer_size())?);
        registry.register(
            MagikaMimeDetectorProvider::new().managed_runtime(Arc::clone(&runtime)),
        )?;
        let selection = ProviderSelection::named("magika")?;
        let detector = registry.resolve_selected(&selection)?.create_configured(&config)?;
        Ok((registry, detector, Some(runtime)))
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let library = std::env::var_os("ORT_LIBRARY_PATH")
        .map(std::path::PathBuf::from)
        .ok_or("ORT_LIBRARY_PATH must name the ONNX Runtime shared library")?;
    ort::init_from(library)?.commit();

    let (registry, detector, runtime) = create_detector()?;
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
    drop(detector);
    if let Some(runtime) = runtime {
        let report = runtime.shutdown_blocking(
            qubit_operation::ShutdownMode::Drain,
            std::time::Duration::from_secs(5),
        );
        assert!(report.worker_stopped);
    }
    drop(registry);
    Ok(())
}
