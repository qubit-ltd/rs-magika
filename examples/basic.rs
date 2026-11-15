// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Creates one Magika detector at application startup and shares it with
//! library code.

use std::error::Error;
use std::sync::Arc;

use qubit_fs::AsyncFileSystem;
use qubit_fs::Path;
use qubit_magika::MagikaMimeDetector;
use qubit_magika::MagikaMimeDetectorProvider;
use qubit_mime::MimeConfig;
use qubit_mime::MimeDetector;
use qubit_mime::MimeDetectorRegistry;
use qubit_mime::MimeResult;
use qubit_operation::{GroupOptions, ManagerOptions, OperationManager, OperationOptions, ShutdownMode};
use qubit_spi::ProviderSelection;

/// Registers Magika and creates one detector for the application.
///
/// # Returns
///
/// A shared detector that can be passed to downstream libraries.
///
/// # Errors
///
/// Returns an error when provider registration, selection, or Magika session
/// initialization fails.
fn create_detector() -> Result<Arc<dyn MimeDetector>, Box<dyn Error>> {
    let registry = MimeDetectorRegistry::global();
    registry.register(MagikaMimeDetectorProvider::new())?;
    let selection = ProviderSelection::named("magika")?;
    registry
        .set_default_selection(selection.clone())
        .expect("default selection should be valid");
    let provider = registry.resolve_selected(&selection)?;
    Ok(provider.create_configured(&MimeConfig::default())?)
}

/// Uses a caller-owned detector for content detection.
///
/// # Parameters
///
/// * `detector` - Detector initialized and shared by the application.
///
/// # Returns
///
/// The detected MIME type, or `None` when no type is recognized.
fn library_detect_content(detector: &dyn MimeDetector) -> MimeResult<Option<String>> {
    detector.detect_by_content(b"#!/usr/bin/env python3\nprint('hello')\n")
}

/// Uses the same caller-owned detector for filename detection.
///
/// # Parameters
///
/// * `detector` - Detector initialized and shared by the application.
///
/// # Returns
///
/// The detected MIME type, or `None` when no repository rule matches.
fn library_detect_filename(detector: &dyn MimeDetector) -> MimeResult<Option<String>> {
    detector.detect_by_filename("document.pdf")
}

/// Prepares one operation-aware detector and keeps its supervisor available
/// for request scopes. Building the detector starts its shared worker/session.
#[allow(dead_code)]
fn prepare_managed_detector() -> Result<(Arc<MagikaMimeDetector>, OperationManager), Box<dyn Error>> {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let config = MimeConfig::default();
    let runtime = Arc::new(qubit_magika::ManagedInferenceRuntime::new(
        manager.clone(),
        config.max_buffer_size(),
    )?);
    let detector = Arc::new(
        MagikaMimeDetector::builder()
            .mime_config(config)
            .managed_runtime(runtime)
            .build()?,
    );
    Ok((detector, manager))
}

/// Performs a scoped async provider-path detection and observes both result
/// and worker release before closing the request group.
#[allow(dead_code)]
async fn managed_provider_path(
    detector: Arc<MagikaMimeDetector>,
    manager: &OperationManager,
    file_system: Arc<AsyncFileSystem>,
    path: Path,
) -> Result<(), Box<dyn Error>> {
    let requests = manager.group(GroupOptions { capacity: 4 })?;
    let request_detector = Arc::clone(&detector);
    let mut request = requests.as_async().try_submit(OperationOptions::default(), move |scope| async move {
        request_detector
            .detect_async_provider_path_managed(&scope, &file_system, &path, 1024 * 1024)
            .await
    })?;
    let _business_result = request.wait_outcome().await?;
    let _settlement = request.observer().wait_settlement().await;
    requests.close_admission();
    Ok(())
}

/// Drains inference before shutting down the application-owned supervisor.
#[allow(dead_code)]
fn shutdown_managed(detector: &MagikaMimeDetector, manager: &OperationManager) {
    let detector_report = detector.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(5));
    assert!(detector_report.worker_stopped);
    let _manager_shutdown = manager.request_shutdown(ShutdownMode::Drain);
}

/// Runs the shared-detector example.
///
/// # Errors
///
/// Returns an error when detector setup fails.
fn main() -> Result<(), Box<dyn Error>> {
    let detector = create_detector()?;
    assert_eq!(
        Some("text/x-python".to_owned()),
        library_detect_content(detector.as_ref())?,
    );
    assert_eq!(
        Some("application/pdf".to_owned()),
        library_detect_filename(detector.as_ref())?,
    );
    Ok(())
}
