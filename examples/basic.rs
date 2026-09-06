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

use qubit_magika::MagikaMimeDetectorProvider;
use qubit_mime::MimeConfig;
use qubit_mime::MimeDetector;
use qubit_mime::MimeDetectorRegistry;
use qubit_mime::MimeResult;
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
    registry.register(MagikaMimeDetectorProvider)?;
    let selection = ProviderSelection::named("magika")?;
    registry.set_default_selection(selection.clone());
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
