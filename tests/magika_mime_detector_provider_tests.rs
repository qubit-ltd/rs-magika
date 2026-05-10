/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use qubit_magika::MagikaMimeDetectorProvider;
use qubit_mime::{
    MimeConfig,
    MimeDetectorRegistry,
    ServiceProvider,
};

/// Test provider metadata and registry alias registration.
#[test]
fn test_magika_mime_detector_provider_metadata_and_registration() {
    let provider = MagikaMimeDetectorProvider;
    let descriptor = provider
        .descriptor()
        .expect("magika provider descriptor should be valid");

    assert_eq!(descriptor.id().as_str(), "magika");
    assert_eq!(
        descriptor.aliases_as_str(),
        vec!["magika-mime-detector", "magikamimedetector"],
    );
    assert_eq!(descriptor.priority(), 20);

    let mut registry = MimeDetectorRegistry::builtin();
    registry
        .register(MagikaMimeDetectorProvider)
        .expect("magika provider should register");

    assert!(registry.find_provider("magika").is_some());
    assert!(registry.find_provider("magika-mime-detector").is_some());
    assert!(registry.find_provider("MagikaMimeDetector").is_some());
}

/// Test provider create returns a detector when the Magika runtime is available.
#[test]
fn test_magika_mime_detector_provider_create_uses_mime_config() {
    let provider = MagikaMimeDetectorProvider;
    let config = MimeConfig::default();

    let Ok(detector) = provider.create_box(&config) else {
        return;
    };

    assert_eq!(
        Some("application/pdf".to_owned()),
        detector.detect_by_filename("document.pdf"),
    );
}
