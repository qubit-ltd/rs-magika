/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use qubit_magika::{
    MagikaMimeDetectorProvider,
    register_mime_detector,
};
use qubit_mime::{
    MimeConfig,
    MimeDetectorProvider,
    MimeDetectorRegistry,
};

/// Test provider metadata and registry alias registration.
#[test]
fn test_magika_mime_detector_provider_metadata_and_registration() {
    let provider = MagikaMimeDetectorProvider;

    assert_eq!(provider.id(), "magika");
    assert_eq!(
        provider.aliases(),
        &["magika-mime-detector", "MagikaMimeDetector"],
    );
    assert_eq!(provider.priority(), 20);

    let mut registry = MimeDetectorRegistry::builtin();
    register_mime_detector(&mut registry).expect("magika provider should register");

    assert!(registry.find_provider("magika").is_some());
    assert!(registry.find_provider("magika-mime-detector").is_some());
    assert!(registry.find_provider("MagikaMimeDetector").is_some());
}

/// Test provider create returns a detector when the Magika runtime is available.
#[test]
fn test_magika_mime_detector_provider_create_uses_mime_config() {
    let provider = MagikaMimeDetectorProvider;
    let config = MimeConfig::default();

    let Ok(detector) = provider.create(&config) else {
        return;
    };

    assert_eq!(
        Some("application/pdf".to_owned()),
        detector.detect_by_filename("document.pdf"),
    );
}
