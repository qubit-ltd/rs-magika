// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_magika::{
    MagikaMimeDetectorProvider,
    magika_mime_detector_descriptor,
};
use qubit_mime::{
    MimeConfig,
    MimeDetectorRegistry,
};
use qubit_spi::ServiceProvider;

/// Test provider metadata and registry alias registration.
#[test]
fn test_magika_mime_detector_provider_metadata_and_registration() {
    let descriptor = magika_mime_detector_descriptor();

    assert_eq!(descriptor.id().as_str(), "magika");
    assert_eq!(
        descriptor
            .aliases()
            .iter()
            .map(|alias| alias.as_str())
            .collect::<Vec<_>>(),
        vec!["magika-mime-detector", "magikamimedetector"],
    );
    assert_eq!(descriptor.priority(), 20);

    let mut builder = MimeDetectorRegistry::builder();
    builder
        .register(descriptor, MagikaMimeDetectorProvider)
        .expect("magika provider should register");
    let registry = builder.build();

    assert_eq!(vec!["magika"], registry.provider_ids());
}

/// Test provider create returns a detector when the Magika runtime is
/// available.
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
