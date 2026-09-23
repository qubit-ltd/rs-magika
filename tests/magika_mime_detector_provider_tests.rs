// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#[cfg(feature = "bundled-onnxruntime")]
use std::sync::Arc;

use qubit_magika::MagikaMimeDetectorProvider;
#[cfg(feature = "bundled-onnxruntime")]
use qubit_mime::MediaStreamType;
#[cfg(feature = "bundled-onnxruntime")]
use qubit_mime::MimeConfig;
use qubit_mime::MimeDetectorRegistry;
use qubit_spi::ProviderMetadata;
#[cfg(feature = "bundled-onnxruntime")]
use qubit_spi::ProviderSelection;

#[cfg(feature = "bundled-onnxruntime")]
use crate::support::StaticMediaStreamClassifier;

/// Tests provider self-description and one-argument registration.
#[test]
fn test_magika_mime_detector_provider_metadata_and_registration() {
    let descriptor = MagikaMimeDetectorProvider::new().descriptor();

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

    let registry = MimeDetectorRegistry::default();
    registry
        .register(MagikaMimeDetectorProvider::new())
        .expect("magika provider should register");

    assert_eq!(
        vec!["magika"],
        registry.provider_ids().iter().map(|id| id.as_str()).collect::<Vec<_>>(),
    );
}

#[cfg(feature = "bundled-onnxruntime")]
#[test]
fn test_provider_debug_and_classifier_configuration() {
    let provider = MagikaMimeDetectorProvider::with_media_stream_classifier(Arc::new(
        StaticMediaStreamClassifier::new(MediaStreamType::AudioOnly, None),
    ));
    assert!(format!("{provider:?}").contains("configured"));
}

/// Tests explicit Registry selection before configured detector creation.
#[cfg(feature = "bundled-onnxruntime")]
#[test]
fn test_resolve_explicit_then_create_with_mime_config() {
    let registry = MimeDetectorRegistry::default();
    registry
        .register(MagikaMimeDetectorProvider::new())
        .expect("magika provider should register");
    let selection = ProviderSelection::named("magika").expect("selection should be valid");
    let provider = registry
        .resolve_selected(&selection)
        .expect("magika provider should resolve");
    let config = MimeConfig::default();

    let detector = provider
        .create_configured(&config)
        .expect("bundled ONNX Runtime should initialize Magika");

    assert_eq!(
        Some("application/pdf".to_owned()),
        detector
            .detect_by_filename("document.pdf")
            .expect("filename detection should succeed"),
    );
}

/// Tests App startup registration and library-side explicit/default use.
#[cfg(all(feature = "bundled-onnxruntime", not(feature = "inventory")))]
#[test]
fn test_global_registry_resolve_explicit_and_default_then_create() {
    let registry = MimeDetectorRegistry::global();
    registry
        .register(MagikaMimeDetectorProvider::new())
        .expect("App startup should register the Magika provider");
    let selection = ProviderSelection::named("magika").expect("Magika selection should be valid");

    let explicit_provider = registry
        .resolve_selected(&selection)
        .expect("library code should explicitly resolve the App provider");
    let config = MimeConfig::default();
    let explicit_detector = explicit_provider
        .create_configured(&config)
        .expect("bundled ONNX Runtime should initialize Magika");
    assert_eq!(
        Some("application/pdf".to_owned()),
        explicit_detector
            .detect_by_filename("document.pdf")
            .expect("filename detection should succeed"),
    );

    registry
        .set_default_selection(selection)
        .expect("default selection should be valid");

    let provider = registry
        .resolve()
        .expect("library code should resolve the App-selected provider");
    let detector = provider
        .create()
        .expect("bundled ONNX Runtime should initialize Magika");

    assert_eq!(
        Some("application/pdf".to_owned()),
        detector
            .detect_by_filename("document.pdf")
            .expect("filename detection should succeed"),
    );
}

/// An inventory-enabled build discovers the default Magika provider while
/// retaining repository selection.
#[cfg(feature = "inventory")]
#[test]
fn test_magika_provider_is_discovered_with_repository_default() {
    let registry = MimeDetectorRegistry::builtin();
    assert!(registry.provider_ids().iter().any(|id| id.as_str() == "magika"));
    assert_eq!(
        qubit_spi::ProviderSelection::named("repository").expect("valid repository ID"),
        registry.default_selection(),
    );
    let selection = qubit_spi::ProviderSelection::named("magika").expect("valid magika ID");
    registry.resolve_selected(&selection).expect("Magika should resolve");
}
