// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_magika::MagikaMimeDetectorProvider;
use qubit_mime::{
    MimeConfig,
    MimeDetectorRegistry,
};
use qubit_spi::{
    ProviderDefinition,
    ProviderSelection,
    ServiceProvider,
};

/// Tests provider self-description and one-argument registration.
#[test]
fn test_magika_mime_detector_provider_metadata_and_registration() {
    let descriptor = MagikaMimeDetectorProvider.descriptor();

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
        .register(MagikaMimeDetectorProvider)
        .expect("magika provider should register");

    assert_eq!(
        vec!["magika"],
        registry
            .provider_ids()
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>(),
    );
}

/// Tests explicit Registry selection before configured detector creation.
#[test]
fn test_resolve_explicit_then_create_with_mime_config() {
    let registry = MimeDetectorRegistry::default();
    registry
        .register(MagikaMimeDetectorProvider)
        .expect("magika provider should register");
    let selection =
        ProviderSelection::named("magika").expect("selection should be valid");
    let provider = registry
        .resolve_selected(&selection)
        .expect("magika provider should resolve");
    let config = MimeConfig::default();

    let Ok(detector) = provider.create_configured(&config) else {
        return;
    };

    assert_eq!(
        Some("application/pdf".to_owned()),
        detector.detect_by_filename("document.pdf"),
    );
}

/// Tests App startup registration and library-side explicit/default use.
#[test]
fn test_global_registry_resolve_explicit_and_default_then_create() {
    let registry = MimeDetectorRegistry::global();
    registry
        .register(MagikaMimeDetectorProvider)
        .expect("App startup should register the Magika provider");
    let selection = ProviderSelection::named("magika")
        .expect("Magika selection should be valid");

    let explicit_provider = registry
        .resolve_selected(&selection)
        .expect("library code should explicitly resolve the App provider");
    let config = MimeConfig::default();
    let Ok(explicit_detector) = explicit_provider.create_configured(&config)
    else {
        return;
    };
    assert_eq!(
        Some("application/pdf".to_owned()),
        explicit_detector.detect_by_filename("document.pdf"),
    );

    registry.set_default_selection(selection);

    let provider = registry
        .resolve()
        .expect("library code should resolve the App-selected provider");
    let Ok(detector) = provider.create() else {
        return;
    };

    assert_eq!(
        Some("application/pdf".to_owned()),
        detector.detect_by_filename("document.pdf"),
    );
}
