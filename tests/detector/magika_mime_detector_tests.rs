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
    MagikaMimeDetector,
    MagikaMimeDetectorProvider,
    register_mime_detector,
};
use qubit_mime::{
    CONFIG_MIME_DETECTOR_DEFAULT,
    MimeConfig,
    MimeDetectionPolicy,
    MimeDetector,
    MimeDetectorProvider,
    MimeDetectorRegistry,
};

#[test]
fn test_provider_registers_magika_aliases_with_mime_registry() {
    let mut registry = MimeDetectorRegistry::with_builtin();
    register_mime_detector(&mut registry).expect("magika provider should register");

    assert!(registry.find_provider("magika").is_some());
    assert!(registry.find_provider("magika-mime-detector").is_some());
    assert!(registry.find_provider("MagikaMimeDetector").is_some());
}

#[test]
fn test_provider_creates_magika_detector_when_runtime_is_available() {
    let mut registry = MimeDetectorRegistry::with_builtin();
    register_mime_detector(&mut registry).expect("magika provider should register");
    let config = detector_config("magika");

    let Ok(detector) = registry.create_default(&config) else {
        return;
    };

    assert_eq!(
        Some("text/x-python".to_owned()),
        detector.detect_by_content(b"#!/usr/bin/env python3\nprint('hello')\n"),
    );
}

#[test]
fn test_magika_detector_delegates_filename_detection_to_repository() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };

    assert_eq!(
        Some("application/pdf".to_owned()),
        detector.detect_by_filename("document.pdf")
    );
}

#[test]
fn test_magika_detector_restores_reader_position() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };
    let mut reader = std::io::Cursor::new(b"#!/bin/sh\necho hello\n".to_vec());
    reader.set_position(2);

    let detected = detector
        .detect_reader(
            &mut reader,
            Some("script.sh"),
            MimeDetectionPolicy::VerifyContent,
        )
        .expect("reader detection should not fail");

    assert_eq!(Some("text/x-shellscript".to_owned()), detected);
    assert_eq!(2, reader.position());
}

#[test]
fn test_provider_metadata_is_stable() {
    let provider = MagikaMimeDetectorProvider;

    assert_eq!("magika", provider.id());
    assert!(provider.aliases().contains(&"magika-mime-detector"));
    assert!(provider.priority() > 0);
}

fn detector_config(default: &str) -> MimeConfig {
    let mut config = qubit_config::Config::new();
    config
        .set(CONFIG_MIME_DETECTOR_DEFAULT, default)
        .expect("detector default should be configurable");
    MimeConfig::from_config(&config).expect("detector config should parse")
}
