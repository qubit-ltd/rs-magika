// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
#![cfg(feature = "bundled-onnxruntime")]

use std::fs::File;
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use qubit_magika::MagikaMimeDetector;
use qubit_mime::{
    CONFIG_MIME_AMBIGUOUS_MIME_MAPPING, CONFIG_MIME_DETECTOR_DEFAULT,
    CONFIG_MIME_ENABLE_PRECISE_DETECTION, CONFIG_MIME_PRECISE_DETECTION_PATTERNS, MediaStreamType,
    MimeConfig, MimeDetectionPolicy, MimeDetector, MimeError,
};
use tempfile::NamedTempFile;

use crate::support::{FailingReadSeek, RealFileCase, StaticMediaStreamClassifier, detector};

/// Real fixture files used to exercise filesystem-backed Magika detection.
const REAL_FILE_CASES: &[RealFileCase] = &[
    RealFileCase {
        relative_path: "tests/fixtures/real_files/script.py",
        expected_mime: "text/x-python",
    },
    RealFileCase {
        relative_path: "tests/fixtures/real_files/script.sh",
        expected_mime: "text/x-shellscript",
    },
    RealFileCase {
        relative_path: "tests/fixtures/real_files/page.html",
        expected_mime: "text/html",
    },
    RealFileCase {
        relative_path: "tests/fixtures/real_files/data.json",
        expected_mime: "application/json",
    },
];

/// Verifies filename-only detection delegates to the MIME repository.
#[test]
fn test_magika_detector_delegates_filename_detection_to_repository() {
    let detector = detector();

    assert_eq!(
        Some("application/pdf".to_owned()),
        detector
            .detect_by_filename("document.pdf")
            .expect("filename detection should succeed")
    );
}

/// Verifies accessors expose the configured core and repository.
#[test]
fn test_magika_detector_exposes_core_and_repository() {
    let mut detector =
        MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize Magika");

    assert_eq!(
        Some("application/pdf".to_owned()),
        detector
            .core()
            .merge_results(&[], &["application/pdf".to_owned()])
    );
    assert!(detector.repository().get("application/pdf").is_some());

    detector.core_mut().set_media_stream_classifier(None);
    assert!(detector.core().media_stream_classifier().is_none());
}

/// Verifies filename-preferred detection skips conflicting content inference.
#[test]
fn test_magika_detector_detect_prefers_filename_without_content_detection() {
    let detector = detector();

    assert_eq!(
        Some("application/pdf".to_owned()),
        detector
            .detect(
                b"not a pdf",
                Some("document.pdf"),
                MimeDetectionPolicy::PreferFilename,
            )
            .expect("combined detection should succeed")
    );
}

/// Verifies content verification can override a misleading filename.
#[test]
fn test_magika_detector_detect_verifies_content_when_policy_requires() {
    let detector = detector();

    assert_eq!(
        Some("text/x-python".to_owned()),
        detector
            .detect(
                b"#!/usr/bin/env python3\nprint('hello')\n",
                Some("script.txt"),
                MimeDetectionPolicy::VerifyContent,
            )
            .expect("combined detection should succeed")
    );
}

/// Verifies filename-preferred reader detection performs no content read.
#[test]
fn test_magika_detector_reader_detection_prefers_filename_without_reading() {
    let detector = detector();
    let mut reader = FailingReadSeek::new(b"not a pdf".to_vec(), true, None);

    let detected = detector
        .detect_reader(
            &mut reader,
            Some("document.pdf"),
            MimeDetectionPolicy::PreferFilename,
        )
        .expect("filename-preferred reader detection should skip content reads");

    assert_eq!(Some("application/pdf".to_owned()), detected);
}

/// Verifies successful reader detection restores the original position.
#[test]
fn test_magika_detector_restores_reader_position() {
    let detector = detector();
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

/// Verifies reader detection uses the configured stream classifier for
/// precise MIME refinement without consuming the reader position.
#[test]
fn test_magika_detector_reader_detection_refines_media_type() {
    let mut raw_config = qubit_config::Config::new();
    raw_config
        .set(CONFIG_MIME_DETECTOR_DEFAULT, "magika")
        .expect("detector default should be configurable");
    raw_config
        .set(CONFIG_MIME_ENABLE_PRECISE_DETECTION, true)
        .expect("precise detection should be configurable");
    raw_config
        .set(CONFIG_MIME_PRECISE_DETECTION_PATTERNS, "sh")
        .expect("precise patterns should be configurable");
    raw_config
        .set(
            CONFIG_MIME_AMBIGUOUS_MIME_MAPPING,
            "sh:text/x-shellscript,audio/x-shellscript",
        )
        .expect("ambiguous mapping should be configurable");
    let config =
        MimeConfig::from_config(&raw_config).expect("precise detector config should parse");
    let mut detector = MagikaMimeDetector::from_mime_config(config)
        .expect("bundled ONNX Runtime should initialize Magika");
    detector
        .core_mut()
        .set_media_stream_classifier(Some(Arc::new(StaticMediaStreamClassifier::new(
            MediaStreamType::AudioOnly,
            Some(b'#'),
        ))));
    let mut reader = Cursor::new(b"#!/bin/sh\necho hello\n".to_vec());
    reader.set_position(2);

    let detected = detector
        .detect_reader(
            &mut reader,
            Some("script.sh"),
            MimeDetectionPolicy::VerifyContent,
        )
        .expect("reader detection should refine the MIME type");

    assert_eq!(Some("audio/x-shellscript".to_owned()), detected);
    assert_eq!(2, reader.position());
}

/// Verifies file detection reads content when verification is requested.
#[test]
fn test_magika_detector_detect_file_reads_content_when_policy_requires() {
    let detector = detector();
    let mut file = NamedTempFile::with_suffix(".txt").expect("temp file should be created");
    file.write_all(b"#!/usr/bin/env python3\nprint('hello')\n")
        .expect("temp file should be writable");

    let detected = detector
        .detect_file(file.path(), MimeDetectionPolicy::VerifyContent)
        .expect("file detection should succeed");

    assert_eq!(Some("text/x-python".to_owned()), detected);
}

/// Verifies filename-preferred file detection does not access a missing file.
#[test]
fn test_magika_detector_detect_file_prefers_filename_without_reading() {
    let detector = detector();

    let detected = detector
        .detect_file(
            Path::new("missing-document.pdf"),
            MimeDetectionPolicy::PreferFilename,
        )
        .expect("filename-preferred file detection should skip content reads");

    assert_eq!(Some("application/pdf".to_owned()), detected);
}

/// Verifies missing files produce an I/O error when content is required.
#[test]
fn test_magika_detector_detect_file_reports_missing_file_io_error() {
    let detector = detector();

    let error = detector
        .detect_file(
            Path::new("missing-file-without-extension"),
            MimeDetectionPolicy::VerifyContent,
        )
        .expect_err("missing file should report an I/O error");

    assert!(matches!(error, MimeError::Io(_)));
}

/// Verifies filesystem detection recognizes the real fixture corpus.
#[test]
fn test_magika_detector_detect_file_recognizes_real_fixture_files() {
    let detector = detector();

    for case in REAL_FILE_CASES {
        let path = fixture_path(case.relative_path);
        let detected = detector
            .detect_file(&path, MimeDetectionPolicy::VerifyContent)
            .expect("real fixture file detection should succeed");

        assert_eq!(
            Some(case.expected_mime.to_owned()),
            detected,
            "fixture {} should be detected correctly",
            case.relative_path,
        );
    }
}

/// Verifies reader detection recognizes fixtures and restores every position.
#[test]
fn test_magika_reader_detects_real_fixtures_without_consuming_position() {
    let detector = detector();

    for case in REAL_FILE_CASES {
        let path = fixture_path(case.relative_path);
        let mut file = File::open(&path).expect("real fixture file should be readable");
        file.seek(SeekFrom::Start(1))
            .expect("real fixture file should be seekable");

        let filename = path.to_string_lossy();
        let detected = detector
            .detect_reader(
                &mut file,
                Some(&filename),
                MimeDetectionPolicy::VerifyContent,
            )
            .expect("real fixture reader detection should succeed");

        assert_eq!(
            Some(case.expected_mime.to_owned()),
            detected,
            "fixture {} should be detected correctly from a reader",
            case.relative_path,
        );
        assert_eq!(
            1,
            file.stream_position()
                .expect("real fixture reader position should be readable"),
            "fixture {} reader position should be restored",
            case.relative_path,
        );
    }
}

/// Verifies Magika maps empty content to the empty-file MIME type.
#[test]
fn test_magika_detector_empty_content_returns_empty_file_mime_type() {
    let detector = detector();

    assert_eq!(
        Some("inode/x-empty".to_owned()),
        detector
            .detect_by_content(b"")
            .expect("content detection should succeed")
    );
}

/// Builds an absolute path to a crate fixture file.
///
/// # Parameters
///
/// * `relative_path` - Fixture path relative to the crate root.
///
/// # Returns
///
/// Absolute path to the requested fixture.
fn fixture_path(relative_path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative_path)
}
