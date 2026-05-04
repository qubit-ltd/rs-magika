/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/

use std::io::{
    Cursor,
    Error,
    ErrorKind,
    Read,
    Seek,
    SeekFrom,
    Write,
};
use std::path::Path;

use qubit_magika::{
    MagikaMimeDetector,
    MagikaMimeDetectorProvider,
    register_default_mime_detector,
    register_mime_detector,
};
use qubit_mime::{
    BoxMimeDetector,
    CONFIG_MIME_DETECTOR_DEFAULT,
    MimeConfig,
    MimeDetectionPolicy,
    MimeDetector,
    MimeDetectorProvider,
    MimeDetectorRegistry,
    MimeError,
};
use tempfile::NamedTempFile;

#[cfg(all(coverage, feature = "ort"))]
use qubit_magika::coverage_map_non_io_magika_error;
#[cfg(coverage)]
use qubit_magika::{
    coverage_map_session_lock_error,
    coverage_undefined_content_type_to_mime,
};

#[test]
fn test_provider_registers_magika_aliases_with_mime_registry() {
    let mut registry = MimeDetectorRegistry::builtin();
    register_mime_detector(&mut registry).expect("magika provider should register");

    assert!(registry.find_provider("magika").is_some());
    assert!(registry.find_provider("magika-mime-detector").is_some());
    assert!(registry.find_provider("MagikaMimeDetector").is_some());
}

#[test]
fn test_provider_creates_magika_detector_when_runtime_is_available() {
    let mut registry = MimeDetectorRegistry::builtin();
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
fn test_register_default_mime_detector_makes_wrappers_create_magika() {
    register_default_mime_detector().expect("magika provider should register globally");
    let config = detector_config("magika");

    let Ok(detector) = BoxMimeDetector::from_config(&config) else {
        return;
    };

    assert_eq!(
        Some("text/x-python".to_owned()),
        detector.detect_by_content(b"#!/usr/bin/env python3\nprint('hello')\n")
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
fn test_magika_detector_exposes_core_and_repository() {
    let Ok(mut detector) = MagikaMimeDetector::new() else {
        return;
    };

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

#[test]
fn test_magika_detector_detect_prefers_filename_without_content_detection() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };

    assert_eq!(
        Some("application/pdf".to_owned()),
        detector.detect(
            b"not a pdf",
            Some("document.pdf"),
            MimeDetectionPolicy::PreferFilename,
        )
    );
}

#[test]
fn test_magika_detector_detect_verifies_content_when_policy_requires() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };

    assert_eq!(
        Some("text/x-python".to_owned()),
        detector.detect(
            b"#!/usr/bin/env python3\nprint('hello')\n",
            Some("script.txt"),
            MimeDetectionPolicy::VerifyContent,
        )
    );
}

#[test]
fn test_magika_detector_reader_detection_prefers_filename_without_reading() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };
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
fn test_magika_detector_reader_detection_restores_position_after_read_error() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };
    let mut reader = FailingReadSeek::new(b"#!/bin/sh\necho hello\n".to_vec(), true, None);
    reader
        .seek(SeekFrom::Start(2))
        .expect("test reader should seek to original position");

    let error = detector
        .detect_reader(&mut reader, None, MimeDetectionPolicy::VerifyContent)
        .expect_err("reader read failure should be reported");

    assert!(matches!(error, MimeError::Io(_)));
    assert_eq!(2, reader.position());
}

#[test]
fn test_magika_detector_reader_detection_reports_restore_error() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };
    let mut reader = FailingReadSeek::new(b"#!/bin/sh\necho hello\n".to_vec(), false, Some(2));
    reader
        .seek(SeekFrom::Start(2))
        .expect("test reader should seek to original position");

    let error = detector
        .detect_reader(&mut reader, None, MimeDetectionPolicy::VerifyContent)
        .expect_err("reader restore failure should be reported");

    assert!(matches!(error, MimeError::Io(_)));
}

#[test]
fn test_magika_detector_detect_file_reads_content_when_policy_requires() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };
    let mut file = NamedTempFile::with_suffix(".txt").expect("temp file should be created");
    file.write_all(b"#!/usr/bin/env python3\nprint('hello')\n")
        .expect("temp file should be writable");

    let detected = detector
        .detect_file(file.path(), MimeDetectionPolicy::VerifyContent)
        .expect("file detection should succeed");

    assert_eq!(Some("text/x-python".to_owned()), detected);
}

#[test]
fn test_magika_detector_detect_file_prefers_filename_without_reading() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };

    let detected = detector
        .detect_file(
            Path::new("missing-document.pdf"),
            MimeDetectionPolicy::PreferFilename,
        )
        .expect("filename-preferred file detection should skip content reads");

    assert_eq!(Some("application/pdf".to_owned()), detected);
}

#[test]
fn test_magika_detector_detect_file_reports_missing_file_io_error() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };

    let error = detector
        .detect_file(
            Path::new("missing-file-without-extension"),
            MimeDetectionPolicy::VerifyContent,
        )
        .expect_err("missing file should report an I/O error");

    assert!(matches!(error, MimeError::Io(_)));
}

#[test]
fn test_magika_detector_empty_content_returns_empty_file_mime_type() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };

    assert_eq!(
        Some("inode/x-empty".to_owned()),
        detector.detect_by_content(b"")
    );
}

#[test]
fn test_provider_metadata_is_stable() {
    let provider = MagikaMimeDetectorProvider;

    assert_eq!("magika", provider.id());
    assert!(provider.aliases().contains(&"magika-mime-detector"));
    assert!(provider.priority() > 0);
}

#[test]
fn test_provider_creates_detector_directly() {
    let provider = MagikaMimeDetectorProvider;
    let config = MimeConfig::default();

    let Ok(detector) = provider.create(&config) else {
        return;
    };

    assert_eq!(
        Some("text/x-python".to_owned()),
        detector.detect_by_content(b"#!/usr/bin/env python3\nprint('hello')\n")
    );
}

#[test]
#[cfg(coverage)]
fn test_coverage_only_error_conversion_helpers_return_errors() {
    assert!(matches!(
        coverage_map_session_lock_error(),
        MimeError::DetectorBackend { .. },
    ));
    assert_eq!(None, coverage_undefined_content_type_to_mime());

    #[cfg(feature = "ort")]
    assert!(matches!(
        coverage_map_non_io_magika_error(),
        MimeError::DetectorBackend { .. },
    ));
}

/// Seekable reader that can fail reads or restoration seeks on demand.
#[derive(Debug)]
struct FailingReadSeek {
    /// Wrapped in-memory reader.
    inner: Cursor<Vec<u8>>,
    /// Whether reads should fail.
    fail_reads: bool,
    /// Start offset that should fail after a read has been attempted.
    fail_restore_to: Option<u64>,
    /// Whether a read has been attempted.
    read_attempted: bool,
}

impl FailingReadSeek {
    /// Creates a seekable reader with configurable failure behavior.
    fn new(content: Vec<u8>, fail_reads: bool, fail_restore_to: Option<u64>) -> Self {
        Self {
            inner: Cursor::new(content),
            fail_reads,
            fail_restore_to,
            read_attempted: false,
        }
    }

    /// Gets the current reader position.
    fn position(&self) -> u64 {
        self.inner.position()
    }
}

impl Read for FailingReadSeek {
    /// Reads from the wrapped cursor or returns a configured read error.
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.read_attempted = true;
        if self.fail_reads {
            Err(Error::new(ErrorKind::UnexpectedEof, "forced read failure"))
        } else {
            self.inner.read(buffer)
        }
    }
}

impl Seek for FailingReadSeek {
    /// Seeks in the wrapped cursor or returns a configured restore error.
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        if let SeekFrom::Start(offset) = position
            && self.read_attempted
            && Some(offset) == self.fail_restore_to
        {
            return Err(Error::other("forced restore failure"));
        }
        self.inner.seek(position)
    }
}

fn detector_config(default: &str) -> MimeConfig {
    let mut config = qubit_config::Config::new();
    config
        .set(CONFIG_MIME_DETECTOR_DEFAULT, default)
        .expect("detector default should be configurable");
    MimeConfig::from_config(&config).expect("detector config should parse")
}
