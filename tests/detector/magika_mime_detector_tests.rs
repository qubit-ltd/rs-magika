// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fs::File;
use std::io::{
    Cursor,
    Error,
    ErrorKind,
    Read,
    Seek,
    SeekFrom,
    Write,
};
use std::path::{
    Path,
    PathBuf,
};

use qubit_magika::{
    MagikaMimeDetector,
    MagikaMimeDetectorProvider,
};
use qubit_mime::{
    CONFIG_MIME_DETECTOR_DEFAULT,
    MimeConfig,
    MimeDetectionPolicy,
    MimeDetector,
    MimeDetectorRegistry,
    MimeDetectorSpec,
    MimeError,
};
use qubit_spi::{
    ProviderMetadata,
    ProviderRegistry,
    ProviderSelection,
    ServiceProvider,
};
use tempfile::NamedTempFile;

#[cfg(all(coverage, feature = "ort"))]
use qubit_magika::coverage_map_non_io_magika_error;
#[cfg(coverage)]
use qubit_magika::{
    coverage_map_provider_create_error,
    coverage_map_session_lock_error,
    coverage_undefined_content_type_to_mime,
};

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

/// Expected MIME result for a real fixture file.
#[derive(Debug)]
struct RealFileCase {
    /// Path relative to the crate root.
    relative_path: &'static str,
    /// Expected MIME type name.
    expected_mime: &'static str,
}

#[test]
fn test_provider_registers_magika_aliases_with_mime_registry() {
    let registry = ProviderRegistry::<MimeDetectorSpec>::default();
    registry
        .register(MagikaMimeDetectorProvider)
        .expect("magika provider should register");

    for selector in ["magika", "magika-mime-detector", "MagikaMimeDetector"] {
        let selection = ProviderSelection::named(selector)
            .expect("Magika selector should be valid");
        assert!(
            registry.resolve_selected(&selection).is_ok(),
            "selector {selector} should resolve",
        );
    }
}

#[test]
fn test_provider_creates_magika_detector_when_runtime_is_available() {
    let registry = MimeDetectorRegistry::default();
    registry
        .register(MagikaMimeDetectorProvider)
        .expect("magika provider should register");
    let config = detector_config("magika");
    let provider = registry
        .resolve_selected(config.mime_detector_selection())
        .expect("configured Magika provider should resolve");

    let Ok(detector) = provider.create_configured(&config) else {
        return;
    };

    assert_eq!(
        Some("text/x-python".to_owned()),
        detector.detect_by_content(b"#!/usr/bin/env python3\nprint('hello')\n"),
    );
}

#[test]
fn test_explicit_provider_assembly_creates_magika_without_global_state() {
    let registry = MimeDetectorRegistry::default();
    registry
        .register(MagikaMimeDetectorProvider)
        .expect("magika provider should register");
    let selection =
        ProviderSelection::named("magika").expect("selection should be valid");
    let provider = registry
        .resolve_selected(&selection)
        .expect("Magika provider should resolve");

    let Ok(detector) = provider.create() else {
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
        .expect(
            "filename-preferred reader detection should skip content reads",
        );

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
    let mut reader =
        FailingReadSeek::new(b"#!/bin/sh\necho hello\n".to_vec(), true, None);
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
    let mut reader = FailingReadSeek::new(
        b"#!/bin/sh\necho hello\n".to_vec(),
        false,
        Some(2),
    );
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
    let mut file = NamedTempFile::with_suffix(".txt")
        .expect("temp file should be created");
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
fn test_magika_detector_detect_file_recognizes_real_fixture_files() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };

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

#[test]
fn test_magika_reader_detects_real_fixtures_without_consuming_position() {
    let Ok(detector) = MagikaMimeDetector::new() else {
        return;
    };

    for case in REAL_FILE_CASES {
        let path = fixture_path(case.relative_path);
        let mut file =
            File::open(&path).expect("real fixture file should be readable");
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
    let descriptor = MagikaMimeDetectorProvider.descriptor();

    assert_eq!("magika", descriptor.id().as_str());
    assert!(
        descriptor
            .aliases()
            .iter()
            .any(|alias| alias.as_str() == "magika-mime-detector")
    );
    assert!(descriptor.priority() > 0);
}

#[test]
fn test_provider_creates_detector_directly() {
    let provider = MagikaMimeDetectorProvider;
    let config = MimeConfig::default();

    let Ok(detector) = provider.create_configured(&config) else {
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
    let provider_error = coverage_map_provider_create_error();
    assert!(
        provider_error
            .to_string()
            .contains("coverage provider failure")
    );
    assert!(std::error::Error::source(&provider_error).is_some(),);

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
    fn new(
        content: Vec<u8>,
        fail_reads: bool,
        fail_restore_to: Option<u64>,
    ) -> Self {
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

/// Builds an absolute path to a crate fixture file.
fn fixture_path(relative_path: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative_path)
}
