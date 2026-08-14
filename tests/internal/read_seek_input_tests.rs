// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::io::Seek;
use std::io::SeekFrom;

use qubit_mime::MimeDetectionPolicy;
use qubit_mime::MimeDetector;
use qubit_mime::MimeError;

use crate::support::FailingReadSeek;
use crate::support::detector;

/// Verifies the reader adapter restores its original position after a read
/// error.
#[test]
fn test_read_seek_input_restores_position_after_read_error() {
    let mut reader =
        FailingReadSeek::new(b"#!/bin/sh\necho hello\n".to_vec(), true, None);
    reader
        .seek(SeekFrom::Start(2))
        .expect("test reader should seek to original position");

    let error = detector()
        .detect_reader(&mut reader, None, MimeDetectionPolicy::VerifyContent)
        .expect_err("reader read failure should be reported");

    assert!(matches!(error, MimeError::Io(_)));
    assert_eq!(2, reader.position());
}

/// Verifies the reader adapter reports a failed restoration seek.
#[test]
fn test_read_seek_input_reports_restore_error() {
    let mut reader = FailingReadSeek::new(
        b"#!/bin/sh\necho hello\n".to_vec(),
        false,
        Some(2),
    );
    reader
        .seek(SeekFrom::Start(2))
        .expect("test reader should seek to original position");

    let error = detector()
        .detect_reader(&mut reader, None, MimeDetectionPolicy::VerifyContent)
        .expect_err("reader restore failure should be reported");

    assert!(matches!(error, MimeError::Io(_)));
}
