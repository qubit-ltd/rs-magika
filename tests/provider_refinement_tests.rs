// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt::Debug;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_config::Config;
use qubit_fs::Path as FsPath;
use qubit_mime::CONFIG_MIME_AMBIGUOUS_MIME_MAPPING;
use qubit_mime::CONFIG_MIME_DETECTOR_DEFAULT;
use qubit_mime::CONFIG_MIME_ENABLE_PRECISE_DETECTION;
use qubit_mime::CONFIG_MIME_PRECISE_DETECTION_PATTERNS;
use qubit_mime::MediaStreamClassifierBackend;
use qubit_mime::MediaStreamType;
use qubit_mime::MimeConfig;
use qubit_mime::MimeDetectionPolicy;
use qubit_mime::MimeDetector;
use qubit_mime::MimeResult;

use crate::support::ProviderFileSystemSpi;
use crate::support::run_ready;

const SHELL_SCRIPT: &[u8] = b"#!/bin/sh\necho hello\n";

#[derive(Debug)]
struct CountingClassifier(Arc<AtomicUsize>);

impl MediaStreamClassifierBackend for CountingClassifier {
    fn classify_by_local_file(&self, _file: &Path) -> MimeResult<MediaStreamType> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(MediaStreamType::AudioOnly)
    }

    fn classify_by_content(&self, reader: &mut dyn Read) -> MimeResult<MediaStreamType> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes)?;
        Ok(MediaStreamType::AudioOnly)
    }
}

fn configured_detector(calls: Arc<AtomicUsize>) -> qubit_magika::MagikaMimeDetector {
    let mut raw_config = Config::new();
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
            "sh:Text/X-ShellScript,Audio/X-ShellScript",
        )
        .expect("ambiguous mapping should be configurable");
    let mime_config = MimeConfig::from_config(&raw_config).expect("refinement config should parse");
    qubit_magika::MagikaMimeDetector::builder()
        .mime_config(mime_config)
        .media_stream_classifier(Some(Arc::new(CountingClassifier(calls))))
        .build()
        .expect("bundled Magika session should initialize")
}

fn assert_provider_path_without_refinement(
    detector: &qubit_magika::MagikaMimeDetector,
    calls: &AtomicUsize,
    provider: ProviderFileSystemSpi,
    asynchronous: bool,
    expected: &str,
) {
    let path = FsPath::parse("/script.sh").expect("test path should be valid");
    let result = if asynchronous {
        run_ready(detector.detect_async_path(
            &provider.async_file_system(),
            &path,
            8192,
            MimeDetectionPolicy::VerifyContent,
        ))
    } else {
        detector.detect_path(&provider.file_system(), &path, 8192, MimeDetectionPolicy::VerifyContent)
    };
    assert_eq!(
        result.expect("provider path should detect content"),
        Some(expected.to_owned())
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "provider path must not call classifier"
    );
}

#[test]
fn test_provider_path_routes_do_not_refine_after_full_content_positive_control() {
    let calls = Arc::new(AtomicUsize::new(0));
    let detector = configured_detector(Arc::clone(&calls));

    assert_eq!(
        detector
            .detect_by_content(SHELL_SCRIPT)
            .expect("complete-content detection should succeed"),
        Some("Audio/X-ShellScript".to_owned())
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "positive control must call classifier");

    // Random-access session route.
    assert_provider_path_without_refinement(
        &detector,
        &calls,
        ProviderFileSystemSpi::new(SHELL_SCRIPT.to_vec()).with_random_access(),
        false,
        "text/x-shellscript",
    );
    assert_provider_path_without_refinement(
        &detector,
        &calls,
        ProviderFileSystemSpi::new(SHELL_SCRIPT.to_vec()).with_random_access(),
        true,
        "text/x-shellscript",
    );

    // Legacy range route.
    assert_provider_path_without_refinement(
        &detector,
        &calls,
        ProviderFileSystemSpi::new(SHELL_SCRIPT.to_vec()).with_range(),
        false,
        "text/x-shellscript",
    );
    assert_provider_path_without_refinement(
        &detector,
        &calls,
        ProviderFileSystemSpi::new(SHELL_SCRIPT.to_vec()).with_range(),
        true,
        "text/x-shellscript",
    );

    // Full read_all fallback route.
    assert_provider_path_without_refinement(
        &detector,
        &calls,
        ProviderFileSystemSpi::new(SHELL_SCRIPT.to_vec()),
        false,
        "text/x-shellscript",
    );
    assert_provider_path_without_refinement(
        &detector,
        &calls,
        ProviderFileSystemSpi::new(SHELL_SCRIPT.to_vec()),
        true,
        "text/x-shellscript",
    );
}
