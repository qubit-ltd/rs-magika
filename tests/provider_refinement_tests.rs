// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt::Debug;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_config::Config;
use qubit_fs::Path as FsPath;
use qubit_magika::MagikaMimeDetectorProvider;
use qubit_mime::CONFIG_MIME_AMBIGUOUS_MIME_MAPPING;
use qubit_mime::CONFIG_MIME_DETECTOR_DEFAULT;
use qubit_mime::CONFIG_MIME_ENABLE_PRECISE_DETECTION;
use qubit_mime::CONFIG_MIME_PRECISE_DETECTION_PATTERNS;
use qubit_mime::MediaStreamClassifier;
use qubit_mime::MediaStreamClassifierBackend;
use qubit_mime::MediaStreamType;
use qubit_mime::MimeConfig;
use qubit_mime::MimeDetectionPolicy;
use qubit_mime::MimeDetector;
use qubit_mime::MimeDetectorRegistry;
use qubit_mime::MimeResult;
use qubit_spi::ProviderSelection;
use tempfile::tempdir;

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
            "sh:application/x-shellscript,text/x-shellscript",
        )
        .expect("ambiguous mapping should be configurable");
    let mime_config = MimeConfig::from_config(&raw_config).expect("refinement config should parse");
    configured_detector_with_classifier(mime_config, Arc::new(CountingClassifier(calls)))
}

fn configured_detector_with_classifier(
    mime_config: MimeConfig,
    classifier: Arc<dyn MediaStreamClassifier>,
) -> qubit_magika::MagikaMimeDetector {
    qubit_magika::MagikaMimeDetector::builder()
        .mime_config(mime_config)
        .media_stream_classifier(Some(classifier))
        .build()
        .expect("bundled Magika session should initialize")
}

#[derive(Debug)]
struct OverrideClassifier {
    content_calls: Arc<AtomicUsize>,
    reader_calls: Arc<AtomicUsize>,
}

impl MediaStreamClassifier for OverrideClassifier {
    fn classify_file(&self, _file: &Path) -> MimeResult<MediaStreamType> {
        Ok(MediaStreamType::None)
    }

    fn classify_reader(&self, _reader: &mut dyn Read) -> MimeResult<MediaStreamType> {
        self.reader_calls.fetch_add(1, Ordering::SeqCst);
        Ok(MediaStreamType::AudioOnly)
    }

    fn classify_content(&self, _content: &[u8]) -> MimeResult<MediaStreamType> {
        self.content_calls.fetch_add(1, Ordering::SeqCst);
        Ok(MediaStreamType::VideoOnly)
    }
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
        Some("text/x-shellscript".to_owned())
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

#[test]
fn test_registry_configured_classifier_refines_content() {
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
            "sh:application/x-shellscript,text/x-shellscript",
        )
        .expect("ambiguous mapping should be configurable");
    let config = MimeConfig::from_config(&raw_config).expect("refinement config should parse");

    let calls = Arc::new(AtomicUsize::new(0));
    let registry = MimeDetectorRegistry::default();
    registry
        .register(MagikaMimeDetectorProvider::with_media_stream_classifier(Arc::new(
            OverrideClassifier {
                content_calls: Arc::clone(&calls),
                reader_calls: Arc::new(AtomicUsize::new(0)),
            },
        )))
        .expect("configured Magika provider should register");
    let selection = ProviderSelection::named("magika").expect("canonical selection should parse");
    let provider = registry
        .resolve_selected(&selection)
        .expect("canonical magika provider should resolve");
    let detector = provider
        .create_configured(&config)
        .expect("configured provider should create its detector");

    assert_eq!(
        Some("application/x-shellscript".to_owned()),
        detector
            .detect_by_content(SHELL_SCRIPT)
            .expect("registry detector should refine content"),
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "registry path must pass classifier");

    // With precise detection disabled, the initial candidate stays at the
    // other shell-script mapping, so a missing classifier cannot pass unnoticed.
    let mut control_raw_config = Config::new();
    control_raw_config
        .set(CONFIG_MIME_DETECTOR_DEFAULT, "magika")
        .expect("detector default should be configurable");
    control_raw_config
        .set(CONFIG_MIME_ENABLE_PRECISE_DETECTION, false)
        .expect("precise detection should be disableable");
    control_raw_config
        .set(CONFIG_MIME_PRECISE_DETECTION_PATTERNS, "sh")
        .expect("precise patterns should be configurable");
    control_raw_config
        .set(
            CONFIG_MIME_AMBIGUOUS_MIME_MAPPING,
            "sh:application/x-shellscript,text/x-shellscript",
        )
        .expect("ambiguous mapping should be configurable");
    let control_config = MimeConfig::from_config(&control_raw_config).expect("control config should parse");
    let initial_detector = qubit_magika::MagikaMimeDetector::builder()
        .mime_config(control_config)
        .build()
        .expect("control detector should initialize");
    assert_eq!(
        Some("text/x-shellscript".to_owned()),
        initial_detector
            .detect_by_content(SHELL_SCRIPT)
            .expect("initial candidate detection should succeed"),
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn test_local_file_refinement_applies_only_to_regular_files() {
    let directory = tempdir().expect("temporary directory should be created");
    let file = directory.path().join("script.sh");
    fs::write(&file, SHELL_SCRIPT).expect("shell script should be written");
    let same_name_directory = directory.path().join("folder.sh");
    fs::create_dir(&same_name_directory).expect("same-name directory should be created");

    let calls = Arc::new(AtomicUsize::new(0));
    let detector = configured_detector(Arc::clone(&calls));
    assert!(format!("{detector:?}").contains("RegularFileMediaStreamClassifier"));

    assert_eq!(
        detector
            .detect_file(&file, MimeDetectionPolicy::Freedesktop)
            .expect("ordinary file detection should succeed")
            .as_deref(),
        Some("text/x-shellscript")
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let directory_candidate = detector
        .detect_by_filename(same_name_directory.to_str().expect("path should be UTF-8"))
        .expect("filename detection should succeed")
        .expect("directory filename should have a MIME mapping");
    assert_eq!(
        detector
            .detect_file(&same_name_directory, MimeDetectionPolicy::Freedesktop)
            .expect("directory detection should fall back to its filename")
            .expect("directory filename fallback should return a MIME type"),
        directory_candidate
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        detector
            .detect_file(&same_name_directory, MimeDetectionPolicy::VerifyContent)
            .expect("directory content verification should succeed without content"),
        None
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let link = directory.path().join("linked.sh");
        symlink(&file, &link).expect("stable symlink should be created");
        let link_candidate = detector
            .detect_by_filename(link.to_str().expect("path should be UTF-8"))
            .expect("symlink filename detection should succeed")
            .expect("symlink filename should have a MIME mapping");
        assert_eq!(
            detector
                .detect_file(&link, MimeDetectionPolicy::Freedesktop)
                .expect("symlink detection should fall back to its filename")
                .expect("symlink filename fallback should return a MIME type"),
            link_candidate
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            detector
                .detect_file(&link, MimeDetectionPolicy::VerifyContent)
                .expect("symlink content verification should succeed without content"),
            None
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn test_local_file_adapter_preserves_content_and_reader_overrides() {
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
            "sh:application/x-shellscript,text/x-shellscript",
        )
        .expect("ambiguous mapping should be configurable");
    let mime_config = MimeConfig::from_config(&raw_config).expect("refinement config should parse");
    let content_calls = Arc::new(AtomicUsize::new(0));
    let reader_calls = Arc::new(AtomicUsize::new(0));
    let detector = configured_detector_with_classifier(
        mime_config,
        Arc::new(OverrideClassifier {
            content_calls: Arc::clone(&content_calls),
            reader_calls: Arc::clone(&reader_calls),
        }),
    );

    assert_eq!(
        detector
            .detect_by_content(SHELL_SCRIPT)
            .expect("content detection should succeed"),
        Some("application/x-shellscript".to_owned())
    );
    assert_eq!(content_calls.load(Ordering::SeqCst), 1);
    assert_eq!(reader_calls.load(Ordering::SeqCst), 0);

    let mut reader = std::io::Cursor::new(SHELL_SCRIPT);
    assert_eq!(
        detector
            .detect_reader(&mut reader, Some("script.sh"), MimeDetectionPolicy::Freedesktop)
            .expect("reader detection should succeed"),
        Some("text/x-shellscript".to_owned())
    );
    assert_eq!(content_calls.load(Ordering::SeqCst), 1);
    assert_eq!(reader_calls.load(Ordering::SeqCst), 1);
}
