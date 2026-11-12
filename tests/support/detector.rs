// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::OnceLock;

use qubit_magika::MagikaMimeDetector;

/// Returns the thread-local detector used by inference tests.
///
/// # Returns
///
/// A detector initialized once per test thread with the bundled ONNX Runtime.
#[must_use]
pub(crate) fn detector() -> &'static MagikaMimeDetector {
    thread_local! {
        static DETECTOR: OnceLock<&'static MagikaMimeDetector> = const { OnceLock::new() };
    }
    DETECTOR.with(|detector| {
        *detector.get_or_init(|| {
            Box::leak(Box::new(
                MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize Magika"),
            ))
        })
    })
}
