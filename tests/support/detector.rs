// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::OnceLock;

use qubit_magika::MagikaMimeDetector;

/// Returns the shared detector used by inference tests.
///
/// # Returns
///
/// A detector initialized once with the bundled ONNX Runtime.
pub(crate) fn detector() -> &'static MagikaMimeDetector {
    static DETECTOR: OnceLock<MagikaMimeDetector> = OnceLock::new();
    DETECTOR.get_or_init(|| MagikaMimeDetector::new().expect("bundled ONNX Runtime should initialize Magika"))
}
