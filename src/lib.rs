// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Qubit Magika
//!
//! Magika-backed MIME detector integration for `qubit-mime`.
// qubit-style: allow coverage-cfg

mod magika_mime_detector;
mod magika_mime_detector_provider;

pub use magika_mime_detector::MagikaMimeDetector;
#[cfg(all(coverage, feature = "ort"))]
pub use magika_mime_detector::coverage_map_non_io_magika_error;
#[cfg(coverage)]
pub use magika_mime_detector::{
    coverage_map_session_lock_error,
    coverage_undefined_content_type_to_mime,
};
pub use magika_mime_detector_provider::MagikaMimeDetectorProvider;
#[cfg(coverage)]
pub use magika_mime_detector_provider::coverage_map_provider_create_error;
