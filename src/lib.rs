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

mod internal;
mod magika_mime_detector;
mod magika_mime_detector_builder;
mod magika_mime_detector_provider;

pub use magika_mime_detector::MagikaMimeDetector;
pub use magika_mime_detector_builder::MagikaMimeDetectorBuilder;
pub use magika_mime_detector_provider::MagikaMimeDetectorProvider;
