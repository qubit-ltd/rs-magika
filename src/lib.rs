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

pub(crate) mod internal;
#[cfg(feature = "inventory")]
mod inventory_submission;
mod magika_mime_detector;
mod magika_mime_detector_builder;
mod magika_mime_detector_provider;
mod managed_runtime;
#[cfg(test)]
mod tests;

pub use magika_mime_detector::MagikaMimeDetector;
pub use magika_mime_detector_builder::MagikaMimeDetectorBuilder;
pub use magika_mime_detector_provider::MagikaMimeDetectorProvider;
pub use managed_runtime::ManagedInferenceRuntime;
pub use managed_runtime::ManagedInferenceShutdownReport;
