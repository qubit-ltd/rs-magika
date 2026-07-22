// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Integration tests for `qubit-magika`.

#[cfg(feature = "bundled-onnxruntime")]
mod internal;
mod magika_mime_detector_provider_tests;
#[cfg(feature = "bundled-onnxruntime")]
mod magika_mime_detector_tests;
#[cfg(feature = "bundled-onnxruntime")]
mod support;
