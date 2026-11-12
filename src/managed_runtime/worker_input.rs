// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Input variants submitted to the managed inference worker.

use std::sync::Arc;

use magika::Error as MagikaError;
use magika::Features;
use qubit_mime::MimeError;

use crate::internal::async_sample_bridge::AsyncSampleBridge;

/// Owned work item consumed by the session-owning inference thread.
pub(super) enum WorkerInput {
    /// Input bytes whose sampling and features are owned by the worker.
    Bytes(
        /// Bounded payload retained while sampling and inference run.
        Box<[u8]>,
    ),
    /// Async provider input accessed through a capacity-one sampling bridge.
    Bridge(
        /// Bridge used to transfer provider ranges to the worker.
        Arc<AsyncSampleBridge>,
        /// Maximum bytes reserved for this provider request.
        usize,
        /// Route-specific Magika error conversion.
        fn(MagikaError) -> MimeError,
    ),
    /// Extracted features retained until session inference finishes.
    Features(
        /// Owned features extracted from the input payload.
        Features,
        /// Route-specific Magika error conversion.
        fn(MagikaError) -> MimeError,
    ),
}
