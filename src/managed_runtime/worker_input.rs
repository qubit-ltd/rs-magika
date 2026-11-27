// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Input variants submitted to the managed inference worker.

#[cfg(test)]
use std::sync::Arc;

use magika::Error as MagikaError;
use magika::Features;
use qubit_mime::MimeError;

#[cfg(test)]
use crate::internal::async_sample_bridge::AsyncSampleBridge;

/// Owned byte payload moved through the inference worker.
#[cfg(not(test))]
pub(crate) type WorkerBytes = Box<[u8]>;

/// Constructs the production byte payload without changing its representation.
///
/// # Parameters
///
/// * `bytes` - Owned byte slice retained by the worker.
#[cfg(not(test))]
pub(super) fn worker_bytes(bytes: Box<[u8]>) -> WorkerBytes {
    bytes
}

/// Test payload that can pause destruction after its bytes have been dropped.
#[cfg(test)]
pub(crate) struct WorkerBytes {
    /// Owned input bytes.
    bytes: Box<[u8]>,
    /// Optional test observer dropped after the byte allocation.
    drop_probe: Option<Box<dyn Send>>,
}

#[cfg(test)]
impl WorkerBytes {
    /// Creates a test input whose destruction is observable.
    pub(super) fn with_drop_probe(bytes: Box<[u8]>, drop_probe: Box<dyn Send>) -> Self {
        Self {
            bytes,
            drop_probe: Some(drop_probe),
        }
    }

    /// Converts the owned bytes into a vector for the test inference engine.
    pub(super) fn into_vec(self) -> Vec<u8> {
        let Self { bytes, drop_probe } = self;
        drop(drop_probe);
        bytes.into_vec()
    }
}

#[cfg(test)]
impl From<Box<[u8]>> for WorkerBytes {
    fn from(bytes: Box<[u8]>) -> Self {
        Self {
            bytes,
            drop_probe: None,
        }
    }
}

/// Constructs the test byte payload used by the worker.
///
/// # Parameters
///
/// * `bytes` - Owned byte slice retained by the worker.
#[cfg(test)]
pub(super) fn worker_bytes(bytes: Box<[u8]>) -> WorkerBytes {
    WorkerBytes::from(bytes)
}

#[cfg(test)]
impl From<Vec<u8>> for WorkerBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from(bytes.into_boxed_slice())
    }
}

#[cfg(test)]
impl std::ops::Deref for WorkerBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

/// Owned work item consumed by the session-owning inference thread.
pub(crate) enum WorkerInput {
    /// Input bytes whose sampling and features are owned by the worker.
    Bytes(
        /// Bounded payload retained while sampling and inference run.
        WorkerBytes,
    ),
    /// Test input that exercises the isolated sampling bridge.
    #[cfg(test)]
    Bridge(
        /// Bridge used to transfer provider ranges to the worker.
        Arc<AsyncSampleBridge>,
        /// Maximum bytes reserved for this provider request.
        usize,
        /// Route-specific Magika error conversion.
        fn(MagikaError) -> MimeError,
    ),
    /// MIME result already determined by Magika's sampling rules.
    Ruled(
        /// MIME mapping, or `None` when the rules identify an unmapped type.
        Option<String>,
    ),
    /// Extracted features retained until session inference finishes.
    Features(
        /// Owned features extracted from the input payload.
        Features,
        /// Route-specific Magika error conversion.
        fn(MagikaError) -> MimeError,
    ),
}
