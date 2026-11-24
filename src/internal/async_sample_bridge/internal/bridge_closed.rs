// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Error source that distinguishes a bridge close from Provider interruption.

/// Identifies an artificial bridge close rather than Provider I/O returning
/// `Interrupted`.
#[derive(Debug)]
pub(in crate::internal::async_sample_bridge) struct BridgeClosed;

impl std::fmt::Display for BridgeClosed {
    /// Writes the bridge-close marker into the supplied formatter.
    ///
    /// # Parameters
    ///
    /// * `formatter` - Destination for the marker's diagnostic text.
    ///
    /// # Returns
    ///
    /// The result returned by the formatter.
    ///
    /// # Errors
    ///
    /// Returns the formatting error if the destination cannot accept the text.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("async sample bridge closed")
    }
}

impl std::error::Error for BridgeClosed {}
