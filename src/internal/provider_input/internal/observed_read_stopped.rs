// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Error source for an observer-reported Provider read stop.

/// Distinguishes an observer-reported stop from a Provider I/O interruption.
#[derive(Debug)]
pub(in crate::internal::provider_input) struct ObservedReadStopped;

impl std::fmt::Display for ObservedReadStopped {
    /// Writes the observer-stop marker into the supplied formatter.
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
        formatter.write_str("provider read stopped")
    }
}

impl std::error::Error for ObservedReadStopped {}
