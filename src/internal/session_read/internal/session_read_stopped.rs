// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Error source for an actively stopped Provider session read.

/// Distinguishes a session stop from ordinary Provider I/O failures.
#[derive(Debug)]
pub(in crate::internal::session_read) struct SessionReadStopped;

impl std::fmt::Display for SessionReadStopped {
    /// Writes the session-stop marker into the supplied formatter.
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
        formatter.write_str("provider session read stopped")
    }
}

impl std::error::Error for SessionReadStopped {}
