// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Events observed while waiting for an async provider read.

use magika::Error as MagikaError;
use qubit_mime::MimeResult;

/// One event that can finish an async provider read wait.
pub(super) enum ProviderReadEvent {
    /// Worker inference completed before the provider read.
    Result(
        /// MIME inference result produced by the worker.
        MimeResult<Option<String>>,
    ),
    /// A runtime or request stop was observed.
    Stopped,
    /// The provider bridge closed before the read completed.
    Closed,
    /// The provider read completed with its result.
    Read(
        /// Result returned by the provider read operation.
        Result<(), MagikaError>,
    ),
}
