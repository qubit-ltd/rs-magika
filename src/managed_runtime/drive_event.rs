// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Events selected while driving worker sampling from an async caller.

use qubit_mime::MimeResult;

use crate::internal::async_sample_bridge::ReadRequest;

/// One event produced by the provider bridge or the worker result future.
pub(super) enum DriveEvent {
    /// A sample range requested by the worker, or bridge closure.
    Request(
        /// Request delivered to the async provider driver, if still open.
        Option<ReadRequest>,
    ),
    /// Terminal business result returned by the worker.
    Result(
        /// MIME inference result observed by the managed caller.
        MimeResult<Option<String>>,
    ),
}
