// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Logical reader windows used by the detector backends.

/// Defines the logical resource exposed to a reader based backend.
#[derive(Clone, Copy)]
pub(crate) enum ReaderScope {
    /// The whole seekable resource, starting at byte zero.
    WholeResource,
    /// The resource from the caller's current position to EOF.
    Remaining,
}
