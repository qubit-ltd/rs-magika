// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Admission and shutdown gate shared with provider starts.

/// Shared admission and cancellation state serialized with provider starts.
pub(super) struct AdmissionGate {
    /// Rejects new requests after runtime shutdown begins.
    pub(super) closed: bool,
    /// Prevents provider I/O starts after cancellation begins.
    pub(super) cancelling: bool,
}

impl AdmissionGate {
    /// Creates an open gate that admits requests and provider starts.
    pub(super) const fn new() -> Self {
        Self {
            closed: false,
            cancelling: false,
        }
    }
}
