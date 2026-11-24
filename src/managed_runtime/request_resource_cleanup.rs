// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cleanup policy for scope-owned request resources.

use qubit_operation::AsyncCleanup;
use qubit_operation::CleanupContext;
use qubit_operation::CleanupFuture;

use super::request_resource_owner::RequestResourceOwner;

/// Cleanup policy attached to each scope-owned request resource.
pub(super) struct RequestResourceCleanup;

impl AsyncCleanup<std::sync::Arc<RequestResourceOwner>> for RequestResourceCleanup {
    /// Applies the request owner's release or retention policy.
    ///
    /// # Parameters
    ///
    /// * `resource` - Request resources whose completion evidence is evaluated.
    /// * `_context` - Cleanup context supplied by the operation manager.
    ///
    /// # Type Parameters
    ///
    /// * `RequestResourceOwner` - Resource payload governed by this policy.
    fn cleanup(
        self: std::sync::Arc<Self>,
        resource: std::sync::Arc<RequestResourceOwner>,
        _context: CleanupContext,
    ) -> CleanupFuture<std::sync::Arc<RequestResourceOwner>> {
        Box::pin(async move { resource.cleanup_outcome() })
    }
}
