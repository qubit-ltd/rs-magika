// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Ends an attached resource scope when its request lifecycle completes.

use qubit_operation::ResourceScope;

/// Owns the request's attached resource scope until it is explicitly ended.
pub(super) struct RequestResourceScopeGuard {
    /// Scope whose admission and cleanup are bounded to one request.
    scope: Option<ResourceScope>,
}

impl RequestResourceScopeGuard {
    /// Creates a guard for an attached request scope.
    pub(super) fn new(scope: ResourceScope) -> Self {
        Self { scope: Some(scope) }
    }

    /// Borrows the attached scope while its guard remains active.
    pub(super) fn scope(&self) -> &ResourceScope {
        self.scope
            .as_ref()
            .expect("an active request scope guard owns its scope")
    }

    /// Ends the scope and releases its parent quota exactly once.
    pub(super) fn end(&mut self) {
        if let Some(scope) = self.scope.take() {
            scope.end();
        }
    }
}

impl Drop for RequestResourceScopeGuard {
    /// Ends an untransferred scope on rejection or unwind.
    fn drop(&mut self) {
        self.end();
    }
}
