// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous checks for an operation scope's cancellation state.

use std::sync::Arc;

use qubit_operation::OperationContext;

/// Reads scope cancellation directly at provider operation boundaries.
pub(crate) struct StopWatcher {
    /// Context whose stop state belongs to the managed provider request.
    context: OperationContext,
}

impl StopWatcher {
    /// Creates a watcher sharing the supplied operation context's stop tree.
    ///
    /// # Parameters
    ///
    /// * `context` - Operation context whose stop state should be observed.
    ///
    /// # Returns
    ///
    /// A shared watcher that can check cancellation synchronously.
    pub(crate) fn new(context: OperationContext) -> Arc<Self> {
        Arc::new(Self { context })
    }

    /// Reports whether any ancestor or this operation has accepted a stop.
    ///
    /// # Returns
    ///
    /// `true` when the operation context has a stop reason, or `false` while
    /// the context remains active.
    pub(crate) fn is_stopped(&self) -> bool {
        self.context.stop_reason().is_some()
    }
}
