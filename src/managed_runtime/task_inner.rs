// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mutex-protected mutable state for one inference child.

use std::task::Waker;

use qubit_mime::MimeResult;
use qubit_operation::report::ChildBusinessOutcome;

/// Business and resource facts observed by the control adapter.
pub(super) struct TaskInner {
    /// Whether the worker has started consuming this job.
    pub(super) started: bool,
    /// Whether cancellation has been requested.
    pub(super) stop_requested: bool,
    /// Result available to the inference future, if finished.
    pub(super) result: Option<MimeResult<Option<String>>>,
    /// Terminal business outcome reported to the operation scope.
    pub(super) business: Option<ChildBusinessOutcome>,
    /// Whether worker-owned input and permits have been dropped.
    pub(super) released: bool,
    /// Futures waiting for a result or release transition.
    pub(super) wakers: Vec<Waker>,
}

impl TaskInner {
    /// Creates an idle child state before worker admission.
    pub(super) const fn new() -> Self {
        Self {
            started: false,
            stop_requested: false,
            result: None,
            business: None,
            released: false,
            wakers: Vec::new(),
        }
    }
}
