// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! State retained while the managed inference worker is running or joined.

/// Retains the worker handle until exit and records the completed join result.
pub(super) struct WorkerState {
    /// Handle used to observe completion and reap the worker thread.
    pub(super) handle: Option<std::thread::JoinHandle<()>>,
    /// Whether the worker has exited and its handle has been joined.
    pub(super) stopped: bool,
    /// Whether joining the worker observed a thread panic.
    pub(super) panicked: bool,
}

impl WorkerState {
    /// Starts tracking a newly spawned worker thread.
    ///
    /// # Parameters
    ///
    /// * `handle` - Join handle for the session-owning worker.
    ///
    /// # Returns
    ///
    /// State that retains `handle` and reports neither stop nor panic yet.
    pub(super) fn new(handle: std::thread::JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
            stopped: false,
            panicked: false,
        }
    }
}
