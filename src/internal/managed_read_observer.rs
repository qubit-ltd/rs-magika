// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation and actual-I/O tracking for aggregate provider reads.

use std::ops::ControlFlow;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use qubit_fs::read::ReadIoEvent;
use qubit_fs::read::ReadIoObserver;
use qubit_fs::read::ReadIoPhase;

use crate::internal::StopWatcher;
use crate::managed_runtime::task_state::TaskState;

/// Tracks actual provider reads and stops before later aggregate I/O.
pub(crate) struct ManagedReadObserver {
    /// Managed request state set by runtime cancellation.
    state: Option<Arc<TaskState>>,
    /// Shared marker for an actual provider poll that has not reported Ready.
    in_flight: Arc<AtomicBool>,
    /// Scope stop state checked directly between provider operations.
    stop_watcher: Option<Arc<StopWatcher>>,
}

impl ManagedReadObserver {
    /// Creates an observer for one aggregate provider read.
    ///
    /// # Parameters
    ///
    /// * `state` - Optional runtime state used to observe managed cancellation.
    /// * `in_flight` - Shared marker updated for provider I/O events.
    /// * `stop_watcher` - Optional parent-scope cancellation watcher.
    ///
    /// # Returns
    ///
    /// An observer that reports provider events and stops before subsequent
    /// I/O when either cancellation source has fired.
    pub(crate) fn new(
        state: Option<Arc<TaskState>>,
        in_flight: Arc<AtomicBool>,
        stop_watcher: Option<Arc<StopWatcher>>,
    ) -> Self {
        Self {
            state,
            in_flight,
            stop_watcher,
        }
    }
}

impl ReadIoObserver for ManagedReadObserver {
    /// Stops the read loop when runtime or scope cancellation is visible.
    ///
    /// # Parameters
    ///
    /// * `_phase` - Provider operation that the aggregate reader is about to
    ///   start.
    ///
    /// # Returns
    ///
    /// `Break(())` after cancellation, otherwise `Continue(())`.
    fn before_io(&mut self, _phase: ReadIoPhase) -> ControlFlow<()> {
        let stopped = self
            .state
            .as_ref()
            .is_some_and(|state| !state.try_start_provider_operation())
            || self
                .stop_watcher
                .as_ref()
                .is_some_and(|watcher| watcher.is_stopped());
        if stopped {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    /// Updates the marker that distinguishes actual pending I/O from yields.
    ///
    /// # Parameters
    ///
    /// * `event` - Provider I/O lifecycle event reported by rs-fs.
    fn on_io_event(&mut self, event: ReadIoEvent) {
        match event {
            ReadIoEvent::Started { .. } => self.in_flight.store(true, Ordering::Release),
            ReadIoEvent::Pending { .. } => self.in_flight.store(true, Ordering::Release),
            ReadIoEvent::Ready { .. } => self.in_flight.store(false, Ordering::Release),
        }
    }
}
