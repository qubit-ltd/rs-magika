// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous provider-backed input for Magika feature extraction.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use magika::AsyncInput;
use magika::Result as MagikaResult;
use qubit_fs::AsyncFileSystem;
use qubit_fs::Path;
use qubit_fs::metadata::ResourceVersion;
use qubit_fs::read::AsyncRandomAccessReader;

use super::AsyncSessionReadOptions;
use super::async_route::AsyncRoute;
use super::managed_read_observer::ManagedReadObserver;
use super::provider_input::read_async_observed;
use super::provider_input::read_options;
use super::read_budget::ReadBudget;
use super::session_read::read_async_session_with_stop;
use super::stop_watcher::StopWatcher;
use crate::managed_runtime::task_state::TaskState;

/// Adapts asynchronous filesystem ranges to Magika's random-access input.
pub(crate) struct AsyncProviderInput {
    /// Filesystem supplying the requested ranges.
    route: AsyncRoute,
    /// Complete logical resource length reported to Magika.
    length: u64,
    /// Optional version constraint applied to each range read.
    version: Option<ResourceVersion>,
    /// Cumulative limit shared by all reads in one detection.
    budget: ReadBudget,
    /// Managed request state used to stop between confirmed session I/Os.
    stop_state: Option<Arc<TaskState>>,
    /// Scope stop state read directly between aggregate provider I/Os.
    stop_watcher: Option<Arc<StopWatcher>>,
    /// Tracks actual provider futures, excluding aggregate scheduling yields.
    io_in_flight: Arc<AtomicBool>,
}

impl AsyncProviderInput {
    /// Creates an owned filesystem input suitable for managed operation
    /// submission, whose body must be `'static`.
    ///
    /// # Parameters
    ///
    /// * `file_system` - Filesystem used to open provider ranges.
    /// * `path` - Resource path used for provider access.
    /// * `length` - Input length reported by the provider.
    /// * `version` - Optional version used to guard conditional reads.
    /// * `limit` - Maximum bytes allowed for sampling.
    pub(crate) fn new_owned(
        file_system: AsyncFileSystem,
        path: Path,
        length: u64,
        version: Option<ResourceVersion>,
        limit: usize,
    ) -> Self {
        AsyncProviderInput {
            route: AsyncRoute::OwnedLegacy { file_system, path },
            length,
            version,
            budget: ReadBudget::new(limit),
            stop_state: None,
            stop_watcher: None,
            io_in_flight: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Creates a session-backed input retaining the opened object through
    /// feature extraction.
    ///
    /// # Parameters
    ///
    /// * `reader` - Random-access session opened for this provider path.
    /// * `limit` - Maximum cumulative bytes reserved across reads.
    /// * `max_segment` - Optional finite maximum for an individual provider
    ///   read.
    ///
    /// # Returns
    /// An asynchronous input using the session's open-time length.
    pub(crate) fn from_session(reader: AsyncRandomAccessReader, limit: usize, max_segment: Option<usize>) -> Self {
        let length = reader.len();
        Self {
            route: AsyncRoute::Session { reader, max_segment },
            length,
            version: None,
            budget: ReadBudget::new(limit),
            stop_state: None,
            stop_watcher: None,
            io_in_flight: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Observes request cancellation between confirmed provider session reads.
    ///
    /// # Parameters
    ///
    /// * `state` - Runtime state that is set when this request is cancelled.
    ///
    /// # Returns
    ///
    /// This input configured to stop before later session reads.
    pub(crate) fn with_stop_state(mut self, state: Arc<TaskState>) -> Self {
        self.stop_state = Some(state);
        self
    }

    /// Observes operation-scope cancellation between provider operations.
    ///
    /// # Parameters
    ///
    /// * `watcher` - Scope cancellation state checked at each I/O boundary.
    ///
    /// # Returns
    ///
    /// This input configured to stop before later provider I/O after the
    /// operation scope accepts cancellation.
    pub(crate) fn with_stop_watcher(mut self, watcher: Arc<StopWatcher>) -> Self {
        self.stop_watcher = Some(watcher);
        self
    }

    /// Clones the actual-I/O marker shared with the managed driver.
    ///
    /// # Returns
    ///
    /// A shared marker that is true only while provider I/O is being polled.
    pub(crate) fn io_in_flight(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.io_in_flight)
    }
}

impl AsyncInput for AsyncProviderInput {
    /// Returns the complete logical resource length captured at construction.
    ///
    /// # Returns
    ///
    /// The length of the provider resource.
    #[inline]
    fn length(&self) -> impl Future<Output = MagikaResult<u64>> {
        std::future::ready(Ok(self.length))
    }

    /// Reads one validated range from the provider.
    ///
    /// Reserves bytes before asynchronous I/O and rejects short ranges or
    /// provider failures as Magika I/O errors.
    ///
    /// # Parameters
    ///
    /// * `buffer` - Destination for the requested range.
    /// * `offset` - Start offset of the requested range in the resource.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the range is invalid, exceeds the cumulative
    /// budget, or cannot be read exactly from the provider.
    async fn read_at(&mut self, buffer: &mut [u8], offset: u64) -> MagikaResult<()> {
        let stop_state = self.stop_state.as_ref().map(Arc::clone);
        let stop_watcher = self.stop_watcher.as_ref().map(Arc::clone);
        let io_in_flight = Arc::clone(&self.io_in_flight);
        match &mut self.route {
            AsyncRoute::OwnedLegacy { file_system, path } => {
                let mut observer = ManagedReadObserver::new(stop_state, io_in_flight, stop_watcher);
                read_async_observed(
                    file_system,
                    path,
                    self.length,
                    read_options(offset, buffer.len(), self.version.as_ref()),
                    &mut self.budget,
                    buffer,
                    &mut observer,
                )
                .await
            }
            AsyncRoute::Session { reader, max_segment } => {
                read_async_session_with_stop(
                    reader,
                    self.length,
                    &mut self.budget,
                    buffer,
                    offset,
                    AsyncSessionReadOptions {
                        max_segment: *max_segment,
                        should_stop: move || {
                            stop_state
                                .as_ref()
                                .is_some_and(|state| !state.try_start_provider_operation())
                                || stop_watcher.as_ref().is_some_and(|watcher| watcher.is_stopped())
                        },
                        io_in_flight: Some(io_in_flight.as_ref()),
                    },
                )
                .await
            }
        }
    }
}
