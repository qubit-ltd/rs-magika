// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous provider-backed input for Magika feature extraction.

use std::future::Future;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use magika::AsyncInput;
use magika::Result as MagikaResult;
use qubit_fs::AsyncFileSystem;
use qubit_fs::Path;
use qubit_fs::metadata::ResourceVersion;
use qubit_fs::read::AsyncRandomAccessReader;

use super::async_route::AsyncRoute;
use super::provider_input::read_async_observed;
use super::read_budget::ReadBudget;
use super::session_read::read_async_session;
use super::session_read::read_async_session_with_stop;
use crate::managed_runtime::task_state::TaskState;
use qubit_fs::read::ReadIoEvent;
use qubit_fs::read::ReadIoObserver;
use qubit_fs::read::ReadIoPhase;

/// Tracks real provider reads while allowing stop between aggregate-read I/Os.
pub(crate) struct ManagedReadObserver {
    state: Option<Arc<TaskState>>,
    in_flight: Arc<AtomicBool>,
}

impl ManagedReadObserver {
    pub(crate) fn new(state: Option<Arc<TaskState>>, in_flight: Arc<AtomicBool>) -> Self {
        Self { state, in_flight }
    }
}

impl ReadIoObserver for ManagedReadObserver {
    fn before_io(&mut self, _phase: ReadIoPhase) -> ControlFlow<()> {
        if self.state.as_ref().is_some_and(|state| state.stop_requested()) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }

    fn on_io_event(&mut self, event: ReadIoEvent) {
        match event {
            ReadIoEvent::Started { .. } => self.in_flight.store(true, Ordering::Release),
            ReadIoEvent::Pending { .. } => self.in_flight.store(true, Ordering::Release),
            ReadIoEvent::Ready { .. } => self.in_flight.store(false, Ordering::Release),
        }
    }
}

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
    /// Tracks actual provider futures, excluding aggregate scheduling yields.
    io_in_flight: Option<Arc<AtomicBool>>,
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
            io_in_flight: Some(Arc::new(AtomicBool::new(false))),
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
            io_in_flight: None,
        }
    }

    /// Observes request cancellation between confirmed provider session reads.
    pub(crate) fn with_stop_state(mut self, state: Arc<TaskState>) -> Self {
        self.stop_state = Some(state);
        self
    }

    pub(crate) fn io_in_flight(&self) -> Option<Arc<AtomicBool>> {
        self.io_in_flight.as_ref().map(Arc::clone)
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
        match &mut self.route {
            AsyncRoute::OwnedLegacy { file_system, path } => {
                if let Some(io_in_flight) = &self.io_in_flight {
                    let mut observer =
                        ManagedReadObserver::new(self.stop_state.as_ref().map(Arc::clone), Arc::clone(io_in_flight));
                    read_async_observed(
                        file_system,
                        path,
                        self.length,
                        self.version.as_ref(),
                        &mut self.budget,
                        buffer,
                        offset,
                        &mut observer,
                    )
                    .await
                } else {
                    unreachable!("owned async provider inputs track observed I/O")
                }
            }
            AsyncRoute::Session { reader, max_segment } => {
                if let Some(state) = &self.stop_state {
                    read_async_session_with_stop(
                        reader,
                        self.length,
                        &mut self.budget,
                        buffer,
                        offset,
                        *max_segment,
                        || state.stop_requested(),
                    )
                    .await
                } else {
                    read_async_session(reader, self.length, &mut self.budget, buffer, offset, *max_segment).await
                }
            }
        }
    }
}
