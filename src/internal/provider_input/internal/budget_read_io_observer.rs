// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Counts completed provider operations while preserving the caller observer.

use std::ops::ControlFlow;

use qubit_fs::read::ReadIoEvent;
use qubit_fs::read::ReadIoObserver;
use qubit_fs::read::ReadIoPhase;

/// Forwards observer callbacks and counts completed Open and Read operations.
///
/// # Type Parameters
/// * `'a` - Lifetime of the wrapped observer borrow.
/// * `O` - Observer receiving the forwarded callbacks.
pub struct BudgetReadIoObserver<'a, O> {
    /// Wrapped observer receiving the forwarded callbacks.
    observer: &'a mut O,
    /// Number of completed operations in the current accounting interval.
    completed_events: usize,
}

impl<'a, O> BudgetReadIoObserver<'a, O> {
    /// Wraps an observer with a zeroed completed-operation count.
    ///
    /// # Parameters
    /// * `observer` - Observer receiving forwarded callbacks.
    ///
    /// # Returns
    /// A wrapper with no completed operations recorded.
    pub fn new(observer: &'a mut O) -> Self {
        Self {
            observer,
            completed_events: 0,
        }
    }

    /// Returns completed operations since the last cooperative yield.
    pub fn completed_events(&self) -> usize {
        self.completed_events
    }

    /// Starts a new accounting interval after a segment-boundary yield.
    pub fn reset(&mut self) {
        self.completed_events = 0;
    }
}

impl<O: ReadIoObserver> ReadIoObserver for BudgetReadIoObserver<'_, O> {
    /// Forwards the pre-I/O decision to the wrapped observer.
    ///
    /// # Parameters
    /// * `phase` - I/O phase about to start.
    fn before_io(&mut self, phase: ReadIoPhase) -> ControlFlow<()> {
        self.observer.before_io(phase)
    }

    /// Counts ready events and forwards the event to the wrapped observer.
    ///
    /// # Parameters
    /// * `event` - I/O event emitted by the provider.
    fn on_io_event(&mut self, event: ReadIoEvent) {
        if matches!(event, ReadIoEvent::Ready { .. }) {
            self.completed_events += 1;
        }
        self.observer.on_io_event(event);
    }

    /// Forwards the byte count to the wrapped observer.
    ///
    /// # Parameters
    /// * `bytes` - Number of bytes read.
    fn on_read_bytes(&mut self, bytes: usize) {
        self.observer.on_read_bytes(bytes);
    }
}
