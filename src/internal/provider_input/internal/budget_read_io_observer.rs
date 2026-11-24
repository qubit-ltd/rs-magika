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
pub struct BudgetReadIoObserver<'a, O> {
    observer: &'a mut O,
    completed_events: usize,
}

impl<'a, O> BudgetReadIoObserver<'a, O> {
    /// Wraps an observer with a zeroed completed-operation count.
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
    fn before_io(&mut self, phase: ReadIoPhase) -> ControlFlow<()> {
        self.observer.before_io(phase)
    }

    fn on_io_event(&mut self, event: ReadIoEvent) {
        if matches!(event, ReadIoEvent::Ready { .. }) {
            self.completed_events += 1;
        }
        self.observer.on_io_event(event);
    }

    fn on_read_bytes(&mut self, bytes: usize) {
        self.observer.on_read_bytes(bytes);
    }
}
