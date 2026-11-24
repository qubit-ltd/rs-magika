// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Owns provider sampling resources and reports only confirmed completion.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::internal::async_sample_bridge::AsyncSampleDriver;
use crate::managed_runtime::task_state::TaskState;

/// Keeps the provider input and bridge driver alive until sampling has ended.
///
/// On drop, the provider input and bridge driver are destroyed locally and
/// driver completion is published. If provider I/O was still pending, its
/// native completion remains unknown, so request capacity stays reserved even
/// though the local future, input, and driver have been dropped.
///
/// # Type Parameters
///
/// * `I` - Provider input held while its bridge driver is active.
pub(super) struct ProviderDriverGuard<I> {
    /// Provider input borrowed by active read futures.
    pub(super) input: Option<I>,
    /// Bridge driver used to receive worker read requests.
    pub(super) driver: Option<AsyncSampleDriver>,
    /// Request lifecycle state notified when the driver completes.
    pub(super) state: Arc<TaskState>,
    /// Whether a polled provider read has not yet returned `Ready`.
    pub(super) unconfirmed_io: AtomicBool,
    /// Optional actual-provider I/O state exposed by an aggregate reader.
    pub(super) io_in_flight: Option<Arc<AtomicBool>>,
}

impl<I> ProviderDriverGuard<I> {
    /// Creates a guard for a successfully dispatched provider input.
    ///
    /// # Parameters
    ///
    /// * `input` - Provider input used to fulfill worker read requests.
    /// * `driver` - Bridge driver returned by the successful dispatch.
    /// * `state` - Request state whose driver completion is reported on drop.
    ///
    /// # Returns
    ///
    /// A guard that owns the input and driver until sampling ends, then drops
    /// both locally before publishing driver completion. Unconfirmed provider
    /// I/O is reported separately and keeps request capacity reserved.
    #[cfg(test)]
    pub(super) fn new(input: I, driver: AsyncSampleDriver, state: Arc<TaskState>) -> Self {
        Self::new_with_io_status(input, driver, state, None)
    }

    /// Creates a guard that distinguishes real provider Pending from yields.
    ///
    /// # Type Parameters
    ///
    /// * `I` - Provider input held while the bridge driver is active.
    ///
    /// # Parameters
    ///
    /// * `input` - Provider input used to fulfill worker read requests.
    /// * `driver` - Bridge driver returned by the successful dispatch.
    /// * `state` - Request state notified when the bridge driver completes.
    /// * `io_in_flight` - Optional marker for actual I/O status reported by an
    ///   aggregate reader.
    ///
    /// # Returns
    ///
    /// A guard that retains input resources while provider I/O is unconfirmed
    /// and drops both local values before publishing driver completion. A
    /// pending provider read remains unconfirmed; dropping its local future
    /// does not establish native I/O completion.
    pub(super) fn new_with_io_status(
        input: I,
        driver: AsyncSampleDriver,
        state: Arc<TaskState>,
        io_in_flight: Option<Arc<AtomicBool>>,
    ) -> Self {
        Self {
            input: Some(input),
            driver: Some(driver),
            state,
            unconfirmed_io: AtomicBool::new(false),
            io_in_flight,
        }
    }
}

/// Destroys local input and driver values, then reports completion facts.
impl<I> Drop for ProviderDriverGuard<I> {
    /// Drops the input and driver before publishing driver completion. A
    /// pending provider read is marked unconfirmed first; this notification
    /// does not claim that its native operation completed.
    fn drop(&mut self) {
        if self
            .io_in_flight
            .as_ref()
            .is_some_and(|status| status.load(Ordering::Acquire))
        {
            self.unconfirmed_io.store(true, Ordering::Relaxed);
        }
        // The read future is scoped inside the driver loop and drops before
        // this owner. Drop the input before closing the bridge, then publish
        // completion only when no provider-native work is unconfirmed.
        drop(self.input.take());
        drop(self.driver.take());
        if self.unconfirmed_io.load(Ordering::Relaxed) {
            self.state.mark_unconfirmed_provider_io();
        }
        self.state.mark_driver_done();
    }
}
