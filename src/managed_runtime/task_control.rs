// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async task notification helpers.

use std::task::Waker;

/// Registers a waker unless an equivalent waker is already present.
///
/// # Parameters
///
/// * `wakers` - Registered task wakers.
/// * `candidate` - Cloned waker to register when not already present. It is
///   taken only when a new registration is required.
pub(super) fn register_waker(wakers: &mut Vec<Waker>, candidate: &mut Option<Waker>) {
    let Some(waker) = candidate.as_ref() else {
        return;
    };
    if !wakers.iter().any(|registered| registered.will_wake(waker)) {
        if let Some(waker) = candidate.take() {
            wakers.push(waker);
        }
    }
}
