// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Async task notification helpers.

use std::task::Waker;

/// Registers a waker unless an equivalent waker is already present.
pub(super) fn register_waker(wakers: &mut Vec<Waker>, waker: &Waker) {
    if !wakers.iter().any(|registered| registered.will_wake(waker)) {
        wakers.push(waker.clone());
    }
}
