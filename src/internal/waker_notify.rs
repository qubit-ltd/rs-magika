// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Panic-isolated notification for externally supplied task wakers.

use std::panic::AssertUnwindSafe;
use std::task::Waker;

/// Wakes one task while preventing an external waker panic from interrupting
/// the caller's state transition or cleanup.
pub(crate) fn wake(waker: Waker) {
    let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(|| waker.wake())) else {
        return;
    };
    drop_panic_payload(payload);
}

/// Wakes every task, continuing after a faulty external waker.
pub(crate) fn wake_all(wakers: impl IntoIterator<Item = Waker>) {
    for waker in wakers {
        wake(waker);
    }
}

/// Drops a caught panic payload without allowing a custom payload destructor
/// to interrupt notification cleanup. If that destructor panics, forgetting
/// its second payload stops recursive destructor panics.
fn drop_panic_payload(payload: Box<dyn std::any::Any + Send>) {
    if let Err(second_payload) = std::panic::catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        // The second panic is caused by destruction of the first payload. Its
        // own payload may have the same destructor, so deliberately leak it.
        std::mem::forget(second_payload);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;
    use std::task::Waker;

    use super::wake_all;

    struct PayloadProbe(Arc<AtomicUsize>);

    impl Drop for PayloadProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("injected panic payload destructor panic");
        }
    }

    struct PanickingWake(Arc<AtomicUsize>);

    impl Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            std::panic::panic_any(PayloadProbe(Arc::clone(&self.0)));
        }
    }

    struct CountingWake(Arc<AtomicUsize>);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn wake_all_contains_panicking_waker_and_payload_destructor() {
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let normal_wakes = Arc::new(AtomicUsize::new(0));
        wake_all([
            Waker::from(Arc::new(PanickingWake(Arc::clone(&payload_drops)))),
            Waker::from(Arc::new(CountingWake(Arc::clone(&normal_wakes)))),
        ]);
        assert_eq!(1, payload_drops.load(Ordering::SeqCst));
        assert_eq!(1, normal_wakes.load(Ordering::SeqCst));
    }
}
