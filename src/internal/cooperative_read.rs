// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// =============================================================================

//! Shared cooperative scheduling helpers for asynchronous provider reads.

use std::future::poll_fn;
use std::task::Poll;

pub(crate) const READ_STEPS_PER_POLL: usize = 64;

/// Yields once to the executor and schedules the future to be polled again.
pub(crate) async fn yield_once() {
    let mut yielded = false;
    poll_fn(|context| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            if super::waker_notify::wake_by_ref(context.waker()) {
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    })
    .await;
}
