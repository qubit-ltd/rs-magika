// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One range request's response synchronization.

use std::io;
use std::sync::Condvar;
use std::sync::Mutex;

/// Stores one result and wakes the synchronous sampler when it arrives.
pub(super) struct Response {
    /// Provider read result, if the async driver has answered.
    pub(super) result: Mutex<Option<io::Result<Vec<u8>>>>,
    /// Notifies the worker when `result` becomes available.
    pub(super) ready: Condvar,
}

impl Response {
    /// Creates an unanswered response slot.
    pub(super) const fn new() -> Self {
        Self {
            result: Mutex::new(None),
            ready: Condvar::new(),
        }
    }
}

/// Publishes the first response and wakes its blocked worker.
///
/// # Parameters
///
/// * `response` - Request's one-shot response slot.
/// * `result` - Exact bytes or the asynchronous read error.
pub(super) fn set_response(response: &Response, result: io::Result<Vec<u8>>) {
    let mut current = response.result.lock().unwrap_or_else(|error| error.into_inner());
    if current.is_none() {
        *current = Some(result);
        response.ready.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::Response;
    use super::set_response;

    #[test]
    fn test_response_publication_recovers_a_poisoned_result_lock() {
        let response = std::sync::Arc::new(Response::new());
        let poisoned = std::sync::Arc::clone(&response);
        assert!(
            std::thread::spawn(move || {
                let _guard = poisoned.result.lock().unwrap();
                panic!("poison response lock before publication");
            })
            .join()
            .is_err()
        );

        set_response(&response, Ok(vec![3]));

        assert_eq!(
            response
                .result
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap(),
            &[3]
        );
    }

    #[test]
    fn test_response_keeps_the_first_published_result() {
        let response = Response::new();
        set_response(&response, Ok(vec![1, 2]));
        set_response(&response, Err(std::io::Error::other("later response")));

        assert_eq!(
            response.result.lock().unwrap().as_ref().unwrap().as_ref().unwrap(),
            &[1, 2]
        );
    }
}
