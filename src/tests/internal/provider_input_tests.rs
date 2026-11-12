// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::time::Duration;
use std::time::Instant;

use magika::AsyncInput;
use magika::Error;
use magika::SyncInput;
use qubit_fs::Path;
use qubit_fs::metadata::ResourceVersion;
use qubit_mime::MimeError;

use super::provider_input_fixture::ProviderFileSystemSpi;
use crate::internal::AsyncProviderInput;
use crate::internal::SyncProviderInput;
use crate::internal::budget_exceeded::BudgetExceeded;
use crate::internal::provider_input::copy_exact;
use crate::internal::provider_input::invalid_input;
use crate::internal::provider_input::map_provider_magika_error;
use crate::internal::provider_input::read_async_segments;
use crate::internal::provider_input::read_sync_segments;
use crate::internal::provider_input::validate_request;
use crate::internal::read_budget::ReadBudget;

fn run_ready<F: Future>(future: F) -> F::Output {
    let wake = Arc::new(ThreadWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match Future::poll(Pin::as_mut(&mut future), &mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => loop {
                if wake.notified.swap(false, Ordering::AcqRel) {
                    break;
                }
                let now = Instant::now();
                assert!(now < deadline, "future did not complete before deadline");
                std::thread::park_timeout(deadline.saturating_duration_since(now));
            },
        }
    }
}

struct ThreadWake {
    thread: std::thread::Thread,
    notified: AtomicBool,
}
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.notified.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

#[test]
fn test_provider_input_helper_errors_are_displayable() {
    assert_eq!(
        "invalid provider input",
        invalid_input("invalid provider input").to_string()
    );
    assert!(
        BudgetExceeded { requested: 9, limit: 8 }.to_string().contains("9 > 8"),
        "the budget error should report the requested and allowed byte counts"
    );
}

#[test]
fn test_provider_input_validation_and_copy_errors_are_mapped() {
    let mut budget = ReadBudget::new(8);
    assert!(
        validate_request(8, &mut budget, 0, 99).is_ok(),
        "zero-length reads should not require an in-range offset"
    );
    assert!(
        validate_request(8, &mut budget, 2, 7).is_err(),
        "reads extending beyond the logical length should fail"
    );
    assert!(
        validate_request(8, &mut budget, 2, u64::MAX).is_err(),
        "overflowing read offsets should fail"
    );
    let mut output = [0_u8; 2];
    assert!(
        copy_exact(&mut output, vec![1]).is_err(),
        "short provider ranges should fail"
    );
    assert!(
        copy_exact(&mut output, vec![1, 2]).is_ok(),
        "provider ranges matching the buffer length should be copied"
    );
    let mapped = map_provider_magika_error(Error::IOError(std::io::Error::other("provider failure")));
    assert!(
        matches!(mapped, MimeError::Io(_)),
        "ordinary provider I/O errors should remain MIME I/O errors"
    );

    let error = BudgetExceeded {
        requested: 16,
        limit: 8,
    };
    let mapped = map_provider_magika_error(Error::IOError(std::io::Error::new(
        std::io::ErrorKind::FileTooLarge,
        error,
    )));
    assert!(
        matches!(
            mapped,
            MimeError::BufferLimitExceeded {
                requested: 16,
                limit: 8
            }
        ),
        "provider budget failures should retain their requested and allowed byte counts"
    );
}

#[test]
fn test_sync_legacy_provider_reads_bounded_ranges_and_reserves_budget_once() {
    let provider = ProviderFileSystemSpi::new(vec![0, 1, 2, 3, 4, 5])
        .with_range()
        .with_max_read_range_bytes(2);
    let observations = provider.observations();
    let file_system = provider.file_system();
    let path = Path::parse("/same").unwrap();
    let mut input = SyncProviderInput::new(&file_system, &path, 6, None, 4);
    let mut output = [0; 4];

    SyncInput::read_at(&mut input, &mut output, 1).unwrap();
    assert_eq!(output, [1, 2, 3, 4]);
    let options = observations.options.lock().unwrap();
    assert_eq!(options.len(), 2);
    assert_eq!(options[0].offset(), Some(1));
    assert_eq!(options[0].length(), Some(2));
    assert_eq!(options[1].offset(), Some(3));
    assert_eq!(options[1].length(), Some(2));
    drop(options);

    let opens_before = observations.opens.load(std::sync::atomic::Ordering::SeqCst);
    let mut another_byte = [0];
    let error = SyncInput::read_at(&mut input, &mut another_byte, 0).unwrap_err();
    assert!(matches!(
        map_provider_magika_error(error),
        MimeError::BufferLimitExceeded { requested: 5, limit: 4 }
    ));
    assert_eq!(
        opens_before,
        observations.opens.load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[test]
fn test_async_legacy_provider_reads_bounded_ranges_and_reserves_budget_once() {
    let provider = ProviderFileSystemSpi::new(vec![0, 1, 2, 3, 4, 5])
        .with_range()
        .with_max_read_range_bytes(2);
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let path = Path::parse("/same").unwrap();
    let mut input = AsyncProviderInput::new(&file_system, &path, 6, None, 4);
    let mut output = [0; 4];

    run_ready(AsyncInput::read_at(&mut input, &mut output, 1)).unwrap();
    assert_eq!(output, [1, 2, 3, 4]);
    let options = observations.options.lock().unwrap();
    assert_eq!(options.len(), 2);
    assert_eq!(options[0].offset(), Some(1));
    assert_eq!(options[0].length(), Some(2));
    assert_eq!(options[1].offset(), Some(3));
    assert_eq!(options[1].length(), Some(2));
    drop(options);

    let opens_before = observations.opens.load(std::sync::atomic::Ordering::SeqCst);
    let mut another_byte = [0];
    let error = run_ready(AsyncInput::read_at(&mut input, &mut another_byte, 0)).unwrap_err();
    assert!(matches!(
        map_provider_magika_error(error),
        MimeError::BufferLimitExceeded { requested: 5, limit: 4 }
    ));
    assert_eq!(
        opens_before,
        observations.opens.load(std::sync::atomic::Ordering::SeqCst)
    );
}

#[test]
fn test_legacy_provider_empty_unknown_zero_short_and_conditional_ranges() {
    let path = Path::parse("/same").unwrap();

    let empty_provider = ProviderFileSystemSpi::new(vec![0, 1, 2]);
    let empty_observations = empty_provider.observations();
    let empty_fs = empty_provider.file_system();
    let mut empty_input = SyncProviderInput::new(&empty_fs, &path, 3, None, 0);
    SyncInput::read_at(&mut empty_input, &mut [], u64::MAX).unwrap();
    assert_eq!(0, empty_observations.opens.load(std::sync::atomic::Ordering::SeqCst));

    let unknown_provider = ProviderFileSystemSpi::new(vec![0, 1, 2]).with_range();
    let unknown_observations = unknown_provider.observations();
    let unknown_fs = unknown_provider.file_system();
    let mut unknown_input = SyncProviderInput::new(&unknown_fs, &path, 3, None, 3);
    let mut unknown_output = [0; 3];
    SyncInput::read_at(&mut unknown_input, &mut unknown_output, 0).unwrap();
    assert_eq!(1, unknown_observations.options.lock().unwrap().len());

    let zero_provider = ProviderFileSystemSpi::new(vec![0, 1]).with_range();
    let zero_observations = zero_provider.observations();
    let zero_fs = zero_provider.file_system();
    assert!(read_sync_segments(&zero_fs, &path, None, &mut [0; 1], 0, 0).is_err());
    let zero_async_fs = zero_provider.async_file_system();
    assert!(run_ready(read_async_segments(&zero_async_fs, &path, None, &mut [0; 1], 0, 0)).is_err());
    assert_eq!(0, zero_observations.opens.load(std::sync::atomic::Ordering::SeqCst));

    let short_provider = ProviderFileSystemSpi::new(vec![0, 1, 2])
        .with_range()
        .with_max_read_range_bytes(2)
        .short_reads();
    let short_fs = short_provider.file_system();
    let mut short_input = SyncProviderInput::new(&short_fs, &path, 3, None, 3);
    assert!(SyncInput::read_at(&mut short_input, &mut [0; 3], 0).is_err());
    assert_eq!(
        1,
        short_provider
            .observations()
            .opens
            .load(std::sync::atomic::Ordering::SeqCst)
    );

    let version = ResourceVersion::new("v1");
    let conditional_provider = ProviderFileSystemSpi::new(vec![0, 1, 2, 3])
        .with_conditional()
        .with_max_read_range_bytes(2)
        .fail_conditional_on_open(1);
    let conditional_observations = conditional_provider.observations();
    let conditional_fs = conditional_provider.file_system();
    let mut conditional_input = SyncProviderInput::new(&conditional_fs, &path, 4, Some(version), 4);
    assert!(SyncInput::read_at(&mut conditional_input, &mut [0; 4], 0).is_err());
    let options = conditional_observations.options.lock().unwrap();
    assert_eq!(2, options.len());
    assert!(
        options
            .iter()
            .all(|option| option.if_match().is_some_and(|etag| etag.as_str() == "v1"))
    );
    drop(options);

    let async_conditional_provider = ProviderFileSystemSpi::new(vec![0, 1, 2, 3])
        .with_conditional()
        .with_max_read_range_bytes(2)
        .fail_conditional_on_open(1);
    let async_conditional_observations = async_conditional_provider.observations();
    let async_conditional_fs = async_conditional_provider.async_file_system();
    let mut async_conditional_input =
        AsyncProviderInput::new(&async_conditional_fs, &path, 4, Some(ResourceVersion::new("v1")), 4);
    assert!(run_ready(AsyncInput::read_at(&mut async_conditional_input, &mut [0; 4], 0)).is_err());
    let options = async_conditional_observations.options.lock().unwrap();
    assert_eq!(2, options.len());
    assert!(
        options
            .iter()
            .all(|option| option.if_match().is_some_and(|etag| etag.as_str() == "v1"))
    );
}
