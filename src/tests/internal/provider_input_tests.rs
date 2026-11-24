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
use qubit_fs::error::FsError;
use qubit_fs::error::FsErrorKind;
use qubit_fs::error::FsOperation;
use qubit_fs::metadata::ResourceVersion;
use qubit_fs::read::ReadIoEvent;
use qubit_fs::read::ReadIoObserver;
use qubit_fs::read::ReadIoPhase;
use qubit_mime::MimeError;

use super::provider_input_fixture::ProviderFileSystemSpi;
use super::provider_input_fixture::ProviderObservations;
use crate::internal::AsyncProviderInput;
use crate::internal::SyncProviderInput;
use crate::internal::is_session_stop_error;
use crate::internal::provider_input::copy_exact;
use crate::internal::provider_input::invalid_input;
use crate::internal::provider_input::map_provider_magika_error;
use crate::internal::provider_input::read_async_segments_observed;
use crate::internal::provider_input::read_options;
use crate::internal::provider_input::read_sync_segments;
use crate::internal::provider_input::validate_request;
use crate::internal::read_budget::ReadBudget;
use crate::internal::session_read_stopped_io_error;

fn run_ready<F: Future>(future: F) -> F::Output {
    let wake = Arc::new(ThreadWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut polls = 0usize;
    loop {
        polls += 1;
        assert!(polls <= 100_000, "future exceeded the bounded poll count");
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

#[derive(Default)]
struct CountingReadObserver {
    ready_events: Arc<std::sync::atomic::AtomicUsize>,
    pending_read_events: Option<Arc<std::sync::atomic::AtomicUsize>>,
}

impl ReadIoObserver for CountingReadObserver {
    fn on_io_event(&mut self, event: ReadIoEvent) {
        match event {
            ReadIoEvent::Ready { .. } => {
                self.ready_events.fetch_add(1, Ordering::SeqCst);
            }
            ReadIoEvent::Pending {
                phase: ReadIoPhase::Read,
            } => {
                if let Some(pending_read_events) = &self.pending_read_events {
                    pending_read_events.fetch_add(1, Ordering::SeqCst);
                }
            }
            _ => {}
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
}

#[test]
fn test_legacy_async_provider_yields_within_completed_io_budget_for_short_reads() {
    let content: Vec<u8> = (0..4096).map(|index| (index % 251) as u8).collect();
    let provider = ProviderFileSystemSpi::new(content.clone())
        .with_range()
        .with_max_read_range_bytes(64)
        .one_byte_reads();
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let path = Path::parse("/cooperative-legacy").unwrap();
    let mut output = vec![0; content.len()];
    let event_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut observer = CountingReadObserver {
        ready_events: Arc::clone(&event_count),
        pending_read_events: None,
    };
    let mut future = Box::pin(read_async_segments_observed(
        &file_system,
        &path,
        None,
        &mut output,
        0,
        64,
        &mut observer,
    ));
    let wake = Arc::new(ThreadWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);

    // Baseline negative: before counting actual Ready events, this first poll
    // completed all 64 prefixes (4,096 one-byte reads) and returned Ready.
    // The event budget must instead bound each real poll to 128 completions.
    let mut pending_polls = 0;
    let mut max_completed_this_poll = 0;
    loop {
        let before = event_count.load(Ordering::SeqCst);
        let poll = future.as_mut().poll(&mut context);
        let completed_this_poll = event_count.load(Ordering::SeqCst) - before;
        assert!(
            completed_this_poll <= 128,
            "one poll completed {completed_this_poll} provider operations; maximum is 128"
        );
        max_completed_this_poll = max_completed_this_poll.max(completed_this_poll);
        match poll {
            Poll::Ready(result) => {
                result.expect("cooperative reads should eventually fill the request");
                assert!(pending_polls > 0, "long short-read request must yield");
                break;
            }
            Poll::Pending => {
                pending_polls += 1;
                assert!(
                    wake.notified.swap(false, Ordering::AcqRel),
                    "cooperative yield must self-wake"
                );
            }
        }
    }
    assert!(pending_polls >= 1);
    assert_eq!(65, max_completed_this_poll);
    assert_eq!(4160, event_count.load(Ordering::SeqCst));
    assert_eq!(64, observations.opens.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(4096, observations.read_bytes.load(std::sync::atomic::Ordering::SeqCst));
    drop(future);
    assert_eq!(content, output);
}

#[test]
fn test_legacy_io_budget_survives_real_provider_pending() {
    let content: Vec<u8> = (0..126).map(|index| (index % 251) as u8).collect();
    let provider_pending_observed = Arc::new(AtomicBool::new(false));
    let provider = ProviderFileSystemSpi::new(content.clone())
        .with_range()
        .with_max_read_range_bytes(63)
        .one_byte_reads()
        .with_pending_first_async_read(Arc::clone(&provider_pending_observed));
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let path = Path::parse("/pending-legacy-budget").unwrap();
    let mut output = vec![0; content.len()];
    let ready_events = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let pending_read_events = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut observer = CountingReadObserver {
        ready_events: Arc::clone(&ready_events),
        pending_read_events: Some(Arc::clone(&pending_read_events)),
    };
    let mut future = Box::pin(read_async_segments_observed(
        &file_system,
        &path,
        None,
        &mut output,
        0,
        63,
        &mut observer,
    ));
    let wake = Arc::new(ThreadWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let mut ready_events_per_poll = Vec::new();
    let mut poll_count = 0;

    loop {
        poll_count += 1;
        assert!(poll_count <= 4, "provider read exceeded its expected poll count");
        let before = ready_events.load(Ordering::SeqCst);
        let poll = future.as_mut().poll(&mut context);
        let completed_this_poll = ready_events.load(Ordering::SeqCst) - before;
        assert!(
            completed_this_poll <= 128,
            "one poll completed {completed_this_poll} provider operations; maximum is 128"
        );
        ready_events_per_poll.push(completed_this_poll);
        match poll {
            Poll::Ready(result) => {
                result.expect("read should complete after the provider resumes");
                break;
            }
            Poll::Pending => {
                // This fixture deliberately leaves wake-up control to the test:
                // the next poll resumes the real provider read or yield.
                if !wake.notified.swap(false, Ordering::AcqRel) {
                    waker.wake_by_ref();
                    assert!(wake.notified.swap(false, Ordering::AcqRel));
                }
            }
        }
    }

    assert!(provider_pending_observed.load(Ordering::SeqCst));
    assert_eq!(4, poll_count);
    assert_eq!(2, pending_read_events.load(Ordering::SeqCst));
    assert_eq!(vec![1, 63, 1, 63], ready_events_per_poll);
    assert_eq!(128, ready_events.load(Ordering::SeqCst));
    assert_eq!(2, observations.opens.load(Ordering::SeqCst));
    assert_eq!(126, observations.read_bytes.load(Ordering::SeqCst));
    drop(future);
    assert_eq!(content, output);
}

#[test]
fn test_legacy_yield_clears_actual_io_before_observing_cancellation() {
    let provider = ProviderFileSystemSpi::new(vec![7; 128])
        .with_range()
        .with_max_read_range_bytes(64)
        .one_byte_reads();
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let path = Path::parse("/cancel-at-cooperative-yield").unwrap();
    let mut output = [0; 128];
    let in_flight = Arc::new(AtomicBool::new(false));
    let stop_seen = Arc::new(AtomicBool::new(false));

    struct CancelAtYield {
        in_flight: Arc<AtomicBool>,
        stop_requested: bool,
        stop_seen: Arc<AtomicBool>,
        read_bytes: usize,
    }
    impl ReadIoObserver for CancelAtYield {
        fn before_io(&mut self, phase: ReadIoPhase) -> std::ops::ControlFlow<()> {
            if self.stop_requested && phase == ReadIoPhase::Open {
                assert!(
                    !self.in_flight.load(Ordering::Acquire),
                    "cooperative yield must not retain an in-flight I/O marker"
                );
                self.stop_seen.store(true, Ordering::Release);
                std::ops::ControlFlow::Break(())
            } else {
                std::ops::ControlFlow::Continue(())
            }
        }

        fn on_io_event(&mut self, event: ReadIoEvent) {
            match event {
                ReadIoEvent::Started { .. } | ReadIoEvent::Pending { .. } => {
                    self.in_flight.store(true, Ordering::Release);
                }
                ReadIoEvent::Ready { .. } => self.in_flight.store(false, Ordering::Release),
            }
        }

        fn on_read_bytes(&mut self, bytes: usize) {
            self.read_bytes += bytes;
            if self.read_bytes == 64 {
                self.stop_requested = true;
            }
        }
    }

    let mut observer = CancelAtYield {
        in_flight: Arc::clone(&in_flight),
        stop_requested: false,
        stop_seen: Arc::clone(&stop_seen),
        read_bytes: 0,
    };
    let mut future = Box::pin(read_async_segments_observed(
        &file_system,
        &path,
        None,
        &mut output,
        0,
        64,
        &mut observer,
    ));
    let wake = Arc::new(ThreadWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);

    assert!(future.as_mut().poll(&mut context).is_pending());
    assert!(wake.notified.swap(false, Ordering::AcqRel));
    assert!(!in_flight.load(Ordering::Acquire));
    assert!(!stop_seen.load(Ordering::Acquire));
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Ready(Err(_))));
    assert!(stop_seen.load(Ordering::Acquire));
    assert_eq!(1, observations.opens.load(Ordering::SeqCst));
    assert_eq!(64, observations.read_bytes.load(Ordering::SeqCst));
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

    let error = MimeError::BufferLimitExceeded {
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
fn test_provider_cancellation_is_not_classified_as_session_stop() {
    let provider_error = FsError::new(FsErrorKind::Cancelled, FsOperation::Read, "provider cancelled");
    let io_error = provider_error.into_io_error();
    assert!(!is_session_stop_error(&io_error));

    let mapped = map_provider_magika_error(Error::IOError(io_error));
    let MimeError::Io(error) = mapped else {
        panic!("provider cancellation must remain a MIME I/O error");
    };
    let source = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<FsError>())
        .expect("provider FsError must remain available in the source chain");
    assert_eq!(source.kind(), FsErrorKind::Cancelled);

    let interrupted = FsError::new(FsErrorKind::Interrupted, FsOperation::Read, "provider interrupted").into_io_error();
    assert!(!is_session_stop_error(&interrupted));
    assert!(is_session_stop_error(&session_read_stopped_io_error()));
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
    let mut input = AsyncProviderInput::new_owned(file_system.clone(), path.clone(), 6, None, 4);
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
fn test_async_legacy_provider_stops_between_completed_ranges() {
    let provider = ProviderFileSystemSpi::new(vec![0, 1, 2, 3, 4, 5])
        .with_range()
        .with_max_read_range_bytes(2);
    let observations = provider.observations();
    let file_system = provider.async_file_system();
    let path = Path::parse("/stop-between-ranges").unwrap();
    struct StopAfterFirstRange {
        observations: Arc<ProviderObservations>,
    }
    impl ReadIoObserver for StopAfterFirstRange {
        fn before_io(&mut self, phase: ReadIoPhase) -> std::ops::ControlFlow<()> {
            if phase == ReadIoPhase::Open && !self.observations.options.lock().unwrap().is_empty() {
                std::ops::ControlFlow::Break(())
            } else {
                std::ops::ControlFlow::Continue(())
            }
        }
    }
    let mut observer = StopAfterFirstRange {
        observations: Arc::clone(&observations),
    };
    let mut budget = crate::internal::read_budget::ReadBudget::new(4);
    let mut output = [0; 4];

    let result = run_ready(crate::internal::provider_input::read_async_observed(
        &file_system,
        &path,
        6,
        read_options(0, output.len(), None),
        &mut budget,
        &mut output,
        &mut observer,
    ));

    assert!(
        result.is_err(),
        "stop after one completed range must prevent the next range"
    );
    assert_eq!(1, observations.options.lock().unwrap().len());
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
    let mut observer = CountingReadObserver::default();
    let async_error = run_ready(read_async_segments_observed(
        &zero_async_fs,
        &path,
        None,
        &mut [0; 1],
        0,
        0,
        &mut observer,
    ))
    .expect_err("zero range limit should be rejected before provider I/O");
    assert!(matches!(
        map_provider_magika_error(async_error),
        MimeError::Io(ref error) if error.kind() == std::io::ErrorKind::InvalidInput
    ));
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
    let mut async_conditional_input = AsyncProviderInput::new_owned(
        async_conditional_fs.clone(),
        path.clone(),
        4,
        Some(ResourceVersion::new("v1")),
        4,
    );
    assert!(run_ready(AsyncInput::read_at(&mut async_conditional_input, &mut [0; 4], 0)).is_err());
    let options = async_conditional_observations.options.lock().unwrap();
    assert_eq!(2, options.len());
    assert!(
        options
            .iter()
            .all(|option| option.if_match().is_some_and(|etag| etag.as_str() == "v1"))
    );
}
