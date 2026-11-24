// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Exact reads from Magika's opened random-access sessions.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Poll;

use magika::Result as MagikaResult;
use qubit_fs::error::FsEffectState;
use qubit_fs::error::FsError;
use qubit_fs::error::FsErrorKind;
use qubit_fs::read::AsyncRandomAccessReader;
use qubit_fs::read::RandomAccessReader;

mod internal;

use self::internal::SessionReadStopped;
use super::cooperative_read::READ_STEPS_PER_POLL;
use super::cooperative_read::yield_once;
use super::provider_input::invalid_input;
use super::provider_input::validate_request;
use super::read_budget::ReadBudget;

const MAX_CONSECUTIVE_INTERRUPTED_RETRIES: usize = 8;

/// Fills one Magika request directly from the opened object, accounting for
/// its cumulative byte budget once and splitting only at the provider limit.
///
/// # Parameters
///
/// * `reader` - Random-access reader retaining the opened resource.
/// * `length` - Logical resource length captured when the reader was opened.
/// * `budget` - Per-detection cumulative byte budget.
/// * `buffer` - Destination for the requested resource bytes.
/// * `offset` - Start offset of the requested range.
/// * `max_segment` - Optional maximum byte count for one session read.
///
/// # Errors
///
/// Returns an I/O error when validation or budget reservation fails, the
/// provider returns a short range, or a session read fails.
pub(crate) fn read_sync_session(
    reader: &mut RandomAccessReader,
    length: u64,
    budget: &mut ReadBudget,
    buffer: &mut [u8],
    offset: u64,
    max_segment: Option<usize>,
) -> MagikaResult<()> {
    validate_request(length, budget, buffer.len(), offset)?;
    let segment_limit = max_segment.unwrap_or(usize::MAX);
    if !buffer.is_empty() && segment_limit == 0 {
        return Err(invalid_input("provider range limit does not permit a nonempty read").into());
    }
    let mut filled = 0;
    while filled < buffer.len() {
        let step = (buffer.len() - filled).min(segment_limit);
        let read_offset = offset
            .checked_add(filled as u64)
            .ok_or_else(|| invalid_input("provider input offset overflow"))?;
        let count = read_sync_at_retry(reader, read_offset, &mut buffer[filled..filled + step])
            .map_err(FsError::into_io_error)?;
        if count == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "provider returned a short range").into());
        }
        filled += count;
    }
    Ok(())
}

/// Retries only interruptions known not to have changed the session's effect
/// state.
///
/// # Parameters
///
/// * `reader` - Open session used for the read attempts.
/// * `offset` - Fixed resource offset used for every attempt.
/// * `output` - Buffer filled by a successful attempt.
///
/// # Returns
///
/// The byte count returned by the first non-retry result.
///
/// # Errors
///
/// Returns the first error that is not a safe, unchanged interruption.
fn read_sync_at_retry(reader: &mut RandomAccessReader, offset: u64, output: &mut [u8]) -> qubit_fs::FsResult<usize> {
    let mut retries = 0;
    loop {
        match reader.read_at(offset, output) {
            Err(error)
                if error.kind() == FsErrorKind::Interrupted
                    && matches!(error.effect_state(), None | Some(FsEffectState::Unchanged))
                    && retries < MAX_CONSECUTIVE_INTERRUPTED_RETRIES =>
            {
                retries += 1;
                continue;
            }
            result => return result,
        }
    }
}

/// Retries only asynchronous interruptions known not to have changed the
/// session's effect state.
///
/// # Parameters
///
/// * `reader` - Open asynchronous session used for the read attempts.
/// * `offset` - Fixed resource offset used for every attempt.
/// * `output` - Buffer filled by a successful attempt.
/// * `should_stop` - Predicate checked before each provider attempt.
///
/// # Returns
///
/// The byte count returned by the first non-retry result.
///
/// # Errors
///
/// Returns an I/O error for the first provider result that is not a safe,
/// unchanged interruption. When `should_stop` is true, returns a private
/// interrupted-read marker instead of a Provider error.
async fn read_async_at_retry(
    reader: &mut AsyncRandomAccessReader,
    offset: u64,
    output: &mut [u8],
    should_stop: &impl Fn() -> bool,
    completed_steps: &mut usize,
    io_in_flight: Option<&AtomicBool>,
) -> io::Result<usize> {
    let mut retries = 0;
    loop {
        if should_stop() {
            return Err(session_read_stopped_io_error());
        }
        let mut read = Box::pin(reader.read_at(offset, output));
        let result = std::future::poll_fn(|context| {
            if let Some(io_in_flight) = io_in_flight {
                io_in_flight.store(true, Ordering::Release);
            }
            match Pin::as_mut(&mut read).poll(context) {
                Poll::Ready(result) => {
                    if let Some(io_in_flight) = io_in_flight {
                        io_in_flight.store(false, Ordering::Release);
                    }
                    *completed_steps += 1;
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        })
        .await;
        match result {
            Err(error)
                if error.kind() == FsErrorKind::Interrupted
                    && matches!(error.effect_state(), None | Some(FsEffectState::Unchanged))
                    && retries < MAX_CONSECUTIVE_INTERRUPTED_RETRIES =>
            {
                retries += 1;
                if *completed_steps >= READ_STEPS_PER_POLL {
                    *completed_steps = 0;
                    yield_once().await;
                }
                continue;
            }
            Err(error) => return Err(error.into_io_error()),
            Ok(count) => return Ok(count),
        }
    }
}

/// Asynchronous session read that checks for cancellation between provider
/// operations, segments, and safe interruption retries.
///
/// The stop predicate is checked before the first session read, before each
/// retry, and before each later segment. A read already polled is allowed to
/// return before the predicate is checked again.
///
/// # Parameters
///
/// * `reader` - Asynchronous session retaining the opened resource.
/// * `length` - Logical resource length captured when the session was opened.
/// * `budget` - Per-detection cumulative read budget.
/// * `buffer` - Destination for the requested resource bytes.
/// * `offset` - Start offset of the requested range.
/// * `max_segment` - Optional maximum byte count for one session read.
/// * `should_stop` - Predicate reporting whether the owning request stopped.
/// * `io_in_flight` - Optional marker set while a provider read is pending.
///
/// # Returns
///
/// Success after all requested bytes have been filled exactly.
///
/// # Errors
///
/// Returns an I/O error when validation or budget reservation fails, a
/// provider read fails or returns zero bytes, or `should_stop` returns true.
/// A stopped request is reported as an interrupted session read.
pub(crate) async fn read_async_session_with_stop(
    reader: &mut AsyncRandomAccessReader,
    length: u64,
    budget: &mut ReadBudget,
    buffer: &mut [u8],
    offset: u64,
    max_segment: Option<usize>,
    should_stop: impl Fn() -> bool,
    io_in_flight: Option<&AtomicBool>,
) -> MagikaResult<()> {
    validate_request(length, budget, buffer.len(), offset)?;
    let segment_limit = max_segment.unwrap_or(usize::MAX);
    if !buffer.is_empty() && segment_limit == 0 {
        return Err(invalid_input("provider range limit does not permit a nonempty read").into());
    }
    let mut filled = 0;
    let mut completed_steps = 0;
    while filled < buffer.len() {
        if should_stop() {
            return Err(session_read_stopped_io_error().into());
        }
        let step = (buffer.len() - filled).min(segment_limit);
        let read_offset = offset
            .checked_add(filled as u64)
            .ok_or_else(|| invalid_input("provider input offset overflow"))?;
        let count = read_async_at_retry(
            reader,
            read_offset,
            &mut buffer[filled..filled + step],
            &should_stop,
            &mut completed_steps,
            io_in_flight,
        )
        .await?;
        if count == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "provider returned a short range").into());
        }
        filled += count;
        if filled < buffer.len() && completed_steps >= READ_STEPS_PER_POLL {
            completed_steps = 0;
            yield_once().await;
        }
    }
    Ok(())
}

/// Returns whether an I/O error represents an actively stopped session read.
///
/// # Parameters
///
/// * `error` - I/O error produced by the attempted session read.
///
/// # Returns
///
/// `true` when the error carries the private session-stop marker.
pub(crate) fn is_session_stop_error(error: &io::Error) -> bool {
    error.get_ref().is_some_and(|source| source.is::<SessionReadStopped>())
}

/// Creates an interruption marker for an actively stopped session read.
///
/// # Returns
///
/// An I/O error carrying the private session-stop marker as its source.
pub(crate) fn session_read_stopped_io_error() -> io::Error {
    io::Error::new(io::ErrorKind::Interrupted, SessionReadStopped)
}
