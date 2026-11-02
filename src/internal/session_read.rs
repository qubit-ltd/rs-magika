//! Exact reads from Magika's opened random-access sessions.

use std::io;

use magika::Result as MagikaResult;
use qubit_fs::AsyncRandomAccessReader;
use qubit_fs::read::RandomAccessReader;

use super::provider_input::invalid_input;
use super::provider_input::validate_request;
use super::read_budget::ReadBudget;

/// Fills one Magika request directly from the opened object, accounting for
/// its cumulative byte budget once and splitting only at the provider limit.
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
        let count = reader
            .read_at(read_offset, &mut buffer[filled..filled + step])
            .map_err(|error| error.into_io_error())?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "provider returned a short range",
            )
            .into());
        }
        filled += count;
    }
    Ok(())
}

/// Asynchronous counterpart to [`read_sync_session`].
pub(crate) async fn read_async_session(
    reader: &mut AsyncRandomAccessReader,
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
        let count = reader
            .read_at(read_offset, &mut buffer[filled..filled + step])
            .await
            .map_err(|error| error.into_io_error())?;
        if count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "provider returned a short range",
            )
            .into());
        }
        filled += count;
    }
    Ok(())
}
