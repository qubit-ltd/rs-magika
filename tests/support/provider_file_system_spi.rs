// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::io;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Waker;

use qubit_fs::AsyncFileSystem;
use qubit_fs::FileSystem;
use qubit_fs::FsError;
use qubit_fs::FsResult;
use qubit_fs::Path;
use qubit_fs::error::FsErrorKind;
use qubit_fs::error::FsOperation;
use qubit_fs::metadata::FileKind;
use qubit_fs::metadata::FileMetadata;
use qubit_fs::metadata::FileSystemCapabilities;
use qubit_fs::metadata::FileSystemCapability;
use qubit_fs::metadata::FileSystemId;
use qubit_fs::metadata::FileSystemInfo;
use qubit_fs::metadata::FileSystemLimit;
use qubit_fs::metadata::FileSystemLimits;
use qubit_fs::metadata::OpenedFileInfo;
use qubit_fs::metadata::ResourceVersion;
use qubit_fs::metadata::SymlinkPolicy;
use qubit_fs::path::PathConstraints;
use qubit_fs::path::PathSemantics;
use qubit_fs::read::ReadOptions;
use qubit_fs::spi::AsyncFileSystemSpi;
use qubit_fs::spi::FileSystemSpi;
use qubit_fs::spi::OpenReaderRequest;
use qubit_fs::spi::OpenedAsyncReader;
use qubit_fs::spi::OpenedRandomAccessReader;
use qubit_fs::spi::OpenedReader;
use qubit_fs::spi::ProviderOperation;
use qubit_fs::spi::ProviderOperations;
use qubit_fs::spi::ProviderProperties;
use qubit_fs::spi::RandomAccessReadSession;
use qubit_fs::spi::SpiFuture;
use qubit_fs::spi::StatRequest;
use qubit_fs::spi::StatResponse;
use qubit_io::AsyncInput;
use qubit_io::Input;

#[derive(Clone)]
pub(crate) struct ProviderFileSystemSpi {
    content: Arc<Vec<u8>>,
    length: Option<u64>,
    range: bool,
    conditional: bool,
    stat_supported: bool,
    short_read: bool,
    fail_reads: bool,
    read_error: Option<FsErrorKind>,
    interrupt_session_once: bool,
    random_access: bool,
    random_open_error: Option<FsErrorKind>,
    max_read_range_bytes: Option<u64>,
    observations: Arc<ProviderObservations>,
    async_gate: Option<Arc<AsyncProviderGate>>,
    completion_gate: Option<Arc<ProviderReadCompletionGate>>,
    interruption_gate: Option<Arc<ProviderReadCompletionGate>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AsyncProviderGatePoint {
    Stat,
    Open,
    Read,
}

/// Pauses after the selected byte count is read but before the async provider
/// poll returns `Ready` to the filesystem reader.
pub(crate) struct ProviderReadCompletionGate {
    threshold: usize,
    state: Mutex<ProviderReadCompletionGateState>,
    changed: Condvar,
}

#[derive(Default)]
struct ProviderReadCompletionGateState {
    entered: bool,
    released: bool,
}

impl ProviderReadCompletionGate {
    pub(crate) fn new(threshold: usize) -> Arc<Self> {
        Arc::new(Self {
            threshold,
            state: Mutex::new(ProviderReadCompletionGateState::default()),
            changed: Condvar::new(),
        })
    }

    fn pause_if_reached(&self, count: usize) {
        if count != self.threshold {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    pub(crate) fn wait_until_entered(&self, timeout: std::time::Duration) {
        let deadline = std::time::Instant::now() + timeout;
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        while !state.entered {
            let now = std::time::Instant::now();
            if now >= deadline {
                drop(state);
                panic!("provider read did not reach completion gate");
            }
            let (next, result) = self
                .changed
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            if result.timed_out() && !state.entered {
                drop(state);
                panic!("provider read did not reach completion gate");
            }
        }
    }

    pub(crate) fn release(&self) {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .released = true;
        self.changed.notify_all();
    }

    pub(crate) fn is_entered(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entered
    }
}

pub(crate) struct AsyncProviderGate {
    point: AsyncProviderGatePoint,
    state: Mutex<AsyncProviderGateState>,
    changed: Condvar,
}

#[derive(Default)]
struct AsyncProviderGateState {
    entered: bool,
    released: bool,
    wait_dropped: bool,
    waker: Option<Waker>,
}

impl AsyncProviderGate {
    pub(crate) fn new(point: AsyncProviderGatePoint) -> Arc<Self> {
        Arc::new(Self {
            point,
            state: Mutex::new(AsyncProviderGateState::default()),
            changed: Condvar::new(),
        })
    }
    async fn wait(self: &Arc<Self>) {
        let _drop_observed = AsyncProviderGateDropObserver(Arc::clone(self));
        std::future::poll_fn(|cx| self.poll_wait(cx)).await
    }
    fn poll_wait(&self, cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.released {
            return std::task::Poll::Ready(());
        }
        state.waker = Some(cx.waker().clone());
        state.entered = true;
        self.changed.notify_all();
        if state.released {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    }
    pub(crate) fn wait_until_entered(&self, timeout: std::time::Duration) {
        let deadline = std::time::Instant::now() + timeout;
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        while !state.entered {
            let now = std::time::Instant::now();
            if now >= deadline {
                drop(state);
                panic!("provider operation {:?} did not reach its gate", self.point);
            }
            let (next, result) = self
                .changed
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            if result.timed_out() && !state.entered {
                drop(state);
                panic!("provider operation {:?} did not reach its gate", self.point);
            }
        }
    }
    pub(crate) fn wait_until_wait_dropped(&self, timeout: std::time::Duration) {
        let deadline = std::time::Instant::now() + timeout;
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        while !state.wait_dropped {
            let now = std::time::Instant::now();
            if now >= deadline {
                drop(state);
                panic!("provider operation {:?} was not dropped", self.point);
            }
            let (next, result) = self
                .changed
                .wait_timeout(state, deadline.saturating_duration_since(now))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            if result.timed_out() && !state.wait_dropped {
                drop(state);
                panic!("provider operation {:?} was not dropped", self.point);
            }
        }
    }

    pub(crate) fn is_wait_dropped(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .wait_dropped
    }
    pub(crate) fn release(&self) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.released = true;
        let waker = state.waker.take();
        drop(state);
        self.changed.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    pub(crate) fn is_entered(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entered
    }
}

struct AsyncProviderGateDropObserver(Arc<AsyncProviderGate>);

impl Drop for AsyncProviderGateDropObserver {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .wait_dropped = true;
        self.0.changed.notify_all();
    }
}

#[derive(Default)]
pub(crate) struct ProviderObservations {
    pub(crate) stats: AtomicUsize,
    pub(crate) opens: AtomicUsize,
    pub(crate) random_opens: AtomicUsize,
    pub(crate) random_reads: AtomicUsize,
    pub(crate) session_read_lengths: Mutex<Vec<usize>>,
    pub(crate) session_read_ranges: Mutex<Vec<(u64, usize)>>,
    pub(crate) session_drops: AtomicUsize,
    pub(crate) read_bytes: AtomicUsize,
    pub(crate) range_reads: AtomicUsize,
    pub(crate) options: Mutex<Vec<ReadOptions>>,
}

impl ProviderFileSystemSpi {
    pub(crate) fn new(content: Vec<u8>) -> Self {
        Self {
            length: Some(content.len() as u64),
            content: Arc::new(content),
            range: false,
            conditional: false,
            stat_supported: true,
            short_read: false,
            fail_reads: false,
            read_error: None,
            interrupt_session_once: false,
            random_access: false,
            random_open_error: None,
            max_read_range_bytes: None,
            observations: Arc::new(ProviderObservations::default()),
            async_gate: None,
            completion_gate: None,
            interruption_gate: None,
        }
    }

    pub(crate) fn logical(mut self, length: u64) -> Self {
        self.length = Some(length);
        self
    }

    pub(crate) fn without_range(mut self) -> Self {
        self.range = false;
        self
    }

    pub(crate) fn with_range(mut self) -> Self {
        self.range = true;
        self
    }

    pub(crate) fn with_max_read_range_bytes(mut self, maximum: u64) -> Self {
        self.max_read_range_bytes = Some(maximum);
        self
    }

    pub(crate) fn with_conditional(mut self) -> Self {
        self.range = true;
        self.conditional = true;
        self
    }

    pub(crate) fn without_stat(mut self) -> Self {
        self.stat_supported = false;
        self
    }

    pub(crate) fn unknown_length(mut self) -> Self {
        self.length = None;
        self
    }

    pub(crate) fn short_reads(mut self) -> Self {
        self.short_read = true;
        self
    }

    pub(crate) fn fail_reads(mut self) -> Self {
        self.fail_reads = true;
        self
    }

    pub(crate) fn with_read_error(mut self, kind: FsErrorKind) -> Self {
        self.read_error = Some(kind);
        self
    }

    pub(crate) fn interrupt_session_once(mut self) -> Self {
        self.interrupt_session_once = true;
        self
    }

    pub(crate) fn with_random_access(mut self) -> Self {
        self.random_access = true;
        self
    }

    pub(crate) fn with_async_gate(mut self, gate: Arc<AsyncProviderGate>) -> Self {
        self.async_gate = Some(gate);
        self
    }

    pub(crate) fn with_read_completion_gate(mut self, gate: Arc<ProviderReadCompletionGate>) -> Self {
        self.completion_gate = Some(gate);
        self
    }

    pub(crate) fn with_session_interruption_gate(mut self, gate: Arc<ProviderReadCompletionGate>) -> Self {
        self.interruption_gate = Some(gate);
        self
    }

    pub(crate) fn random_open_error(mut self, kind: FsErrorKind) -> Self {
        self.random_access = true;
        self.random_open_error = Some(kind);
        self
    }

    pub(crate) fn observations(&self) -> Arc<ProviderObservations> {
        Arc::clone(&self.observations)
    }

    pub(crate) fn file_system(&self) -> FileSystem {
        FileSystem::from_spi(self.clone()).expect("test filesystem should construct")
    }

    pub(crate) fn async_file_system(&self) -> AsyncFileSystem {
        AsyncFileSystem::from_spi(self.clone()).expect("test async filesystem should construct")
    }

    fn properties_snapshot(&self) -> ProviderProperties {
        let mut capabilities = FileSystemCapabilities::new().with_guaranteed(FileSystemCapability::Read);
        if self.range {
            capabilities = capabilities.with_guaranteed(FileSystemCapability::RangeRead);
        }
        if self.conditional {
            capabilities = capabilities.with_guaranteed(FileSystemCapability::ConditionalRead);
        }
        let mut operations = ProviderOperations::new().with(ProviderOperation::OpenReader);
        if self.random_access {
            operations = operations.with(ProviderOperation::OpenRandomAccessReader);
        }
        if self.stat_supported {
            operations = operations.with(ProviderOperation::Stat);
        }
        ProviderProperties::new(
            FileSystemInfo::new(
                FileSystemId::new("magika-test").expect("test id should be valid"),
                "magika-test",
                PathSemantics::Hierarchical,
            ),
            operations,
            capabilities,
            self.max_read_range_bytes
                .map_or_else(FileSystemLimits::unknown, |maximum| {
                    FileSystemLimits::unknown().with_max_read_range_bytes(FileSystemLimit::Maximum(maximum))
                }),
            PathConstraints::absolute(),
            SymlinkPolicy::Reject,
        )
        .expect("test properties should be valid")
    }

    fn metadata(&self, path: &Path) -> FileMetadata {
        let mut metadata = FileMetadata::new(FileKind::File).with_len(self.length);
        if self.conditional {
            metadata = metadata.with_etag(Some(ResourceVersion::new("v1")));
        }
        let _ = path;
        metadata
    }

    fn open_reader_inner(&self, request: OpenReaderRequest<'_>) -> FsResult<(OpenedFileInfo, Vec<u8>)> {
        self.observations.opens.fetch_add(1, Ordering::SeqCst);
        let options = request.options().options().clone();
        self.observations.options.lock().unwrap().push(options.clone());
        let offset = options.offset().unwrap_or(0);
        let resource_length = self.length.unwrap_or(self.content.len() as u64);
        let requested = options.length().unwrap_or(resource_length);
        let available = requested.min(resource_length.saturating_sub(offset));
        let returned = if self.short_read {
            available.saturating_sub(1)
        } else {
            available
        };
        let mut bytes = Vec::with_capacity(returned as usize);
        for index in 0..returned {
            let absolute = offset + index;
            bytes.push(self.content[(absolute as usize) % self.content.len()]);
        }
        Ok((
            OpenedFileInfo::new(FileSystemId::new("magika-test").unwrap(), request.path().clone()),
            bytes,
        ))
    }

    fn unsupported_stat(path: &Path) -> FsError {
        FsError::new(FsErrorKind::UnsupportedOperation, FsOperation::Stat, "stat unavailable").with_path(path.clone())
    }
}

impl FileSystemSpi for ProviderFileSystemSpi {
    fn properties(&self) -> ProviderProperties {
        self.properties_snapshot()
    }

    fn stat(&self, request: StatRequest<'_>) -> FsResult<StatResponse> {
        self.observations.stats.fetch_add(1, Ordering::SeqCst);
        if !self.stat_supported {
            return Err(Self::unsupported_stat(request.path()));
        }
        Ok(StatResponse::new(request.path().clone(), self.metadata(request.path())))
    }

    fn open_reader(&self, request: OpenReaderRequest<'_>) -> FsResult<OpenedReader> {
        let (info, bytes) = self.open_reader_inner(request)?;
        Ok(OpenedReader::new(
            info,
            Box::new(ProviderReader {
                bytes,
                position: 0,
                fail_reads: self.fail_reads,
                read_error: self.read_error,
                short_read: self.short_read,
                observations: Arc::clone(&self.observations),
                async_gate: self.async_gate.clone(),
                completion_gate: self.completion_gate.clone(),
            }),
        ))
    }

    fn open_random_access_reader(&self, request: OpenReaderRequest<'_>) -> FsResult<OpenedRandomAccessReader> {
        self.observations.random_opens.fetch_add(1, Ordering::SeqCst);
        if let Some(kind) = self.random_open_error {
            return Err(FsError::new(
                kind,
                FsOperation::OpenReader,
                "configured random-access open failure",
            ));
        }
        let info = OpenedFileInfo::new(FileSystemId::new("magika-test").unwrap(), request.path().clone())
            .with_metadata(self.metadata(request.path()));
        Ok(OpenedRandomAccessReader::new(
            info,
            Box::new(ProviderSession {
                content: Arc::clone(&self.content),
                length: self.length.unwrap_or(self.content.len() as u64),
                short_read: self.short_read,
                fail_reads: self.fail_reads,
                read_error: self.read_error,
                interrupt_pending: self.interrupt_session_once,
                observations: Arc::clone(&self.observations),
                async_gate: self.async_gate.clone(),
                completion_gate: self.completion_gate.clone(),
                interruption_gate: self.interruption_gate.clone(),
            }),
        ))
    }
}

impl AsyncFileSystemSpi for ProviderFileSystemSpi {
    fn properties(&self) -> ProviderProperties {
        self.properties_snapshot()
    }

    fn stat<'a>(&'a self, request: StatRequest<'a>) -> SpiFuture<'a, FsResult<StatResponse>> {
        Box::pin(async move {
            if let Some(gate) = self
                .async_gate
                .as_ref()
                .filter(|g| g.point == AsyncProviderGatePoint::Stat)
            {
                gate.wait().await;
            }
            FileSystemSpi::stat(self, request)
        })
    }

    fn open_reader<'a>(&'a self, request: OpenReaderRequest<'a>) -> SpiFuture<'a, FsResult<OpenedAsyncReader>> {
        Box::pin(async move {
            if let Some(gate) = self
                .async_gate
                .as_ref()
                .filter(|g| g.point == AsyncProviderGatePoint::Open)
            {
                gate.wait().await;
            }
            let (info, bytes) = self.open_reader_inner(request)?;
            Ok(OpenedAsyncReader::new(
                info,
                Box::new(ProviderReader {
                    bytes,
                    position: 0,
                    fail_reads: self.fail_reads,
                    read_error: self.read_error,
                    short_read: self.short_read,
                    observations: Arc::clone(&self.observations),
                    async_gate: self.async_gate.clone(),
                    completion_gate: self.completion_gate.clone(),
                }),
            ))
        })
    }

    fn open_random_access_reader<'a>(
        &'a self,
        request: OpenReaderRequest<'a>,
    ) -> SpiFuture<'a, FsResult<qubit_fs::spi::OpenedAsyncRandomAccessReader>> {
        Box::pin(async move {
            if let Some(gate) = self
                .async_gate
                .as_ref()
                .filter(|g| g.point == AsyncProviderGatePoint::Open)
            {
                gate.wait().await;
            }
            self.observations.random_opens.fetch_add(1, Ordering::SeqCst);
            if let Some(kind) = self.random_open_error {
                return Err(FsError::new(
                    kind,
                    FsOperation::OpenReader,
                    "configured random-access open failure",
                ));
            }
            let info = OpenedFileInfo::new(FileSystemId::new("magika-test").unwrap(), request.path().clone())
                .with_metadata(self.metadata(request.path()));
            Ok(qubit_fs::spi::OpenedAsyncRandomAccessReader::new(
                info,
                Box::new(ProviderSession {
                    content: Arc::clone(&self.content),
                    length: self.length.unwrap_or(self.content.len() as u64),
                    short_read: self.short_read,
                    fail_reads: self.fail_reads,
                    read_error: self.read_error,
                    interrupt_pending: self.interrupt_session_once,
                    observations: Arc::clone(&self.observations),
                    async_gate: self.async_gate.clone(),
                    completion_gate: self.completion_gate.clone(),
                    interruption_gate: self.interruption_gate.clone(),
                }),
            ))
        })
    }
}

struct ProviderSession {
    content: Arc<Vec<u8>>,
    length: u64,
    short_read: bool,
    fail_reads: bool,
    read_error: Option<FsErrorKind>,
    interrupt_pending: bool,
    observations: Arc<ProviderObservations>,
    async_gate: Option<Arc<AsyncProviderGate>>,
    completion_gate: Option<Arc<ProviderReadCompletionGate>>,
    interruption_gate: Option<Arc<ProviderReadCompletionGate>>,
}

impl Drop for ProviderSession {
    fn drop(&mut self) {
        self.observations.session_drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl RandomAccessReadSession for ProviderSession {
    fn read_at(&mut self, offset: u64, output: &mut [u8]) -> FsResult<usize> {
        self.observations
            .session_read_lengths
            .lock()
            .unwrap()
            .push(output.len());
        self.observations
            .session_read_ranges
            .lock()
            .unwrap()
            .push((offset, output.len()));
        if self.fail_reads {
            return Err(FsError::from_io(
                io::Error::other("configured provider read failure"),
                FsOperation::Read,
            ));
        }
        if let Some(kind) = self.read_error.take() {
            return Err(FsError::new(kind, FsOperation::Read, "configured provider read error"));
        }
        if self.interrupt_pending {
            self.interrupt_pending = false;
            if let Some(gate) = &self.interruption_gate {
                gate.pause_if_reached(1);
            }
            return Err(FsError::new(
                FsErrorKind::Interrupted,
                FsOperation::Read,
                "configured unchanged session interruption",
            )
            .with_effect_state(qubit_fs::error::FsEffectState::Unchanged));
        }
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let available = usize::try_from(self.length.saturating_sub(offset)).unwrap_or(usize::MAX);
        let count = output
            .len()
            .min(available)
            .min(if self.short_read { 1 } else { usize::MAX });
        for (index, byte) in output[..count].iter_mut().enumerate() {
            *byte = self.content[start.saturating_add(index) % self.content.len()];
        }
        self.observations.random_reads.fetch_add(count, Ordering::SeqCst);
        if let Some(gate) = &self.completion_gate {
            gate.pause_if_reached(self.observations.random_reads.load(Ordering::SeqCst));
        }
        Ok(count)
    }
}

impl qubit_fs::spi::AsyncRandomAccessReadSession for ProviderSession {
    fn read_at_async<'a>(
        self: std::pin::Pin<&'a mut Self>,
        offset: u64,
        output: &'a mut [u8],
    ) -> SpiFuture<'a, FsResult<usize>> {
        Box::pin(async move {
            if let Some(gate) = self
                .as_ref()
                .get_ref()
                .async_gate
                .as_ref()
                .filter(|g| g.point == AsyncProviderGatePoint::Read)
            {
                gate.wait().await;
            }
            self.get_mut().read_at(offset, output)
        })
    }
}

struct ProviderReader {
    bytes: Vec<u8>,
    position: usize,
    fail_reads: bool,
    read_error: Option<FsErrorKind>,
    short_read: bool,
    observations: Arc<ProviderObservations>,
    async_gate: Option<Arc<AsyncProviderGate>>,
    completion_gate: Option<Arc<ProviderReadCompletionGate>>,
}

impl ProviderReader {
    fn read_bytes(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.observations.range_reads.fetch_add(1, Ordering::SeqCst);
        if let Some(kind) = self.read_error.take() {
            return Err(FsError::new(kind, FsOperation::Read, "configured provider read error").into_io_error());
        }
        if self.fail_reads {
            return Err(io::Error::other("configured provider read failure"));
        }
        let count = output
            .len()
            .min(self.bytes.len().saturating_sub(self.position))
            .min(if self.short_read { 1 } else { usize::MAX });
        output[..count].copy_from_slice(&self.bytes[self.position..self.position + count]);
        self.position += count;
        let total = self.observations.read_bytes.fetch_add(count, Ordering::SeqCst) + count;
        if let Some(gate) = &self.completion_gate {
            gate.pause_if_reached(total);
        }
        Ok(count)
    }
}

impl Input for ProviderReader {
    type Item = u8;

    unsafe fn read_unchecked(&mut self, output: &mut [u8], index: usize, count: usize) -> io::Result<usize> {
        self.read_bytes(&mut output[index..index + count])
    }
}

impl AsyncInput for ProviderReader {
    type Item = u8;

    unsafe fn poll_read_unchecked(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        output: &mut [u8],
        index: usize,
        count: usize,
    ) -> std::task::Poll<io::Result<usize>> {
        if let Some(gate) = self
            .async_gate
            .as_ref()
            .filter(|gate| gate.point == AsyncProviderGatePoint::Read)
            && gate.poll_wait(cx).is_pending()
        {
            return std::task::Poll::Pending;
        }
        std::task::Poll::Ready(self.read_bytes(&mut output[index..index + count]))
    }
}
