// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Session-backed Magika adapter contracts without loading the model.
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::time::Duration;
use std::time::Instant;

use magika::AsyncInput;
use magika::SyncInput;
use qubit_fs::AsyncFileSystem;
use qubit_fs::FileSystem;
use qubit_fs::FsResult;
use qubit_fs::Path;
use qubit_fs::error::FsEffectState;
use qubit_fs::error::FsError;
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
use qubit_fs::metadata::SymlinkPolicy;
use qubit_fs::path::PathConstraints;
use qubit_fs::path::PathSemantics;
use qubit_fs::read::ReadOptions;
use qubit_fs::spi::AsyncFileSystemSpi;
use qubit_fs::spi::AsyncRandomAccessReadSession;
use qubit_fs::spi::FileSystemSpi;
use qubit_fs::spi::OpenReaderRequest;
use qubit_fs::spi::OpenedAsyncRandomAccessReader;
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
use qubit_io::AsyncInput as AsyncStreamInput;
use qubit_mime::MimeConfig;
use qubit_mime::MimeDetectorBackend;
use qubit_operation::GroupOptions;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;

use crate::internal::AsyncSessionReadOptions;

const ID: &str = "magika-session-adapter-test";

#[derive(Clone)]
pub(crate) struct Fixture {
    opens: Arc<AtomicUsize>,
    stats: Arc<AtomicUsize>,
    path_opens: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    reads: Arc<std::sync::Mutex<Vec<(u64, usize)>>>,
    bytes: Arc<std::sync::RwLock<Arc<[u8]>>>,
    max_read: FileSystemLimit,
    interrupt_effect: Option<Option<FsEffectState>>,
    interrupt_kind: FsErrorKind,
    interrupt_pattern: Option<Vec<bool>>,
    stop_on_interrupt: Option<Arc<qubit_operation::OperationContext>>,
    attempts: Arc<AtomicUsize>,
    async_read_gate: Option<Arc<TestAsyncReadGate>>,
    async_open_gate: Option<Arc<TestAsyncReadGate>>,
    async_stat_gate: Option<Arc<TestAsyncReadGate>>,
    async_stream_read_gate: Option<Arc<TestAsyncReadGate>>,
    async_random_access_unsupported: bool,
    session_drop_observer: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl Fixture {
    pub(crate) fn new(bytes: &'static [u8], max_read: FileSystemLimit) -> Self {
        Self {
            opens: Arc::new(AtomicUsize::new(0)),
            stats: Arc::new(AtomicUsize::new(0)),
            path_opens: Arc::new(AtomicUsize::new(0)),
            drops: Arc::new(AtomicUsize::new(0)),
            reads: Arc::new(std::sync::Mutex::new(Vec::new())),
            bytes: Arc::new(std::sync::RwLock::new(Arc::from(bytes))),
            max_read,
            interrupt_effect: None,
            interrupt_kind: FsErrorKind::Interrupted,
            interrupt_pattern: None,
            stop_on_interrupt: None,
            attempts: Arc::new(AtomicUsize::new(0)),
            async_read_gate: None,
            async_open_gate: None,
            async_stat_gate: None,
            async_stream_read_gate: None,
            async_random_access_unsupported: false,
            session_drop_observer: None,
        }
    }
    pub(crate) fn with_async_read_gate(mut self, gate: Arc<TestAsyncReadGate>) -> Self {
        self.async_read_gate = Some(gate);
        self
    }
    pub(crate) fn with_async_open_gate(mut self, gate: Arc<TestAsyncReadGate>) -> Self {
        self.async_open_gate = Some(gate);
        self
    }
    pub(crate) fn with_async_stat_gate(mut self, gate: Arc<TestAsyncReadGate>) -> Self {
        self.async_stat_gate = Some(gate);
        self
    }
    pub(crate) fn with_async_stream_read_gate(mut self, gate: Arc<TestAsyncReadGate>) -> Self {
        self.async_stream_read_gate = Some(gate);
        self
    }
    pub(crate) fn without_async_random_access(mut self) -> Self {
        self.async_random_access_unsupported = true;
        self
    }
    fn interrupt_once(mut self, effect: Option<FsEffectState>) -> Self {
        self.interrupt_effect = Some(effect);
        self
    }
    fn stop_after_interruption(mut self, stop: Arc<qubit_operation::OperationContext>) -> Self {
        self.stop_on_interrupt = Some(stop);
        self
    }
    fn fail_once(mut self, kind: FsErrorKind, effect: Option<FsEffectState>) -> Self {
        self.interrupt_effect = Some(effect);
        self.interrupt_kind = kind;
        self
    }
    fn interrupt_pattern(mut self, pattern: Vec<bool>, effect: Option<FsEffectState>) -> Self {
        self.interrupt_pattern = Some(pattern);
        self.interrupt_effect = Some(effect);
        self
    }
    fn properties(&self) -> ProviderProperties {
        ProviderProperties::new(
            FileSystemInfo::new(FileSystemId::new(ID).unwrap(), "fixture", PathSemantics::Hierarchical),
            ProviderOperations::new()
                .with(ProviderOperation::OpenReader)
                .with(ProviderOperation::OpenRandomAccessReader),
            FileSystemCapabilities::new().with_guaranteed(FileSystemCapability::Read),
            FileSystemLimits::unknown().with_max_read_range_bytes(self.max_read),
            PathConstraints::absolute(),
            SymlinkPolicy::Reject,
        )
        .unwrap()
    }
    fn info(&self, path: &Path) -> OpenedFileInfo {
        let length = self.bytes.read().unwrap().len() as u64;
        OpenedFileInfo::new(FileSystemId::new(ID).unwrap(), path.clone())
            .with_metadata(FileMetadata::new(FileKind::File).with_len(Some(length)))
    }
    fn session(&self) -> Session {
        self.opens.fetch_add(1, Ordering::SeqCst);
        Session {
            bytes: self.bytes.read().unwrap().clone(),
            reads: self.reads.clone(),
            drops: self.drops.clone(),
            interrupt_effect: self.interrupt_effect,
            interrupt_kind: self.interrupt_kind,
            interrupt_pattern: self.interrupt_pattern.clone(),
            stop_on_interrupt: self.stop_on_interrupt.clone(),
            attempts: self.attempts.clone(),
            async_read_gate: self.async_read_gate.clone(),
            drop_observer: self.session_drop_observer.clone(),
        }
    }
}

impl FileSystemSpi for Fixture {
    fn properties(&self) -> ProviderProperties {
        Fixture::properties(self)
    }
    fn stat(&self, request: StatRequest<'_>) -> FsResult<StatResponse> {
        self.stats.fetch_add(1, Ordering::SeqCst);
        Ok(StatResponse::new(
            request.path().clone(),
            FileMetadata::new(FileKind::File).with_len(Some(self.bytes.read().unwrap().len() as u64)),
        ))
    }
    fn open_reader(&self, _request: OpenReaderRequest<'_>) -> FsResult<OpenedReader> {
        self.path_opens.fetch_add(1, Ordering::SeqCst);
        Err(FsError::new(
            FsErrorKind::UnsupportedOperation,
            FsOperation::OpenReader,
            "legacy reader unused",
        ))
    }
    fn open_random_access_reader(&self, request: OpenReaderRequest<'_>) -> FsResult<OpenedRandomAccessReader> {
        Ok(OpenedRandomAccessReader::new(
            self.info(request.path()),
            Box::new(self.session()),
        ))
    }
}

impl AsyncFileSystemSpi for Fixture {
    fn properties(&self) -> ProviderProperties {
        Fixture::properties(self)
    }
    fn stat<'a>(&'a self, request: StatRequest<'a>) -> SpiFuture<'a, FsResult<StatResponse>> {
        let gate = self.async_stat_gate.clone();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.wait().await;
            }
            FileSystemSpi::stat(self, request)
        })
    }
    fn open_reader<'a>(&'a self, request: OpenReaderRequest<'a>) -> SpiFuture<'a, FsResult<OpenedAsyncReader>> {
        let Some(gate) = self.async_stream_read_gate.clone() else {
            return Box::pin(async {
                Err(FsError::new(
                    FsErrorKind::UnsupportedOperation,
                    FsOperation::OpenReader,
                    "legacy reader unused",
                ))
            });
        };
        let info = self.info(request.path());
        let bytes = self.bytes.read().unwrap().clone();
        Box::pin(async move {
            Ok(OpenedAsyncReader::new(
                info,
                Box::new(AsyncFixtureStream {
                    bytes,
                    position: 0,
                    gate: Some(gate),
                    gate_wait: None,
                }),
            ))
        })
    }
    fn open_random_access_reader<'a>(
        &'a self,
        request: OpenReaderRequest<'a>,
    ) -> SpiFuture<'a, FsResult<OpenedAsyncRandomAccessReader>> {
        if self.async_random_access_unsupported {
            return Box::pin(async move {
                Err(FsError::new(
                    FsErrorKind::UnsupportedOperation,
                    FsOperation::OpenReader,
                    "random access is disabled by the fixture",
                ))
            });
        }
        let gate = self.async_open_gate.clone();
        Box::pin(async move {
            if let Some(gate) = gate {
                gate.wait().await;
            }
            Ok(OpenedAsyncRandomAccessReader::new(
                self.info(request.path()),
                Box::new(self.session()),
            ))
        })
    }
}

struct AsyncFixtureStream {
    bytes: Arc<[u8]>,
    position: usize,
    gate: Option<Arc<TestAsyncReadGate>>,
    gate_wait: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
}

impl AsyncStreamInput for AsyncFixtureStream {
    type Item = u8;

    unsafe fn poll_read_unchecked(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut [u8],
        index: usize,
        count: usize,
    ) -> Poll<std::io::Result<usize>> {
        let this = self.as_mut().get_mut();
        if count == 0 {
            return Poll::Ready(Ok(0));
        }
        if this.gate_wait.is_none()
            && let Some(gate) = this.gate.take()
        {
            this.gate_wait = Some(Box::pin(async move { gate.wait().await }));
        }
        if let Some(gate_wait) = &mut this.gate_wait {
            if gate_wait.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
            this.gate_wait.take();
        }
        let read = count.min(this.bytes.len().saturating_sub(this.position));
        output[index..index + read].copy_from_slice(&this.bytes[this.position..this.position + read]);
        this.position += read;
        Poll::Ready(Ok(read))
    }
}

struct Session {
    bytes: Arc<[u8]>,
    reads: Arc<std::sync::Mutex<Vec<(u64, usize)>>>,
    drops: Arc<AtomicUsize>,
    interrupt_effect: Option<Option<FsEffectState>>,
    interrupt_kind: FsErrorKind,
    interrupt_pattern: Option<Vec<bool>>,
    stop_on_interrupt: Option<Arc<qubit_operation::OperationContext>>,
    attempts: Arc<AtomicUsize>,
    async_read_gate: Option<Arc<TestAsyncReadGate>>,
    drop_observer: Option<Arc<dyn Fn() + Send + Sync>>,
}

pub(crate) struct TestAsyncReadGate {
    state: std::sync::Mutex<(bool, bool, Option<std::task::Waker>)>,
    changed: std::sync::Condvar,
}

impl TestAsyncReadGate {
    pub(crate) fn new() -> Self {
        Self {
            state: std::sync::Mutex::new((false, false, None)),
            changed: std::sync::Condvar::new(),
        }
    }

    pub(crate) fn started(&self) -> bool {
        self.state.lock().unwrap().1
    }

    pub(crate) fn wait_until_started(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock().unwrap();
        while !state.1 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "provider read did not reach its gate");
            let (next, result) = self.changed.wait_timeout(state, remaining).unwrap();
            state = next;
            assert!(!result.timed_out() || state.1, "provider read did not reach its gate");
        }
    }

    pub(crate) fn release(&self) {
        let waker = {
            let mut state = self.state.lock().unwrap();
            state.0 = true;
            state.2.take()
        };
        self.changed.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    async fn wait(&self) {
        std::future::poll_fn(|cx| {
            let mut state = self.state.lock().unwrap();
            state.1 = true;
            self.changed.notify_all();
            if state.0 {
                Poll::Ready(())
            } else {
                state.2 = Some(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if let Some(observer) = &self.drop_observer {
            observer();
        }
    }
}
impl RandomAccessReadSession for Session {
    fn read_at(&mut self, offset: u64, output: &mut [u8]) -> FsResult<usize> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        self.reads.lock().unwrap().push((offset, output.len()));
        let should_interrupt = self
            .interrupt_pattern
            .as_ref()
            .and_then(|pattern| pattern.get(attempt))
            .copied()
            .unwrap_or(attempt == 0 && self.interrupt_effect.is_some());
        if should_interrupt {
            if let Some(stop) = &self.stop_on_interrupt {
                stop.request_stop(qubit_operation::StopReason::UserCancelled);
            }
            let effect = self.interrupt_effect.unwrap_or(None);
            let error = FsError::new(self.interrupt_kind, FsOperation::Read, "fixture interruption");
            return Err(match effect {
                Some(effect) => error.with_effect_state(effect),
                None => error,
            });
        }
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let count = output.len().min(1).min(self.bytes.len().saturating_sub(start));
        output[..count].copy_from_slice(&self.bytes[start..start + count]);
        Ok(count)
    }
}
impl AsyncRandomAccessReadSession for Session {
    fn read_at_async<'a>(self: Pin<&'a mut Self>, offset: u64, output: &'a mut [u8]) -> SpiFuture<'a, FsResult<usize>> {
        Box::pin(async move {
            let this = self.get_mut();
            if let Some(gate) = this.async_read_gate.clone() {
                gate.wait().await;
            }
            this.read_at(offset, output)
        })
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let wake = Arc::new(ThreadWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(Arc::clone(&wake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut polls = 0usize;
    loop {
        polls += 1;
        assert!(polls <= 100_000, "future exceeded the bounded poll count");
        match future.as_mut().poll(&mut context) {
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

/// Opens a real async random-access session for managed setup lifetime tests.
pub(crate) type ManagedSetupSession = (
    qubit_fs::read::AsyncRandomAccessReader,
    Arc<AtomicUsize>,
    Arc<std::sync::Mutex<Vec<(u64, usize)>>>,
);

pub(crate) fn opened_async_session_for_managed_setup_test(
    on_drop: impl Fn() + Send + Sync + 'static,
) -> ManagedSetupSession {
    let mut fixture = Fixture::new(b"sample", FileSystemLimit::Maximum(4));
    fixture.session_drop_observer = Some(Arc::new(on_drop));
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let reader = block_on(
        filesystem.open_random_access_reader(&Path::parse("/managed-setup.py").unwrap(), ReadOptions::default()),
    )
    .unwrap();
    (reader, Arc::clone(&fixture.drops), Arc::clone(&fixture.reads))
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
fn test_sync_session_fills_direct_buffer_in_short_reads_and_obeys_segment_limit() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Maximum(2));
    let filesystem = FileSystem::from_spi(fixture.clone()).unwrap();
    let reader = filesystem
        .open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())
        .unwrap();
    *fixture.bytes.write().unwrap() = Arc::from(&b"BBBBBB"[..]);
    let mut input = crate::internal::SyncProviderInput::from_session(reader, 6, Some(2));
    let mut output = [0; 4];
    SyncInput::read_at(&mut input, &mut output, 2).unwrap();
    assert_eq!(*b"cdef", output, "session reads must remain bound to the opened object");
    assert_eq!(6, SyncInput::length(&input).unwrap());
    assert!(
        SyncInput::read_at(&mut input, &mut [0; 3], 0).is_err(),
        "segmented reads must reserve the budget once per Magika request"
    );
    assert_eq!(1, fixture.opens.load(Ordering::SeqCst));
    assert_eq!(0, fixture.stats.load(Ordering::SeqCst));
    assert_eq!(0, fixture.path_opens.load(Ordering::SeqCst));
    assert_eq!(vec![(2, 2), (3, 2), (4, 2), (5, 1)], *fixture.reads.lock().unwrap());
    drop(input);
    assert_eq!(1, fixture.drops.load(Ordering::SeqCst));
}

#[test]
fn test_async_session_fills_direct_buffer_in_short_reads_and_obeys_segment_limit() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Maximum(2));
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let reader =
        block_on(filesystem.open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())).unwrap();
    *fixture.bytes.write().unwrap() = Arc::from(&b"BBBBBB"[..]);
    let mut input = crate::internal::AsyncProviderInput::from_session(reader, 6, Some(2));
    let mut output = [0; 4];
    block_on(AsyncInput::read_at(&mut input, &mut output, 2)).unwrap();
    assert_eq!(*b"cdef", output, "session reads must remain bound to the opened object");
    assert_eq!(6, block_on(AsyncInput::length(&input)).unwrap());
    assert!(
        block_on(AsyncInput::read_at(&mut input, &mut [0; 3], 0)).is_err(),
        "segmented reads must reserve the budget once per Magika request"
    );
    assert_eq!(1, fixture.opens.load(Ordering::SeqCst));
    assert_eq!(0, fixture.stats.load(Ordering::SeqCst));
    assert_eq!(0, fixture.path_opens.load(Ordering::SeqCst));
    assert_eq!(vec![(2, 2), (3, 2), (4, 2), (5, 1)], *fixture.reads.lock().unwrap());
    drop(input);
    assert_eq!(1, fixture.drops.load(Ordering::SeqCst));
}

#[test]
fn test_async_session_yields_after_sixty_four_ready_short_reads() {
    let fixture = Fixture::new(b"", FileSystemLimit::Unknown);
    let content: Vec<u8> = (0..4096).map(|index| (index % 251) as u8).collect();
    *fixture.bytes.write().unwrap() = Arc::from(content.clone());
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let mut reader = block_on(
        filesystem.open_random_access_reader(&Path::parse("/cooperative-session").unwrap(), ReadOptions::default()),
    )
    .unwrap();
    let mut budget = crate::internal::read_budget::ReadBudget::new(content.len());
    let mut output = vec![0; content.len()];
    let mut future = Box::pin(crate::internal::read_async_session_with_stop(
        &mut reader,
        content.len() as u64,
        &mut budget,
        &mut output,
        0,
        AsyncSessionReadOptions {
            max_segment: None,
            should_stop: || false,
            io_in_flight: None,
        },
    ));
    let wake = Arc::new(ThreadWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(wake);
    let mut context = Context::from_waker(&waker);

    assert!(future.as_mut().poll(&mut context).is_pending());
    assert_eq!(64, fixture.attempts.load(Ordering::SeqCst));
    block_on(future).expect("cooperative reads should eventually fill the request");
    assert_eq!(content, output);
}

#[test]
fn test_async_session_io_marker_tracks_pending_and_ready_provider_poll() {
    let gate = Arc::new(TestAsyncReadGate::new());
    let fixture = Fixture::new(b"x", FileSystemLimit::Maximum(1)).with_async_read_gate(Arc::clone(&gate));
    let filesystem = AsyncFileSystem::from_spi(fixture).unwrap();
    let mut reader = block_on(
        filesystem.open_random_access_reader(&Path::parse("/session-io-marker").unwrap(), ReadOptions::default()),
    )
    .unwrap();
    let mut budget = crate::internal::read_budget::ReadBudget::new(1);
    let mut output = [0; 1];
    let io_in_flight = Arc::new(AtomicBool::new(false));
    let mut future = Box::pin(crate::internal::read_async_session_with_stop(
        &mut reader,
        1,
        &mut budget,
        &mut output,
        0,
        AsyncSessionReadOptions {
            max_segment: None,
            should_stop: || false,
            io_in_flight: Some(&io_in_flight),
        },
    ));
    let wake = Arc::new(ThreadWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(wake);
    let mut context = Context::from_waker(&waker);

    assert!(future.as_mut().poll(&mut context).is_pending());
    assert!(gate.started(), "provider read must reach its real Pending gate");
    assert!(
        io_in_flight.load(Ordering::Acquire),
        "Pending provider poll must be marked in flight"
    );
    gate.release();
    block_on(future).expect("released provider read should complete");
    assert_eq!(*b"x", output);
    assert!(
        !io_in_flight.load(Ordering::Acquire),
        "Ready provider poll must clear the marker"
    );
}

#[test]
fn test_async_session_stop_after_cooperative_yield_does_not_start_next_read() {
    let fixture = Fixture::new(b"", FileSystemLimit::Unknown);
    let content: Vec<u8> = (0..4096).map(|index| (index % 251) as u8).collect();
    *fixture.bytes.write().unwrap() = Arc::from(content.clone());
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let mut reader = block_on(
        filesystem.open_random_access_reader(&Path::parse("/cooperative-stop").unwrap(), ReadOptions::default()),
    )
    .unwrap();
    let mut budget = crate::internal::read_budget::ReadBudget::new(content.len());
    let mut output = vec![0; content.len()];
    let stop = Arc::new(AtomicBool::new(false));
    let should_stop = Arc::clone(&stop);
    let io_in_flight = Arc::new(AtomicBool::new(false));
    let mut future = Box::pin(crate::internal::read_async_session_with_stop(
        &mut reader,
        content.len() as u64,
        &mut budget,
        &mut output,
        0,
        AsyncSessionReadOptions {
            max_segment: None,
            should_stop: move || should_stop.load(Ordering::Acquire),
            io_in_flight: Some(&io_in_flight),
        },
    ));
    let wake = Arc::new(ThreadWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(wake);
    let mut context = Context::from_waker(&waker);

    assert!(future.as_mut().poll(&mut context).is_pending());
    assert_eq!(64, fixture.attempts.load(Ordering::SeqCst));
    assert!(
        !io_in_flight.load(Ordering::Acquire),
        "cooperative yield must not claim actual I/O"
    );
    stop.store(true, Ordering::Release);
    assert!(matches!(future.as_mut().poll(&mut context), Poll::Ready(Err(_))));
    assert_eq!(
        64,
        fixture.attempts.load(Ordering::SeqCst),
        "stop after yielding must block read 65"
    );
}

#[test]
fn test_async_session_yields_before_retry_after_sixty_fourth_ready_interruption() {
    let mut pattern = Vec::with_capacity(65);
    for _ in 0..7 {
        pattern.extend([true; 7]);
        pattern.push(false);
    }
    pattern.extend([true; 8]);
    pattern.push(false);
    let fixture =
        Fixture::new(b"", FileSystemLimit::Unknown).interrupt_pattern(pattern, Some(FsEffectState::Unchanged));
    let content: Vec<u8> = (0..4096).map(|index| (index % 251) as u8).collect();
    *fixture.bytes.write().unwrap() = Arc::from(content.clone());
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let mut reader = block_on(filesystem.open_random_access_reader(
        &Path::parse("/cooperative-interrupted-session").unwrap(),
        ReadOptions::default(),
    ))
    .unwrap();
    let mut budget = crate::internal::read_budget::ReadBudget::new(content.len());
    let mut output = vec![0; content.len()];
    let mut future = Box::pin(crate::internal::read_async_session_with_stop(
        &mut reader,
        content.len() as u64,
        &mut budget,
        &mut output,
        0,
        AsyncSessionReadOptions {
            max_segment: None,
            should_stop: || false,
            io_in_flight: None,
        },
    ));
    let wake = Arc::new(ThreadWake {
        thread: std::thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(wake);
    let mut context = Context::from_waker(&waker);

    assert!(future.as_mut().poll(&mut context).is_pending());
    assert_eq!(64, fixture.attempts.load(Ordering::SeqCst));
    block_on(future).expect("safe interruptions should remain retryable after yielding");
    assert_eq!(content, output);
}

#[test]
fn test_async_session_stops_between_ready_short_reads() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Maximum(1));
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let mut reader = block_on(filesystem.open_random_access_reader(
        &Path::parse("/cancel-between-short-reads").unwrap(),
        ReadOptions::default(),
    ))
    .unwrap();
    let mut budget = crate::internal::read_budget::ReadBudget::new(6);
    let mut output = [0; 3];
    let result = block_on(crate::internal::read_async_session_with_stop(
        &mut reader,
        6,
        &mut budget,
        &mut output,
        0,
        AsyncSessionReadOptions {
            max_segment: Some(1),
            should_stop: || !fixture.reads.lock().unwrap().is_empty(),
            io_in_flight: None,
        },
    ));

    assert!(
        result.is_err(),
        "stop after the first completed read must prevent the next read"
    );
    assert_eq!(vec![(0, 1)], *fixture.reads.lock().unwrap());
}

#[test]
fn test_managed_provider_session_bridge_transfers_ranges_and_keeps_executor_live() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(GroupOptions { capacity: 2 }).unwrap();
    let fixture = Fixture::new(b"sample", FileSystemLimit::Maximum(2));
    let expected = b"sample".to_vec();
    let engine = Box::new(move |bytes: Vec<u8>| {
        assert_eq!(expected, bytes);
        Ok(Some("application/x-fixture".to_owned()))
    });
    let max_in_flight = MimeConfig::default().max_buffer_size();
    let runtime = Arc::new(crate::managed_runtime::ManagedInferenceRuntime::with_engine(
        supervisor.clone(),
        max_in_flight,
        engine,
        Box::new(|| {}),
    ));
    let detector = Arc::new(
        crate::MagikaMimeDetector::builder()
            .managed_runtime(Arc::clone(&runtime))
            .build()
            .unwrap(),
    );
    let heartbeats = Arc::new(AtomicUsize::new(0));
    let body_detector = Arc::clone(&detector);
    let body_heartbeats = Arc::clone(&heartbeats);
    let mut handle = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            let file_system = AsyncFileSystem::from_spi(fixture).unwrap();
            let path = Path::parse("/bridge/sample").unwrap();
            let mut detection =
                Box::pin(body_detector.detect_async_provider_path_managed(&scope, &file_system, &path, 6));
            let result = std::future::poll_fn(|cx| {
                body_heartbeats.fetch_add(1, Ordering::SeqCst);
                cx.waker().wake_by_ref();
                detection.as_mut().poll(cx)
            })
            .await
            .unwrap();
            assert_eq!(Some("application/x-fixture".to_owned()), result);
            Ok::<(), ()>(())
        })
        .unwrap();
    assert!(matches!(
        block_on(handle.wait_outcome()).unwrap(),
        qubit_operation::OperationOutcome::Succeeded(())
    ));
    assert!(heartbeats.load(Ordering::SeqCst) > 1);
    let report = detector.shutdown_blocking(qubit_operation::ShutdownMode::Drain, Duration::from_secs(30));
    assert!(report.worker_stopped);
}

#[test]
fn test_legacy_async_provider_bridge_samples_on_worker_without_a_parent_scope() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let fixture = Fixture::new(b"sample", FileSystemLimit::Maximum(2));
    let expected = b"sample".to_vec();
    let engine_gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let worker_gate = Arc::clone(&engine_gate);
    let engine = Box::new(move |bytes: Vec<u8>| {
        assert_eq!(expected, bytes);
        let (released, changed) = &*worker_gate;
        let mut released = released.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        Ok(Some("application/x-fixture".to_owned()))
    });
    let runtime = Arc::new(crate::managed_runtime::ManagedInferenceRuntime::with_engine(
        supervisor,
        MimeConfig::default().max_buffer_size(),
        engine,
        Box::new(|| {}),
    ));
    let detector = crate::MagikaMimeDetector::builder()
        .managed_runtime(runtime)
        .build()
        .unwrap();
    let file_system = AsyncFileSystem::from_spi(fixture).unwrap();
    let path = Path::parse("/bridge/legacy").unwrap();
    #[derive(Debug, PartialEq)]
    enum WorkerEvent {
        DetectionPending,
        Heartbeat,
        ReadyBeforePending,
    }
    let (events_tx, events_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let mut heartbeats = 0usize;
        let mut detection = Box::pin(MimeDetectorBackend::guess_from_async_provider_path(
            &detector,
            &file_system,
            &path,
            6,
        ));
        let mut pending_observed = false;
        let mut heartbeat_observed = false;
        let result = block_on(std::future::poll_fn(|cx| {
            match detection.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    if !pending_observed {
                        let _ = events_tx.send(WorkerEvent::ReadyBeforePending);
                    }
                    Poll::Ready(result)
                }
                Poll::Pending => {
                    if !pending_observed {
                        pending_observed = true;
                        let _ = events_tx.send(WorkerEvent::DetectionPending);
                    }
                    heartbeats += 1;
                    if !heartbeat_observed && heartbeats > 1 {
                        heartbeat_observed = true;
                        let _ = events_tx.send(WorkerEvent::Heartbeat);
                    }
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }));
        (result, heartbeats)
    });

    let first_poll = crate::tests::internal::bridge_wait::wait_for_stage(
        &events_rx,
        &WorkerEvent::DetectionPending,
        "legacy detection first Pending",
        Duration::from_secs(5),
        || {
            let (released, changed) = &*engine_gate;
            *released.lock().unwrap() = true;
            changed.notify_all();
        },
    );
    if let Err(stage) = first_poll {
        let _ = crate::tests::internal::bridge_wait::join_before(
            worker,
            &stage,
            Duration::from_secs(5),
        );
        panic!("{stage}");
    }
    let heartbeat = crate::tests::internal::bridge_wait::wait_for_stage(
        &events_rx,
        &WorkerEvent::Heartbeat,
        "legacy detection worker heartbeat after Pending",
        Duration::from_secs(5),
        || {
            let (released, changed) = &*engine_gate;
            *released.lock().unwrap() = true;
            changed.notify_all();
        },
    );
    if let Err(stage) = heartbeat {
        let _ = crate::tests::internal::bridge_wait::join_before(
            worker,
            &stage,
            Duration::from_secs(5),
        );
        panic!("{stage}");
    }
    let (released, changed) = &*engine_gate;
    *released.lock().unwrap() = true;
    changed.notify_all();
    let (detection_result, heartbeats) = crate::tests::internal::bridge_wait::join_before(
        worker,
        "legacy provider bridge completion",
        Duration::from_secs(5),
    );
    let candidates = detection_result.unwrap().0;
    assert_eq!(vec!["application/x-fixture"], candidates);
    assert!(heartbeats > 1);
}

#[test]
fn test_blocked_single_poll_is_supervised_and_released_by_parent_thread() {
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let worker_gate = Arc::clone(&gate);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (finished_tx, finished_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        struct BlockInsidePoll {
            gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
            entered: std::sync::mpsc::Sender<()>,
        }
        impl Future for BlockInsidePoll {
            type Output = ();

            fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
                let _ = self.entered.send(());
                let (released, changed) = &*self.gate;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = changed.wait(released).unwrap();
                }
                Poll::Ready(())
            }
        }

        let wake = Arc::new(ThreadWake {
            thread: std::thread::current(),
            notified: AtomicBool::new(false),
        });
        let waker = std::task::Waker::from(wake);
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(BlockInsidePoll {
            gate: worker_gate,
            entered: entered_tx,
        });
        let result = future.as_mut().poll(&mut context);
        let ready = result.is_ready();
        let _ = finished_tx.send(ready);
        ready
    });

    let entered = crate::tests::internal::bridge_wait::wait_for_stage(
        &entered_rx,
        &(),
        "synthetic poll entered its blocking call",
        Duration::from_secs(5),
        || {
            let (released, changed) = &*gate;
            *released.lock().unwrap() = true;
            changed.notify_all();
        },
    );
    if let Err(stage) = entered {
        let _ = crate::tests::internal::bridge_wait::join_before(
            worker,
            &stage,
            Duration::from_secs(5),
        );
        panic!("{stage}");
    }

    let blocked_poll = crate::tests::internal::bridge_wait::wait_for_stage(
        &finished_rx,
        &true,
        "synthetic poll remained blocked through watchdog deadline",
        Duration::from_millis(50),
        || {
            let (released, changed) = &*gate;
            *released.lock().unwrap() = true;
            changed.notify_all();
        },
    );
    let timeout_failure = match blocked_poll {
        Err(stage) => stage,
        Ok(()) => {
            let _ = crate::tests::internal::bridge_wait::join_before(
                worker,
                "synthetic poll unexpectedly completed before watchdog deadline",
                Duration::from_secs(5),
            );
            panic!("synthetic poll completed before the watchdog deadline");
        }
    };
    let ready_after_release = crate::tests::internal::bridge_wait::join_before(
        worker,
        &timeout_failure,
        Duration::from_secs(5),
    );
    assert!(
        timeout_failure.contains("timed out"),
        "watchdog failure should remain recorded: {timeout_failure}"
    );
    assert!(
        ready_after_release,
        "poll should return Ready after its gate is opened"
    );
}

#[test]
fn test_dropping_managed_provider_driver_keeps_release_pending_without_io_completion() {
    let supervisor = OperationManager::new(ManagerOptions::default()).unwrap();
    let service = supervisor.group(GroupOptions { capacity: 2 }).unwrap();
    let provider_gate = Arc::new(TestAsyncReadGate::new());
    let fixture = Fixture::new(b"sample", FileSystemLimit::Maximum(2)).with_async_read_gate(Arc::clone(&provider_gate));
    let (worker_held_tx, worker_held_rx) = std::sync::mpsc::channel();
    let release_gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let after_business_gate = Arc::clone(&release_gate);
    let engine = Box::new(|_bytes: Vec<u8>| Ok(Some("application/x-fixture".to_owned())));
    let max_in_flight = MimeConfig::default().max_buffer_size();
    let runtime = Arc::new(crate::managed_runtime::ManagedInferenceRuntime::with_engine(
        supervisor.clone(),
        max_in_flight,
        engine,
        Box::new(move || {
            worker_held_tx.send(()).unwrap();
            let (lock, ready) = &*after_business_gate;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
        }),
    ));
    let detector = Arc::new(
        crate::MagikaMimeDetector::builder()
            .managed_runtime(Arc::clone(&runtime))
            .build()
            .unwrap(),
    );
    let body_detector = Arc::clone(&detector);
    let body_gate = Arc::clone(&provider_gate);
    let mut handle = service
        .as_async()
        .try_submit(OperationOptions::default(), move |scope| async move {
            let file_system = AsyncFileSystem::from_spi(fixture).unwrap();
            let path = Path::parse("/bridge/cancel").unwrap();
            let mut detection = Some(Box::pin(body_detector.detect_async_provider_path_managed(
                &scope,
                &file_system,
                &path,
                6,
            )));
            std::future::poll_fn(|cx| {
                if let Some(future) = detection.as_mut()
                    && future.as_mut().poll(cx).is_ready()
                {
                    panic!("provider gate should keep managed detection pending");
                }
                if body_gate.started() {
                    detection.take();
                    return Poll::Ready(());
                }
                cx.waker().wake_by_ref();
                Poll::Pending
            })
            .await;
            Ok::<(), ()>(())
        })
        .unwrap();

    worker_held_rx.recv_timeout(Duration::from_secs(30)).unwrap();
    let _terminal = block_on(handle.wait_outcome()).unwrap();
    let (lock, ready) = &*release_gate;
    *lock.lock().unwrap() = true;
    ready.notify_all();
    let report = detector.shutdown_blocking(qubit_operation::ShutdownMode::Cancel, Duration::from_millis(50));
    assert!(report.worker_stopped);
    assert!(report.admission.snapshot.service_resources != 0);
}

#[test]
fn test_sync_session_retries_unchanged_interruption_at_same_range() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown).interrupt_once(Some(FsEffectState::Unchanged));
    let filesystem = FileSystem::from_spi(fixture.clone()).unwrap();
    let reader = filesystem
        .open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())
        .unwrap();
    let mut input = crate::internal::SyncProviderInput::from_session(reader, 6, None);
    let mut output = [0; 1];

    SyncInput::read_at(&mut input, &mut output, 3).unwrap();

    assert_eq!(*b"d", output);
    assert_eq!(2, fixture.attempts.load(Ordering::SeqCst));
    assert_eq!(vec![(3, 1), (3, 1)], *fixture.reads.lock().unwrap());
}

#[test]
fn test_sync_session_allows_eight_consecutive_safe_interruptions() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown).interrupt_pattern(
        vec![true, true, true, true, true, true, true, true, false],
        Some(FsEffectState::Unchanged),
    );
    let filesystem = FileSystem::from_spi(fixture.clone()).unwrap();
    let reader = filesystem
        .open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())
        .unwrap();
    let mut input = crate::internal::SyncProviderInput::from_session(reader, 1, None);
    let mut output = [0; 1];

    SyncInput::read_at(&mut input, &mut output, 3).unwrap();

    assert_eq!(*b"d", output);
    assert_eq!(9, fixture.attempts.load(Ordering::SeqCst));
    assert!(SyncInput::read_at(&mut input, &mut [0; 1], 3).is_err());
    assert_eq!(
        9,
        fixture.attempts.load(Ordering::SeqCst),
        "overlapping logical request must reserve budget again before I/O"
    );
}

#[test]
fn test_sync_session_stops_after_ninth_safe_interruption_and_preserves_error() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown).interrupt_pattern(vec![true; 9], None);
    let filesystem = FileSystem::from_spi(fixture.clone()).unwrap();
    let reader = filesystem
        .open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())
        .unwrap();
    let mut input = crate::internal::SyncProviderInput::from_session(reader, 6, None);
    let error = SyncInput::read_at(&mut input, &mut [0; 1], 3).expect_err("ninth interruption must propagate");
    let magika::Error::IOError(io_error) = error else {
        panic!("expected I/O error")
    };
    let source = io_error
        .get_ref()
        .and_then(|source| source.downcast_ref::<FsError>())
        .unwrap();
    assert_eq!(FsErrorKind::Interrupted, source.kind());
    assert_eq!(None, source.effect_state());
    assert_eq!(9, fixture.attempts.load(Ordering::SeqCst));
}

#[test]
fn test_sync_session_resets_retry_count_after_short_read_progress() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown).interrupt_pattern(
        vec![
            true, true, true, true, true, true, true, true, false, true, true, true, true, true, true, true, true,
            false,
        ],
        Some(FsEffectState::Unchanged),
    );
    let filesystem = FileSystem::from_spi(fixture.clone()).unwrap();
    let reader = filesystem
        .open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())
        .unwrap();
    let mut input = crate::internal::SyncProviderInput::from_session(reader, 6, None);
    let mut output = [0; 2];

    SyncInput::read_at(&mut input, &mut output, 2).unwrap();

    assert_eq!(*b"cd", output);
    assert_eq!(18, fixture.attempts.load(Ordering::SeqCst));
    assert_eq!(
        vec![(2, 2); 9].into_iter().chain(vec![(3, 1); 9]).collect::<Vec<_>>(),
        *fixture.reads.lock().unwrap()
    );
}

#[test]
fn test_sync_session_retries_interruption_without_effect_state() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown).interrupt_once(None);
    let filesystem = FileSystem::from_spi(fixture.clone()).unwrap();
    let reader = filesystem
        .open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())
        .unwrap();
    let mut input = crate::internal::SyncProviderInput::from_session(reader, 6, None);
    let mut output = [0; 1];

    SyncInput::read_at(&mut input, &mut output, 3).unwrap();

    assert_eq!(*b"d", output);
    assert_eq!(2, fixture.attempts.load(Ordering::SeqCst));
    assert_eq!(vec![(3, 1), (3, 1)], *fixture.reads.lock().unwrap());
}

#[test]
fn test_sync_session_does_not_retry_indeterminate_interruption() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown).interrupt_once(Some(FsEffectState::Indeterminate));
    let filesystem = FileSystem::from_spi(fixture.clone()).unwrap();
    let reader = filesystem
        .open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())
        .unwrap();
    let mut input = crate::internal::SyncProviderInput::from_session(reader, 6, None);
    let mut output = [0; 1];

    let error = SyncInput::read_at(&mut input, &mut output, 3).expect_err("indeterminate interruption must propagate");

    let magika::Error::IOError(io_error) = error else {
        panic!("filesystem interruption must remain a Magika I/O error");
    };
    let source = io_error
        .get_ref()
        .and_then(|source| source.downcast_ref::<FsError>())
        .expect("typed filesystem error should be retained as the I/O source");
    assert_eq!(FsErrorKind::Interrupted, source.kind());
    assert_eq!(Some(FsEffectState::Indeterminate), source.effect_state());
    assert_eq!(1, fixture.attempts.load(Ordering::SeqCst));
    assert_eq!(vec![(3, 1)], *fixture.reads.lock().unwrap());
}

#[test]
fn test_async_session_retries_interruption_without_effect_state() {
    assert_async_session_retries_safe_interruption(None);
}

#[test]
fn test_async_session_allows_eight_consecutive_safe_interruptions() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown).interrupt_pattern(
        vec![true, true, true, true, true, true, true, true, false],
        Some(FsEffectState::Unchanged),
    );
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let reader =
        block_on(filesystem.open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())).unwrap();
    let mut input = crate::internal::AsyncProviderInput::from_session(reader, 1, None);
    let mut output = [0; 1];

    block_on(AsyncInput::read_at(&mut input, &mut output, 3)).unwrap();

    assert_eq!(*b"d", output);
    assert_eq!(9, fixture.attempts.load(Ordering::SeqCst));
    assert!(block_on(AsyncInput::read_at(&mut input, &mut [0; 1], 3)).is_err());
    assert_eq!(
        9,
        fixture.attempts.load(Ordering::SeqCst),
        "overlapping logical request must reserve budget again before I/O"
    );
}

#[test]
fn test_async_session_stops_after_ninth_safe_interruption_and_preserves_error() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown).interrupt_pattern(vec![true; 9], None);
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let reader =
        block_on(filesystem.open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())).unwrap();
    let mut input = crate::internal::AsyncProviderInput::from_session(reader, 6, None);
    let error =
        block_on(AsyncInput::read_at(&mut input, &mut [0; 1], 3)).expect_err("ninth interruption must propagate");
    let magika::Error::IOError(io_error) = error else {
        panic!("expected I/O error")
    };
    let source = io_error
        .get_ref()
        .and_then(|source| source.downcast_ref::<FsError>())
        .unwrap();
    assert_eq!(FsErrorKind::Interrupted, source.kind());
    assert_eq!(None, source.effect_state());
    assert_eq!(9, fixture.attempts.load(Ordering::SeqCst));
}

#[test]
fn test_async_session_resets_retry_count_after_short_read_progress() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown).interrupt_pattern(
        vec![
            true, true, true, true, true, true, true, true, false, true, true, true, true, true, true, true, true,
            false,
        ],
        Some(FsEffectState::Unchanged),
    );
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let reader =
        block_on(filesystem.open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())).unwrap();
    let mut input = crate::internal::AsyncProviderInput::from_session(reader, 6, None);
    let mut output = [0; 2];

    block_on(AsyncInput::read_at(&mut input, &mut output, 2)).unwrap();

    assert_eq!(*b"cd", output);
    assert_eq!(18, fixture.attempts.load(Ordering::SeqCst));
    assert_eq!(
        vec![(2, 2); 9].into_iter().chain(vec![(3, 1); 9]).collect::<Vec<_>>(),
        *fixture.reads.lock().unwrap()
    );
}

#[test]
fn test_async_session_retries_unchanged_interruption_at_same_range() {
    assert_async_session_retries_safe_interruption(Some(FsEffectState::Unchanged));
}

#[test]
fn test_async_session_cancelled_interruption_does_not_retry() {
    let stop = Arc::new(qubit_operation::OperationContext::new(qubit_operation::ContextOptions::default()).unwrap());
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown)
        .interrupt_once(Some(FsEffectState::Unchanged))
        .stop_after_interruption(Arc::clone(&stop));
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let mut reader =
        block_on(filesystem.open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())).unwrap();
    let mut budget = crate::internal::read_budget::ReadBudget::new(6);
    let watcher = crate::internal::StopWatcher::new(stop.as_ref().clone());
    let error = block_on(crate::internal::read_async_session_with_stop(
        &mut reader,
        6,
        &mut budget,
        &mut [0; 1],
        3,
        AsyncSessionReadOptions {
            max_segment: None,
            should_stop: || watcher.is_stopped(),
            io_in_flight: None,
        },
    ))
    .expect_err("stop after interruption must prevent a retry");

    let magika::Error::IOError(error) = error else {
        panic!("expected I/O cancellation error")
    };
    assert_eq!(std::io::ErrorKind::Interrupted, error.kind());
    assert_eq!("provider session read stopped", error.to_string());
    assert!(crate::internal::is_session_stop_error(&error));
    assert_eq!(1, fixture.attempts.load(Ordering::SeqCst));
    assert_eq!(vec![(3, 1)], *fixture.reads.lock().unwrap());
}

fn assert_async_session_retries_safe_interruption(effect: Option<FsEffectState>) {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown).interrupt_once(effect);
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let reader =
        block_on(filesystem.open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())).unwrap();
    let mut input = crate::internal::AsyncProviderInput::from_session(reader, 6, None);
    let mut output = [0; 1];

    block_on(AsyncInput::read_at(&mut input, &mut output, 3)).unwrap();

    assert_eq!(*b"d", output);
    assert_eq!(2, fixture.attempts.load(Ordering::SeqCst));
    assert_eq!(vec![(3, 1), (3, 1)], *fixture.reads.lock().unwrap());
    assert!(block_on(AsyncInput::read_at(&mut input, &mut [0; 6], 0)).is_err());
    assert_eq!(
        2,
        fixture.attempts.load(Ordering::SeqCst),
        "budget rejection must happen before I/O"
    );
}

#[test]
fn test_async_session_preserves_indeterminate_interruption_without_retry() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown).interrupt_once(Some(FsEffectState::Indeterminate));
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let reader =
        block_on(filesystem.open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())).unwrap();
    let mut input = crate::internal::AsyncProviderInput::from_session(reader, 6, None);
    let error = block_on(AsyncInput::read_at(&mut input, &mut [0; 1], 3))
        .expect_err("indeterminate interruption must propagate");
    let magika::Error::IOError(io_error) = error else {
        panic!("expected I/O error")
    };
    let source = io_error
        .get_ref()
        .and_then(|source| source.downcast_ref::<FsError>())
        .expect("typed filesystem error should be retained");
    assert_eq!(FsErrorKind::Interrupted, source.kind());
    assert_eq!(Some(FsEffectState::Indeterminate), source.effect_state());
    assert_eq!(1, fixture.attempts.load(Ordering::SeqCst));
    assert_eq!(vec![(3, 1)], *fixture.reads.lock().unwrap());
    drop(input);
    assert_eq!(1, fixture.drops.load(Ordering::SeqCst));
}

#[test]
fn test_async_session_preserves_ordinary_error_without_retry() {
    let fixture = Fixture::new(b"abcdef", FileSystemLimit::Unknown)
        .fail_once(FsErrorKind::PermissionDenied, Some(FsEffectState::Unchanged));
    let filesystem = AsyncFileSystem::from_spi(fixture.clone()).unwrap();
    let reader =
        block_on(filesystem.open_random_access_reader(&Path::parse("/same").unwrap(), ReadOptions::default())).unwrap();
    let mut input = crate::internal::AsyncProviderInput::from_session(reader, 6, None);
    let error = block_on(AsyncInput::read_at(&mut input, &mut [0; 1], 3))
        .expect_err("ordinary filesystem error must propagate");
    let magika::Error::IOError(io_error) = error else {
        panic!("expected I/O error")
    };
    let source = io_error
        .get_ref()
        .and_then(|source| source.downcast_ref::<FsError>())
        .expect("typed filesystem error should be retained");
    assert_eq!(FsErrorKind::PermissionDenied, source.kind());
    assert_eq!(Some(FsEffectState::Unchanged), source.effect_state());
    assert_eq!(1, fixture.attempts.load(Ordering::SeqCst));
    assert_eq!(vec![(3, 1)], *fixture.reads.lock().unwrap());
    drop(input);
    assert_eq!(1, fixture.drops.load(Ordering::SeqCst));
}
