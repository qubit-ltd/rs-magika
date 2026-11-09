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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

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

const ID: &str = "magika-session-adapter-test";

#[derive(Clone)]
struct Fixture {
    opens: Arc<AtomicUsize>,
    stats: Arc<AtomicUsize>,
    path_opens: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
    reads: Arc<std::sync::Mutex<Vec<(u64, usize)>>>,
    bytes: Arc<std::sync::RwLock<Arc<[u8]>>>,
    max_read: FileSystemLimit,
    interrupt_effect: Option<Option<FsEffectState>>,
    interrupt_kind: FsErrorKind,
    attempts: Arc<AtomicUsize>,
}

impl Fixture {
    fn new(bytes: &'static [u8], max_read: FileSystemLimit) -> Self {
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
            attempts: Arc::new(AtomicUsize::new(0)),
        }
    }
    fn interrupt_once(mut self, effect: Option<FsEffectState>) -> Self {
        self.interrupt_effect = Some(effect);
        self
    }
    fn fail_once(mut self, kind: FsErrorKind, effect: Option<FsEffectState>) -> Self {
        self.interrupt_effect = Some(effect);
        self.interrupt_kind = kind;
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
            attempts: self.attempts.clone(),
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
        Box::pin(async move { FileSystemSpi::stat(self, request) })
    }
    fn open_reader<'a>(&'a self, _request: OpenReaderRequest<'a>) -> SpiFuture<'a, FsResult<OpenedAsyncReader>> {
        Box::pin(async {
            Err(FsError::new(
                FsErrorKind::UnsupportedOperation,
                FsOperation::OpenReader,
                "legacy reader unused",
            ))
        })
    }
    fn open_random_access_reader<'a>(
        &'a self,
        request: OpenReaderRequest<'a>,
    ) -> SpiFuture<'a, FsResult<OpenedAsyncRandomAccessReader>> {
        Box::pin(async move {
            Ok(OpenedAsyncRandomAccessReader::new(
                self.info(request.path()),
                Box::new(self.session()),
            ))
        })
    }
}

struct Session {
    bytes: Arc<[u8]>,
    reads: Arc<std::sync::Mutex<Vec<(u64, usize)>>>,
    drops: Arc<AtomicUsize>,
    interrupt_effect: Option<Option<FsEffectState>>,
    interrupt_kind: FsErrorKind,
    attempts: Arc<AtomicUsize>,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl RandomAccessReadSession for Session {
    fn read_at(&mut self, offset: u64, output: &mut [u8]) -> FsResult<usize> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
        self.reads.lock().unwrap().push((offset, output.len()));
        if attempt == 0
            && let Some(effect) = self.interrupt_effect
        {
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
        Box::pin(async move { self.get_mut().read_at(offset, output) })
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }
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
fn test_async_session_retries_unchanged_interruption_at_same_range() {
    assert_async_session_retries_safe_interruption(Some(FsEffectState::Unchanged));
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
