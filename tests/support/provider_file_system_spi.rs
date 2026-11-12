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
    random_access: bool,
    random_open_error: Option<FsErrorKind>,
    max_read_range_bytes: Option<u64>,
    observations: Arc<ProviderObservations>,
    async_gate: Option<Arc<AsyncProviderGate>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AsyncProviderGatePoint {
    Stat,
    Open,
    Read,
}

pub(crate) struct AsyncProviderGate {
    point: AsyncProviderGatePoint,
    entered: std::sync::atomic::AtomicBool,
    released: std::sync::atomic::AtomicBool,
    waker: Mutex<Option<Waker>>,
    changed: Condvar,
}

impl AsyncProviderGate {
    pub(crate) fn new(point: AsyncProviderGatePoint) -> Arc<Self> {
        Arc::new(Self {
            point,
            entered: std::sync::atomic::AtomicBool::new(false),
            released: std::sync::atomic::AtomicBool::new(false),
            waker: Mutex::new(None),
            changed: Condvar::new(),
        })
    }
    async fn wait(self: &Arc<Self>) {
        std::future::poll_fn(|cx| {
            if self.released.load(Ordering::Acquire) {
                return std::task::Poll::Ready(());
            }
            *self.waker.lock().unwrap() = Some(cx.waker().clone());
            self.entered.store(true, Ordering::Release);
            self.changed.notify_all();
            if self.released.load(Ordering::Acquire) {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        })
        .await
    }
    pub(crate) fn wait_until_entered(&self, timeout: std::time::Duration) {
        let deadline = std::time::Instant::now() + timeout;
        let mut waker = self.waker.lock().unwrap();
        while !self.entered.load(Ordering::Acquire) {
            let now = std::time::Instant::now();
            assert!(
                now < deadline,
                "provider operation {:?} did not reach its gate",
                self.point
            );
            let (next, result) = self
                .changed
                .wait_timeout(waker, deadline.saturating_duration_since(now))
                .unwrap();
            waker = next;
            assert!(
                !result.timed_out() || self.entered.load(Ordering::Acquire),
                "provider operation {:?} did not reach its gate",
                self.point
            );
        }
    }
    pub(crate) fn release(&self) {
        self.released.store(true, Ordering::Release);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

#[derive(Default)]
pub(crate) struct ProviderObservations {
    pub(crate) stats: AtomicUsize,
    pub(crate) opens: AtomicUsize,
    pub(crate) random_opens: AtomicUsize,
    pub(crate) random_reads: AtomicUsize,
    pub(crate) session_read_lengths: Mutex<Vec<usize>>,
    pub(crate) session_drops: AtomicUsize,
    pub(crate) read_bytes: AtomicUsize,
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
            random_access: false,
            random_open_error: None,
            max_read_range_bytes: None,
            observations: Arc::new(ProviderObservations::default()),
            async_gate: None,
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

    pub(crate) fn with_random_access(mut self) -> Self {
        self.random_access = true;
        self
    }

    pub(crate) fn with_async_gate(mut self, gate: Arc<AsyncProviderGate>) -> Self {
        self.async_gate = Some(gate);
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
                observations: Arc::clone(&self.observations),
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
                observations: Arc::clone(&self.observations),
                async_gate: self.async_gate.clone(),
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
                    observations: Arc::clone(&self.observations),
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
                    observations: Arc::clone(&self.observations),
                    async_gate: self.async_gate.clone(),
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
    observations: Arc<ProviderObservations>,
    async_gate: Option<Arc<AsyncProviderGate>>,
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
        if self.fail_reads {
            return Err(FsError::from_io(
                io::Error::other("configured provider read failure"),
                FsOperation::Read,
            ));
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
    observations: Arc<ProviderObservations>,
}

impl ProviderReader {
    fn read_bytes(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.fail_reads {
            return Err(io::Error::other("configured provider read failure"));
        }
        let count = output.len().min(self.bytes.len().saturating_sub(self.position));
        output[..count].copy_from_slice(&self.bytes[self.position..self.position + count]);
        self.position += count;
        self.observations.read_bytes.fetch_add(count, Ordering::SeqCst);
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
        _: &mut std::task::Context<'_>,
        output: &mut [u8],
        index: usize,
        count: usize,
    ) -> std::task::Poll<io::Result<usize>> {
        std::task::Poll::Ready(self.read_bytes(&mut output[index..index + count]))
    }
}
