// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

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
use qubit_fs::spi::OpenedReader;
use qubit_fs::spi::ProviderOperation;
use qubit_fs::spi::ProviderOperations;
use qubit_fs::spi::ProviderProperties;
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
    observations: Arc<ProviderObservations>,
}

#[derive(Default)]
pub(crate) struct ProviderObservations {
    pub(crate) stats: AtomicUsize,
    pub(crate) opens: AtomicUsize,
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
            observations: Arc::new(ProviderObservations::default()),
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
            FileSystemLimits::unknown(),
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
                observations: Arc::clone(&self.observations),
            }),
        ))
    }
}

impl AsyncFileSystemSpi for ProviderFileSystemSpi {
    fn properties(&self) -> ProviderProperties {
        self.properties_snapshot()
    }

    fn stat<'a>(&'a self, request: StatRequest<'a>) -> SpiFuture<'a, FsResult<StatResponse>> {
        Box::pin(async move { FileSystemSpi::stat(self, request) })
    }

    fn open_reader<'a>(&'a self, request: OpenReaderRequest<'a>) -> SpiFuture<'a, FsResult<OpenedAsyncReader>> {
        Box::pin(async move {
            let (info, bytes) = self.open_reader_inner(request)?;
            Ok(OpenedAsyncReader::new(
                info,
                Box::new(ProviderReader {
                    bytes,
                    position: 0,
                    observations: Arc::clone(&self.observations),
                }),
            ))
        })
    }
}

struct ProviderReader {
    bytes: Vec<u8>,
    position: usize,
    observations: Arc<ProviderObservations>,
}

impl ProviderReader {
    fn read_bytes(&mut self, output: &mut [u8]) -> usize {
        let count = output.len().min(self.bytes.len().saturating_sub(self.position));
        output[..count].copy_from_slice(&self.bytes[self.position..self.position + count]);
        self.position += count;
        self.observations.read_bytes.fetch_add(count, Ordering::SeqCst);
        count
    }
}

impl Input for ProviderReader {
    type Item = u8;

    unsafe fn read_unchecked(&mut self, output: &mut [u8], index: usize, count: usize) -> io::Result<usize> {
        Ok(self.read_bytes(&mut output[index..index + count]))
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
        std::task::Poll::Ready(Ok(self.read_bytes(&mut output[index..index + count])))
    }
}
