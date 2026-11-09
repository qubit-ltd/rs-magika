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
use qubit_fs::spi::OpenedReader;
use qubit_fs::spi::ProviderOperation;
use qubit_fs::spi::ProviderOperations;
use qubit_fs::spi::ProviderProperties;
use qubit_fs::spi::SpiFuture;
use qubit_fs::spi::StatRequest;
use qubit_fs::spi::StatResponse;
use qubit_io::AsyncInput;
use qubit_io::Input;

const ID: &str = "magika-provider-input-test";

#[derive(Clone)]
pub(crate) struct ProviderFileSystemSpi {
    content: Arc<Vec<u8>>,
    range: bool,
    max_read_range_bytes: Option<u64>,
    conditional: bool,
    short_read: bool,
    fail_conditional_on_open: Option<usize>,
    observations: Arc<ProviderObservations>,
}

#[derive(Default)]
pub(crate) struct ProviderObservations {
    pub(crate) opens: AtomicUsize,
    pub(crate) options: Mutex<Vec<ReadOptions>>,
}

impl ProviderFileSystemSpi {
    pub(crate) fn new(content: Vec<u8>) -> Self {
        Self {
            content: Arc::new(content),
            range: false,
            max_read_range_bytes: None,
            conditional: false,
            short_read: false,
            fail_conditional_on_open: None,
            observations: Arc::new(ProviderObservations::default()),
        }
    }

    pub(crate) fn with_range(mut self) -> Self {
        self.range = true;
        self
    }

    pub(crate) fn with_max_read_range_bytes(mut self, maximum: u64) -> Self {
        self.max_read_range_bytes = Some(maximum);
        self
    }

    pub(crate) fn short_reads(mut self) -> Self {
        self.short_read = true;
        self
    }

    pub(crate) fn with_conditional(mut self) -> Self {
        self.range = true;
        self.conditional = true;
        self
    }

    pub(crate) fn fail_conditional_on_open(mut self, index: usize) -> Self {
        self.fail_conditional_on_open = Some(index);
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

    fn metadata(&self, _path: &Path) -> FileMetadata {
        let mut metadata = FileMetadata::new(FileKind::File).with_len(Some(self.content.len() as u64));
        if self.conditional {
            metadata = metadata.with_etag(Some(ResourceVersion::new("v1")));
        }
        metadata
    }

    fn open_reader_inner(&self, request: OpenReaderRequest<'_>) -> FsResult<(OpenedFileInfo, Vec<u8>)> {
        let open_index = self.observations.opens.fetch_add(1, Ordering::SeqCst);
        let options = request.options().options().clone();
        self.observations.options.lock().unwrap().push(options.clone());
        if self.fail_conditional_on_open == Some(open_index) && options.if_match().is_some() {
            return Err(FsError::new(
                FsErrorKind::PreconditionFailed,
                FsOperation::OpenReader,
                "configured conditional read failure",
            ));
        }

        let offset = options.offset().unwrap_or(0);
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        let requested = options.length().unwrap_or(self.content.len() as u64);
        let available = requested.min((self.content.len() as u64).saturating_sub(offset));
        let returned = if self.short_read {
            available.saturating_sub(1)
        } else {
            available
        };
        let end = start.saturating_add(usize::try_from(returned).unwrap_or(usize::MAX));
        let bytes = self.content.get(start..end).unwrap_or_default().to_vec();
        let info = OpenedFileInfo::new(FileSystemId::new(ID).unwrap(), request.path().clone())
            .with_metadata(self.metadata(request.path()));
        Ok((info, bytes))
    }

    fn properties_snapshot(&self) -> ProviderProperties {
        let mut capabilities = FileSystemCapabilities::new().with_guaranteed(FileSystemCapability::Read);
        if self.range {
            capabilities = capabilities.with_guaranteed(FileSystemCapability::RangeRead);
        }
        if self.conditional {
            capabilities = capabilities.with_guaranteed(FileSystemCapability::ConditionalRead);
        }
        let limits = self
            .max_read_range_bytes
            .map_or_else(FileSystemLimits::unknown, |maximum| {
                FileSystemLimits::unknown().with_max_read_range_bytes(FileSystemLimit::Maximum(maximum))
            });
        ProviderProperties::new(
            FileSystemInfo::new(FileSystemId::new(ID).unwrap(), "fixture", PathSemantics::Hierarchical),
            ProviderOperations::new()
                .with(ProviderOperation::OpenReader)
                .with(ProviderOperation::Stat),
            capabilities,
            limits,
            PathConstraints::absolute(),
            SymlinkPolicy::Reject,
        )
        .unwrap()
    }
}

impl FileSystemSpi for ProviderFileSystemSpi {
    fn properties(&self) -> ProviderProperties {
        self.properties_snapshot()
    }

    fn stat(&self, request: StatRequest<'_>) -> FsResult<StatResponse> {
        Ok(StatResponse::new(request.path().clone(), self.metadata(request.path())))
    }

    fn open_reader(&self, request: OpenReaderRequest<'_>) -> FsResult<OpenedReader> {
        let (info, bytes) = self.open_reader_inner(request)?;
        Ok(OpenedReader::new(info, Box::new(ProviderReader::new(bytes))))
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
            Ok(OpenedAsyncReader::new(info, Box::new(ProviderReader::new(bytes))))
        })
    }
}

struct ProviderReader {
    bytes: Vec<u8>,
    position: usize,
}

impl ProviderReader {
    fn new(bytes: Vec<u8>) -> Self {
        Self { bytes, position: 0 }
    }

    fn read_bytes(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = output.len().min(self.bytes.len().saturating_sub(self.position));
        output[..count].copy_from_slice(&self.bytes[self.position..self.position + count]);
        self.position += count;
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
