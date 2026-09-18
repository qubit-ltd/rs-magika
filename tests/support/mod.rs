// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod detector;
mod failing_read_seek;
mod provider_file_system_spi;
mod real_file_case;
mod sparse_read_seek;
mod static_media_stream_classifier;

pub(crate) use detector::detector;
pub(crate) use failing_read_seek::FailingReadSeek;
pub(crate) use provider_file_system_spi::ProviderFileSystemSpi;
pub(crate) use real_file_case::RealFileCase;
pub(crate) use sparse_read_seek::SparseReadSeek;
pub(crate) use static_media_stream_classifier::StaticMediaStreamClassifier;
