// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pure route selection shared by synchronous and asynchronous providers.

use qubit_fs::error::FsError;
use qubit_fs::error::FsErrorKind;
use qubit_fs::metadata::ResourceVersion;

/// Route selected after session open and metadata lookup have completed.
pub(crate) enum ProviderPathRoute {
    /// Sample known-length content using provider range reads.
    Range {
        /// Complete logical resource length.
        length: u64,
        /// Conditional version, only when advertised and available.
        version: Option<ResourceVersion>,
    },
    /// Read the complete resource within the caller's bound.
    ReadAll,
    /// Known-length content exceeds the bound and cannot use ranges.
    TooLarge { requested: usize },
}

/// Identifies the unsupported errors that provider-route callers handle specially.
///
/// Callers decide the meaning by operation: unsupported session open permits
/// a legacy route, while unsupported stat means only that length is unknown.
pub(crate) fn is_unsupported_provider_error(error: &FsError) -> bool {
    matches!(
        error.kind(),
        FsErrorKind::UnsupportedOperation | FsErrorKind::UnsupportedCapability
    )
}

/// Selects the legacy route after random-access sessions were unsupported.
///
/// # Parameters
///
/// * `length` - Optional length returned by stat.
/// * `etag` - Optional ETag returned by stat.
/// * `supports_range` - Whether the provider declares range reads.
/// * `supports_conditional` - Whether conditional reads are declared.
/// * `limit` - Maximum allowed full-read size and cumulative sampling budget.
///
/// # Returns
///
/// The same route decision for sync and async providers.
pub(crate) fn select_provider_path_route(
    length: Option<u64>,
    etag: Option<ResourceVersion>,
    supports_range: bool,
    supports_conditional: bool,
    limit: usize,
) -> ProviderPathRoute {
    if let Some(length) = length {
        if supports_range {
            return ProviderPathRoute::Range {
                length,
                version: supports_conditional.then_some(etag).flatten(),
            };
        }
        if length > limit as u64 {
            return ProviderPathRoute::TooLarge {
                requested: usize::try_from(length).unwrap_or(usize::MAX),
            };
        }
    }
    ProviderPathRoute::ReadAll
}
