// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private implementation details for the Magika detector.

mod async_provider_input;
mod async_route;
pub(crate) mod async_sample_bridge;
mod operation_outcome_context;
pub(crate) mod provider_input;
mod provider_path_route;
pub(crate) mod read_budget;
mod read_seek_input;
mod reader_scope;
mod session_read;
mod sync_provider_input;
mod sync_route;

pub(crate) use async_provider_input::AsyncProviderInput;
pub(crate) use async_provider_input::ManagedReadObserver;
pub(crate) use async_provider_input::StopWatcher;
pub(crate) use operation_outcome_context::INFERENCE_OUTCOME;
pub(crate) use operation_outcome_context::PROVIDER_OUTCOME;
pub(crate) use operation_outcome_context::map_operation_outcome;
pub(crate) use provider_input::map_provider_magika_error;
pub(crate) use provider_path_route::ProviderPathRoute;
pub(crate) use provider_path_route::is_unsupported_provider_error;
pub(crate) use provider_path_route::select_provider_path_route;
pub(crate) use read_seek_input::ReadSeekInput;
pub(crate) use reader_scope::ReaderScope;
#[cfg(test)]
pub(crate) use session_read::read_async_session_with_stop;
pub(crate) use sync_provider_input::SyncProviderInput;
