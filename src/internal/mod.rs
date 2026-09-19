// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private implementation details for the Magika detector.

mod async_provider_input;
mod budget_exceeded;
mod provider_input;
mod read_budget;
mod read_seek_input;
mod reader_scope;
mod sync_provider_input;

pub(crate) use async_provider_input::AsyncProviderInput;
pub(crate) use provider_input::map_provider_magika_error;
pub(crate) use read_seek_input::ReadSeekInput;
pub(crate) use reader_scope::ReaderScope;
pub(crate) use sync_provider_input::SyncProviderInput;
