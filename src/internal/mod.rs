// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private implementation details for the Magika detector.

mod provider_input;
mod read_seek_input;

pub(crate) use provider_input::AsyncProviderInput;
pub(crate) use provider_input::SyncProviderInput;
pub(crate) use provider_input::map_provider_magika_error;
pub(crate) use read_seek_input::ReadSeekInput;
