// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private error sources for provider input handling.

mod observed_read_stopped;

pub(super) use observed_read_stopped::ObservedReadStopped;
