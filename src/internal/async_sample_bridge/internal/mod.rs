// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private error sources used by the asynchronous sample bridge.

mod bridge_closed;

pub(super) use bridge_closed::BridgeClosed;
