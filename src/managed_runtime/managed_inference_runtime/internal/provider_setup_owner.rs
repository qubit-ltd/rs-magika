// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Ordered ownership of input and admission during provider setup.

use super::super::request_admission::RequestAdmission;
use crate::internal::AsyncProviderInput;

/// Keeps setup input ahead of its admission lease during destruction.
pub(super) struct ProviderSetupOwner {
    /// Owned provider input is destroyed before capacity is released.
    pub(super) input: AsyncProviderInput,
    /// Capacity retained until setup input destruction or worker dispatch.
    pub(super) admission: RequestAdmission,
}
