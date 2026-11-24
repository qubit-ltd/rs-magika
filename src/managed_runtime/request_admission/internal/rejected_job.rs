// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! A job rejected before request admission completes.

use super::super::super::job::Job;

/// A job whose request admission was rejected.
pub(crate) struct RejectedJob {
    /// The original job that must be settled after rejection.
    pub(in crate::managed_runtime::request_admission) job: Job,
    /// The reason reported when admission rejected the job.
    pub(in crate::managed_runtime::request_admission) reason: &'static str,
}
