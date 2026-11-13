// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded asynchronous sampling and inference on one session-owning worker.

pub(super) mod bridge_sync_input;
pub(super) mod byte_budget;
pub(super) mod byte_permit;
pub(super) mod drive_event;
pub(super) mod inference_future;
pub(super) mod job;
mod managed_inference_runtime;
mod managed_inference_shutdown_report;
pub(super) mod request_admission;
pub(super) mod task_control;
pub(super) mod task_inner;
pub(super) mod task_state;
pub(super) mod worker_input;

#[cfg(test)]
pub(crate) use managed_inference_runtime::ManagedInferenceEngine;
pub use managed_inference_runtime::ManagedInferenceRuntime;
pub use managed_inference_shutdown_report::ManagedInferenceShutdownReport;
