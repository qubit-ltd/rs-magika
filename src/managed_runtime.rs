// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded asynchronous sampling and inference on one session-owning worker.

pub(super) mod admission_gate;
pub(super) mod bridge_sync_input;
pub(super) mod byte_budget;
pub(super) mod byte_permit;
pub(super) mod drive_event;
pub(super) mod inference_future;
pub(super) mod job;
mod managed_inference_runtime;
mod managed_inference_shutdown_report;
mod managed_inference_snapshot;
pub(super) mod provider_driver_guard;
pub(super) mod request_admission;
mod request_resource_cleanup;
mod request_resource_completion_facts;
pub(super) mod request_resource_owner;
pub(super) mod service_permit;
pub(super) mod task_control;
pub(super) mod task_inner;
pub(super) mod task_state;
pub(super) mod worker_input;

pub use managed_inference_runtime::ManagedInferenceRuntime;
pub use managed_inference_shutdown_report::ManagedInferenceShutdownReport;
pub use managed_inference_snapshot::ManagedInferenceSnapshot;
