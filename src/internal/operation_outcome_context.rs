// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Translation of private operation outcomes into MIME errors.

use qubit_mime::MimeError;
use qubit_mime::MimeResult;
use qubit_operation::OperationOutcome;

/// Existing wording used by the detector inference entry points.
pub(crate) const INFERENCE_OUTCOME: OperationOutcomeContext = OperationOutcomeContext {
    stopped: "inference stopped",
    child_failed: "inference child failed",
    panicked: "inference operation panicked",
    executor_failed: "inference executor failed",
};

/// Existing wording used by provider path entry points.
pub(crate) const PROVIDER_OUTCOME: OperationOutcomeContext = OperationOutcomeContext {
    stopped: "provider inference stopped",
    child_failed: "provider child failed",
    panicked: "provider operation panicked",
    executor_failed: "provider executor failed",
};

/// Context-specific text retained when an operation has no successful result.
#[derive(Clone, Copy)]
pub(crate) struct OperationOutcomeContext {
    /// Text used when the operation is stopped.
    pub(crate) stopped: &'static str,
    /// Text used when a child operation fails.
    pub(crate) child_failed: &'static str,
    /// Text used when the operation panics.
    pub(crate) panicked: &'static str,
    /// Text used when the executor fails.
    pub(crate) executor_failed: &'static str,
}

/// Converts a returned operation outcome without replacing a business failure.
///
/// # Parameters
///
/// * `outcome` - Outcome returned by the operation manager.
/// * `context` - Existing entry-point wording for non-business failures.
///
/// # Type Parameters
///
/// * `T` - Successful value returned by the operation.
///
/// # Returns
///
/// The successful value.
///
/// # Errors
///
/// Preserves `Failed` errors and maps other unsuccessful outcomes to detector
/// backend errors using the supplied context.
pub(crate) fn map_operation_outcome<T>(
    outcome: OperationOutcome<T, MimeError>,
    context: OperationOutcomeContext,
) -> MimeResult<T> {
    match outcome {
        OperationOutcome::Succeeded(result) => Ok(result),
        OperationOutcome::Failed(error) => Err(error),
        OperationOutcome::Stopped(_) => Err(MimeError::detector_backend("magika", context.stopped)),
        OperationOutcome::ChildFailed(summary) => Err(MimeError::detector_backend(
            "magika",
            format!("{}: {summary:?}", context.child_failed),
        )),
        OperationOutcome::Panicked(_) => Err(MimeError::detector_backend("magika", context.panicked)),
        OperationOutcome::ExecutionFailed(_) => Err(MimeError::detector_backend("magika", context.executor_failed)),
    }
}
