// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! First event that closes a request admission handoff.

/// Records why the request admission handoff stopped accepting transfers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum HandoffCloseReason {
    /// The request or its parent scope stopped before transfer completed.
    RequestStopped(
        /// Backend detail explaining which stop signal closed the handoff.
        &'static str,
    ),
    /// The caller abandoned registration before completing the handoff.
    Abandoned,
}

impl HandoffCloseReason {
    /// Returns the stable detail used to report this handoff stop.
    ///
    /// # Returns
    ///
    /// The request stop detail, or the fixed abandoned-handoff description.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::RequestStopped(reason) => reason,
            Self::Abandoned => "request admission handoff closed",
        }
    }
}
