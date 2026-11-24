// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Drop guard that closes an unfinished request admission handoff.

use std::sync::Arc;

use super::super::RequestAdmissionHandoff;

/// Closes an unfinished handoff when its caller is dropped.
pub(crate) struct RequestAdmissionHandoffGuard {
    /// Shared state closed unless registration is handed to the child.
    pub(in crate::managed_runtime::request_admission_handoff) handoff: Arc<RequestAdmissionHandoff>,
    /// Whether dropping this guard should close the shared handoff.
    active: bool,
}

impl RequestAdmissionHandoffGuard {
    /// Disarms caller-drop closure after the child owns the admission.
    pub(in crate::managed_runtime::request_admission_handoff) fn finish(&mut self) {
        self.active = false;
    }
}

impl Drop for RequestAdmissionHandoffGuard {
    /// Closes an abandoned handoff and wakes its child outside the mutex.
    fn drop(&mut self) {
        if self.active {
            self.handoff.close();
        }
    }
}
