// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Blocking helpers for tests exercising the public async operation API.

use std::time::Duration;

use qubit_operation::OperationGroup;
use qubit_operation::OperationManager;
use qubit_operation::ShutdownMode;
use qubit_operation::ShutdownSnapshot;

pub(crate) struct BlockingShutdown {
    pub(crate) snapshot: ShutdownSnapshot,
}

pub(crate) trait ShutdownBlocking {
    fn shutdown_blocking(&self, mode: ShutdownMode, timeout: Duration) -> BlockingShutdown;
}

impl ShutdownBlocking for OperationGroup {
    fn shutdown_blocking(&self, mode: ShutdownMode, timeout: Duration) -> BlockingShutdown {
        let handle = self.request_shutdown(mode);
        let _ = handle.wait_for(timeout).expect("group shutdown should be observable");
        BlockingShutdown { snapshot: handle.snapshot() }
    }
}

impl ShutdownBlocking for OperationManager {
    fn shutdown_blocking(&self, mode: ShutdownMode, timeout: Duration) -> BlockingShutdown {
        let handle = self.request_shutdown(mode);
        let _ = handle.wait_for(timeout).expect("manager shutdown should be observable");
        BlockingShutdown { snapshot: handle.snapshot() }
    }
}
