// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Route choices for asynchronous provider inputs.

use qubit_fs::AsyncFileSystem;
use qubit_fs::Path;
use qubit_fs::read::AsyncRandomAccessReader;

/// Selects how an asynchronous provider input obtains resource ranges.
pub(super) enum AsyncRoute {
    /// Filesystem and path owned by the submitted managed operation.
    OwnedLegacy {
        /// Filesystem used to open each range.
        file_system: AsyncFileSystem,
        /// Resource path used for each range.
        path: Path,
    },
    /// Reads ranges from one retained random-access session.
    Session {
        /// Opened reader retained for the input's lifetime.
        reader: AsyncRandomAccessReader,
        /// Optional limit on the number of bytes requested by one read call.
        max_segment: Option<usize>,
    },
}
