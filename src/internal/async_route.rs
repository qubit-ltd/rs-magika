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
pub(super) enum AsyncRoute<'a> {
    /// Opens each requested range through the filesystem and resource path.
    Legacy {
        /// Filesystem used to open each range.
        file_system: &'a AsyncFileSystem,
        /// Resource path used for each range.
        path: &'a Path,
    },
    /// Reads ranges from one retained random-access session.
    Session {
        /// Opened reader retained for the input's lifetime.
        reader: AsyncRandomAccessReader,
        /// Optional limit on the number of bytes requested by one read call.
        max_segment: Option<usize>,
    },
}
