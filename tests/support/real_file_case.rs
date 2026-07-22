// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Expected MIME result for a real fixture file.
#[derive(Debug)]
pub(crate) struct RealFileCase {
    /// Path relative to the crate root.
    pub(crate) relative_path: &'static str,
    /// Expected MIME type name.
    pub(crate) expected_mime: &'static str,
}
