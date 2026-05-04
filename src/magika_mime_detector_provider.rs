/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Provider for registering the Magika MIME detector with `qubit-mime`.

use qubit_mime::{
    MimeConfig,
    MimeDetector,
    MimeDetectorProvider,
    MimeDetectorRegistry,
    MimeResult,
};

use crate::MagikaMimeDetector;

/// Provider for [`MagikaMimeDetector`].
#[derive(Debug, Clone, Copy, Default)]
pub struct MagikaMimeDetectorProvider;

impl MimeDetectorProvider for MagikaMimeDetectorProvider {
    /// Gets the canonical provider identifier.
    fn id(&self) -> &'static str {
        "magika"
    }

    /// Gets Magika detector aliases.
    fn aliases(&self) -> &'static [&'static str] {
        &["magika-mime-detector", "MagikaMimeDetector"]
    }

    /// Gives Magika higher auto priority than built-in detectors.
    fn priority(&self) -> i32 {
        20
    }

    /// Creates a Magika-backed detector.
    fn create(&self, config: &MimeConfig) -> MimeResult<Box<dyn MimeDetector>> {
        let detector = MagikaMimeDetector::from_mime_config(config.clone())?;
        Ok(Box::new(detector))
    }
}

/// Registers the Magika MIME detector provider.
///
/// # Parameters
/// - `registry`: Registry to extend.
///
/// # Errors
/// Returns [`MimeError::DuplicateDetectorName`](qubit_mime::MimeError::DuplicateDetectorName)
/// when a Magika id or alias is already registered.
pub fn register_mime_detector(registry: &mut MimeDetectorRegistry) -> MimeResult<()> {
    registry.register(MagikaMimeDetectorProvider)
}
