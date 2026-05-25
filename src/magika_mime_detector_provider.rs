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
// qubit-style: allow coverage-cfg

#[cfg(coverage)]
use qubit_mime::MimeError;
use qubit_mime::{
    MimeConfig,
    MimeDetector,
    MimeDetectorSpec,
    ProviderCreateError,
    ProviderDescriptor,
    ProviderRegistryError,
    ServiceProvider,
};

use crate::MagikaMimeDetector;

/// Provider for [`MagikaMimeDetector`].
#[derive(Debug, Clone, Copy, Default)]
pub struct MagikaMimeDetectorProvider;

impl ServiceProvider<MimeDetectorSpec> for MagikaMimeDetectorProvider {
    /// Gets Magika detector metadata.
    #[inline]
    fn descriptor(&self) -> Result<ProviderDescriptor, ProviderRegistryError> {
        let descriptor = ProviderDescriptor::new("magika")
            .expect("Magika detector provider id should be valid")
            .with_aliases(&["magika-mime-detector", "MagikaMimeDetector"])
            .expect("Magika detector provider aliases should be valid")
            .with_priority(20);
        Ok(descriptor)
    }

    /// Creates a Magika-backed detector.
    #[inline]
    fn create_box(&self, config: &MimeConfig) -> Result<Box<dyn MimeDetector>, ProviderCreateError> {
        MagikaMimeDetector::from_mime_config(config.clone())
            .map(|detector| Box::new(detector) as Box<dyn MimeDetector>)
            .map_err(|error| ProviderCreateError::failed(&error.to_string()))
    }
}

/// Exercises provider creation error conversion in coverage builds.
///
/// # Returns
/// Provider creation error converted from a synthetic Magika detector error.
#[cfg(coverage)]
pub fn coverage_map_provider_create_error() -> ProviderCreateError {
    let error = MimeError::detector_backend("magika", "coverage provider failure");
    ProviderCreateError::failed(&error.to_string())
}
