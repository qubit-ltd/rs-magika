// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider for registering the Magika MIME detector with `qubit-mime`.
// qubit-style: allow coverage-cfg

use std::sync::Arc;

use qubit_mime::{
    MimeConfig,
    MimeDetector,
    MimeDetectorSpec,
    MimeError,
};
use qubit_spi::error::ProviderError;
use qubit_spi::{
    ProviderDescriptor,
    ProviderId,
    ProviderMetadata,
    ServiceProvider,
};

use crate::MagikaMimeDetector;

/// Provider for [`MagikaMimeDetector`].
#[derive(Debug, Clone, Copy, Default)]
pub struct MagikaMimeDetectorProvider;

impl ServiceProvider<MimeDetectorSpec> for MagikaMimeDetectorProvider {
    /// Creates a Magika-backed detector.
    ///
    /// # Parameters
    ///
    /// * `config` - MIME detector configuration copied into the created
    ///   detector.
    ///
    /// # Returns
    ///
    /// A shared Magika-backed MIME detector.
    ///
    /// # Errors
    ///
    /// Returns [`ProviderError`] when Magika or ONNX Runtime cannot
    /// initialize. The error preserves the underlying [`MimeError`] source and
    /// is classified as initialization failure because Magika's upstream error
    /// does not reliably distinguish a missing runtime from an invalid model or
    /// configuration. `OnAbsence` fallback therefore does not suppress it.
    fn create_configured(
        &self,
        config: &MimeConfig,
    ) -> Result<Arc<dyn MimeDetector>, ProviderError> {
        MagikaMimeDetector::from_mime_config(config.clone())
            .map(|detector| Arc::new(detector) as Arc<dyn MimeDetector>)
            .map_err(map_provider_create_error)
    }
}

impl ProviderMetadata for MagikaMimeDetectorProvider {
    /// Returns the stable Magika provider identity and selection metadata.
    ///
    /// # Returns
    ///
    /// The `magika` descriptor, accepted aliases, and automatic-selection
    /// priority.
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(
            ProviderId::new("magika")
                .expect("Magika provider ID should be valid"),
        )
        .with_aliases(["magika-mime-detector", "MagikaMimeDetector"])
        .expect("Magika provider aliases should be valid")
        .with_priority(20)
    }
}

/// Converts a detector initialization error while preserving its source.
///
/// # Parameters
///
/// * `error` - Magika detector initialization error to classify.
///
/// # Returns
///
/// A direct provider creation error classified as initialization failure.
fn map_provider_create_error(error: MimeError) -> ProviderError {
    let reason =
        format!("failed to initialize the Magika MIME detector: {error}");
    ProviderError::initialization_failed_with_source(reason, error)
}

/// Exercises source-preserving provider error conversion in coverage builds.
///
/// # Returns
///
/// A deterministic initialization failure retaining a detector error source.
#[cfg(coverage)]
pub fn coverage_map_provider_create_error() -> ProviderError {
    map_provider_create_error(MimeError::detector_backend(
        "magika",
        "coverage provider failure",
    ))
}
