// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider for registering the Magika MIME detector with `qubit-mime`.

use std::sync::Arc;

use qubit_mime::{
    MimeConfig,
    MimeDetector,
    MimeDetectorSpec,
    MimeError,
};
use qubit_spi::error::ProviderFailure;
use qubit_spi::{
    ProviderDescriptor,
    ProviderMetadata,
    ServiceProvider,
    provider_descriptor,
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
    /// Returns [`ProviderFailure`] when Magika or ONNX Runtime cannot
    /// initialize. The error preserves the underlying [`MimeError`] source and
    /// is classified as initialization failure because Magika's upstream error
    /// does not reliably distinguish a missing runtime from an invalid model or
    /// configuration. `OnAbsence` fallback therefore does not suppress it.
    #[inline(always)]
    fn create_configured(
        &self,
        config: &MimeConfig,
    ) -> Result<Arc<dyn MimeDetector>, ProviderFailure<MimeError>> {
        MagikaMimeDetector::from_mime_config(config.clone())
            .map(|detector| Arc::new(detector) as Arc<dyn MimeDetector>)
            .map_err(ProviderFailure::initialization_failed)
    }
}

impl ProviderMetadata for MagikaMimeDetectorProvider {
    /// Returns the stable Magika provider identity and selection metadata.
    ///
    /// # Returns
    ///
    /// The `magika` descriptor, accepted aliases, and automatic-selection
    /// priority.
    #[inline]
    fn descriptor(&self) -> ProviderDescriptor {
        provider_descriptor!(
            "magika",
            aliases: ["magika-mime-detector", "magikamimedetector"],
            priority: 20,
        )
    }
}
