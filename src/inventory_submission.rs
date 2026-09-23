// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Link-time submission of the default Magika detector provider.

qubit_spi::submit_sync_provider! {
    inventory_entry = qubit_mime::detector::mime_detector_inventory::Entry;
    spec = qubit_mime::MimeDetectorSpec;
    provider = crate::MagikaMimeDetectorProvider::new();
}
