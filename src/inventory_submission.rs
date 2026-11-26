// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Link-time submission of the default Magika detector provider.

use qubit_mime::MimeDetectorSpec;
use qubit_mime::detector::mime_detector_inventory::Entry;
use qubit_spi::submit_sync_provider;

submit_sync_provider! {
    inventory_entry = Entry;
    spec = MimeDetectorSpec;
    provider = crate::MagikaMimeDetectorProvider::new();
}
