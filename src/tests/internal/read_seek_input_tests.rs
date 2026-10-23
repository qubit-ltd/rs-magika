// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::io::Cursor;

use magika::SyncInput;

use crate::internal::ReadSeekInput;

#[test]
fn test_bounded_input_reads_from_base_offset() {
    let mut reader = Cursor::new(b"prefix-script".to_vec());
    let mut input = ReadSeekInput::new(&mut reader, 7, 6);
    let mut buffer = [0_u8; 6];
    SyncInput::read_at(&mut input, &mut buffer, 0).expect("bounded read should succeed");
    assert_eq!(&buffer, b"script");
    assert_eq!(6, SyncInput::length(&input).expect("length should be available"));
}

#[test]
fn test_bounded_input_rejects_out_of_range_reads() {
    let mut reader = Cursor::new(b"script".to_vec());
    let mut input = ReadSeekInput::new(&mut reader, 0, 6);
    let mut buffer = [0_u8; 1];
    assert!(
        SyncInput::read_at(&mut input, &mut buffer, 6).is_err(),
        "out-of-range read should be rejected"
    );
}

#[test]
fn test_bounded_input_rejects_base_offset_overflow() {
    let mut reader = Cursor::new(vec![0_u8; 1]);
    let mut input = ReadSeekInput::new(&mut reader, u64::MAX, 1);
    let mut buffer = [0_u8; 1];
    assert!(
        SyncInput::read_at(&mut input, &mut buffer, 0).is_err(),
        "overflowing base offset should be rejected"
    );
}

#[test]
fn test_bounded_input_rejects_relative_offset_overflow() {
    let mut reader = Cursor::new(vec![0_u8; 1]);
    let mut input = ReadSeekInput::new(&mut reader, 0, u64::MAX);
    let mut buffer = [0_u8; 1];
    assert!(
        SyncInput::read_at(&mut input, &mut buffer, u64::MAX).is_err(),
        "overflowing relative offset should be rejected"
    );
}
