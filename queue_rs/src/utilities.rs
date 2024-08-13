use crate::{MAX_ALLOWED_INDEX, U64_BYTE_LEN};
use chrono::Utc;

pub fn u64_from_byte_vec(v: &[u8]) -> u64 {
    let mut buf = [0u8; U64_BYTE_LEN];
    buf.copy_from_slice(v);
    u64::from_le_bytes(buf)
}

pub fn index_to_key(index: u64) -> [u8; U64_BYTE_LEN] {
    index.to_le_bytes()
}

pub fn key_to_index(v: Box<[u8]>) -> u64 {
    let mut buf = [0u8; U64_BYTE_LEN];
    buf.copy_from_slice(&v);
    u64::from_le_bytes(buf)
}

pub fn next_index(index: u64) -> u64 {
    let mut next = index + 1;
    if next == MAX_ALLOWED_INDEX {
        next = 0;
    }
    next
}

pub fn previous_index(index: u64) -> u64 {
    if index == 0 {
        MAX_ALLOWED_INDEX - 1
    } else {
        index - 1
    }
}

pub fn current_timestamp() -> u64 {
    Utc::now().timestamp_nanos_opt().unwrap() as u64
}
