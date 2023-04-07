pub fn bytes_to_4xu64(bytes: &[u8]) -> (u64, u64, u64, u64) {
    let lo_lo = u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    let lo_hi = u64::from_be_bytes([
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ]);
    let hi_lo = u64::from_be_bytes([
        bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
    ]);
    let hi_hi = u64::from_be_bytes([
        bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30], bytes[31],
    ]);

    (lo_lo, lo_hi, hi_lo, hi_hi)
}

pub fn bytes_to_2xu64_1xu32(bytes: &[u8]) -> (u64, u64, u32) {
    let lo_lo = u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]);
    let lo_hi = u64::from_be_bytes([
        bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
    ]);
    let hi = u32::from_be_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);

    (lo_lo, lo_hi, hi)
}
