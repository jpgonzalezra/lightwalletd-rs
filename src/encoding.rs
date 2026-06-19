//! Conversions between display order (big-endian hex, as zebrad reports hashes and txids) and
//! protocol wire order (little-endian bytes, as used on the gRPC wire and in protobuf).

/// Decode a display-order (big-endian) hex hash/txid into protocol (little-endian) wire bytes.
pub fn display_hex_to_wire(hex: &str) -> Result<Vec<u8>, hex::FromHexError> {
    let mut bytes = hex::decode(hex)?;
    bytes.reverse();
    Ok(bytes)
}

/// Reverse protocol (little-endian) wire bytes into display-order (big-endian) bytes.
pub fn wire_to_display_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut display = bytes.to_vec();
    display.reverse();
    display
}

/// Encode protocol (little-endian) wire bytes into display-order (big-endian) hex.
pub fn wire_to_display_hex(bytes: &[u8]) -> String {
    hex::encode(wire_to_display_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_hex_to_wire_reverses_byte_order() {
        assert_eq!(
            display_hex_to_wire("00112233").unwrap(),
            vec![0x33, 0x22, 0x11, 0x00]
        );
    }

    #[test]
    fn wire_to_display_bytes_reverses_byte_order() {
        assert_eq!(
            wire_to_display_bytes(&[0x33, 0x22, 0x11, 0x00]),
            vec![0x00, 0x11, 0x22, 0x33]
        );
    }

    #[test]
    fn wire_to_display_hex_reverses_byte_order() {
        assert_eq!(wire_to_display_hex(&[0x33, 0x22, 0x11, 0x00]), "00112233");
    }

    #[test]
    fn display_and_wire_round_trip() {
        let display = "00000000005a1db0281385a6eeb05d7beff2a42f17cedc94280215f087b5e07d";
        assert_eq!(
            wire_to_display_hex(&display_hex_to_wire(display).unwrap()),
            display
        );
    }

    #[test]
    fn display_hex_to_wire_rejects_invalid_hex() {
        assert!(display_hex_to_wire("zz").is_err());
    }
}
