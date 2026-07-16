//! Authentication for the Raft peer transport.

use std::fmt;

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use thiserror::Error;

const KEY_BYTES: usize = 32;
const SIGNATURE_BYTES: usize = 32;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct RaftKey([u8; KEY_BYTES]);

#[derive(Debug, Error)]
#[error("Raft key must be exactly 64 hexadecimal characters")]
pub struct InvalidRaftKey;

impl RaftKey {
    pub fn from_hex(value: &str) -> Result<Self, InvalidRaftKey> {
        decode_hex(value).map(Self).ok_or(InvalidRaftKey)
    }

    pub(crate) fn signature(&self, body: &[u8]) -> Option<String> {
        let mut mac = <HmacSha256 as KeyInit>::new_from_slice(&self.0).ok()?;
        mac.update(body);
        Some(encode_hex(&mac.finalize().into_bytes()))
    }

    pub(crate) fn verify(&self, body: &[u8], signature: &str) -> bool {
        let Some(signature) = decode_hex::<SIGNATURE_BYTES>(signature) else {
            return false;
        };
        let Ok(mut mac) = <HmacSha256 as KeyInit>::new_from_slice(&self.0) else {
            return false;
        };
        mac.update(body);
        mac.verify_slice(&signature).is_ok()
    }
}

impl fmt::Debug for RaftKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RaftKey([REDACTED])")
    }
}

fn decode_hex<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 {
        return None;
    }
    let mut decoded = [0; N];
    for (output, pair) in decoded.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *output = nibble(pair[0])?.checked_mul(16)?.checked_add(nibble(pair[1])?)?;
    }
    Some(decoded)
}

fn nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invariant_signature_binds_key_and_body() {
        let key = RaftKey::from_hex(&"01".repeat(KEY_BYTES)).unwrap();
        let other_key = RaftKey::from_hex(&"02".repeat(KEY_BYTES)).unwrap();
        let signature = key.signature(b"message").unwrap();

        assert!(key.verify(b"message", &signature));
        assert!(!key.verify(b"tampered", &signature));
        assert!(!other_key.verify(b"message", &signature));
    }

    #[test]
    fn malformed_keys_and_signatures_are_rejected() {
        assert!(RaftKey::from_hex("short").is_err());
        assert!(RaftKey::from_hex(&"zz".repeat(KEY_BYTES)).is_err());
        let key = RaftKey::from_hex(&"01".repeat(KEY_BYTES)).unwrap();
        assert!(!key.verify(b"message", "invalid"));
    }
}
