use std::io::{ErrorKind, Read};
use std::path::Path;

use crc32fast::Hasher;

use crate::error::{Corruption, Result, WalError};
use crate::record::{Lsn, Record, RecordKind};

pub(crate) const MAGIC: u32 = 0x4339_574c; // C9WL
pub(crate) const VERSION: u16 = 1;
pub(crate) const HEADER_LEN: usize = 32;
pub(crate) const HEADER_LEN_U64: u64 = 32;
const HEADER_CRC_END: usize = 28;

pub(crate) enum ReadOne {
    Record { record: Record, next_offset: u64 },
    Eof,
    Incomplete,
}

pub(crate) fn encode_record(kind: RecordKind, payload: &[u8]) -> Result<Vec<u8>> {
    let encoded_len = encoded_len(payload.len())?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| WalError::PayloadTooLarge { len: payload.len() })?;
    let mut header = [0_u8; HEADER_LEN];
    header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&kind.get().to_le_bytes());
    header[8..12].copy_from_slice(&payload_len.to_le_bytes());
    header[12..16].copy_from_slice(&crc32(payload).to_le_bytes());
    let header_crc = crc32(&header[..HEADER_CRC_END]);
    header[28..32].copy_from_slice(&header_crc.to_le_bytes());

    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.extend_from_slice(&header);
    encoded.extend_from_slice(payload);
    Ok(encoded)
}

pub(crate) fn read_one(
    path: &Path,
    reader: &mut impl Read,
    lsn: Lsn,
    segment_size: u64,
    physical_len: u64,
) -> Result<ReadOne> {
    let mut header = [0_u8; HEADER_LEN];
    match read_exact_or_tail(path, reader, &mut header)? {
        ReadExact::Eof => return Ok(ReadOne::Eof),
        ReadExact::Incomplete => return Ok(ReadOne::Incomplete),
        ReadExact::Complete => {}
    }

    let kind = decode_header(&header, lsn)?;
    let payload_len = u64::from(read_u32(&header, 8));
    let record_len = HEADER_LEN_U64.checked_add(payload_len).ok_or(WalError::CorruptRecord {
        lsn,
        reason: Corruption::RecordTooLarge { len: u64::MAX, segment_size },
    })?;
    if record_len > segment_size {
        return Err(WalError::CorruptRecord {
            lsn,
            reason: Corruption::RecordTooLarge { len: record_len, segment_size },
        });
    }
    let next_offset = lsn.offset.checked_add(record_len).ok_or(WalError::CorruptRecord {
        lsn,
        reason: Corruption::SegmentTooLarge { len: u64::MAX, segment_size },
    })?;
    if next_offset > segment_size {
        return Err(WalError::CorruptRecord {
            lsn,
            reason: Corruption::SegmentTooLarge { len: next_offset, segment_size },
        });
    }
    if next_offset > physical_len {
        return Ok(ReadOne::Incomplete);
    }
    let payload_len = usize::try_from(payload_len).map_err(|_| WalError::CorruptRecord {
        lsn,
        reason: Corruption::RecordTooLarge { len: record_len, segment_size },
    })?;

    let mut payload = vec![0; payload_len];
    if matches!(
        read_exact_or_tail(path, reader, &mut payload)?,
        ReadExact::Incomplete | ReadExact::Eof
    ) {
        return Ok(ReadOne::Incomplete);
    }
    if crc32(&payload) != read_u32(&header, 12) {
        return Err(WalError::CorruptRecord { lsn, reason: Corruption::PayloadChecksum });
    }
    Ok(ReadOne::Record { record: Record { kind, payload }, next_offset })
}

pub(crate) fn encoded_len(payload_len: usize) -> Result<usize> {
    u32::try_from(payload_len).map_err(|_| WalError::PayloadTooLarge { len: payload_len })?;
    HEADER_LEN.checked_add(payload_len).ok_or(WalError::PayloadTooLarge { len: payload_len })
}

enum ReadExact {
    Complete,
    Eof,
    Incomplete,
}

fn read_exact_or_tail(path: &Path, reader: &mut impl Read, buf: &mut [u8]) -> Result<ReadExact> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) if filled == 0 => return Ok(ReadExact::Eof),
            Ok(0) => return Ok(ReadExact::Incomplete),
            Ok(n) => filled += n,
            Err(source) if source.kind() == ErrorKind::Interrupted => {}
            Err(source) => return Err(WalError::io(path.to_path_buf(), source)),
        }
    }
    Ok(ReadExact::Complete)
}

fn decode_header(header: &[u8; HEADER_LEN], lsn: Lsn) -> Result<RecordKind> {
    let magic = read_u32(header, 0);
    if magic != MAGIC {
        return Err(WalError::CorruptRecord { lsn, reason: Corruption::BadMagic { found: magic } });
    }
    let version = read_u16(header, 4);
    if version != VERSION {
        return Err(WalError::CorruptRecord {
            lsn,
            reason: Corruption::UnsupportedVersion { found: version },
        });
    }
    if crc32(&header[..HEADER_CRC_END]) != read_u32(header, 28) {
        return Err(WalError::CorruptRecord { lsn, reason: Corruption::HeaderChecksum });
    }
    RecordKind::new(read_u16(header, 6))
        .map_err(|_| WalError::CorruptRecord { lsn, reason: Corruption::ReservedRecordKind })
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn read_u16(bytes: &[u8], start: usize) -> u16 {
    let mut out = [0; 2];
    out.copy_from_slice(&bytes[start..start + 2]);
    u16::from_le_bytes(out)
}

fn read_u32(bytes: &[u8], start: usize) -> u32 {
    let mut out = [0; 4];
    out.copy_from_slice(&bytes[start..start + 4]);
    u32::from_le_bytes(out)
}
