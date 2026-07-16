use std::io::{Seek, SeekFrom, Write};

use fs_err::{self as fs, File, OpenOptions};

use crate::segment::segment_path;
use crate::*;

fn kind(value: u16) -> RecordKind {
    RecordKind::new(value).unwrap()
}

fn payload(value: u8) -> Vec<u8> {
    vec![value; 8]
}

#[test]
fn append_and_recover_records() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();

    let first = wal.append(kind(1), payload(1)).unwrap();
    let second = wal.append(kind(2), payload(2)).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
    let records = wal.records().unwrap().collect::<Result<Vec<_>>>().unwrap();

    assert_eq!(records.len(), 2);
    assert_eq!(records[0].lsn, first);
    assert_eq!(records[0].record.kind, kind(1));
    assert_eq!(records[1].lsn, second);
    assert_eq!(records[1].record.payload, payload(2));
}

#[test]
fn rotates_segments() {
    let dir = tempfile::tempdir().unwrap();
    let options = WalOptions { segment_size: 96, ..WalOptions::default() };
    let mut wal = Wal::open(dir.path(), options.clone()).unwrap();

    let first = wal.append(kind(1), payload(1)).unwrap();
    let second = wal.append(kind(1), payload(2)).unwrap();
    let third = wal.append(kind(1), payload(3)).unwrap();
    wal.sync().unwrap();
    drop(wal);

    assert_eq!(first.segment_id, 0);
    assert_eq!(second.segment_id, 0);
    assert_eq!(third.segment_id, 1);

    let wal = Wal::open(dir.path(), options).unwrap();
    let records = wal.records().unwrap().collect::<Result<Vec<_>>>().unwrap();
    assert_eq!(records.iter().map(|r| r.record.payload[0]).collect::<Vec<_>>(), [1, 2, 3]);
}

#[test]
fn recovery_truncates_incomplete_tail() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
    wal.append(kind(1), payload(1)).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let segment = segment_path(dir.path(), 0);
    let mut file = OpenOptions::new().append(true).open(&segment).unwrap();
    file.write_all(&[1, 2, 3]).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
    let records = wal.records().unwrap().collect::<Result<Vec<_>>>().unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record.payload, payload(1));
    assert_eq!(fs::metadata(segment).unwrap().len(), crate::format::HEADER_LEN_U64 + 8);
}

#[test]
fn invariant_recovery_never_discards_later_segments() {
    let dir = tempfile::tempdir().unwrap();
    let options = WalOptions { segment_size: 40, ..WalOptions::default() };
    let mut wal = Wal::open(dir.path(), options.clone()).unwrap();
    wal.append(kind(1), payload(1)).unwrap();
    wal.append(kind(1), payload(2)).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let first = segment_path(dir.path(), 0);
    OpenOptions::new().write(true).open(first).unwrap().set_len(39).unwrap();

    let err = Wal::open(dir.path(), options).unwrap_err();
    assert!(matches!(
        err,
        WalError::CorruptRecord {
            lsn: Lsn { segment_id: 0, offset: 0 },
            reason: Corruption::IncompleteRecord,
        }
    ));
    assert_eq!(fs::metadata(segment_path(dir.path(), 1)).unwrap().len(), 40);
}

#[test]
fn recovery_rejects_checksum_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal = Wal::open(dir.path(), WalOptions::default()).unwrap();
    wal.append(kind(1), payload(1)).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let segment = segment_path(dir.path(), 0);
    let mut file = OpenOptions::new().read(true).write(true).open(segment).unwrap();
    file.seek(SeekFrom::Start(crate::format::HEADER_LEN_U64)).unwrap();
    file.write_all(&[9]).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let err = Wal::open(dir.path(), WalOptions::default()).unwrap_err();
    assert!(matches!(err, WalError::CorruptRecord { reason: Corruption::PayloadChecksum, .. }));
}

#[test]
fn recovery_rejects_missing_segment() {
    let dir = tempfile::tempdir().unwrap();
    File::create(segment_path(dir.path(), 0)).unwrap();
    File::create(segment_path(dir.path(), 2)).unwrap();

    let err = Wal::open(dir.path(), WalOptions::default()).unwrap_err();
    assert!(matches!(err, WalError::MissingSegment { expected: 1, found: 2 }));
}

#[test]
fn recovery_rejects_record_larger_than_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal =
        Wal::open(dir.path(), WalOptions { segment_size: 40, ..WalOptions::default() }).unwrap();
    wal.append(kind(1), payload(1)).unwrap();
    wal.sync().unwrap();
    drop(wal);

    let err = Wal::open(dir.path(), WalOptions { segment_size: 39, ..WalOptions::default() })
        .unwrap_err();
    assert!(matches!(
        err,
        WalError::CorruptRecord {
            reason: Corruption::RecordTooLarge { len: 40, segment_size: 39 },
            ..
        }
    ));
}

#[test]
fn rejects_reserved_kind() {
    let err = RecordKind::new(0).unwrap_err();
    assert!(matches!(err, WalError::ReservedRecordKind));
}

#[test]
fn rejects_records_larger_than_segment() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal =
        Wal::open(dir.path(), WalOptions { segment_size: 39, ..WalOptions::default() }).unwrap();

    let err = wal.append(kind(1), payload(1)).unwrap_err();
    assert!(matches!(err, WalError::RecordTooLarge { .. }));
}

#[test]
fn invariant_failed_rotation_preserves_active_segment() {
    let dir = tempfile::tempdir().unwrap();
    let options = WalOptions { segment_size: 40, ..WalOptions::default() };
    let mut wal = Wal::open(dir.path(), options).unwrap();
    wal.append(kind(1), payload(1)).unwrap();

    let next = segment_path(dir.path(), 1);
    fs::create_dir(&next).unwrap();
    assert!(wal.append(kind(1), payload(2)).is_err());
    fs::remove_dir(next).unwrap();

    let lsn = wal.append(kind(1), payload(2)).unwrap();
    assert_eq!(lsn.segment_id, 1);
}

#[test]
fn invariant_wal_has_single_writer() {
    let dir = tempfile::tempdir().unwrap();
    let wal = Wal::open(dir.path(), WalOptions::default()).unwrap();

    assert!(Wal::open(dir.path(), WalOptions::default()).is_err());
    drop(wal);
    assert!(Wal::open(dir.path(), WalOptions::default()).is_ok());
}

#[test]
fn invariant_recovery_enforces_segment_size() {
    let dir = tempfile::tempdir().unwrap();
    let mut wal =
        Wal::open(dir.path(), WalOptions { segment_size: 120, ..WalOptions::default() }).unwrap();
    for value in 0..3 {
        wal.append(kind(1), payload(value)).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    let error = Wal::open(dir.path(), WalOptions { segment_size: 80, ..WalOptions::default() })
        .unwrap_err();
    assert!(matches!(
        error,
        WalError::CorruptRecord {
            reason: Corruption::SegmentTooLarge { len: 120, segment_size: 80 },
            ..
        }
    ));
}

#[test]
fn invariant_wal_capacity_is_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let options = WalOptions { segment_size: 80, max_size: 80, sync_on_append: false };
    let mut wal = Wal::open(dir.path(), options).unwrap();
    wal.append(kind(1), payload(1)).unwrap();
    wal.append(kind(1), payload(2)).unwrap();

    assert!(matches!(wal.append(kind(1), payload(3)), Err(WalError::WalFull { .. })));
}
