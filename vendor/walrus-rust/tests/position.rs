mod common;

use common::TestEnv;
use walrus_rust::{WalPosition, Walrus};

fn setup() -> TestEnv {
    TestEnv::new()
}

#[test]
fn current_position_origin_for_unwritten_topic() {
    let _env = setup();
    let wal = Walrus::new().unwrap();
    let pos = wal.current_position("never-written").unwrap();
    assert_eq!(pos, WalPosition::ORIGIN);
    assert!(pos.is_origin());
}

#[test]
fn current_position_advances_with_writes() {
    let _env = setup();
    let wal = Walrus::new().unwrap();
    let topic = "advances";

    let p0 = wal.current_position(topic).unwrap();

    wal.append_for_topic(topic, b"first").unwrap();
    let p1 = wal.current_position(topic).unwrap();
    assert!(p1.block_id > 0, "block_id should be assigned after first append");
    assert!(p1.offset > p0.offset || p1.block_id != p0.block_id);

    wal.append_for_topic(topic, b"second").unwrap();
    let p2 = wal.current_position(topic).unwrap();
    assert!(
        (p2.block_id, p2.offset) > (p1.block_id, p1.offset),
        "position should be monotonically increasing across appends; p1={:?} p2={:?}",
        p1,
        p2
    );
}

#[test]
fn set_position_to_current_skips_all_entries() {
    let _env = setup();
    let wal = Walrus::new().unwrap();
    let topic = "skip-all";

    wal.append_for_topic(topic, b"a").unwrap();
    wal.append_for_topic(topic, b"b").unwrap();
    wal.append_for_topic(topic, b"c").unwrap();

    let tail = wal.current_position(topic).unwrap();
    wal.set_persisted_read_position(topic, tail).unwrap();

    // Cursor is now at tail — read_next should return None until new appends arrive.
    assert!(wal.read_next(topic, true).unwrap().is_none(), "no entries should be readable past tail");

    // New append shows up.
    wal.append_for_topic(topic, b"d").unwrap();
    let entry = wal.read_next(topic, true).unwrap().expect("new append must be visible");
    assert_eq!(entry.data, b"d");
}

#[test]
fn set_position_to_origin_replays_from_start() {
    let _env = setup();
    let wal = Walrus::new().unwrap();
    let topic = "replay-from-origin";

    wal.append_for_topic(topic, b"x").unwrap();
    wal.append_for_topic(topic, b"y").unwrap();

    // Consume both so cursor advances.
    assert!(wal.read_next(topic, true).unwrap().is_some());
    assert!(wal.read_next(topic, true).unwrap().is_some());
    assert!(wal.read_next(topic, true).unwrap().is_none());

    // Reset cursor to origin → entries should be readable again.
    wal.set_persisted_read_position(topic, WalPosition::ORIGIN).unwrap();
    let first = wal.read_next(topic, true).unwrap().expect("first entry must replay");
    assert_eq!(first.data, b"x");
    let second = wal.read_next(topic, true).unwrap().expect("second entry must replay");
    assert_eq!(second.data, b"y");
}

#[test]
fn position_snapshot_then_set_recreates_cursor() {
    let _env = setup();
    let wal = Walrus::new().unwrap();
    let topic = "snapshot-then-set";

    // Append three entries.
    wal.append_for_topic(topic, b"1").unwrap();
    wal.append_for_topic(topic, b"2").unwrap();

    // Snapshot position after 2 entries — this is where we want to "checkpoint".
    let watermark = wal.current_position(topic).unwrap();

    // More entries arrive past the watermark.
    wal.append_for_topic(topic, b"3").unwrap();
    wal.append_for_topic(topic, b"4").unwrap();

    // Forcibly set cursor to the watermark — should skip "1" and "2", return "3" then "4".
    wal.set_persisted_read_position(topic, watermark).unwrap();
    let a = wal.read_next(topic, true).unwrap().expect("entry 3 expected");
    assert_eq!(a.data, b"3");
    let b = wal.read_next(topic, true).unwrap().expect("entry 4 expected");
    assert_eq!(b.data, b"4");
    assert!(wal.read_next(topic, true).unwrap().is_none());
}
