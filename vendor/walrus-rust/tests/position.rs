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
    assert!((p2.block_id, p2.offset) > (p1.block_id, p1.offset), "position should be monotonically increasing across appends; p1={:?} p2={:?}", p1, p2);
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

/// Regression: a Delta-derived cursor fast-forward (TimeFusion's
/// `derive_wal_cursor_for_table`) used to bypass the normal block-checkpoint
/// accounting that `read_next` performs lazily. With the bypass, blocks the
/// cursor skipped stayed `is_checkpointed=false` forever, so their
/// containing files never satisfied `checkpointed >= total` and were never
/// reclaimed. This produced the prod symptom of ever-growing WAL files
/// (346 GB stuck at 347 files on the timefusion node).
/// Fill > 1 default block (10 MiB) so the first block gets sealed and
/// pushed into the reader chain — the precondition for both regression
/// tests below.
fn fill_two_blocks(wal: &Walrus, topic: &str) -> WalPosition {
    // Slightly larger than DEFAULT_BLOCK_SIZE so the second append spills
    // into a new block, sealing the first one.
    let big = vec![0xAB; 11 * 1024 * 1024];
    wal.append_for_topic(topic, &big).unwrap();
    wal.append_for_topic(topic, b"tail-marker").unwrap();
    wal.current_position(topic).unwrap()
}

/// Regression: a Delta-derived cursor fast-forward (TimeFusion's
/// `derive_wal_cursor_for_table`) used to bypass the normal block-checkpoint
/// accounting that `read_next` performs lazily. With the bypass, sealed
/// blocks the cursor skipped stayed `is_checkpointed=false` forever, so
/// their containing files never satisfied `checkpointed >= total` and
/// were never reclaimed. This produced the prod symptom of ever-growing
/// WAL files (346 GB stuck at 347 files on the timefusion node).
#[test]
fn set_persisted_read_position_checkpoints_skipped_blocks() {
    let _env = setup();
    let wal = Walrus::new().unwrap();
    let topic = "fastforward-checkpoints";

    // Force at least one block to seal so we have a chain entry the fix
    // can actually checkpoint.
    let watermark = fill_two_blocks(&wal, topic);
    assert!(!watermark.is_origin(), "watermark must point past origin");

    // The sealed block has id < watermark.block_id (the watermark is in
    // the *active* block). Look up its file state before the fast-forward:
    // checkpointed must be 0 because we haven't read anything yet.
    let sealed_block_id = watermark.block_id - 1;
    let before = Walrus::block_file_checkpoint_state(sealed_block_id).expect("sealed block must have a file state entry");
    assert_eq!(before.0, 0, "sealed block must start un-checkpointed (before={:?})", before);

    // Fast-forward the cursor directly to the watermark — the path
    // TimeFusion uses when Delta is ahead of walrus's locally-fsynced
    // cursor.
    wal.set_persisted_read_position(topic, watermark).unwrap();

    // The sealed block MUST be marked checkpointed now so its file
    // becomes reclaimable. Prior to the fix this stayed 0.
    let after = Walrus::block_file_checkpoint_state(sealed_block_id).expect("sealed block must still resolve");
    assert!(
        after.0 >= 1,
        "fast-forward must checkpoint sealed blocks behind the cursor — \
         before=(checkpointed={}, total={}), after=(checkpointed={}, total={})",
        before.0,
        before.1,
        after.0,
        after.1
    );

    // And the cursor itself is past tail — confirms behavior wasn't
    // regressed.
    assert!(wal.read_next(topic, true).unwrap().is_none(), "cursor must be at tail");
}

#[test]
fn set_checkpointed_true_is_idempotent_per_block() {
    let _env = setup();
    let wal = Walrus::new().unwrap();
    let topic = "checkpoint-idempotent";

    let watermark = fill_two_blocks(&wal, topic);
    let sealed_block_id = watermark.block_id - 1;

    // Fast-forward twice. Without the idempotence fix the second call
    // would double-increment `checkpoint_block_ctr` past `total`, masking
    // latent accounting drift; with the fix the counter is stable.
    wal.set_persisted_read_position(topic, watermark).unwrap();
    let after_first = Walrus::block_file_checkpoint_state(sealed_block_id).unwrap().0;
    wal.set_persisted_read_position(topic, watermark).unwrap();
    let after_second = Walrus::block_file_checkpoint_state(sealed_block_id).unwrap().0;

    assert!(after_first >= 1, "first fast-forward must checkpoint the sealed block");
    assert_eq!(after_first, after_second, "second fast-forward must not double-increment (after_first={}, after_second={})", after_first, after_second);
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

/// `request_reclaim_sweep` must get a fully-consumed, fully-allocated file
/// physically deleted within a background tick or two — TimeFusion calls it
/// after boot recovery parks the cursors, so a consumed replay backlog frees
/// its disk immediately instead of waiting the periodic 1000-tick (~200s)
/// sweep cadence. Differential: without the forced sweep this test times out
/// (the periodic sweep is minutes away).
///
/// IGNORED by default: `BlockStateTracker`/`FileStateTracker` are
/// process-global keyed by per-INSTANCE block ids, so parallel tests'
/// Walrus instances collide (all allocate ids 1,2,3…) and corrupt each
/// other's checkpoint counters — this test's 100 concurrent allocations
/// make that latent flake near-certain. Prod runs a single Walrus, where
/// the accounting is sound. Run standalone:
///   cargo test --test position -- --ignored request_reclaim_sweep
#[test]
#[ignore = "global block-id trackers collide across parallel Walrus instances; run standalone with --ignored"]
fn request_reclaim_sweep_deletes_consumed_files_promptly() {
    let _env = setup();
    let wal = Walrus::new().unwrap();
    let topic = "sweep-reclaim";
    let dir = common::current_wal_dir();
    // Data files are millis-named (all digits); everything else (index db,
    // meta) is excluded.
    let data_files =
        || std::fs::read_dir(&dir).map(|rd| rd.flatten().filter(|e| e.file_name().to_string_lossy().chars().all(|c| c.is_ascii_digit())).count()).unwrap_or(0);

    // Fill file 1 completely (100 x 10MiB blocks) and roll into file 2 so
    // file 1 becomes fully_allocated: ~9MiB entries take one block each,
    // the 101st allocation rolls the file.
    let entry = vec![0xCD; 9 * 1024 * 1024];
    for _ in 0..101 {
        wal.append_for_topic(topic, &entry).unwrap();
    }
    assert!(data_files() >= 2, "the 101st block must have rolled into a second data file");

    // Consume everything: fast-forward checkpoints all sealed blocks, which
    // enqueues file 1 as deletable (flush_check).
    let tail = wal.current_position(topic).unwrap();
    wal.set_persisted_read_position(topic, tail).unwrap();

    wal.request_reclaim_sweep();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let n = data_files();
        if n <= 1 {
            break; // fully-consumed file reclaimed; only the active file remains
        }
        if std::time::Instant::now() >= deadline {
            let states: Vec<_> = Walrus::file_reclaim_states().into_iter().filter(|(p, ..)| p.starts_with(dir.to_string_lossy().as_ref())).collect();
            panic!("consumed file not reclaimed within 15s of request_reclaim_sweep (data files={n}); states={states:?}");
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}
