use std::path::PathBuf;

use serial_test::serial;
use test_case::test_case;

use super::*;
use crate::{config::AppConfig, schema::get_default_schema, support::test_helpers::*};

#[test_case(1 => 1; "a one-lane pool cannot reserve a lane")]
#[test_case(2 => 1; "a two-lane pool splits hot and sealed")]
#[test_case(5 => 1; "production leaves four of five lanes available to sealed debt")]
#[test_case(6 => 2; "larger pools keep one third for the open day")]
fn sealed_catch_up_keeps_hot_packing_to_one_third(light_permits: usize) -> usize {
    hot_packing_permits(light_permits)
}

#[tokio::test]
async fn run_unit_preserves_unrelated_journal_tasks() -> Result<()> {
    use crate::maintenance_coordinator::{Operation, TaskKey, TaskState, TimeSlice};
    let db = Database::with_config(create_test_config("targeted-run-unit")).await?;
    db.cancel_maintenance();
    let day = (Utc::now() - chrono::Duration::days(3)).date_naive();
    let start = midnight_micros(day);
    let unrelated = TaskKey {
        physical_table: "otel_logs_and_spans".into(),
        source: "otel_logs_and_spans".into(),
        project_id: "unrelated-unit-project".into(),
        slice: TimeSlice::new(start, start + 3_600_000_000)?,
        operation: Operation::Dedup,
    };
    let before = {
        let mut journal = db.journal();
        journal.enqueue(unrelated.clone(), 0, 1024, 0);
        serde_json::to_value(journal.tasks().find(|task| task.key == unrelated).unwrap())?
    };
    db.run_unit_once("otel_logs_and_spans", "requested-unit-project", day, Operation::Dedup, 1, 0).await?;
    let journal = db.journal();
    let after = serde_json::to_value(journal.tasks().find(|task| task.key == unrelated).unwrap())?;
    assert_eq!(before, after, "running one unit must not rewrite another task");
    drop(journal);

    let report = db.run_unit_once("otel_logs_and_spans", "requested-unit-project", day, Operation::DerivedRollup, 1, 0).await?;
    assert_eq!(report.state, Some(TaskState::Pending), "missing base coverage must block a derived claim");
    assert!(!db.journal().tasks().any(|task| task.key.operation == Operation::BaseRollup), "a CLI run must not fabricate base completion");

    let running = TaskKey { project_id: "running-unit-project".into(), ..unrelated };
    let running_before = {
        let mut journal = db.journal();
        journal.enqueue(running.clone(), 0, 1024, 0);
        assert!(journal.claim_exact(&running, -1, false).is_none(), "manual claims respect deadlines");
        let task = journal.claim_exact(&running, 0, false).unwrap();
        serde_json::to_value(task)?
    };
    db.run_unit_once("otel_logs_and_spans", "running-unit-project", day, Operation::Dedup, 1, 0).await?;
    let journal = db.journal();
    assert_eq!(running_before, serde_json::to_value(journal.tasks().find(|task| task.key == running).unwrap())?, "a running task cannot be claimed twice");
    Ok(())
}

/// A rollup tier that predates a measure gains the column, keeps it after a
/// second pass, and makes the measure MATERIALIZABLE — which is what lets
/// `TAG_MEASURES` record it and the read gate serve it.
#[tokio::test]
async fn a_tier_that_predates_a_measure_is_widened_to_hold_it() {
    let source = get_schema("otel_logs_and_spans").expect("source schema");
    let spec = source.rollups.first().expect("declared rollup");
    const MEASURE: &str = "duration_digest";
    let declared = get_schema(&spec.table_name("otel_logs_and_spans")).expect("tier schema");
    let narrow: Vec<_> = declared.columns().expect("tier columns").into_iter().filter(|column| column.name() != MEASURE).collect();

    let dir = tempfile::tempdir().expect("tempdir");
    let table = deltalake::operations::create::CreateBuilder::new()
        .with_location(dir.path().to_string_lossy())
        .with_columns(narrow)
        .with_partition_columns(declared.partitions.clone())
        .await
        .expect("create the narrow tier");
    let table = Arc::new(RwLock::new(table));
    let fields = declared.schema_ref().fields().clone();

    // Every input column is present, so only the TARGET arm can refuse the
    // measure — which it does, on the table as it stands.
    let present: HashSet<String> = source.fields.iter().map(|field| field.name.clone()).collect();
    let stored = |table: &DeltaTable| table.snapshot().expect("snapshot").schema().fields().any(|field| field.name() == MEASURE);
    let narrow_arrow = arrow_schema::Schema::new(fields.iter().filter(|field| field.name() != MEASURE).cloned().collect::<Vec<_>>());
    assert!(!stored(&*table.read().await), "the tier must start without the measure");
    assert!(
        !crate::rollup::materialized_measures(spec, false, &present, &narrow_arrow, None).contains(&MEASURE.to_owned()),
        "a measure the physical tier lacks is not materializable"
    );

    assert_eq!(evolve_table_columns(&table, &fields).await.expect("widen"), vec![MEASURE.to_owned()]);
    assert!(stored(&*table.read().await), "the tier must gain the declared measure");
    assert!(
        crate::rollup::materialized_measures(spec, false, &present, declared.schema_ref().as_ref(), None).contains(&MEASURE.to_owned()),
        "once the column exists the measure is materializable, so TAG_MEASURES records it"
    );

    // Idempotent: every build calls this unconditionally.
    let version = { table.read().await.version() };
    assert!(evolve_table_columns(&table, &fields).await.expect("second widen").is_empty());
    assert_eq!(table.read().await.version(), version, "a no-op widening must not commit");
}

/// A pass deadline no test will reach, for the drain's bounding parameter.
/// `Instant` has no MAX, and adding `Duration::MAX` overflows.
fn far_future() -> std::time::Instant {
    std::time::Instant::now() + std::time::Duration::from_secs(86_400)
}

/// Midnight UTC of `date`, in micros.
fn midnight_micros(date: chrono::NaiveDate) -> i64 {
    date.and_hms_opt(0, 0, 0).expect("midnight").and_utc().timestamp_micros()
}

/// Insert one `otel_logs_and_spans` span at `ts` micros, op name "op".
async fn insert_a_span(db: &Database, project: &str, id: &str, ts: i64) -> Result<()> {
    db.insert_records_batch(project, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts(id, "op", project, ts)])?], true, None).await?;
    Ok(())
}

/// Register a BYO-bucket (custom-storage) tenant on local MinIO, same bucket, distinct prefix.
async fn register_custom_storage(db: &Database, project_id: &str, table_name: &str, s3_prefix: &str) {
    db.storage_configs.write().await.insert(
        (project_id.to_string(), table_name.to_string()),
        StorageConfig {
            project_id: project_id.to_string(),
            table_name: table_name.to_string(),
            s3_bucket: "timefusion-tests".to_string(),
            s3_prefix: s3_prefix.to_string(),
            s3_region: "us-east-1".to_string(),
            s3_access_key_id: "minioadmin".to_string(),
            s3_secret_access_key: "minioadmin".to_string(),
            s3_endpoint: Some("http://127.0.0.1:9000".to_string()),
        },
    );
}

/// A Database on a fresh test config with `edit` applied.
async fn db_where(name: &str, edit: impl FnOnce(&mut AppConfig)) -> Result<Database> {
    Database::with_config(test_config_with(name, edit)).await
}

/// The 35-day backfill horizon the planning tests share; the sealed days they
/// write sit outside the shipped default.
fn wide_backfill(cfg: &mut AppConfig) {
    cfg.maintenance.timefusion_rollup_backfill_days = 35;
}

/// Complete every queued task and hand back the keys, so a planner pass
/// re-derives a day instead of vetoing it as already-queued.
fn complete_all(journal: &mut crate::maintenance_coordinator::TaskJournal) -> Vec<crate::maintenance_coordinator::TaskKey> {
    let keys: Vec<_> = journal.tasks().map(|task| task.key.clone()).collect();
    for key in &keys {
        journal.complete(key);
    }
    keys
}

/// Stuff the journal past the backfill pending ceiling with debt unrelated to rollup.
fn stuff_past_the_ceiling(journal: &mut crate::maintenance_coordinator::TaskJournal) -> Result<()> {
    use crate::maintenance_coordinator::{Operation, TaskKey, TimeSlice};
    for i in 0..25_100i64 {
        journal.enqueue(
            TaskKey {
                physical_table: "debt".to_owned(),
                source: "debt".to_owned(),
                project_id: format!("dummy{i}"),
                slice: TimeSlice::new(i * 60_000_000, (i + 1) * 60_000_000)?,
                operation: Operation::Repair,
            },
            0,
            0,
            0,
        );
    }
    Ok(())
}

/// A db on a wide backfill horizon holding one span on a sealed day three days
/// back, with the journal stuffed past the pending ceiling. `complete_queued`
/// first retires the work the write path already queued for that day.
async fn db_past_the_backfill_ceiling(name: &str, complete_queued: bool) -> Result<Database> {
    let db = db_where(name, wide_backfill).await?;
    let project = format!("ceil_{}", uuid::Uuid::new_v4().simple());
    insert_a_span(&db, &project, "a", (Utc::now() - chrono::Duration::days(3)).timestamp_micros()).await?;
    let mut journal = db.maintenance_tasks.lock().unwrap();
    if complete_queued {
        complete_all(&mut journal);
    }
    stuff_past_the_ceiling(&mut journal)?;
    drop(journal);
    Ok(db)
}

/// One fresh tenant on a sealed day three days back: the parts of its
/// certification key, the instant itself, and the midnight / half-day marks
/// the dedup slice units are cut on.
struct CertDay {
    project: String,
    date: chrono::NaiveDate,
    /// Midnight UTC of the day, in micros.
    start: i64,
    /// Midday: the boundary every "two clean halves" test cuts on.
    half: i64,
    /// The exact instant three days back, inside the day.
    at: i64,
    key: (String, String, String),
}

fn cert_day() -> CertDay {
    use crate::maintenance_coordinator::DAY_MICROS;
    let project = format!("cert_{}", uuid::Uuid::new_v4().simple());
    let day = Utc::now() - chrono::Duration::days(3);
    let date = day.date_naive();
    let start = midnight_micros(date);
    CertDay {
        key: (project.clone(), "otel_logs_and_spans".to_owned(), date.to_string()),
        project,
        date,
        start,
        half: start + DAY_MICROS / 2,
        // Mid-FIRST-half, pinned. Using `day.timestamp_micros()` put the span at
        // whatever time of day the suite happened to run, which since #290 decides
        // the outcome: a write near this span either overlaps already-proved
        // coverage (first half, voided) or is span-disjoint from it (second half,
        // legitimately retained). CI passing at 10:03 UTC and the same commit
        // failing at 23:45 was that, not a flake.
        at: start + DAY_MICROS / 4,
    }
}

/// A unit whose slice has not finished yet must back off past the slice's end,
/// not spin — a flat retry keeps it permanently eligible and monopolises
/// `claim_next`.
#[test]
fn a_slice_that_has_not_finished_backs_off_past_its_own_end() {
    use crate::maintenance_coordinator::{FINALIZATION_DELAY_MICROS, TimeSlice};
    const HOUR: i64 = 3_600 * 1_000_000;
    let now = 20 * HOUR;
    let day = TimeSlice::new(0, 24 * HOUR).expect("day slice");

    // Today's day-wide unit cannot succeed before midnight plus finalization.
    let delay = super::buffered_source_retry_delay(day, now, 12);
    let expected = (24 * HOUR - now + FINALIZATION_DELAY_MICROS) as u64;
    assert_eq!(delay.as_micros() as u64, expected, "a unit must wait out the rest of its own slice, not 5 seconds");
    assert!(delay.as_secs() > 4 * 3_600, "the old 5s retry is what made this an unbounded spin");

    // A sealed slice merely waiting on a slow flush keeps the fast floor.
    let sealed = TimeSlice::new(0, HOUR).expect("sealed slice");
    assert_eq!(super::buffered_source_retry_delay(sealed, now, 0).as_secs(), 5, "a newly blocked sealed slice keeps the fast retry");
    assert_eq!(super::buffered_source_retry_delay(sealed, now, 12).as_secs(), 64, "a persistently blocked slice backs off instead of churning the journal");
}

/// A pending unit for one rollup tier must not veto planning another tier of
/// the same day: the already-queued veto is keyed per tier, not per day.
#[tokio::test]
async fn a_queued_unit_for_one_tier_does_not_veto_planning_another() -> Result<()> {
    use crate::maintenance_coordinator::{Operation, TaskKey, TimeSlice};
    let db = db_where("per-tier-veto", wide_backfill).await?;
    let project = format!("veto_{}", uuid::Uuid::new_v4().simple());
    let day = Utc::now() - chrono::Duration::days(3);
    insert_a_span(&db, &project, "a", day.timestamp_micros()).await?;

    // The day carries a pending frontier slice for ONE tier, nothing for the others.
    let blocking_table = {
        let mut journal = db.maintenance_tasks.lock().unwrap();
        let keys = complete_all(&mut journal);
        let blocking = keys.iter().find(|key| key.operation == Operation::BaseRollup).cloned().expect("the write path queues a base rollup for the day");
        let start = midnight_micros(day.date_naive());
        journal.enqueue(TaskKey { slice: TimeSlice::new(start, start + 600_000_000)?, ..blocking.clone() }, 0, 0, 0);
        blocking.physical_table
    };

    db.plan_rollup_backfill().await?;

    let (blocked, others) = {
        let journal = db.maintenance_tasks.lock().unwrap();
        let day_tasks: Vec<_> = journal
            .tasks()
            .filter(|task| task.key.project_id == project && task.state != crate::maintenance_coordinator::TaskState::Complete)
            .map(|task| task.key.physical_table.clone())
            .collect();
        (day_tasks.iter().filter(|t| **t == blocking_table).count(), day_tasks.iter().filter(|t| **t != blocking_table).count())
    };
    assert_eq!(blocked, 1, "the tier that already has work queued must not be enqueued twice");
    assert!(others > 0, "a sibling tier of the same day must still be planned, got {others}");
    Ok(())
}

/// A zero-row partition file must read as uncovered so the coordinator
/// re-queues it; unknown stats must read as COVERED, or every partition
/// written before stats existed is re-planned forever.
#[test_case(Some(0) => true ; "an explicit zero-row file proves nothing was built")]
#[test_case(Some(1) => false ; "a file with rows is coverage")]
#[test_case(None => false ; "missing stats are unknown, never empty")]
fn an_empty_partition_file_is_not_coverage(num_records: Option<i64>) -> bool {
    super::partition_file_is_empty(num_records)
}

/// While coverage is short, backfill outranks the pending ceiling — the live
/// frontier alone can hold the journal above it forever. Bounded both ways: a
/// pass admits at most `BACKFILL_PARTITIONS_PER_PASS`, and reaching
/// `COVERAGE_SHORT_DAYS` restores the ceiling.
#[tokio::test]
async fn coverage_short_backfill_outranks_the_pending_ceiling() -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    // The write path already queued this day; completing it makes the planner
    // re-derive it from the (missing) tier coverage.
    let db = db_past_the_backfill_ceiling("backfill-ceiling-bypass", true).await?;
    let gauge = &crate::observability::maintenance_stats().rollup_median_contiguous_days;
    let restore = gauge.load(Relaxed);
    gauge.store(30, Relaxed);
    let healthy = db.plan_rollup_backfill().await?;
    gauge.store(0, Relaxed);
    let short = db.plan_rollup_backfill().await?;
    gauge.store(restore, Relaxed);

    assert_eq!(healthy, 0, "at the ceiling with healthy coverage, the backfill still defers");
    assert!(short > 0, "coverage-short goal work outranks the ceiling, got {short}");
    Ok(())
}

/// The backfill ceiling must defer only the enqueue, never the whole pass —
/// the pass also recomputes the `rollup_median_contiguous_days` gauge that
/// `coverage_is_short` reads and the base-tier proof for queued derived units.
#[tokio::test]
async fn the_backfill_ceiling_defers_enqueueing_without_stopping_the_pass() -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let db = db_past_the_backfill_ceiling("backfill-ceiling-pass", false).await?;
    let gauge = &crate::observability::maintenance_stats().rollup_median_contiguous_days;
    let restore = gauge.load(Relaxed);
    // A value the pass cannot legitimately produce, so only a pass that RAN overwrites it.
    gauge.store(u64::MAX, Relaxed);
    let queued = db.plan_rollup_backfill().await?;
    let recomputed = gauge.load(Relaxed);
    gauge.store(restore, Relaxed);

    assert_eq!(queued, 0, "past the ceiling nothing new is enqueued");
    assert_ne!(recomputed, u64::MAX, "but the pass still ran and recomputed the goal gauge");
    Ok(())
}

/// The damage repair must be durable until CONSUMED, never one-shot: a pass
/// truncates to `BACKFILL_PARTITIONS_PER_PASS` newest-first, so forcing the
/// whole list into one pass drops its tail permanently.
#[test]
fn the_damage_repair_forces_its_whole_list_across_passes() {
    const CAP: usize = 3;
    let cells = damaged_like_cells(10);
    let mut cursor = 0usize;
    let mut reached_enqueue: Vec<super::maintain::BackfillCell> = Vec::new();
    let mut first_pass = None;
    for _ in 0..cells.len() {
        let offered = &cells[cursor..(cursor + CAP).min(cells.len())];
        let forced: std::collections::HashSet<_> = offered.iter().cloned().collect();
        // A natural frontier cell newer than every damaged one — the shape that
        // wins newest-first truncation forever.
        let mut want = vec![("frontier".to_owned(), chrono::NaiveDate::from_ymd_opt(2026, 8, 27).expect("date"))];
        want.extend(offered.iter().cloned());
        let (admitted, consumed) = super::maintain::admit_backfill_pass(want, offered, &forced, CAP);
        reached_enqueue.extend(admitted.into_iter().filter(|cell| forced.contains(cell)));
        cursor += consumed;
        first_pass.get_or_insert(cursor);
        if cursor == cells.len() {
            break;
        }
    }
    assert_eq!(first_pass, Some(CAP), "one pass consumes one pass's worth, and leaves the tail for the next");
    assert_eq!(cursor, cells.len(), "and across passes the whole list is consumed");
    assert_eq!(reached_enqueue, cells, "every listed cell must REACH the enqueue path, in list order");
}

/// The cursor advances by what survived truncation, never by what was merely offered.
/// A cell outside the source's horizon and one the already-queued veto ate are
/// RESOLVED, not truncated — holding the cursor on them stalls the tail forever.
#[test_case(0..3, 0..3, 1 => (damaged_like_cells(3)[..1].to_vec(), 1) ; "the per-pass bound admits exactly one, and the two it truncated are re-offered")]
#[test_case(2..3, 1..3, 8 => (damaged_like_cells(3)[2..].to_vec(), 3) ; "only the unvetoed forced cell is planned, but the whole offered prefix is consumed")]
fn the_repair_cursor_advances_only_past_cells_the_pass_resolved(
    want: std::ops::Range<usize>, forced: std::ops::Range<usize>, cap: usize,
) -> (Vec<super::maintain::BackfillCell>, usize) {
    let cells = damaged_like_cells(3);
    let forced: std::collections::HashSet<_> = cells[forced].iter().cloned().collect();
    super::maintain::admit_backfill_pass(cells[want].to_vec(), &cells, &forced, cap)
}

/// The whole wiring: a real pass reads the cursor, forces a prefix, and
/// persists how far it got — so a restart resumes instead of starting over.
#[tokio::test]
async fn a_backfill_pass_persists_how_far_the_damage_repair_reached() -> Result<()> {
    use crate::maintenance_coordinator::TaskJournal;
    let project = format!("dmg_{}", uuid::Uuid::new_v4().simple());
    // The list is a PARAMETER, not the shipped const (empty between repairs, so
    // the guard would pass vacuously), and must be longer than
    // `BACKFILL_PARTITIONS_PER_PASS` or no truncation happens at all.
    let listed: Vec<String> = (0..30).map(|i| format!("{project}:{}", (Utc::now() - chrono::Duration::days(3 + i)).format("%Y-%m-%d"))).collect();
    let db = db_where("damage-repair-cursor", |cfg| {
        wide_backfill(cfg);
        cfg.maintenance.timefusion_damage_repair_cells = listed.clone();
    })
    .await?;
    for i in 0..3 {
        insert_a_span(&db, &project, "a", (Utc::now() - chrono::Duration::days(3 + i)).timestamp_micros()).await?;
    }
    // A pass with nothing to enqueue advances nothing, so the day must be free of
    // queued rollup work before each pass or the veto empties `want`.
    let clear_queue = |db: &Database| {
        complete_all(&mut db.maintenance_tasks.lock().unwrap());
    };
    let cursor = |db: &Database| db.maintenance_tasks.lock().unwrap().repair_cursor(TaskJournal::DAMAGE_REPAIR_MIGRATION, "otel_logs_and_spans");
    assert_eq!(cursor(&db), 0, "a fresh journal has consumed nothing");
    clear_queue(&db);
    db.plan_rollup_backfill().await?;
    let after_one = cursor(&db);
    clear_queue(&db);
    db.plan_rollup_backfill().await?;

    assert!(after_one > 0, "a pass must record the prefix it resolved");
    assert!(after_one < super::maintain::damaged_cells_newest_first(&listed).len(), "and must not swallow the whole list in one pass — that is the v1 bug");
    assert!(cursor(&db) > after_one, "the next pass takes the next prefix, got {} then {}", after_one, cursor(&db));
    Ok(())
}

/// A malformed repair cell is DROPPED, not treated as a wildcard or a panic,
/// and its well-formed neighbours still come back newest-first.
#[test]
fn a_malformed_damage_repair_cell_is_dropped_and_the_rest_survive() {
    let listed: Vec<String> = ["p1:2026-08-20", "no-colon-here", "p2:2026-8-1", " p3 : 2026-08-22 ", "p4:not-a-date"].iter().map(|s| (*s).to_owned()).collect();
    let cells = super::maintain::damaged_cells_newest_first(&listed);
    assert_eq!(
        cells,
        vec![
            ("p3".to_owned(), chrono::NaiveDate::from_ymd_opt(2026, 8, 22).expect("date")),
            ("p1".to_owned(), chrono::NaiveDate::from_ymd_opt(2026, 8, 20).expect("date")),
            // Unpadded is ACCEPTED — chrono parses `2026-8-1` as 2026-08-01.
            // Pinned so nobody "fixes" it into a silent drop.
            ("p2".to_owned(), chrono::NaiveDate::from_ymd_opt(2026, 8, 1).expect("date")),
        ],
        "only well-formed cells survive, newest first, and whitespace is trimmed"
    );
}

/// Ten damage-list-shaped cells, newest first, exactly as the planner orders them.
fn damaged_like_cells(count: u32) -> Vec<super::maintain::BackfillCell> {
    (0..count).map(|i| (format!("p{i}"), chrono::NaiveDate::from_ymd_opt(2026, 8, 20 - i).expect("date"))).collect()
}

/// `set_base_tier_ready` / `set_tier_holes` replace wholesale, so the ready set
/// and hole ranking must be accumulated across ALL sources before they are
/// written — publishing per source inside the loop wipes every earlier one.
#[tokio::test]
async fn coverage_published_for_one_source_survives_planning_the_next() -> Result<()> {
    let db = db_where("multi-source-coverage", wide_backfill).await?;
    let project = format!("multi_{}", uuid::Uuid::new_v4().simple());
    let ts = (Utc::now() - chrono::Duration::days(3)).timestamp_micros();
    // `all_tables` yields several unified tables, so the planner loop runs
    // more than once whatever we write to. Only this source has coverage.
    insert_a_span(&db, &project, "a", ts).await?;

    db.plan_rollup_backfill().await?;

    // `tier_holes` is the same accumulate-across-sources path and is non-empty
    // immediately: the source has a partition, the tier does not.
    let sources = {
        let journal = db.maintenance_tasks.lock().unwrap();
        journal.tier_hole_sources()
    };
    assert!(sources.contains("otel_logs_and_spans"), "cells published for a source must survive planning the others, got {sources:?}");
    Ok(())
}

/// A rewrite carries the `timefusion.*` coverage identity forward only when every
/// input agrees, and never invents it: unioning slices would claim coverage of the
/// gap between inputs, and recovery matches `task.key.slice` exactly anyway.
#[test]
fn a_rewrite_carries_coverage_identity_only_when_every_input_agrees() {
    use crate::maintenance_coordinator::{TAG_GENERATION, TAG_PROJECT, TAG_SLICE_END, TAG_SLICE_START, TAG_SOURCE, TAG_SOURCE_FINGERPRINT};
    let add = |slice_start: &str, generation: &str| deltalake::kernel::Add {
        path: format!("f{slice_start}.parquet"),
        partition_values: Default::default(),
        size: 1,
        modification_time: 0,
        data_change: true,
        tags: Some(HashMap::from([
            (TAG_SOURCE.to_owned(), Some("otel_logs_and_spans".to_owned())),
            (TAG_PROJECT.to_owned(), Some("p".to_owned())),
            (TAG_SLICE_START.to_owned(), Some(slice_start.to_owned())),
            (TAG_SLICE_END.to_owned(), Some("86400000000".to_owned())),
            (TAG_SOURCE_FINGERPRINT.to_owned(), Some("77".to_owned())),
            (TAG_GENERATION.to_owned(), Some(generation.to_owned())),
        ])),
        ..Default::default()
    };

    // One publication cut into two files by the size limit: identical tags.
    let carried = super::carried_coverage_tags(&[add("0", "g1"), add("0", "g1")]);
    assert_eq!(carried.get(TAG_SLICE_START).map(String::as_str), Some("0"), "identical inputs carry their identity forward");
    assert_eq!(carried.len(), 6, "all six coverage tags travel together or not at all");

    assert!(super::carried_coverage_tags(&[add("0", "g1"), add("3600000000", "g1")]).is_empty(), "differing slices must NOT be merged into a span");
    assert!(super::carried_coverage_tags(&[add("0", "g1"), add("0", "g2")]).is_empty(), "differing generations must not be conflated");
    let mut untagged = add("0", "g1");
    untagged.tags = None;
    assert!(super::carried_coverage_tags(&[add("0", "g1"), untagged]).is_empty(), "an untagged input makes the merged coverage unknowable");
    assert!(super::carried_coverage_tags(&[]).is_empty());
}

/// Consolidation admits on SIZE, never on a missing sort tag: the tag is intent,
/// not fact (a flush can stamp a sorted footer without it), so untagged means
/// suspect, not unsorted.
#[tokio::test]
async fn consolidation_admits_on_size_not_on_a_missing_sort_tag() -> Result<()> {
    use crate::maintenance_coordinator::{Operation, TaskState};
    let db = Database::with_config(create_test_config("consolidation-size")).await?;
    let project = format!("size_{}", uuid::Uuid::new_v4().simple());
    // One sealed day, flushed once: a single untagged file well under the target.
    let day = Utc::now() - chrono::Duration::days(3);
    insert_a_span(&db, &project, "a", day.timestamp_micros()).await?;

    db.plan_compaction_debt().await?;

    let sealed_tasks = {
        let journal = db.maintenance_tasks.lock().unwrap();
        journal
            .tasks()
            .filter(|task| {
                task.key.project_id == project
                    && task.key.operation == Operation::SealedConsolidation
                    && matches!(task.state, TaskState::Pending | TaskState::Retry)
            })
            .count()
    };
    assert_eq!(sealed_tasks, 0, "a single untagged file is a Repair suspect, not consolidation debt");
    Ok(())
}

/// A clean day-wide coordinator dedup unit must certify the partition — the
/// dedup cron skips rollup-declared tables, so this is the only grant path for
/// them, and certification is what removes `DedupExec` from a plan.
#[test_case(true ; "persistence enabled")]
#[test_case(false ; "persistence disabled")]
#[serial]
#[tokio::test]
async fn a_clean_day_wide_coordinator_dedup_certifies_the_partition(persist: bool) -> Result<()> {
    use crate::maintenance_coordinator::DAY_MICROS;
    let db = db_where("coord-dedup-certify", |cfg| cfg.maintenance.timefusion_dedup_certification_persist = persist).await?;
    let cert = cert_day();
    // A sealed day with NO duplicates: the pass must drop nothing and leave the
    // file set where it found it, which is what certification requires.
    insert_a_span(&db, &cert.project, "only", cert.at).await?;

    assert!(run_dedup_slice(&db, &cert.project, cert.start, cert.start + DAY_MICROS).await?, "the day-wide dedup unit must be claimed and run");

    assert!(db.dedup_clean_fp.contains_key(&cert.key), "a clean day-wide unit must certify the partition; without it DedupExec survives in every 30d plan");
    let stored = crate::storage::load_sidecar::<crate::storage::StoredCertification>(&db.config.core.timefusion_data_dir, crate::storage::CERTIFICATIONS);
    let persisted =
        stored.iter().any(|entry| entry.project_id == cert.project && entry.table_name == "otel_logs_and_spans" && entry.date == cert.date.to_string());
    assert_eq!(
        persisted, persist,
        "a coordinator-owned table never reaches the legacy sweep's persistence site, so its grant must honor the persistence flag here"
    );
    Ok(())
}

/// Enqueue one Dedup unit over `[start, end)` and run the coordinator once.
async fn run_dedup_slice(db: &Database, project: &str, start: i64, end: i64) -> Result<bool> {
    use crate::maintenance_coordinator::{Operation, TaskKey, TimeSlice};
    let key = TaskKey {
        physical_table: "otel_logs_and_spans".to_owned(),
        source: "otel_logs_and_spans".to_owned(),
        project_id: project.to_owned(),
        slice: TimeSlice::new(start, end)?,
        operation: Operation::Dedup,
    };
    db.maintenance_tasks.lock().unwrap().enqueue(key, 0, 1024, 0);
    db.run_coordinator_dedup_once().await
}

/// Does this certification grant the WHOLE-PARTITION dedup skip? Not the same as
/// "an entry exists": a clean SLICE banks per-file evidence as a `stale` entry,
/// which vouches only for the files it names. Day-wide tests must ask this.
fn grants_whole_partition(db: &Database, key: &(String, String, String)) -> bool {
    db.dedup_clean_fp.get(key).is_some_and(|entry| !entry.value().stale)
}

/// With the per-file skip ON, a partition holding duplicates must still count
/// them once: the answer with certified files present must equal the answer
/// without, so unproved files keep routing through `DedupExec`.
#[tokio::test]
#[serial]
async fn the_per_file_skip_never_over_counts_a_duplicated_partition() -> Result<()> {
    let db = Database::with_config(create_test_config("perfile-nocount")).await?;
    assert!(db.config.maintenance.timefusion_read_dedup_skip_per_file, "this pins the SHIPPED default, not a test-only override");
    let cert = cert_day();
    let project = cert.project.clone();
    // Clean rows early in the day, then a duplicate pair later.
    insert_a_span(&db, &project, "clean", cert.start + 3_600_000_000).await?;
    let dup_ts = cert.half + 3_600_000_000;
    insert_a_span(&db, &project, "dup", dup_ts).await?;
    insert_a_span(&db, &project, "dup", dup_ts).await?;

    let count = async |db: &Database| -> Result<i64> {
        let sql = format!("SELECT COUNT(*)::BIGINT FROM otel_logs_and_spans WHERE project_id = '{project}'");
        Ok(db.query_delta_only(&sql).await?.iter().filter(|b| b.num_rows() > 0).find_map(|b| first_i64(b.column(0))).unwrap_or(0))
    };
    let before = count(&db).await?;
    assert!(run_dedup_slice(&db, &project, cert.start, cert.half).await?, "the clean half-day unit must run");
    assert_eq!(count(&db).await?, before, "certifying clean files must not change the answer for the duplicated ones");
    Ok(())
}

/// A clean slice certifies ONLY the files whose whole span it covered: a file
/// crossing the boundary holds rows the pass never examined, and certifying it
/// would let the read path skip `DedupExec` over them.
#[tokio::test]
#[serial]
async fn a_clean_slice_certifies_only_the_files_it_wholly_covered() -> Result<()> {
    let db = Database::with_config(create_test_config("slice-containment")).await?;
    let day = cert_day();
    // One file inside the first half, one outside it.
    insert_a_span(&db, &day.project, "inside", day.start + 3_600_000_000).await?;
    insert_a_span(&db, &day.project, "outside", day.half + 3_600_000_000).await?;

    assert!(run_dedup_slice(&db, &day.project, day.start, day.half).await?, "the first-half unit must run");
    let cert = db.dedup_clean_fp.get(&day.key).map(|entry| entry.value().clone());
    let files = cert.as_ref().map_or(0, |cert| cert.files.len());
    assert!(files <= 1, "a slice must not certify the file lying outside it; certified {files} files");
    assert!(cert.is_none_or(|cert| cert.stale), "slice-derived evidence must never grant the whole-partition skip");
    Ok(())
}

/// Restored slice evidence must stay `stale`, or a proof about ten minutes comes
/// back indistinguishable from a proof about a whole day.
#[tokio::test]
#[serial]
async fn slice_evidence_stays_stale_across_a_restart() -> Result<()> {
    // One config, built once: the restart must reopen the SAME storage.
    let cfg = create_test_config("slice-stale-restart");
    let db = Database::with_config(cfg.clone()).await?;
    let day = cert_day();
    insert_a_span(&db, &day.project, "only", day.start + 3_600_000_000).await?;

    assert!(run_dedup_slice(&db, &day.project, day.start, day.half).await?, "the half-day unit must run");
    let banked = db.dedup_clean_fp.get(&day.key).is_some();
    drop(db);

    let db = Database::with_config(cfg).await?;
    assert!(!grants_whole_partition(&db, &day.key), "a restored slice certification must still be stale");
    if banked {
        assert!(db.dedup_clean_fp.contains_key(&day.key), "and its file evidence must survive the restart");
    }
    Ok(())
}

/// Two clean half-day units accumulate to certify the partition, and that coverage
/// must survive a restart: the journal marks completed slices Complete durably
/// (never re-run), so in-memory-only coverage means a day straddling any restart can
/// never certify. A write in between moves the fingerprint and voids the accumulated
/// evidence — the evidence is per-file-set, so without the reset a partition written
/// mid-accumulation would certify over a never-re-swept half.
#[test_case("none", true ; "two clean halves cover the day and certify the partition")]
#[test_case("restart", true ; "accumulated slice coverage survives a restart")]
#[test_case("write", false ; "a write INTO proved coverage voids it")]
#[test_case("write_disjoint", true ; "a write the proved half cannot contain leaves it standing")]
#[serial]
#[tokio::test]
async fn clean_slice_units_accumulate_to_certify_the_partition(between: &str, certifies: bool) -> Result<()> {
    use crate::maintenance_coordinator::DAY_MICROS;
    // One config, built once: a restart must reopen the SAME storage.
    let cfg = create_test_config(&format!("slice-cov-{between}"));
    let mut db = Database::with_config(cfg.clone()).await?;
    let day = cert_day();
    insert_a_span(&db, &day.project, "only", day.at).await?;

    assert!(run_dedup_slice(&db, &day.project, day.start, day.half).await?, "first half-day unit must run");
    assert!(!grants_whole_partition(&db, &day.key), "half a day proves nothing about the DAY on its own");
    match between {
        // the restart: coverage evidence must not die with the process
        "restart" => {
            drop(db);
            db = Database::with_config(cfg).await?;
        }
        // A new file lands INSIDE the window the first slice proved. It could hold
        // another version of a row in there, so that coverage must go.
        "write" => insert_a_span(&db, &day.project, "late", day.at + 1).await?,
        // A new file lands in the OTHER half, which the first slice never claimed.
        // It cannot hold a duplicate of anything in the proved half (a duplicate
        // group shares one timestamp), and the second slice sweeps it, so the day
        // is genuinely proved and must certify. Before #290 this was voided too —
        // throwing away proof on every flush is what held certification at 0.4%.
        "write_disjoint" => insert_a_span(&db, &day.project, "late", day.half + DAY_MICROS / 4).await?,
        _ => {}
    }
    assert!(run_dedup_slice(&db, &day.project, day.half, day.start + DAY_MICROS).await?, "second half-day unit must run");
    if certifies {
        assert!(
            db.dedup_clean_fp.contains_key(&day.key),
            "two clean halves cover the day and must certify the partition (between={between}); \
                 losing it on restart is why prod (restarting every 1-2h) never granted"
        );
    } else {
        assert!(!grants_whole_partition(&db, &day.key), "a moved file set voids the first slice's evidence — no whole-partition certification");
    }
    Ok(())
}

/// `min_contiguous_days` returns the worst project (the GOAL metric) AND the
/// median, which is what `coverage_is_short` steers by — one negligible tenant
/// must not pin the fleet into the coverage-short cycle indefinitely.
#[test]
fn a_single_lagging_tenant_pins_the_goal_but_not_the_control_signal() {
    let today = chrono::NaiveDate::from_ymd_opt(2026, 8, 20).expect("date");
    let day = |back: u64| today.checked_sub_days(chrono::Days::new(back)).expect("date");
    let cells = |projects: &[&str], back: std::ops::RangeInclusive<u64>| -> HashSet<(String, chrono::NaiveDate)> {
        back.flat_map(|b| projects.iter().map(move |p| ((*p).to_owned(), day(b)))).collect()
    };
    // Four healthy tenants covered for 10 days back, one laggard covered for 2.
    let all = ["a", "b", "c", "d", "laggard"];
    let (healthy, laggard) = (&all[..4], &all[4..]);
    // Source rows on every day in the horizon, so a project's score is
    // decided by coverage rather than running off the end of its data.
    let source = cells(&all, 1..=CONTIGUITY_HORIZON_DAYS);
    let covered: HashSet<_> = cells(healthy, 1..=10).into_iter().chain(cells(laggard, 1..=2)).collect();
    let active: HashSet<&str> = all.into_iter().collect();

    let (worst, worst_project, median) = min_contiguous_days(&covered, &source, today, &active);
    assert_eq!(worst, 2, "the goal metric still reports the worst tenant");
    assert_eq!(worst_project, Some("laggard"), "and still names it");
    assert_eq!(median, 10, "the control signal must reflect the fleet, not its worst member");
}

/// Age a long-sealed fragmented partition from its seal time, not its scheduling time:
/// hygiene work is re-derived on every restart, so a scheduling-time age never escalates.
#[tokio::test]
async fn a_long_sealed_partition_is_aged_from_when_it_sealed_not_when_rescanned() -> Result<()> {
    use crate::maintenance_coordinator::Operation;
    let db = Database::with_config(create_test_config("hygiene-seal-age")).await?;
    // Two flushes on a long-sealed day => two small files => consolidation debt.
    let project = two_small_files_on_a_sealed_day(&db, "age", 6).await?;

    let created = {
        let journal = db.maintenance_tasks.lock().unwrap();
        journal.tasks().find(|task| task.key.project_id == project && task.key.operation == Operation::SealedConsolidation).map(|task| task.created_unix_ms)
    };
    let created = created.expect("a six-day-old partition with two small files is consolidation debt");
    let now_ms = u64::try_from(crate::support::now_micros().div_euclid(1_000)).unwrap_or_default();
    let waited_hours = (now_ms.saturating_sub(created)) / 3_600_000;
    assert!(
        waited_hours >= 24,
        "a partition sealed six days ago must read as waiting >24h so it can escalate; got {waited_hours}h — \
             aged from the rescan it would read as 0 and starve forever"
    );
    Ok(())
}

/// A planned hygiene unit must carry its file count BEFORE it is ever claimed:
/// `scheduling_class` and `most_indebted_unclaimed` both order on `task.input.files`,
/// so a count written only at claim time scores every unclaimed cell zero.
#[tokio::test]
async fn planned_hygiene_debt_carries_its_file_count_before_any_claim() -> Result<()> {
    use crate::maintenance_coordinator::Operation;
    let db = Database::with_config(create_test_config("hygiene-plan-footprint")).await?;
    // The bigger debt sealed recently; a smaller cell has waited past
    // `STARVATION_MICROS` and legitimately wins the claim — the `outranked_by` arm.
    let indebted = format!("big_{}", uuid::Uuid::new_v4().simple());
    let starved = format!("old_{}", uuid::Uuid::new_v4().simple());
    for (project, days_ago, spans) in [(&indebted, 2_i64, 4_i64), (&starved, 6, 2)] {
        let day = (Utc::now() - chrono::Duration::days(days_ago)).timestamp_micros();
        for id in 0..spans {
            insert_a_span(&db, project, &format!("s{id}"), day + id).await?;
        }
    }
    db.plan_compaction_debt().await?;

    let journal = db.maintenance_tasks.lock().unwrap();
    let files = |project: &str| {
        journal
            .tasks()
            .find(|task| task.key.project_id == project && task.key.operation == Operation::SealedConsolidation)
            .expect("a sealed fragmented partition is consolidation debt")
            .input
            .map_or(0, |input| input.files)
    };
    let (big, small) = (files(&indebted), files(&starved));
    assert!(big >= 2, "the planner selected the file list to decide the partition was out of policy — the queued unit must carry the count, got {big}");
    assert!(big > small, "and the bigger cell must read as the bigger debt, got {big} vs {small}");
    let refusal = journal.most_indebted_unclaimed(Operation::SealedConsolidation, crate::support::now_micros()).expect("the debt is outranked, not claimed");
    assert!(
        refusal.contains(&format!("{indebted:.8}")) && refusal.ends_with(&format!("files={big}")),
        "the instrument must name the genuinely most indebted cell and its debt — got {refusal}"
    );
    Ok(())
}

/// One fresh project with two flushes on a day `days_ago` sealed — two small
/// files, which is consolidation debt — with the debt scan already run.
async fn two_small_files_on_a_sealed_day(db: &Database, prefix: &str, days_ago: i64) -> Result<String> {
    let project = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
    let day = Utc::now() - chrono::Duration::days(days_ago);
    for id in ["a", "b"] {
        insert_a_span(db, &project, id, day.timestamp_micros()).await?;
    }
    db.plan_compaction_debt().await?;
    Ok(project)
}

/// File hygiene must not pack a rollup TIER: packing merges files from different
/// slices, whose coverage tags then disagree, so the packed file proves no coverage
/// and the slices it absorbed lose their only representation.
#[tokio::test]
async fn compaction_debt_skips_rollup_tier_tables() -> Result<()> {
    use crate::maintenance_coordinator::Operation;
    let db = Database::with_config(create_test_config("debt-skips-tiers")).await?;
    two_small_files_on_a_sealed_day(&db, "tier", 4).await?;

    let (on_source, on_tier) = {
        let journal = db.maintenance_tasks.lock().unwrap();
        let hygiene =
            |task: &crate::maintenance_coordinator::MaintenanceTask| matches!(task.key.operation, Operation::HotPacking | Operation::SealedConsolidation);
        (
            journal.tasks().filter(|t| hygiene(t) && t.key.physical_table == "otel_logs_and_spans").count(),
            journal.tasks().filter(|t| hygiene(t) && t.key.physical_table.contains("_rollup_")).count(),
        )
    };
    assert!(on_source > 0, "the scan must still plan hygiene for the source table");
    assert_eq!(on_tier, 0, "a rollup tier must never be packed — packing it erases the coverage it proves");
    Ok(())
}

/// `run-unit` must run the unit it was ASKED for, not whatever ranks first in the
/// journal it shares with the ordinary coordinator runner.
#[tokio::test]
async fn run_unit_runs_the_requested_project_and_not_another() -> Result<()> {
    use crate::maintenance_coordinator::Operation;
    let db = db_where("run-unit-targeting", wide_backfill).await?;
    // The background coordinator would roll up BOTH projects on its own,
    // which is exactly what this test must not mistake for the CLI's doing.
    db.cancel_maintenance();
    // The decoy sits in the LIVE FRONTIER, which `scheduling_class` ranks ahead of
    // every sealed unit — 30 minutes back, so it is past FINALIZATION_DELAY and
    // genuinely claimable, rather than tied with the requested unit.
    let day = (Utc::now() - chrono::Duration::days(12)).date_naive();
    let (wanted, other) = (format!("want_{}", uuid::Uuid::new_v4().simple()), format!("other_{}", uuid::Uuid::new_v4().simple()));
    for (project, at) in [
        (&wanted, day.and_hms_opt(12, 0, 0).expect("noon").and_utc().timestamp_micros()),
        (&other, (Utc::now() - chrono::Duration::minutes(30)).timestamp_micros()),
    ] {
        insert_a_span(&db, project, "a", at).await?;
    }

    // Make the decoy ELIGIBLE. The write path stamps a future deadline
    // (finalization delay), so in a fresh journal the CLI's own key would be
    // the only claimable one and the bug could not show.
    {
        let mut journal = db.maintenance_tasks.lock().unwrap();
        let decoy = journal
            .tasks()
            .find(|task| task.key.project_id == other && task.key.operation == Operation::BaseRollup)
            .map(|task| task.key.clone())
            .expect("the write path queues a base rollup for the decoy");
        journal.enqueue(decoy, 0, crate::maintenance_coordinator::MAX_DECODED_BYTES, 0);
    }

    let report = db.run_unit_once("otel_logs_and_spans", &wanted, day, Operation::BaseRollup, 24, 0).await?;
    assert_eq!(report.state, Some(crate::maintenance_coordinator::TaskState::Complete), "the REQUESTED unit must be the one that ran");

    // A publication, not `state`, is what proves work was actually done for the decoy.
    let decoy_published = {
        let journal = db.maintenance_tasks.lock().unwrap();
        journal.tasks().filter(|task| task.key.project_id == other).any(|task| task.publication.is_some())
    };
    assert!(!decoy_published, "a CLI unit must not run another project's work");
    Ok(())
}

/// The slice-coverage source-row guard, both directions: a freshly built rollup
/// must STILL route, and a source that gains rows afterwards must STOP routing.
/// The second half alone would pass with the guard refusing every rollup.
#[tokio::test]
#[serial]
async fn a_built_rollup_routes_and_stops_routing_once_its_source_grows() -> Result<()> {
    use crate::maintenance_coordinator::Operation;
    let db = db_where("slice-rows-guard", wide_backfill).await?;
    db.cancel_maintenance();
    let project = format!("guard_{}", uuid::Uuid::new_v4().simple());
    let day = (Utc::now() - chrono::Duration::days(3)).date_naive();
    let at = day.and_hms_opt(12, 0, 0).expect("noon").and_utc().timestamp_micros();
    insert_a_span(&db, &project, "a", at).await?;
    db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, 24, 0).await?;

    let covered = |db: &Database| {
        db.rollup_slice_coverage
            .iter()
            .filter(|entry| entry.key().0 == project && entry.key().1 == "otel_logs_and_spans")
            .map(|entry| entry.value().source_rows)
            .collect::<Vec<_>>()
    };
    let witnesses = covered(&db);
    assert!(!witnesses.is_empty(), "the build must publish slice coverage");
    assert!(witnesses.iter().all(Option::is_some), "every published slice must carry its source-row witness: {witnesses:?}");

    // The partition as the build saw it — the same computation the read path compares against.
    let source = db.resolve_table(&project, "otel_logs_and_spans").await?;
    let partition_rows = async || -> Result<Option<u64>> {
        let table = source.read().await;
        Ok(Database::partition_stats_bounded(&table, tiebreak_of("otel_logs_and_spans"), &|_, _| i64::MAX)?
            .remove(&(project.clone(), day.to_string()))
            .and_then(|stats| u64::try_from(stats.rows).ok()))
    };
    let now = partition_rows().await?;
    assert_eq!(witnesses.first().copied().flatten(), now, "a freshly built slice must agree with its source, or the guard refuses every rollup");
    assert!(crate::rollup::slice_coverage_agrees(&witnesses, now), "a fresh build must be trusted");

    // Rows arrive after the build: the witness is stale and the guard must refuse.
    insert_a_span(&db, &project, "b", at + 1).await?;
    let grown = partition_rows().await?;
    assert_ne!(now, grown, "the write must move the partition's row count, or this proves nothing");
    assert!(!crate::rollup::slice_coverage_agrees(&witnesses, grown), "a slice built before the write must not be served afterwards");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn rollup_routing_rejects_legacy_materialization_generations() -> Result<()> {
    use std::hash::{Hash, Hasher};
    let db = Arc::new(Database::with_config(create_test_config("rollup-generation-read")).await?);
    db.cancel_maintenance();
    let project = format!("generation_{}", uuid::Uuid::new_v4().simple());
    let day = (Utc::now() - chrono::Duration::days(3)).date_naive();
    for hour in [1, 7, 13, 19] {
        let at = day.and_hms_opt(hour, 0, 0).unwrap().and_utc().timestamp_micros();
        db.insert_records_batch(
            &project,
            "otel_logs_and_spans",
            vec![json_to_batch(vec![test_span_ts(&format!("row-{hour}"), "op", &project, at)])?],
            true,
            None,
        )
        .await?;
    }
    db.run_unit_once("otel_logs_and_spans", &project, day, crate::maintenance_coordinator::Operation::BaseRollup, 24, 0).await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let state = ctx.state();
    let lo = midnight_micros(day);
    let hi = lo + crate::maintenance_coordinator::DAY_MICROS;
    let sql = format!(
        "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id='{project}' AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi})"
    );
    let plan = state.optimize(&state.create_logical_plan(&sql).await?)?;
    assert!(matches!(db.rollup_sql(&plan, &state).await, Ok(Some(_))), "fresh materializations must route");
    let schema = get_schema("otel_logs_and_spans").unwrap();
    let legacy = |target: &str, coverage: &RollupCoverage| {
        let spec = schema.rollups.iter().find(|s| s.table_name("otel_logs_and_spans") == target).unwrap();
        let restricted = crate::schema::RollupSpec {
            measures: spec.measures.iter().filter(|m| coverage.measures.as_ref().is_none_or(|held| held.contains(&m.name))).cloned().collect(),
            ..spec.clone()
        };
        // The persisted generation algorithm before source-read semantics were versioned.
        let mut hasher = fnv::FnvHasher::default();
        format!("{restricted:?}").hash(&mut hasher);
        ("otel_logs_and_spans", project.as_str(), day.to_string().as_str()).hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    };
    for mut entry in db.rollup_coverage.iter_mut().filter(|entry| entry.key().0 == project) {
        entry.value_mut().generation = legacy(&entry.key().2, entry.value());
    }
    for mut entry in db.rollup_slice_coverage.iter_mut().filter(|entry| entry.key().0 == project) {
        entry.value_mut().generation = legacy(&entry.key().2, entry.value());
    }
    let outcome = db.rollup_sql(&plan, &state).await;
    // The coarser, unbuilt tier may supply the first miss; either coverage
    // refusal is valid, but no legacy generation may produce a rewrite.
    assert!(
        matches!(outcome, Err(crate::rollup::MissReason::StaleCoverage | crate::rollup::MissReason::NotBuilt)),
        "matching source rows cannot validate a pre-fix materialization: {:?}",
        outcome.as_ref().map(|route| route.as_ref().map(|r| r.sql.as_str())).map_err(|reason| reason.label())
    );
    let target = schema.rollups.iter().find(|spec| spec.derive_from.is_none()).unwrap().table_name("otel_logs_and_spans");
    let (key, mut publication) = db.journal().published_rollups("otel_logs_and_spans", &target).into_iter().find(|(key, _)| key.project_id == project).unwrap();
    let derived = db.run_unit_once("otel_logs_and_spans", &project, day, crate::maintenance_coordinator::Operation::DerivedRollup, 24, 0).await?;
    assert_eq!(derived.state, Some(crate::maintenance_coordinator::TaskState::Retry), "a derived unit must wait for a current base generation");

    // Persist the obsolete identity too. Recovery must requeue a completed
    // publication, then a real rebuild must replace its files and restore reads.
    use crate::maintenance_coordinator::{Operation, TAG_GENERATION, TaskState};
    let tier = db.get_or_create_table(&project, &target).await?;
    let coverage = db
        .rollup_slice_coverage
        .get(&(project.clone(), "otel_logs_and_spans".to_owned(), target.clone(), key.slice.start_micros, key.slice.end_micros))
        .unwrap()
        .value()
        .clone();
    assert!(
        !Database::rollup_generation_current("otel_logs_and_spans", &target, &project, &day.to_string(), &coverage),
        "fixture must persist an obsolete generation"
    );
    publication.generation = coverage.generation.clone();
    assert!(db.journal().publish(&key, publication.clone()));
    let obsolete_paths = rewrite_tier_files(&tier, "-fixture", |add| {
        add.tags.as_mut().unwrap().insert(TAG_GENERATION.to_owned(), Some(coverage.generation.clone()));
    })
    .await?;
    assert_eq!(db.journal().tasks().find(|task| task.key == key).unwrap().state, TaskState::Complete);
    db.recover_rollup_coverage("otel_logs_and_spans").await?;
    let task_states = db.journal().tasks().map(|task| (task.key.clone(), task.state)).collect::<Vec<_>>();
    assert_eq!(
        task_states.iter().find(|(task, _)| *task == key).unwrap().1,
        TaskState::Pending,
        "obsolete completed materializations must be rebuilt; key={key:?}, tasks={task_states:?}, coverage={:?}",
        db.rollup_slice_coverage.iter().map(|entry| (entry.key().clone(), entry.value().clone())).collect::<Vec<_>>()
    );
    assert!(!matches!(db.rollup_sql(&plan, &state).await, Ok(Some(_))), "recovery must not restore an obsolete publication");
    let rebuilt = db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, 24, 0).await?;
    assert_eq!(rebuilt.state, Some(TaskState::Complete));
    let live = live_paths(&db, &project, &target).await;
    assert!(obsolete_paths.iter().all(|path| !live.contains(path)), "the rebuild retires the obsolete files");
    let rewrite = db.rollup_sql(&plan, &state).await.map_err(|reason| anyhow::anyhow!("{}", reason.label()))?.expect("rebuilt coverage must route");
    let batches = ctx.sql(&rewrite.sql).await?.collect().await?;
    assert_eq!(first_i64(batches[0].column(0)), Some(4), "rebuilt rollup agrees with the four source rows");

    // A rewrite that loses all tags cannot turn a nonempty base into a
    // trusted empty derived publication. It must request a base rebuild.
    rewrite_tier_files(&tier, "-untagged", |add| add.tags = None).await?;
    let missing_tags = db.run_unit_once("otel_logs_and_spans", &project, day, Operation::DerivedRollup, 24, 0).await?;
    assert_eq!(missing_tags.state, Some(TaskState::Retry), "missing generation evidence must not silently drop base rows");
    assert_eq!(db.journal().tasks().find(|task| task.key == key).unwrap().state, TaskState::Pending, "the refused input must trigger base rebuilding");
    assert!(db.journal().publish(&key, publication));
    assert_eq!(db.journal().tasks().find(|task| task.key == key).unwrap().state, TaskState::Complete);
    db.recover_rollup_coverage("otel_logs_and_spans").await?;
    assert_eq!(
        db.journal().tasks().find(|task| task.key == key).unwrap().state,
        TaskState::Pending,
        "an obsolete journal-only publication must also be requeued"
    );
    db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, 24, 0).await?;
    let repaired = db.run_unit_once("otel_logs_and_spans", &project, day, Operation::DerivedRollup, 24, 0).await?;
    assert_eq!(repaired.state, Some(TaskState::Complete), "base rebuilding must unblock the derived tier");
    let derived_table = schema.rollups.iter().find(|spec| spec.derive_from.is_some()).unwrap().table_name("otel_logs_and_spans");
    let batches = ctx.sql(&format!("SELECT SUM(request_count) FROM {derived_table} WHERE project_id='{project}'")).await?.collect().await?;
    assert_eq!(first_i64(batches[0].column(0)), Some(4));

    Ok(())
}

/// Republish every live file of `tier` at a DISTINCT path with `edit` applied to its
/// `Add`, in one commit, returning the new paths. The path must change: a Remove/Add
/// pair for one path can be replayed as a removal.
async fn rewrite_tier_files(tier: &Arc<RwLock<DeltaTable>>, suffix: &str, edit: impl Fn(&mut deltalake::kernel::Add)) -> Result<Vec<String>> {
    use deltalake::kernel::Action;
    use object_store::ObjectStoreExt as _;
    let store = { tier.read().await.log_store().object_store(None) };
    let adds = live_adds(tier).await;
    let file_count = adds.len();
    assert!(file_count > 0, "the fixture must have files to rewrite");
    let mut paths = Vec::with_capacity(file_count);
    let mut actions = Vec::with_capacity(file_count * 2);
    for mut add in adds {
        actions.push(Action::Remove(remove_for_add(&add, false)));
        let path = format!("{}{suffix}.parquet", add.path.trim_end_matches(".parquet"));
        store.copy(&deltalake::Path::from(add.path.clone()), &deltalake::Path::from(path.clone())).await?;
        add.path = path.clone();
        add.data_change = false;
        edit(&mut add);
        actions.push(Action::Add(add));
        paths.push(path);
    }
    commit_to(tier, actions, append_op(false)).await?;
    assert_eq!(live_adds(tier).await.len(), file_count, "retagging preserves every fixture file");
    Ok(paths)
}

/// The base (`derived = false`) or derived tier table name for `otel_logs_and_spans`.
fn rollup_tier(derived: bool) -> String {
    get_schema("otel_logs_and_spans")
        .and_then(|schema| schema.rollups.iter().find(|spec| spec.derive_from.is_some() == derived).map(|spec| spec.table_name("otel_logs_and_spans")))
        .expect("a rollup tier")
}

/// `create_test_config` with `tweak` applied to it.
fn test_config_with(label: &str, tweak: impl FnOnce(&mut AppConfig)) -> Arc<AppConfig> {
    let mut cfg = (*create_test_config(label)).clone();
    tweak(&mut cfg);
    Arc::new(cfg)
}

/// `create_test_config` with the rollup backfill window widened to `days`.
fn rollup_backfill_config(label: &str, days: u16) -> Arc<AppConfig> {
    test_config_with(label, |cfg| cfg.maintenance.timefusion_rollup_backfill_days = days)
}

/// One project with a single noon row on a three-day-old day, already rolled up
/// into the base tier.
async fn one_rolled_up_day(db: &Database, prefix: &str) -> Result<(String, chrono::NaiveDate)> {
    let project = format!("{prefix}_{}", uuid::Uuid::new_v4().simple());
    let day = (Utc::now() - chrono::Duration::days(3)).date_naive();
    insert_a_span(db, &project, "a", day.and_hms_opt(12, 0, 0).expect("noon").and_utc().timestamp_micros()).await?;
    let report = db.run_unit_once("otel_logs_and_spans", &project, day, crate::maintenance_coordinator::Operation::BaseRollup, 24, 0).await?;
    assert_eq!(report.date, day);
    Ok((project, day))
}

/// One `"a"` span at each of `hours` past `day_start`.
async fn insert_hourly_spans(db: &Database, project: &str, day_start: i64, hours: impl IntoIterator<Item = i64>) -> Result<()> {
    for hour in hours {
        insert_a_span(db, project, "a", day_start + hour * 3_600_000_000).await?;
    }
    Ok(())
}

/// The measure evidence every published slice-coverage cell of `project` carries,
/// for one tier or across all of them.
fn slice_measures(db: &Database, project: &str, tier: Option<&str>) -> Vec<Option<HashSet<String>>> {
    db.rollup_slice_coverage
        .iter()
        .filter(|entry| entry.key().0 == project && tier.is_none_or(|tier| entry.key().2 == tier))
        .map(|entry| entry.value().measures.clone())
        .collect()
}

/// Every date a ticket routes, whether it routes it as a whole-date cell or
/// as a slice.
fn routed_days(ticket: &super::RollupReadTicket) -> HashSet<String> {
    ticket
        .dates
        .iter()
        .map(|((.., date), ..)| date.clone())
        .chain(
            ticket.slices.iter().filter_map(|((.., start, _), ..)| chrono::DateTime::from_timestamp_micros(*start).map(|time| time.date_naive().to_string())),
        )
        .collect()
}

/// One Delta commit against a live table handle, swapping in the resulting snapshot.
async fn commit_to(table_ref: &Arc<RwLock<DeltaTable>>, actions: Vec<deltalake::kernel::Action>, op: deltalake::protocol::DeltaOperation) -> Result<()> {
    let mut table = table_ref.read().await.clone();
    let finalized = deltalake::kernel::transaction::CommitBuilder::default()
        .with_actions(actions)
        .build(Some(table.snapshot()? as &dyn deltalake::kernel::transaction::TableReference), table.log_store(), op)
        .await?;
    table.state = Some(finalized.snapshot());
    *table_ref.write().await = table;
    Ok(())
}

/// The metadata-only `Append` every re-tagging fixture commits.
fn append_op(partitioned: bool) -> deltalake::protocol::DeltaOperation {
    deltalake::protocol::DeltaOperation::Write {
        mode: deltalake::protocol::SaveMode::Append,
        partition_by: partitioned.then(|| get_schema("otel_logs_and_spans").expect("source schema").partitions.clone()),
        predicate: None,
    }
}

/// Model a materialization that predates `measure`: drop it from the tier's measure
/// tags, restamp the generation to match, and recover through the real tag path.
async fn strip_rollup_measure(db: &Database, project: &str, day: chrono::NaiveDate, measure: &str) -> Result<()> {
    use crate::maintenance_coordinator::{TAG_GENERATION, TAG_MEASURES, TAG_PROJECT, TAG_SLICE_START};
    use deltalake::kernel::Action;
    use object_store::ObjectStoreExt as _;
    let source = "otel_logs_and_spans";
    for spec in &get_schema(source).expect("source schema").rollups {
        let tier = db.get_or_create_table(project, &spec.table_name(source)).await?;
        let store = { tier.read().await.log_store().object_store(None) };
        let adds = live_adds(&tier).await;
        let file_count = adds.len();
        let mut actions = Vec::new();
        for mut add in adds {
            let Some(tags) = add.tags.as_mut() else { continue };
            let at = tags.get(TAG_SLICE_START).and_then(Option::as_ref).and_then(|s| s.parse().ok()).and_then(chrono::DateTime::from_timestamp_micros);
            if tags.get(TAG_PROJECT).and_then(Option::as_deref) != Some(project) || at.is_none_or(|at| at.date_naive() != day) {
                continue;
            }
            let held = tags.get(TAG_MEASURES).and_then(Option::as_deref).expect("fresh build proves its measures");
            assert!(held.split(',').any(|name| name == measure));
            let names: Vec<String> = held.split(',').filter(|name| *name != measure).map(str::to_owned).collect();
            tags.insert(TAG_MEASURES.to_owned(), Some(names.join(",")));
            tags.insert(TAG_GENERATION.to_owned(), Some(crate::rollup::generation_id(spec, source, project, &day.to_string(), 0, Some(&names))));
            actions.push(Action::Remove(remove_for_add(&add, false)));
            // A distinct path: a Remove/Add pair for one path can be replayed as a removal.
            let path = format!("{}-fixture.parquet", add.path.trim_end_matches(".parquet"));
            store.copy(&deltalake::Path::from(add.path.clone()), &deltalake::Path::from(path.clone())).await?;
            add.path = path;
            add.data_change = false;
            actions.push(Action::Add(add));
        }
        if actions.is_empty() {
            continue;
        }
        commit_to(&tier, actions, append_op(false)).await?;
        assert_eq!(live_adds(&tier).await.len(), file_count, "retagging preserves every fixture file");
    }
    db.recover_rollup_coverage(source).await?;
    Ok(())
}

/// A date whose cells cannot PROVE they hold a measure must fall to the raw fringe
/// while its siblings keep routing. Delta null-fills a column the files lack and
/// merges skip those nulls, so the wrong answer is plausible rather than visible;
/// generation cannot detect it, since such cells carry the CURRENT generation.
#[tokio::test(flavor = "multi_thread")]
async fn a_date_that_cannot_prove_its_digest_falls_to_the_raw_fringe() -> Result<()> {
    use crate::maintenance_coordinator::Operation;
    const DIGEST: &str = "duration_digest";
    // An `Arc` because `create_session_context` consumes one.
    let db = std::sync::Arc::new(Database::with_config(rollup_backfill_config("measure-not-stored", 35)).await?);
    db.cancel_maintenance();
    let project = format!("meas_{}", uuid::Uuid::new_v4().simple());
    let days: Vec<chrono::NaiveDate> = (3..=4).rev().map(|back| (Utc::now() - chrono::Duration::days(back)).date_naive()).collect();
    for (index, day) in days.iter().enumerate() {
        // Several hours apart so each day's coverage clears the hybrid cost floor.
        for hour in [1, 7, 13, 19] {
            let at = day.and_hms_opt(hour, 0, 0).expect("hour").and_utc().timestamp_micros();
            let row = serde_json::json!({
                "timestamp": at, "id": format!("d{index}h{hour}"), "name": "op", "project_id": project, "hashes": [],
                "summary": ["digest fixture"], "date": day.to_string(), "duration": 100 + hour, "kind": "server", "status_code": "OK",
            });
            db.insert_records_batch(&project, "otel_logs_and_spans", vec![json_to_batch(vec![row])?], true, None).await?;
        }
        db.run_unit_once("otel_logs_and_spans", &project, *day, Operation::BaseRollup, 24, 0).await?;
    }

    // The build must record what it materialized, or stripping it below proves nothing.
    let published = slice_measures(&db, &project, None);
    assert!(!published.is_empty(), "both builds must publish slice coverage");
    assert!(
        published.iter().all(|measures| measures.as_ref().is_some_and(|names| names.contains(DIGEST))),
        "a fresh build materializes the declared digest and must say so: {published:?}"
    );

    let (lo, hi) = (
        days[0].and_hms_opt(0, 0, 0).expect("midnight").and_utc().timestamp_micros(),
        days[1].and_hms_opt(23, 59, 59).expect("end").and_utc().timestamp_micros(),
    );
    // Both shapes: the bare percentile, and a latency widget whose `COUNT(*)` under
    // `duration IS NOT NULL` resolves to `duration_count`, needing TWO measures proven.
    let shapes = [
        "percentile_agg(CAST(duration AS DOUBLE PRECISION)) AS p".to_string(),
        "percentile_agg(CAST(duration AS DOUBLE PRECISION)) AS p, COUNT(*) AS c".to_string(),
    ];
    let sql = |select: &str, widget: bool| {
        format!(
            "SELECT time_bucket('1 hours', timestamp) AS tb, {select} FROM otel_logs_and_spans WHERE project_id = '{project}' \
                 AND timestamp >= to_timestamp_micros({lo}) AND timestamp < to_timestamp_micros({hi}){} GROUP BY 1",
            if widget { " AND duration IS NOT NULL" } else { "" }
        )
    };
    let mut ctx = std::sync::Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let state = ctx.state();
    let route = async |db: &Database, sql: String| {
        let plan = state.optimize(&state.create_logical_plan(&sql).await.expect("parse")).expect("optimize");
        db.rollup_sql(&plan, &state).await.map_err(|reason| anyhow::anyhow!("declined: {}", reason.label()))
    };

    // The control: without it the refusal assertion below also passes when nothing routes.
    for (index, select) in shapes.iter().enumerate() {
        let before = route(&db, sql(select, index == 1)).await?.expect("both days are built and must route");
        let routed = routed_days(&before.ticket);
        assert!(days.iter().all(|day| routed.contains(&day.to_string())), "both days must route before the strip: {routed:?}");
    }

    // The older day's cells lose their proof: the column declared before any file carried it.
    let stripped = days[0].to_string();
    strip_rollup_measure(&db, &project, days[0], DIGEST).await?;

    let misses = || crate::observability::maintenance_stats().rollup_miss_measure_not_stored.load(std::sync::atomic::Ordering::Relaxed);
    for (index, select) in shapes.iter().enumerate() {
        let declines_before = misses();
        let after = route(&db, sql(select, index == 1)).await?.expect("the sibling day still routes");
        assert_eq!(after.mode, "hybrid", "one day on the tier and one raw is a hybrid rewrite, got {}", after.mode);
        assert!(misses() > declines_before, "the decline must be counted, or the refusal is invisible in prod");
        let routed = routed_days(&after.ticket);
        assert!(!routed.contains(&stripped), "a cell that cannot prove the digest must not be read from the tier: {routed:?}");
        assert!(routed.contains(&days[1].to_string()), "the sibling day proves the digest and must keep routing: {routed:?}");
    }

    // TOTAL decline: strip the digest from the sibling day too, so NO date can prove it.
    {
        strip_rollup_measure(&db, &project, days[1], DIGEST).await?;
        let total = route(&db, sql(&shapes[1], true)).await;
        let reason = total.expect_err("no date can prove the digest, so nothing may route").to_string();
        assert!(reason.contains(crate::rollup::MissReason::MeasureNotStored.label()), "a TOTAL measure decline must report measure_not_stored — got {reason}");
    }
    Ok(())
}

/// A slice with no ROW WITNESS must be queued for republish: it is not damaged and
/// not missing but UNVERIFIABLE, so every read refuses it `stale_coverage` forever
/// and nothing else about a sealed, fully-covered day would republish it.
#[tokio::test]
async fn recovery_queues_a_republish_for_a_slice_with_no_row_witness() -> Result<()> {
    let (db, project, day, tier) = published_base_day(rollup_backfill_config("witnessless-republish", 35), "wit").await?;

    // Blank the witness on the journal publication. `Publication::source_rows` is
    // `#[serde(default)]`, so this is what a journal written before the field yields.
    {
        let mut journal = db.maintenance_tasks.lock().unwrap();
        let published = journal.published_rollups("otel_logs_and_spans", &tier);
        assert!(!published.is_empty(), "the unit must have published, or the strip below proves nothing");
        for (key, publication) in published {
            assert!(publication.source_rows.is_some(), "a fresh build must carry the witness this test removes");
            journal.publish(&key, crate::maintenance_coordinator::Publication { source_rows: None, ..publication });
        }
    }
    db.recover_rollup_coverage("otel_logs_and_spans").await?;

    let queued = pending_tier_slices(&db, &project, &tier);
    assert!(!queued.is_empty(), "a slice with no row witness can never be verified and must be queued for republish");
    assert!(
        queued.iter().all(|slice| chrono::DateTime::from_timestamp_micros(slice.start_micros).is_some_and(|time| time.date_naive() == day)),
        "the republish must land on the unverifiable slice's own date, got {queued:?}"
    );
    // The gauge reads the DELTA TAGS, not the journal, and this fixture only blanked
    // the journal — so zero here is correct, and pins that distinction.
    assert_eq!(
        crate::observability::maintenance_stats().rollup_witnessless_slices.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "the backlog gauge must come from the durable tags, not from journal state the enqueue itself mutates"
    );
    Ok(())
}

/// The republish bound keeps the NEWEST slices and defers the rest — dashboards need
/// contiguous recent days, so the bound must cut from the old end.
#[test]
fn the_republish_bound_keeps_the_newest_slices() {
    use crate::maintenance_coordinator::TimeSlice;
    const BOUND: usize = 512;
    let day = 86_400_000_000i64;
    let slices: Vec<_> = (0..600).map(|i| TimeSlice::new(i as i64 * day, (i as i64 + 1) * day).expect("slice")).collect();
    let mut ordered: Vec<_> = slices.iter().collect();
    ordered.sort_unstable_by_key(|slice| std::cmp::Reverse(slice.start_micros));
    let kept: Vec<_> = ordered.into_iter().take(BOUND).map(|slice| slice.start_micros).collect();
    assert_eq!(kept.len(), BOUND, "the bound must cap the pass");
    assert_eq!(kept[0], 599 * day, "the newest slice must be first");
    assert_eq!(*kept.last().expect("bounded"), 88 * day, "the bound must cut from the OLD end, not the new one");
}

/// The publish site must actually CALL `reopen_derived_over` with the right child
/// tier name: a wrong name matches nothing SILENTLY, leaving a derived cell built
/// before its base was rebuilt serving stale rows forever.
#[tokio::test(flavor = "multi_thread")]
async fn republishing_a_base_slice_reopens_the_derived_cell_from_the_publish_site() -> Result<()> {
    use crate::maintenance_coordinator::{Operation, TaskState};
    // The vehicle is a NO-OP republish over an unmoved source, which the no-op skip
    // declines — off here, or the second base run never reaches the publish site.
    let db = Database::with_config(test_config_with("reopen-derived-wiring", |cfg| cfg.maintenance.timefusion_rollup_noop_skip_enabled = false)).await?;
    db.cancel_maintenance();
    let day = (Utc::now() - chrono::Duration::days(3)).date_naive();
    let day_start = midnight_micros(day);
    let project = format!("reopen_{}", uuid::Uuid::new_v4().simple());
    let derived_tier = rollup_tier(true);

    insert_hourly_spans(&db, &project, day_start, [20, 21]).await?;
    db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, 4, 20).await?;
    let derived = db.run_unit_once("otel_logs_and_spans", &project, day, Operation::DerivedRollup, 4, 20).await?;
    assert_eq!(derived.state, Some(TaskState::Complete), "the derived unit must publish first: {:?}", derived.retry_reason);
    let covered = || db.rollup_slice_coverage.iter().filter(|entry| entry.key().0 == project && entry.key().2 == derived_tier).count();
    assert!(covered() > 0, "the derived cell must hold coverage before the base is rebuilt");

    // The base republishes the SAME range: without the edge the derived unit stays
    // Complete and serves the old rows.
    db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, 4, 20).await?;

    let derived_state = db
        .journal()
        .tasks()
        .find(|task| task.key.operation == Operation::DerivedRollup && task.key.project_id == project)
        .map(|task| (task.state, task.publication.is_some()));
    assert_eq!(derived_state, Some((TaskState::Pending, false)), "a base republish must reopen the derived cell over it and drop its publication");
    assert_eq!(covered(), 0, "and drop its slice coverage, so reads fall to the exact raw fringe until it is rebuilt");
    Ok(())
}

/// A DERIVED unit must not publish a cell over a HOLEY base tier. Its witness is the
/// RAW partition but its INPUT is the base tier, so on a sealed day the raw witness
/// agrees forever and a short cell is trusted permanently. Three cases, because the
/// fix has two ways to be wrong: publishing short, and refusing forever.
#[tokio::test(flavor = "multi_thread")]
async fn a_derived_unit_over_a_holey_base_tier_retries_instead_of_publishing_short() -> Result<()> {
    use crate::maintenance_coordinator::{Operation, TaskState};
    let db = Database::with_config(create_test_config("derived-holey-base")).await?;
    db.cancel_maintenance();
    let day = (Utc::now() - chrono::Duration::days(3)).date_naive();
    let day_start = midnight_micros(day);
    let derived_tier = rollup_tier(true);

    // `base_hours` is what the BASE tier is built for, out of 20:00-24:00;
    // the derived unit always asks for `derived` hours from `derived_from`.
    let (db, derived_tier) = (&db, derived_tier.as_str());
    let scenario = |base_hours: i64, derived_from: i64, derived_hours: i64| async move {
        let project = format!("holey_{}", uuid::Uuid::new_v4().simple());
        insert_hourly_spans(db, &project, day_start, [20, 21, 22, 23]).await?;
        db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, base_hours, 20).await?;
        let report = db.run_unit_once("otel_logs_and_spans", &project, day, Operation::DerivedRollup, derived_hours, derived_from).await?;
        let claimed: Vec<(i64, i64)> = db
            .rollup_slice_coverage
            .iter()
            .filter(|entry| entry.key().0 == project && entry.key().2 == derived_tier)
            .map(|entry| (entry.key().3 - day_start, entry.key().4 - day_start))
            .collect();
        anyhow::Ok((report.state, report.retry_reason, claimed))
    };

    const HOUR: i64 = 3_600_000_000;
    // The bug: base covers 20:00-21:00 only, derived asks for 20:00-24:00.
    let (state, reason, claimed) = scenario(1, 20, 4).await?;
    assert!(claimed.is_empty(), "a derived cell must not claim a range its base tier does not cover; claimed {claimed:?}");
    assert_eq!(state, Some(TaskState::Retry), "the unit must be retried, not completed: {reason:?}");
    assert_eq!(reason.as_deref(), Some("base_tier_incomplete"));

    // Base tiles the whole ask: publish.
    let (state, reason, claimed) = scenario(4, 20, 4).await?;
    assert_eq!(state, Some(TaskState::Complete), "a fully covered base must publish: {reason:?}");
    assert_eq!(claimed, vec![(20 * HOUR, 24 * HOUR)]);

    // The day's OPENING hours are not a hole: base units begin at the first row, so a
    // day-wide derived unit is uncovered over 00:00-20:00 where the raw partition is empty.
    let (state, reason, claimed) = scenario(4, 0, 24).await?;
    assert_eq!(state, Some(TaskState::Complete), "a gap below the partition's first row must not block a publish: {reason:?}");
    assert_eq!(claimed, vec![(0, 24 * HOUR)]);
    Ok(())
}

/// A DERIVED cell may not claim a measure its BASE cells never proved: the read gate
/// TRUSTS the tag, and neither the untagged-cell guard nor generation catches a cell
/// that folded Delta's null-fill into an empty state under the current generation.
#[tokio::test(flavor = "multi_thread")]
async fn a_derived_cell_cannot_claim_a_measure_its_base_never_proved() -> Result<()> {
    use crate::maintenance_coordinator::Operation;
    const DIGEST: &str = "duration_digest";
    const COUNT: &str = "request_count";
    let db = Database::with_config(create_test_config("derived-measure-evidence")).await?;
    db.cancel_maintenance();
    let project = format!("evid_{}", uuid::Uuid::new_v4().simple());
    let day = (Utc::now() - chrono::Duration::days(3)).date_naive();
    let day_start = midnight_micros(day);
    let (base_tier, derived_tier) = (rollup_tier(false), rollup_tier(true));
    insert_hourly_spans(&db, &project, day_start, [20, 21, 22, 23]).await?;
    db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, 4, 20).await?;

    // The base cells lose their digest proof while the base tier's SCHEMA keeps it.
    assert!(
        slice_measures(&db, &project, Some(base_tier.as_str())).iter().all(|held| held.as_ref().is_some_and(|held| held.contains(DIGEST))),
        "fresh base proves the digest"
    );
    strip_rollup_measure(&db, &project, day, DIGEST).await?;
    db.run_unit_once("otel_logs_and_spans", &project, day, Operation::DerivedRollup, 4, 20).await?;

    let published = slice_measures(&db, &project, Some(derived_tier.as_str()));
    assert!(!published.is_empty(), "the derived unit must publish over a fully covered base");
    for held in &published {
        let held = held.as_ref().expect("a derived cell must carry measure evidence");
        assert!(!held.contains(DIGEST), "a derived cell whose base never proved the digest must not claim it: {held:?}");
        // The other half: refusing wholesale sends every count chart back to a raw scan.
        assert!(held.contains(COUNT), "a measure the base DID prove must survive: {held:?}");
    }
    Ok(())
}

/// An INTERIOR gap — one with live tagged slices on BOTH sides of it — must be
/// queued, not just a partition with no live tagged range at all.
#[tokio::test]
async fn recovery_queues_an_interior_gap_between_live_tagged_slices() -> Result<()> {
    use crate::maintenance_coordinator::Operation;
    let db = Database::with_config(rollup_backfill_config("untagged-interior-gap", 35)).await?;
    db.cancel_maintenance();
    let project = format!("gap_{}", uuid::Uuid::new_v4().simple());
    let day = (Utc::now() - chrono::Duration::days(3)).date_naive();
    let day_start = midnight_micros(day);
    // Rows at both ends of the day, so the untagged file's statistics span it.
    insert_hourly_spans(&db, &project, day_start, [1, 23]).await?;
    db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, 24, 0).await?;

    let tier = rollup_tier(false);
    let tier_ref = db.get_or_create_table(&project, &tier).await?;
    // Replace the day-wide tagged file with: an UNTAGGED copy spanning the
    // day, plus two tagged slices that leave 12:00-12:11 uncovered.
    const HOLE: (i64, i64) = (12 * 3_600_000_000, 12 * 3_600_000_000 + 11 * 60_000_000);
    {
        let tag = |start: i64, end: i64| {
            std::collections::HashMap::from([
                (crate::maintenance_coordinator::TAG_SLICE_START.to_owned(), Some(start.to_string())),
                (crate::maintenance_coordinator::TAG_SLICE_END.to_owned(), Some(end.to_string())),
                (crate::maintenance_coordinator::TAG_PROJECT.to_owned(), Some(project.clone())),
                (crate::maintenance_coordinator::TAG_SOURCE.to_owned(), Some("otel_logs_and_spans".to_owned())),
            ])
        };
        fork_live_files(&tier_ref, true, |add| {
            [("untagged", None), ("before", Some(tag(day_start, day_start + HOLE.0))), ("after", Some(tag(day_start + HOLE.1, day_start + 24 * 3_600_000_000)))]
                .into_iter()
                .map(|(suffix, tags)| deltalake::kernel::Add { path: sibling_path(&add.path, suffix), tags, ..add.clone() })
                .collect()
        })
        .await?;
    }

    retire_all_tasks(&db);
    db.recover_rollup_coverage("otel_logs_and_spans").await?;

    let queued: Vec<(i64, i64)> = pending_tier_slices(&db, &project, &tier).into_iter().map(|slice| (slice.start_micros, slice.end_micros)).collect();
    assert!(
        queued.iter().any(|(start, end)| *start <= day_start + HOLE.0 && *end >= day_start + HOLE.1),
        "the interior gap {:?} must be queued; got {queued:?}",
        (day_start + HOLE.0, day_start + HOLE.1)
    );
    Ok(())
}

/// A fresh process must route from the durable coverage ledger at boot: until
/// `recover_rollup_coverage` replays every tier's Delta log, routing is not attempted
/// at all, and that replay window is a large share of a short-lived process's uptime.
#[tokio::test(flavor = "multi_thread")]
async fn a_restart_routes_from_the_ledger_before_the_tag_replay_runs() -> Result<()> {
    let cfg = create_test_config("ledger-seed");
    let (db, project, ..) = published_base_day(cfg.clone(), "seed").await?;
    db.recover_rollup_coverage("otel_logs_and_spans").await?;
    let after_replay = db.rollup_slice_coverage.iter().filter(|entry| entry.key().0 == project).count();
    assert!(after_replay > 0, "the replay published routing coverage, or this test proves nothing");
    drop(db);

    // A NEW process over the same data dir, with NO replay run.
    let restarted = Database::with_config(cfg).await?;
    restarted.cancel_maintenance();
    let seeded = restarted.rollup_slice_coverage.iter().filter(|entry| entry.key().0 == project).count();

    assert!(seeded > 0, "routing coverage is available at boot, before any tag replay");
    assert_eq!(seeded, after_replay, "and it is the coverage the replay would have produced, not a subset");
    Ok(())
}

/// The state a container killed mid-rollup leaves behind: the staged parquet exists
/// in object storage and the commit that would have made it live never happened.
/// Reconstructed by REMOVING committed files — objects survive a Remove, so the
/// result is byte-for-byte the killed-mid-stage state.
struct KilledUnit {
    db: Database,
    key: crate::maintenance_coordinator::TaskKey,
    staged: Vec<deltalake::kernel::Add>,
    source_rows: Option<u64>,
    project: String,
    tier: String,
}

/// A stale-generation base file whose span the CURRENT-generation files already
/// reproduce must not demand a base rebuild: `slice_retires` only retires a tagged
/// file CONTAINED in the publishing slice, so a wider offender can never be cleared
/// and the derived tier rebuilds forever. Refusing to READ the file stays correct.
#[tokio::test(flavor = "multi_thread")]
async fn a_stale_base_file_the_current_generation_reproduces_does_not_wedge_the_derived_tier() -> Result<()> {
    use crate::maintenance_coordinator::{Operation, TAG_GENERATION, TaskState};
    let db = Arc::new(Database::with_config(create_test_config("rollup-stale-gen-reproduced")).await?);
    db.cancel_maintenance();
    let project = format!("stalegen_{}", uuid::Uuid::new_v4().simple());
    let day = (Utc::now() - chrono::Duration::days(3)).date_naive();
    for hour in [1i64, 7, 13, 19] {
        insert_a_span(&db, &project, &format!("row-{hour}"), midnight_micros(day) + hour * 3_600_000_000).await?;
    }
    let base_tier = rollup_tier(false);
    db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, 24, 0).await?;

    // A second copy of every base file, same slice tags, GENERATION mangled — what a
    // spec change leaves behind. The originals stay live, so the current generation
    // still reproduces every span the copies claim.
    {
        let tier = db.get_or_create_table(&project, &base_tier).await?;
        assert!(!live_adds(&tier).await.is_empty(), "the base unit must have published files for there to be a stale copy of one");
        fork_live_files(&tier, false, |add| {
            let mut copy = deltalake::kernel::Add { path: sibling_path(&add.path, "stalegen"), data_change: false, ..add.clone() };
            if let Some(tags) = copy.tags.as_mut() {
                tags.insert(TAG_GENERATION.to_owned(), Some("stale-generation".to_owned()));
            }
            vec![copy]
        })
        .await?;
    }

    let derived = db.run_unit_once("otel_logs_and_spans", &project, day, Operation::DerivedRollup, 24, 0).await?;
    assert_eq!(
        derived.state,
        Some(TaskState::Complete),
        "a stale file the live current-generation files already reproduce must not demand a base rebuild: {:?}",
        derived.retry_reason
    );
    let stats = crate::observability::maintenance_stats();
    assert!(
        stats.rollup_base_refusal_reproduced.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "the refusal must still HAPPEN and be counted — this is about the mint, not about reading the stale file"
    );

    // And the derived tier holds the truth, not a short cell.
    let derived_tier = rollup_tier(true);
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let batches = ctx.sql(&format!("SELECT SUM(request_count) FROM {derived_tier} WHERE project_id='{project}'")).await?.collect().await?;
    assert_eq!(batches[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().expect("int64").value(0), 4);
    Ok(())
}

/// Every live path in one tier partition — i.e. "did the commit land?".
async fn live_paths(db: &Database, project: &str, tier: &str) -> std::collections::HashSet<String> {
    let table_ref = db.get_or_create_table(project, tier).await.expect("table");
    let table = table_ref.read().await;
    table.snapshot().expect("snapshot").log_data().iter().map(|file| file.path().to_string()).collect()
}

async fn staged_but_uncommitted_rollup(label: &str) -> Result<KilledUnit> {
    let (db, project, day, tier) = published_base_day(create_test_config(label), "resume").await?;
    let (key, publication) = db
        .journal()
        .published_rollups("otel_logs_and_spans", &tier)
        .into_iter()
        .find(|(key, _)| key.project_id == project)
        .expect("the first run published, or this test proves nothing");

    // The committed output — soon to be the "staged" files.
    let table_ref = db.get_or_create_table(&project, &tier).await?;
    let staged = live_adds(&table_ref).await;
    assert!(!staged.is_empty(), "the build produced files");

    // Un-commit them. The parquet stays in object storage.
    commit_to(
        &table_ref,
        staged.iter().map(|add| deltalake::kernel::Action::Remove(super::remove_for_add(add, true))).collect(),
        deltalake::protocol::DeltaOperation::Write {
            mode: deltalake::protocol::SaveMode::Overwrite,
            partition_by: Some(get_schema(&tier).expect("tier schema").partitions.clone()),
            predicate: None,
        },
    )
    .await?;

    // The intent the killed process would have left, aged past the rolling-deploy
    // window so it is unambiguously a crash leftover.
    db.record_staged_intent(super::StagedIntent {
        wave_id: uuid::Uuid::new_v4().to_string(),
        table_name: tier.clone(),
        project_id: project.clone(),
        recorded_at: crate::support::now_secs() - (super::STAGED_INTENT_MIN_AGE_SECS + 1),
        paths: staged.iter().map(|add| add.path.clone()).collect(),
        target_paths: Vec::new(),
        adds: staged.clone(),
        rollup: Some(super::RollupResume { key: key.clone(), publication: publication.clone(), source_rows: publication.source_rows, date: day.to_string() }),
        instance: None,
    });
    Ok(KilledUnit { db, key, staged, source_rows: publication.source_rows, project, tier })
}

/// A rollup unit killed mid-stage must be COMMITTED on the next claim, not rebuilt —
/// and a unit whose SOURCE moved under it must be discarded, not committed: the
/// replace-set only removes files contained in the slice, so a resumed output and
/// newer files can both end up live and be summed together.
///
/// The repair resume path cannot cover this: `classify_resume` rests on ROW
/// PRESERVATION and a rollup AGGREGATES, so it refuses every rollup by construction.
#[test_case::test_case(false; "the staged output describes reality and is committed")]
#[test_case::test_case(true; "a source that moved under it is discarded")]
#[tokio::test(flavor = "multi_thread")]
async fn a_killed_rollup_resumes_only_while_its_source_has_not_moved(moved: bool) -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let KilledUnit { db, key, staged, source_rows, project, tier } =
        staged_but_uncommitted_rollup(if moved { "rollup-resume-stale" } else { "rollup-resume" }).await?;
    assert!(live_paths(&db, &project, &tier).await.is_empty(), "the reconstructed state has nothing live");

    let stats = crate::observability::maintenance_stats();
    let (resumed, declined) = (stats.rollup_resumed.load(Relaxed), stats.rollup_resume_declined.load(Relaxed));
    // One more source row: the witness the build recorded no longer describes the
    // partition. Passed explicitly, as the unit passes the count it just read.
    let witness = if moved { source_rows.map(|rows| rows + 1) } else { source_rows };
    assert_eq!(db.resume_rollup_unit(&key, witness).await?, !moved, "a staged output resumes exactly when its source is unmoved");

    let live = live_paths(&db, &project, &tier).await;
    for add in &staged {
        assert_eq!(live.contains(&add.path), !moved, "the staged file {} must be committed only when the source is unmoved", add.path);
    }
    if moved {
        assert!(stats.rollup_resume_declined.load(Relaxed) > declined, "and the refusal is visible rather than silent");
        return Ok(());
    }
    assert!(stats.rollup_resumed.load(Relaxed) > resumed, "and it is counted as a resume");
    // Coverage recovery requires `rollup_slice_complete`, so a commit without the
    // publication leaves the planner seeing a hole and re-running the whole scan.
    assert!(
        db.journal().published_rollups("otel_logs_and_spans", &tier).iter().any(|(published, _)| *published == key),
        "the journal is published too, or the planner rebuilds this slice anyway"
    );
    Ok(())
}

/// The state a container killed between STAGING and COMMITTING a compaction bin
/// leaves behind: run the unit for real, then rewind the Delta log. The staged
/// parquet survives a Remove, so what is left is the killed-mid-stage state.
struct KilledBin {
    db: Database,
    project: String,
    day: chrono::NaiveDate,
    table_ref: Arc<RwLock<DeltaTable>>,
    /// The committed-then-un-committed output: what a resume must commit.
    staged: Vec<deltalake::kernel::Add>,
    /// Its inputs, live again.
    targets: Vec<deltalake::kernel::Add>,
    /// What a resume must reproduce EXACTLY: any new path means it re-staged.
    settled: HashSet<String>,
}

async fn live_adds(table_ref: &Arc<RwLock<DeltaTable>>) -> Vec<deltalake::kernel::Add> {
    let table = table_ref.read().await;
    #[allow(deprecated)]
    table.snapshot().expect("snapshot").log_data().iter().map(|file| file.add_action()).collect()
}

impl KilledBin {
    /// The manifest entry the killed process left; `aged`/`foreign` are the two axes
    /// `resume_guarded` decides on.
    fn record_intent(&self, aged: bool, foreign: bool) -> Result<()> {
        self.db.record_staged_intent(super::StagedIntent {
            wave_id: uuid::Uuid::new_v4().to_string(),
            table_name: "otel_logs_and_spans".to_owned(),
            project_id: self.project.clone(),
            recorded_at: crate::support::now_secs() - if aged { super::STAGED_INTENT_MIN_AGE_SECS + 1 } else { 0 },
            paths: self.staged.iter().map(|add| add.path.clone()).collect(),
            target_paths: self.targets.iter().map(|add| add.path.clone()).collect(),
            adds: self.staged.clone(),
            rollup: None,
            instance: None,
        });
        if foreign {
            // `record_staged_intent` stamps OUR instance id unconditionally, so a
            // PREVIOUS process's entry can only be built by rewriting the manifest.
            let path = self.db.staged_intent_path();
            let contents = std::fs::read_to_string(&path)?.replace(crate::observability::instance_id(), &uuid::Uuid::new_v4().to_string());
            std::fs::write(&path, contents)?;
        }
        Ok(())
    }

    async fn run_unit(&self) -> Result<UnitRunReport> {
        run_consolidation(&self.db, &self.project, self.day).await
    }
}

async fn run_consolidation(db: &Database, project: &str, day: chrono::NaiveDate) -> Result<UnitRunReport> {
    db.run_unit_once("otel_logs_and_spans", project, day, crate::maintenance_coordinator::Operation::SealedConsolidation, 24, 0).await
}

/// One metadata-only Delta commit on a partitioned table; touches no object storage.
async fn commit_actions(table_ref: &Arc<RwLock<DeltaTable>>, actions: Vec<deltalake::kernel::Action>) -> Result<()> {
    commit_to(table_ref, actions, append_op(true)).await
}

async fn staged_but_uncommitted_bin(label: &str) -> Result<KilledBin> {
    let db = Database::with_config(create_test_config(label)).await?;
    db.cancel_maintenance();
    let project = format!("repair_{}", uuid::Uuid::new_v4().simple());
    let day = (Utc::now() - chrono::Duration::days(4)).date_naive();
    let noon = day.and_hms_opt(12, 0, 0).expect("noon").and_utc().timestamp_micros();
    // Three flushes: two become the bin, the third is tagged an already-sorted run
    // below so it stays live and leaves the cell owing work after the bin commits.
    for (id, offset) in [("a", 0), ("b", 60_000_000), ("c", 120_000_000)] {
        insert_a_span(&db, &project, id, noon + offset).await?;
    }
    let table_ref = db.get_or_create_table(&project, "otel_logs_and_spans").await?;
    let ingested = live_adds(&table_ref).await;
    assert_eq!(ingested.len(), 3, "three flushes, three files");

    // Packing skips sorted runs while any unsorted candidate remains. Two commits,
    // so a Remove and an Add of the same path never race within one version.
    let mut held = ingested[0].clone();
    commit_actions(&table_ref, vec![deltalake::kernel::Action::Remove(super::remove_for_add(&held, false))]).await?;
    let mut tags = held.tags.clone().unwrap_or_default();
    tags.insert(super::SORTED_RUN_TAG.to_owned(), Some("true".to_owned()));
    held.tags = Some(tags);
    commit_actions(&table_ref, vec![deltalake::kernel::Action::Add(held.clone())]).await?;

    let before = live_adds(&table_ref).await;
    assert_eq!(before.len(), 3, "re-tagging is metadata only and must not lose a file");

    // The process that "died": it packed the two untagged files into one committed output.
    let first = run_consolidation(&db, &project, day).await?;
    assert_eq!(
        first.retry_reason.as_deref(),
        Some("compaction_debt_remaining"),
        "the held sorted run is still live and under target, so the cell owes work — the semantics the resume arm has to match: {first}"
    );
    let after = live_adds(&table_ref).await;
    let paths = |adds: &[deltalake::kernel::Add]| adds.iter().map(|add| add.path.clone()).collect::<HashSet<_>>();
    let (was, settled) = (paths(&before), paths(&after));
    let staged: Vec<_> = after.iter().filter(|add| !was.contains(&add.path)).cloned().collect();
    let targets: Vec<_> = before.iter().filter(|add| !settled.contains(&add.path)).cloned().collect();
    assert_eq!(
        (staged.is_empty(), targets.len()),
        (false, 2),
        "the two untagged files must have been packed into a fresh output, or there is nothing to resume: {first}"
    );

    // Rewind: drop the output from the log, put its inputs back. The
    // output's parquet stays in the object store — that IS the staged bin.
    commit_actions(
        &table_ref,
        staged
            .iter()
            .map(|add| deltalake::kernel::Action::Remove(super::remove_for_add(add, true)))
            .chain(targets.iter().cloned().map(deltalake::kernel::Action::Add))
            .collect(),
    )
    .await?;
    Ok(KilledBin { db, project, day, table_ref, staged, targets, settled })
}

/// A compaction bin staged by a process that died before committing must be
/// COMMITTED on the next claim, not rewritten from scratch.
///
/// The young-intent case matters: `requeue_running` sets `deadline = now`, so a
/// killed unit is re-claimed within minutes while its intent is still young.
#[test_case::test_case(true, true, true; "a previous instance, aged past the legacy gate")]
#[test_case::test_case(true, false, true; "a previous instance, seconds old — THE prod timing")]
#[test_case::test_case(false, false, false; "our own young intent may still be in flight here")]
#[tokio::test(flavor = "multi_thread")]
async fn coordinator_commits_a_resumable_staged_bin_instead_of_restaging(foreign: bool, aged: bool, resumes: bool) -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let killed = staged_but_uncommitted_bin(&format!("repair-resume-{foreign}-{aged}")).await?;
    killed.record_intent(aged, foreign)?;

    let stats = crate::observability::maintenance_stats();
    let (resumed, skipped) = (stats.repair_resumed.load(Relaxed), stats.repair_resume_skipped.load(Relaxed));
    let report = killed.run_unit().await?;
    let live = live_paths(&killed.db, &killed.project, "otel_logs_and_spans").await;

    if !resumes {
        assert_eq!(stats.repair_resumed.load(Relaxed), resumed, "an intent this process may still be staging must not be committed under it");
        assert!(stats.repair_resume_skipped.load(Relaxed) > skipped, "and the guard's refusal must be counted, not silent");
        assert!(killed.staged.iter().all(|add| !live.contains(&add.path)), "the staged output stays uncommitted; the unit rewrote its input instead");
        return Ok(());
    }
    assert_eq!(stats.repair_resumed.load(Relaxed), resumed + 1, "the already-staged bin must be COMMITTED rather than rebuilt");
    assert_eq!(live, killed.settled, "a resume must land EXACTLY the file set the killed process produced — any new path means it paid for the rewrite twice");
    // A resumed bin is ONE bin and Repair hands out `take(1)`, so the cell still
    // owes its other file; completing here would retire live debt.
    assert_eq!(
        report.retry_reason.as_deref(),
        Some("compaction_debt_remaining"),
        "the cell still owes work after the resumed bin, so the unit must requeue — completing it retires live debt"
    );
    assert_eq!(report.state, Some(crate::maintenance_coordinator::TaskState::Retry), "and `complete` clears the reason, so the state has to agree");
    Ok(())
}

/// A resume whose staged parquet is gone must fall through to normal staging,
/// not fail the unit. This is the common way an intent outlives what it
/// describes: the boot-time reconcile deletes exactly these objects.
#[tokio::test(flavor = "multi_thread")]
async fn a_resume_whose_staged_parquet_is_gone_stages_normally() -> Result<()> {
    use object_store::ObjectStoreExt;
    use std::sync::atomic::Ordering::Relaxed;
    let killed = staged_but_uncommitted_bin("repair-resume-gone").await?;
    killed.record_intent(true, true)?;
    let store = { killed.table_ref.read().await.log_store().object_store(None) };
    for add in &killed.staged {
        store.delete(&object_store::path::Path::from(add.path.as_str())).await?;
    }

    let stats = crate::observability::maintenance_stats();
    let (resumed, incomplete) = (stats.repair_resumed.load(Relaxed), stats.repair_resume_declined_incomplete.load(Relaxed));
    let report = killed.run_unit().await?;

    assert_eq!(stats.repair_resumed.load(Relaxed), resumed, "there is nothing to commit — the objects are gone");
    assert!(stats.repair_resume_declined_incomplete.load(Relaxed) > incomplete, "and the decline is counted");
    assert_eq!(report.retry_reason.as_deref(), Some("compaction_debt_remaining"), "the unit staged normally and requeued; it must not fail");
    let live = live_paths(&killed.db, &killed.project, "otel_logs_and_spans").await;
    assert!(killed.targets.iter().all(|add| !live.contains(&add.path)), "the input was genuinely rewritten by the fall-through");
    Ok(())
}

/// The tag replay also records what it read into the coverage ledger, and the
/// ledger must agree with the tag-derived routing map in both directions.
///
/// The cell's date comes from the file's PARTITION, not from `slice_start`: a file
/// in `date=D` cannot hold rows outside `D`.
#[tokio::test(flavor = "multi_thread")]
async fn the_tag_replay_records_what_it_reads_into_the_coverage_ledger() -> Result<()> {
    use crate::storage::CoverageLedger as _;
    let (db, project, day, tier) = published_base_day(create_test_config("ledger-populate"), "ledger").await?;
    assert!(
        db.coverage_ledger.coverage(&("otel_logs_and_spans".to_owned(), project.clone(), tier.clone(), day.to_string())).is_empty(),
        "the ledger is populated BY the replay, so it is empty until one runs"
    );

    db.recover_rollup_coverage("otel_logs_and_spans").await?;

    let cell = ("otel_logs_and_spans".to_owned(), project.clone(), tier, day.to_string());
    let recorded = db.coverage_ledger.coverage(&cell);
    assert!(!recorded.is_empty(), "the replay recorded the published slice");
    assert!(recorded.iter().all(|entry| entry.end_micros > entry.start_micros), "slice ends are exclusive and must be after their start: {recorded:?}");
    // Compared as SETS OF RANGES, not entry-for-entry: the ledger merges adjacent
    // slices of one generation, so it is deliberately coarser.
    let tag_ranges: std::collections::BTreeSet<(i64, i64)> =
        db.rollup_slice_coverage.iter().filter(|entry| entry.key().0 == project).map(|entry| (entry.key().3, entry.key().4)).collect();
    assert!(!tag_ranges.is_empty(), "the tag-derived routing map is populated, or this gate proves nothing");
    let ledger_ranges = db.coverage_ledger.routing_view("otel_logs_and_spans", &cell.2);
    let ledger_ranges: Vec<(i64, i64)> =
        ledger_ranges.get(&project).map(|entries| entries.iter().map(|e| (e.start_micros, e.end_micros)).collect()).unwrap_or_default();
    assert!(!ledger_ranges.is_empty(), "the ledger claims coverage for this project");
    for (start, end) in &tag_ranges {
        assert!(
            ledger_ranges.iter().any(|(lo, hi)| lo <= start && hi >= end),
            "every range the tags cover is covered by the ledger too: {:?} missing from {ledger_ranges:?}",
            (start, end)
        );
    }

    // The reverse direction: a ledger recording MORE than the tag map over-claims,
    // serving coverage the read path deliberately refuses.
    for (lo, hi) in &ledger_ranges {
        assert!(
            tag_ranges.iter().any(|(start, end)| start <= lo && end >= hi),
            "the ledger claims no range the read path refuses: {:?} not in {tag_ranges:?}",
            (lo, hi)
        );
    }

    // Coverage without file identity could only supplement the tags, never replace them.
    assert!(recorded.iter().all(|entry| !entry.files.is_empty()), "every recorded range names the files that serve it: {recorded:?}");

    // Nothing changed between these two replays, so the second must record no drift.
    let before = crate::observability::maintenance_stats().coverage_ledger_disagreements.load(std::sync::atomic::Ordering::Relaxed);
    db.recover_rollup_coverage("otel_logs_and_spans").await?;
    let after = crate::observability::maintenance_stats().coverage_ledger_disagreements.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(after, before, "an unchanged tier replayed twice is not a disagreement");
    assert_eq!(db.coverage_ledger.coverage(&cell), recorded, "and the second replay leaves the same coverage, not a duplicated one");

    // Retirement. Planted under a source/tier this replay NEVER reads — the orphan
    // sweep filters on `(source, target)`, so a cell in the replayed tier would be
    // retired by that sweep regardless and the assertion would prove nothing.
    let ancient = ("never_replayed".to_owned(), project.clone(), cell.2.clone(), "2025-01-01".to_owned());
    db.coverage_ledger.record(&ancient, recorded[0].clone());
    assert!(!db.coverage_ledger.coverage(&ancient).is_empty(), "planted");
    db.recover_rollup_coverage("otel_logs_and_spans").await?;
    assert!(db.coverage_ledger.coverage(&ancient).is_empty(), "a cell past the rollup horizon is retired");
    assert_eq!(db.coverage_ledger.coverage(&cell), recorded, "and an in-window cell is untouched by retirement");

    // Restart seeding, validated independently of Delta replay: recreate the
    // pre-version generation from its real spec.
    use std::hash::{Hash, Hasher};
    let spec = get_schema(&cell.0).unwrap().rollups.iter().find(|spec| spec.table_name(&cell.0) == cell.2).unwrap();
    let mut legacy = recorded.clone();
    for entry in &mut legacy {
        let restricted = crate::schema::RollupSpec {
            measures: spec.measures.iter().filter(|m| entry.measures.as_ref().is_none_or(|names| names.contains(&m.name))).cloned().collect(),
            ..spec.clone()
        };
        let mut hasher = fnv::FnvHasher::default();
        format!("{restricted:?}").hash(&mut hasher);
        (cell.0.as_str(), project.as_str(), cell.3.as_str()).hash(&mut hasher);
        entry.generation = format!("{:016x}", hasher.finish());
    }
    db.coverage_ledger.replace(&cell, legacy);
    db.rollup_slice_coverage.clear();
    assert_eq!(db.seed_routing_from_ledger(), 0, "an old reader's ledger cannot authorize current reads");
    db.coverage_ledger.replace(&cell, recorded);
    assert!(db.seed_routing_from_ledger() > 0, "current persisted coverage survives restart");

    Ok(())
}

/// A published base-tier day with maintenance off: one span at noon three
/// days ago, one BaseRollup unit run over it. Returns `(db, project, day, tier)`.
async fn published_base_day(cfg: Arc<AppConfig>, prefix: &str) -> Result<(Database, String, chrono::NaiveDate, String)> {
    let db = Database::with_config(cfg).await?;
    db.cancel_maintenance();
    let (project, day) = one_rolled_up_day(&db, prefix).await?;
    Ok((db, project, day, rollup_tier(false)))
}

/// `path` with `-{suffix}` before its `.parquet` extension.
fn sibling_path(path: &str, suffix: &str) -> String {
    format!("{}-{suffix}.parquet", path.trim_end_matches(".parquet"))
}

/// Copy every live file's bytes to the sibling paths `variant` names for it and commit
/// the copies; `replace` also removes the originals.
async fn fork_live_files(
    table_ref: &Arc<RwLock<DeltaTable>>, replace: bool, variant: impl Fn(&deltalake::kernel::Add) -> Vec<deltalake::kernel::Add>,
) -> Result<()> {
    use deltalake::kernel::Action;
    use object_store::ObjectStoreExt as _;
    let live = live_adds(table_ref).await;
    assert!(!live.is_empty(), "the publish must have written a file to fork");
    let store = table_ref.read().await.log_store().object_store(None);
    let mut actions = Vec::new();
    for add in &live {
        for copy in variant(add) {
            store.copy(&deltalake::Path::from(add.path.clone()), &deltalake::Path::from(copy.path.clone())).await?;
            actions.push(Action::Add(copy));
        }
    }
    if replace {
        actions.extend(live.iter().map(|add| Action::Remove(remove_for_add(add, true))));
    }
    commit_to(table_ref, actions, append_op(false)).await
}

/// Copy every live file to a sibling with the same bytes and no identity tags, as a
/// delta-rs OPTIMIZE does. `replace` also removes the tagged original, so the
/// partition's only file is untagged.
async fn strip_slice_tags(table_ref: &Arc<RwLock<DeltaTable>>, suffix: &str, replace: bool) -> Result<()> {
    fork_live_files(table_ref, replace, |add| vec![deltalake::kernel::Add { path: sibling_path(&add.path, suffix), tags: None, ..add.clone() }]).await
}

/// Retire every queued task so only work the next pass creates is visible,
/// returning the keys it completed.
fn retire_all_tasks(db: &Database) -> Vec<crate::maintenance_coordinator::TaskKey> {
    let mut journal = db.maintenance_tasks.lock().unwrap();
    let keys: Vec<_> = journal.tasks().map(|task| task.key.clone()).collect();
    for key in &keys {
        journal.complete(key);
    }
    keys
}

/// `(live files, of which untagged)` for one project's tier. Re-resolved on
/// every call — a rebuild republishes the handle.
async fn live_tier_files(db: &Database, project: &str, tier: &str) -> Result<(usize, usize)> {
    let adds = live_adds(&db.get_or_create_table(project, tier).await?).await;
    let untagged = adds.iter().filter(|add| add.tags.as_ref().is_none_or(|tags| !tags.contains_key(crate::maintenance_coordinator::TAG_SLICE_START))).count();
    Ok((adds.len(), untagged))
}

/// The slices still Pending for one project's tier.
fn pending_tier_slices(db: &Database, project: &str, tier: &str) -> Vec<crate::maintenance_coordinator::TimeSlice> {
    let journal = db.maintenance_tasks.lock().unwrap();
    journal
        .tasks()
        .filter(|task| task.key.project_id == project && task.state == crate::maintenance_coordinator::TaskState::Pending && task.key.physical_table == tier)
        .map(|task| task.key.slice)
        .collect()
}

/// A partition still holding an untagged tier file must be QUEUED for rebuild.
///
/// `slice_retires` only retires such a file when something publishes that partition,
/// which never happens for a sealed day that already has coverage.
#[tokio::test]
async fn recovery_queues_a_rebuild_for_a_partition_holding_untagged_tier_files() -> Result<()> {
    use crate::maintenance_coordinator::Operation;
    let (db, project, day, tier) = published_base_day(rollup_backfill_config("untagged-selfheal", 35), "heal").await?;

    let tier_ref = db.get_or_create_table(&project, &tier).await?;
    strip_slice_tags(&tier_ref, "stripped", true).await?;

    // Retire everything so only work THIS recovery creates is visible.
    retire_all_tasks(&db);
    db.recover_rollup_coverage("otel_logs_and_spans").await?;

    let queued = pending_tier_slices(&db, &project, &tier);
    assert!(!queued.is_empty(), "a partition holding an untagged tier file must be queued for rebuild");
    // Bounded by the FILE, not the day: a day-wide unit is over `MAX_DECODED_BYTES`
    // for any real tenant and the preflight shreds it into thousands of slices.
    assert!(
        queued.iter().all(|slice| slice.width() < crate::maintenance_coordinator::DAY_MICROS),
        "the rebuild must target the untagged file's own span, got {queued:?}"
    );
    assert!(
        queued.iter().all(|slice| chrono::DateTime::from_timestamp_micros(slice.start_micros).is_some_and(|time| time.date_naive() == day)),
        "the rebuild must land on the damaged partition's date"
    );

    // The retired COUNTER must equal what actually left the table, not what the
    // replace-set intended. HONEST LIMIT: this only asserts the happy path where the
    // commit lands; it guards double-counting, not a unit abandoned mid-way.
    let before = live_tier_files(&db, &project, &tier).await?.1;
    assert!(before > 0, "the stripped copies must be live for this to prove anything");
    let counter = || crate::observability::maintenance_stats().rollup_tier_untagged_retired.load(std::sync::atomic::Ordering::Relaxed);
    let counted_before = counter();
    db.run_unit_once("otel_logs_and_spans", &project, day, Operation::BaseRollup, 24, 0).await?;
    assert_eq!(
        u64::try_from(before - live_tier_files(&db, &project, &tier).await?.1).unwrap_or_default(),
        counter() - counted_before,
        "the retired counter must equal the untagged files the commit actually removed"
    );
    Ok(())
}

/// An untagged file already CONTAINED in a live tagged slice queues that
/// slice, not the file's own span.
///
/// A slice a wider live file already covers must settle WITHOUT scanning.
///
/// The decision reads only the target tier's committed file tags, so it was
/// always available before the work — but it used to be made after the scan, the
/// aggregate AND the parquet write, every one of which was then discarded. Prod
/// 2026-09-21 took that path 3,829 times against 210 clean completions, most of
/// 608 worker-minutes of scanning per 25 minutes, each leaving uncommitted
/// parquet behind.
///
/// The observable proof is the ORPHAN: a unit that reached the writer uploads
/// objects it never commits, so the tier's object count grows while its LIVE file
/// set does not. Settling early leaves both unchanged.
#[tokio::test]
async fn a_slice_covered_by_a_wider_file_settles_without_scanning() -> Result<()> {
    let (db, project, day, tier) = published_base_day(rollup_backfill_config("covered-preflight", 35), "preflight").await?;
    let tier_ref = db.get_or_create_table(&project, &tier).await?;
    let covering: Vec<(i64, i64)> = live_adds(&tier_ref)
        .await
        .into_iter()
        .filter_map(|add| {
            let tags = add.tags?;
            let tag = |name: &str| tags.get(name).and_then(Option::as_deref)?.parse::<i64>().ok();
            Some((tag(crate::maintenance_coordinator::TAG_SLICE_START)?, tag(crate::maintenance_coordinator::TAG_SLICE_END)?))
        })
        .collect();
    let Some(&(covering_start, covering_end)) = covering.first() else {
        panic!("the published day must leave a tagged live slice, or this tests the other shape");
    };
    assert!(covering_end > covering_start, "a covering slice must be non-empty");

    // A unit STRICTLY INSIDE the covering slice, and it must CONTAIN the fixture's
    // noon span. A narrower slice with no rows aggregates to nothing and reaches no
    // writer, so the old code path wrote no parquet either and the orphan assertion
    // below could not tell the two apart — which is exactly how the first cut of this
    // test passed with the fix reverted.
    let hour = 3_600_000_000i64;
    let (inner_start, inner_end) = (covering_start + 11 * hour, covering_start + 13 * hour);
    assert!(inner_start > covering_start && inner_end < covering_end, "the inner slice must be STRICTLY inside the covering one");
    retire_all_tasks(&db);
    let key = crate::maintenance_coordinator::TaskKey {
        physical_table: tier.clone(),
        source: "otel_logs_and_spans".to_owned(),
        project_id: project.clone(),
        slice: crate::maintenance_coordinator::TimeSlice::new(inner_start, inner_end)?,
        operation: crate::maintenance_coordinator::Operation::BaseRollup,
    };
    {
        let mut journal = db.journal();
        journal.enqueue(key.clone(), 0, 1024, 0);
        journal.checkpoint()?;
    }

    use std::sync::atomic::Ordering::Relaxed;
    let escalations_before = crate::observability::maintenance_stats().rollup_skipped_covered_by_wider.load(Relaxed)
        + crate::observability::maintenance_stats().rollup_escalation_skipped_fresh.load(Relaxed);
    let objects_before = tier_object_count(&db, &tier_ref).await;
    let live_before = live_adds(&tier_ref).await.len();
    let did_work = db.run_coordinator_rollup_selected(crate::database::maintain::TaskSelection::Exact(&key)).await?;
    assert!(did_work, "the unit must be settled, not left claimable");

    // Settled: the narrow unit is gone and the COVERING slice carries the rebuild.
    assert_eq!(db.journal().state(&key), Some(crate::maintenance_coordinator::TaskState::Complete), "the covered unit must complete");
    let queued: Vec<(i64, i64)> = pending_tier_slices(&db, &project, &tier).into_iter().map(|slice| (slice.start_micros, slice.end_micros)).collect();
    assert!(
        queued.is_empty() || queued.contains(&(covering_start, covering_end)),
        "a rebuild must target the COVERING slice, never the contained one; got {queued:?}"
    );

    // The unit must actually have TAKEN the covered-by-wider path. Without this the
    // test passes on a fixture that never reaches it, which is exactly how the first
    // cut of it passed with the fix reverted.
    let escalations_after = crate::observability::maintenance_stats().rollup_skipped_covered_by_wider.load(Relaxed)
        + crate::observability::maintenance_stats().rollup_escalation_skipped_fresh.load(Relaxed);
    assert!(escalations_after > escalations_before, "the fixture must reach the covered-by-wider path, or this test asserts nothing");

    // And the work was never done: no parquet written, committed or orphaned.
    assert_eq!(live_adds(&tier_ref).await.len(), live_before, "a covered unit must publish nothing");
    assert_eq!(
        tier_object_count(&db, &tier_ref).await,
        objects_before,
        "a covered unit must not reach the WRITER — an uncommitted parquet upload is the orphan this check exists to avoid"
    );
    let _ = day;
    Ok(())
}

/// Objects physically present under the tier, committed or not.
async fn tier_object_count(_db: &Database, table_ref: &Arc<RwLock<DeltaTable>>) -> usize {
    use futures::StreamExt;
    let store = table_ref.read().await.log_store().object_store(None);
    store.list(None).filter(|entry| futures::future::ready(entry.is_ok())).count().await
}

/// Publishing the contained span cannot land: `covered_by_wider` refuses it — two
/// overlapping files would both stay live and a dashboard would SUM both.
#[tokio::test]
async fn a_covered_untagged_file_queues_the_covering_slice() -> Result<()> {
    let (db, project, _day, tier) = published_base_day(rollup_backfill_config("untagged-covered", 35), "cover").await?;
    let tier_ref = db.get_or_create_table(&project, &tier).await?;
    // The stripped copy sits BESIDE its tagged original, so a live tagged
    // slice contains the untagged file's span.
    strip_slice_tags(&tier_ref, "stripped", false).await?;
    let covering: Vec<(i64, i64)> = live_adds(&tier_ref)
        .await
        .into_iter()
        .filter_map(|add| {
            let tags = add.tags?;
            let tag = |name: &str| tags.get(name).and_then(Option::as_deref)?.parse::<i64>().ok();
            Some((tag(crate::maintenance_coordinator::TAG_SLICE_START)?, tag(crate::maintenance_coordinator::TAG_SLICE_END)?))
        })
        .collect();
    assert!(!covering.is_empty(), "the tagged original must stay live, or this tests the other shape");

    retire_all_tasks(&db);
    db.recover_rollup_coverage("otel_logs_and_spans").await?;

    let queued: Vec<(i64, i64)> = pending_tier_slices(&db, &project, &tier).into_iter().map(|slice| (slice.start_micros, slice.end_micros)).collect();
    assert!(!queued.is_empty(), "a covered untagged file must still be queued");
    assert!(
        queued.iter().all(|slice| covering.contains(slice)),
        "the rebuild must target the COVERING tagged slice, or `covered_by_wider` refuses it; got {queued:?} against {covering:?}"
    );
    Ok(())
}

/// Columns are IMMUTABLE by default; only declared ones are version-mutable.
///
/// This decides whether a point lookup pushes below the merge-on-read dedup or is
/// stranded above it, forcing the whole window to be materialised keep-greatest first.
#[test]
fn only_declared_and_version_bearing_columns_are_mutable() {
    let mutable = super::ProjectRoutingTable::version_mutable_columns("otel_logs_and_spans").expect("a version_append table");

    assert!(mutable.contains("hashes"), "hashes is declared mutable and must stay above the dedup");
    // Mutable by construction, no declaration needed: `stamp_version` rewrites the
    // tiebreak on every append and a delete appends a row differing only in the tombstone.
    assert!(mutable.contains("updated_at"), "the version tiebreak differs across versions");
    assert!(mutable.contains("deleted"), "the tombstone differs across versions");

    // Everything else is immutable and therefore leg-safe.
    for column in [
        "context___trace_id",
        "context___span_id",
        "kind",
        "parent_id",
        "timestamp",
        "id",
        "project_id",
        "date",
        "name",
        "level",
        "duration",
        "status_code",
        "status_message",
    ] {
        assert!(!mutable.contains(column), "{column} is immutable and its filter must be pushable below the dedup");
    }
}

/// Maintenance must not be hostage to the boot cache preload.
///
/// The gate is set ONLY when every table has been replayed; it is abandoned
/// and left unset if shutdown arrives first. The coordinator, the tantivy
/// reconcile and the coverage recovery all gate on it, so a preload that
/// never finishes would silently disable all three for the life of the
/// container.
///
/// Time is paused, so this asserts the BOUND rather than sleeping for it.
#[tokio::test(start_paused = true)]
async fn maintenance_starts_even_if_the_cache_preload_never_completes() -> Result<()> {
    let db = Database::with_config(test_config_with("preload-wait", |cfg| cfg.maintenance.timefusion_coordinator_preload_wait_secs = 300)).await?;
    // Deliberately never mark the replay complete.
    let cancel = CancellationToken::new();
    let started = tokio::time::Instant::now();
    assert!(db.wait_for_preload(&cancel).await, "a slow preload must release maintenance, not disable it");
    let waited = started.elapsed();
    assert!(waited >= std::time::Duration::from_secs(300), "it must still yield to the warm for the configured budget, waited {waited:?}");
    assert!(waited < std::time::Duration::from_secs(360), "and must not wait appreciably longer, waited {waited:?}");

    // Cancellation is the ONLY reason to abandon a worker.
    let cancel = CancellationToken::new();
    cancel.cancel();
    let db2 = Database::with_config(create_test_config("preload-cancel")).await?;
    assert!(!db2.wait_for_preload(&cancel).await, "a cancelled worker must still stop");
    Ok(())
}

/// The gate is the REPLAY phase, not the paced body warm.
///
/// The wait exists to protect the unpaced phase — resolving each table's Delta log so
/// maintenance is not the first cold loader of a table the foreground needs.
#[tokio::test(start_paused = true)]
async fn maintenance_waits_for_the_replay_phase_not_the_body_warm() -> Result<()> {
    let db = Database::with_config(test_config_with("preload-replay", |cfg| cfg.maintenance.timefusion_coordinator_preload_wait_secs = 300)).await?;
    let cancel = CancellationToken::new();
    db.preload_tables_total.store(2, std::sync::atomic::Ordering::Relaxed);

    // A partially-replayed registry is still a wait: one table resolved
    // says nothing about the other.
    db.mark_table_replayed();
    let started = tokio::time::Instant::now();
    assert!(db.wait_for_preload(&cancel).await);
    assert!(started.elapsed() >= std::time::Duration::from_secs(300), "one of two tables replayed must not release the gate");

    // Both replayed releases immediately, even with the paced body warm still running.
    db.mark_table_replayed();
    let started = tokio::time::Instant::now();
    assert!(db.wait_for_preload(&cancel).await);
    assert!(started.elapsed() < std::time::Duration::from_secs(1), "the replayed registry must release the gate without waiting on the warm");
    Ok(())
}

/// An untagged tier file must not be immortal.
///
/// A replace-set matching on slice tags alone can never remove a file that lost them
/// (a delta-rs OPTIMIZE keeps only its own `sort_by` tag), so every rebuild stacks
/// another version beside it. A day-wide publish reproduces the whole partition and
/// must retire them.
#[tokio::test]
async fn a_day_wide_publish_retires_an_untagged_tier_file() -> Result<()> {
    use crate::maintenance_coordinator::{DAY_MICROS, MAX_DECODED_BYTES, Operation, TaskKey, TimeSlice};
    let db = Database::with_config(rollup_backfill_config("untagged-immortal", 35)).await?;
    let project = format!("untag_{}", uuid::Uuid::new_v4().simple());
    let day_start = midnight_micros((Utc::now() - chrono::Duration::days(3)).date_naive());
    let noon = day_start + DAY_MICROS / 2;
    for i in 0..3 {
        insert_a_span(&db, &project, &format!("s{i}"), noon + i).await?;
    }

    // Publish at DAY width, what `coarsen_sealed_slices` produces for a sealed day.
    let day_unit = |db: &Database| -> Result<TaskKey> {
        let base =
            retire_all_tasks(db).into_iter().find(|key| key.operation == Operation::BaseRollup).expect("the write path queues a base rollup for the day");
        let key = TaskKey { slice: TimeSlice::new(day_start, day_start + DAY_MICROS)?, ..base };
        db.maintenance_tasks.lock().unwrap().enqueue(key.clone(), 0, MAX_DECODED_BYTES, 0);
        Ok(key)
    };
    let key = day_unit(&db)?;
    assert!(db.run_maintenance_units(1024).await? > 0, "the day-wide base unit must run");
    let tier_total = async |db: &Database| -> Result<i64> {
        let sql = format!("SELECT COALESCE(SUM(request_count), 0)::BIGINT FROM {} WHERE project_id = '{project}'", key.physical_table);
        Ok(db
            .query_delta_only(&sql)
            .await?
            .iter()
            .filter(|batch| batch.num_rows() > 0)
            .find_map(|batch| batch.column(0).as_any().downcast_ref::<arrow::array::Int64Array>().map(|column| column.value(0)))
            .unwrap_or(0))
    };
    assert_eq!(tier_total(&db).await?, 3, "the first publish must count each span once");

    let tier_ref = db.get_or_create_table(&project, &key.physical_table).await?;
    strip_slice_tags(&tier_ref, "untagged", false).await?;
    // Count FILES, not summed measures: reads collapse versions, so a read-side sum
    // reports the right answer over a partition still carrying the damage.
    let live_files = async |db: &Database| -> Result<usize> { Ok(live_tier_files(db, &project, &key.physical_table).await?.0) };
    assert_eq!(live_files(&db).await?, 2, "precondition: the untagged copy is live alongside the tagged one");
    assert_eq!(tier_total(&db).await?, 3, "read-time dedup must already hide the duplicate from queries");

    // A late row makes the day genuinely re-eligible, which is how a rebuild reaches
    // a damaged partition.
    insert_a_span(&db, &project, "s3", noon + 3).await?;
    day_unit(&db)?;
    assert!(db.run_maintenance_units(1024).await? > 0, "the rebuild must run");
    assert_eq!(live_files(&db).await?, 1, "a day-wide rebuild must RETIRE the untagged file, not stack a version beside it");
    assert_eq!(tier_total(&db).await?, 4, "and the rebuilt partition counts each span once");
    Ok(())
}

/// A hygiene task for a partition the scan proved compliant must be retired, and a partition
/// the scan never saw must not be.
///
/// File hygiene is stateless work stated as a durable queue. A partition absent from the snapshot
/// is unknown, not clean; retiring its work would silently drop compaction.
#[tokio::test]
async fn compliant_partitions_retire_their_hygiene_tasks_but_unseen_ones_do_not() -> Result<()> {
    use crate::maintenance_coordinator::{Operation, TaskKey, TaskState, TimeSlice};
    let db = Database::with_config(create_test_config("retire-compliant")).await?;
    let project = format!("retire_{}", uuid::Uuid::new_v4().simple());
    let day = Utc::now() - chrono::Duration::days(3);
    insert_a_span(&db, &project, "a", day.timestamp_micros()).await?;

    let task_for = |d: chrono::DateTime<Utc>, operation| -> Result<TaskKey> {
        let start = midnight_micros(d.date_naive());
        Ok(TaskKey {
            physical_table: "otel_logs_and_spans".to_owned(),
            source: "otel_logs_and_spans".to_owned(),
            project_id: project.clone(),
            slice: TimeSlice::new(start, start + DAY_MICROS)?,
            operation,
        })
    };
    // The staleness shape: a HotPacking task minted while the day was TODAY, on a day
    // that has since sealed, so the scan now plans only SealedConsolidation for it.
    let seen_key = task_for(day, Operation::HotPacking)?;
    // And a day with no data at all, which the scan never sees.
    let unseen_key = task_for(Utc::now() - chrono::Duration::days(400), Operation::SealedConsolidation)?;
    {
        let mut journal = db.maintenance_tasks.lock().unwrap();
        journal.enqueue(seen_key.clone(), 0, 1, 0);
        journal.enqueue(unseen_key.clone(), 0, 1, 0);
    }

    db.plan_compaction_debt().await?;

    let (seen_state, unseen_state) = {
        let journal = db.maintenance_tasks.lock().unwrap();
        (journal.state(&seen_key), journal.state(&unseen_key))
    };
    assert_eq!(seen_state, Some(TaskState::Complete), "a partition the scan proved compliant retires its stale task");
    assert_ne!(unseen_state, Some(TaskState::Complete), "a partition the scan never saw is unknown, not clean, and keeps its task");
    Ok(())
}

/// The reweighting must move slots to the rollup chain without starving anything. Each
/// operation keeps a slot so file debt cannot drop to zero and let counts run away.
#[test]
fn the_coverage_short_cycle_favours_rollup_without_starving_file_work() {
    use crate::maintenance_coordinator::{CYCLE_BALANCED, CYCLE_COVERAGE_SHORT, Operation};
    let count = |cycle: &[Operation; 10], op: Operation| cycle.iter().filter(|candidate| **candidate == op).count();
    let chain = |cycle: &[Operation; 10]| count(cycle, Operation::BaseRollup) + count(cycle, Operation::DerivedRollup);

    assert_eq!(chain(&CYCLE_BALANCED), 4, "precondition: the balanced cycle gives the rollup chain 4 of 10");
    assert_eq!(chain(&CYCLE_COVERAGE_SHORT), 6, "the coverage-short cycle must give the rollup chain 6 of 10");
    // Nothing starves.
    for op in [Operation::Dedup, Operation::HotPacking, Operation::SealedConsolidation, Operation::Repair] {
        assert!(count(&CYCLE_COVERAGE_SHORT, op) >= 1, "{op:?} must keep at least one slot");
    }
    assert_eq!(CYCLE_BALANCED.len(), CYCLE_COVERAGE_SHORT.len(), "same length, so the rotating cursor behaves identically");
}

/// The reweighting is self-limiting: it must switch off once coverage reaches
/// the target, or it becomes a permanent tax on file hygiene.
#[test]
fn the_reweighting_switches_off_once_coverage_is_healthy() {
    use std::sync::atomic::Ordering::Relaxed;
    let gauge = &crate::observability::maintenance_stats().rollup_median_contiguous_days;
    let restore = gauge.load(Relaxed);
    gauge.store(0, Relaxed);
    assert!(coverage_is_short(), "zero contiguous days is short");
    gauge.store(COVERAGE_SHORT_DAYS - 1, Relaxed);
    assert!(coverage_is_short(), "one day below target is still short");
    gauge.store(COVERAGE_SHORT_DAYS, Relaxed);
    assert!(!coverage_is_short(), "at the target the balanced cycle returns");
    gauge.store(restore, Relaxed);
}

/// Coverage is intersected across projects, so a dormant tenant that wrote nothing in
/// the window would otherwise refuse the query for everyone.
///
/// The window `[100, 200)` is half-open. The sentinel case is load-bearing: a
/// partition with NO timestamp statistics must be treated as OVERLAPPING, since
/// dropping a project on a missing statistic is the direction that undercounts.
#[test_case::test_case(0, 50 => false ; "wholly before the window")]
#[test_case::test_case(300, 400 => false ; "wholly after the window")]
#[test_case::test_case(200, 250 => false ; "half-open: a row exactly at hi is outside")]
#[test_case::test_case(100, 100 => true ; "half-open: a row exactly at lo is in")]
#[test_case::test_case(50, 150 => true ; "straddling the low edge")]
#[test_case::test_case(150, 250 => true ; "straddling the high edge")]
#[test_case::test_case(0, 1000 => true ; "strictly containing the window")]
#[test_case::test_case(i64::MAX, i64::MIN => true ; "the no-statistics sentinel range must overlap")]
fn a_partition_outside_the_window_does_not_join_the_coverage_requirement(min_ts: i64, max_ts: i64) -> bool {
    PartitionStats { fingerprint: 0, min_ts, max_ts, rows: 0, bytes: 0 }.overlaps(100, 200)
}

/// The safety argument for cross-project routing: a range one project has not
/// covered must NOT be read from the rollup, or its rows are silently absent.
#[test_case::test_case(&[(0, 100)], &[(0, 50)] => vec![(0, 50)] ; "A covered the whole day, B only its first half")]
#[test_case::test_case(&[(0, 40)], &[(60, 100)] => Vec::<(i64, i64)>::new() ; "disjoint coverage yields nothing — everything goes to the raw leg")]
#[test_case::test_case(&[(0, 50)], &[(50, 100)] => Vec::<(i64, i64)>::new() ; "touching but not overlapping is still nothing: the ranges are half-open")]
#[test_case::test_case(&[(0, 100)], &[(0, 30), (70, 100)] => vec![(0, 30), (70, 100)] ; "a hole in one side splits the other's single range in two")]
#[test_case::test_case(&[(10, 20), (30, 40)], &[(10, 20), (30, 40)] => vec![(10, 20), (30, 40)] ; "identical multi-range coverage is preserved")]
#[test_case::test_case(&[(0, 100)], &[(10, 20), (30, 40), (90, 200)] => vec![(10, 20), (30, 40), (90, 100)] ; "neither side advances past a range the other still overlaps")]
fn a_range_only_one_project_covers_is_not_in_the_intersection(left: &[(i64, i64)], right: &[(i64, i64)]) -> Vec<(i64, i64)> {
    intersect_ranges(left, right)
}

#[test]
fn tantivy_backfill_prioritizes_recent_partitions() {
    let mut uris = ["date=2024-01-01/old", "date=2026-08-16/new-b", "date=2025-06-10/middle", "date=2026-08-16/new-a"]
        .map(|p| format!("project_id=p/{p}.parquet"))
        .to_vec();
    super::sort_backfill_uris_newest_first(&mut uris);

    assert!(uris[0].contains("date=2026-08-16") && uris[1].contains("date=2026-08-16"));
    assert!(uris[2].contains("date=2025-06-10"));
    assert!(uris[3].contains("date=2024-01-01"));
}

/// The hot-tail skip: today's partition leaves the backfill queue, every other date
/// stays. Today's files are rewritten within hours and already covered at birth by
/// the flush callback and the inline-reindex paths.
#[test_case::test_case(None => (3, 0) ; "disabled keeps the whole queue")]
#[test_case::test_case(Some("date=2026-08-22") => (2, 1) ; "today is dropped")]
#[test_case::test_case(Some("date=2026-08-2") => (0, 3) ; "a prefix marker is the caller's error, not silently ignored")]
#[test_case::test_case(Some("date=2026-01-01") => (3, 0) ; "a date with no files drops nothing")]
fn tantivy_backfill_drops_the_hot_partition(marker: Option<&str>) -> (usize, u64) {
    let mut uris = ["date=2026-08-22/a.parquet", "date=2026-08-21/b.parquet", "date=2026-08-20/c.parquet"].map(String::from).to_vec();
    let dropped = super::drop_hot_partition(&mut uris, marker);
    (uris.len(), dropped)
}

/// The tail must be REACHABLE, which pure newest-first plus a cap makes impossible.
/// Here the hot partition is 30 files against a cap of 12.
#[test]
fn tantivy_backfill_reserves_a_share_of_the_pass_for_the_oldest() {
    let hot = (0..30).map(|i| (format!("rel/2026-08-22-{i:02}"), format!("uri/2026-08-22-{i:02}")));
    let cold = std::iter::once(("rel/2026-01-01".to_string(), "uri/2026-01-01".to_string()));
    let queues = || vec![("p".to_string(), hot.clone().chain(cold.clone()).collect::<VecDeque<_>>())];
    let uris = |w: Vec<super::TantivyBackfillWork>| w.into_iter().map(|(_, _, uri)| uri).collect::<Vec<_>>();

    let starved = uris(super::fair_tantivy_backfill_work_split(queues(), 12, 0).0);
    assert!(!starved.contains(&"uri/2026-01-01".to_string()), "pure newest-first must starve the tail — otherwise this test proves nothing");

    let (split_work, reserved) = super::fair_tantivy_backfill_work_split(queues(), 12, 33);
    let split = uris(split_work);
    assert_eq!(split.len(), 12, "the reservation is carved OUT of the cap, so pass cost is unchanged");
    assert!(split.contains(&"uri/2026-01-01".to_string()), "the oldest uncovered file must be reachable, got {split:?}");
    // The HEAD is the queue's front (`-00`); `-29` is its tail end, so asserting
    // on `-29` would be satisfied by the reservation rather than by the head.
    assert!(split.contains(&"uri/2026-08-22-00".to_string()), "the hot window must still converge");
    // Reaching the tail LAST is the same starvation one level down: a pass is
    // routinely killed part-way, so a reservation at the end never executes.
    let oldest_at = split.iter().position(|u| u == "uri/2026-01-01").expect("present");
    assert_eq!(oldest_at, 0, "the reserved tail must be served FIRST, or a killed pass never reaches it; it was at {oldest_at} of {}", split.len());
    assert!(reserved.contains("uri/2026-01-01"), "the oldest file must be reported as coming from the reservation, got {reserved:?}");
    assert!(!reserved.contains("uri/2026-08-22-00"), "the hot-window head must not be labelled as reserved");
    assert_eq!(split.iter().collect::<HashSet<_>>().len(), split.len(), "a file must not be scheduled twice in one pass: {split:?}");
}

fn backfill_queues(projects: &[(&str, &[&str])]) -> Vec<(String, VecDeque<(String, String)>)> {
    projects.iter().map(|(project, items)| ((*project).to_owned(), items.iter().map(|item| (format!("rel/{item}"), format!("uri/{item}"))).collect())).collect()
}

fn scheduled_pairs(work: &[super::TantivyBackfillWork]) -> Vec<(&str, &str)> {
    work.iter().map(|(project, _, uri)| (project.as_str(), uri.as_str())).collect()
}

#[test]
fn tantivy_backfill_rotates_projects_before_their_history() {
    let work = super::fair_tantivy_backfill_work(backfill_queues(&[("whale", &["whale-new", "whale-mid", "whale-old"]), ("small", &["small-new"])]));

    assert_eq!(scheduled_pairs(&work), vec![("small", "uri/small-new"), ("whale", "uri/whale-new"), ("whale", "uri/whale-mid"), ("whale", "uri/whale-old")]);
}

/// Truncating a bounded pass must cut the OLDEST round, never one project's
/// whole queue — that is the property that lets the reconcile run hourly
/// without a big tenant's history starving everyone else's recent files.
#[test]
fn a_bounded_backfill_pass_truncates_the_oldest_round_not_one_project() {
    let mut work =
        super::fair_tantivy_backfill_work(backfill_queues(&[("whale", &["whale-new", "whale-mid", "whale-old"]), ("small", &["small-new", "small-old"])]));
    let deferred = work.len().saturating_sub(3);
    work.truncate(3);
    let scheduled = scheduled_pairs(&work);

    assert_eq!(deferred, 2, "the cap must report what it left behind, not swallow it");
    // Virtual time 0 is every project's newest; the third slot goes to the larger
    // backlog (whale 3 files against small's 2), which is the weighting.
    assert_eq!(scheduled, vec![("small", "uri/small-new"), ("whale", "uri/whale-new"), ("whale", "uri/whale-mid")]);
    assert!(scheduled.iter().any(|(p, _)| *p == "small"), "the smaller project keeps its slot under a cap");
    assert!(!scheduled.iter().all(|(p, _)| *p == "whale"), "weighting must never take a project's whole slice — every project's newest file is virtual time 0");
}

/// One unified table holding most of the corpus beside a dozen small projects: equal
/// round-robin would give the largest backlog only 1/12 of every pass.
#[test]
fn tantivy_backfill_weights_each_project_by_its_backlog() {
    let queue = |project: &str, n: usize| {
        (project.to_string(), (0..n).map(|i| (format!("rel/{project}-{i:03}"), format!("uri/{project}-{i:03}"))).collect::<VecDeque<_>>())
    };
    let mut queues = vec![queue("unified", 200)];
    queues.extend((0..11).map(|i| queue(&format!("small{i:02}"), 17)));

    let pass: Vec<_> = super::fair_tantivy_backfill_work(queues).into_iter().take(48).collect();
    let unified = pass.iter().filter(|(project, _, _)| project == "unified").count();

    // 200 of 387 files is 52% of the backlog; an equal split would hand it 4.
    assert!(unified >= 24, "the largest backlog must get a proportional slice, got {unified} of 48");
    // Every project's newest file is virtual time 0, so all twelve appear in one pass.
    for i in 0..11 {
        let project = format!("small{i:02}");
        assert!(pass.iter().any(|(p, _, uri)| *p == project && uri.ends_with("-000")), "{project} lost its newest file to the weighting");
    }
}

/// The merge-on-read gate: `keep_greatest_ordering` yields the lead sort key
/// only when the table declares a `dedup_tiebreak` AND that key is a dedup
/// key of an i64-backed type. Without a tiebreak it must yield `None`, leaving a
/// non-merge-on-read table's plan unchanged (no leg sort, no merge).
#[test]
fn keep_greatest_ordering_requires_a_tiebreak() {
    let otel = get_schema("otel_logs_and_spans").expect("registered");
    let schema = otel.schema_ref();
    let ord = ProjectRoutingTable::keep_greatest_ordering(otel, &schema).expect("otel declares a tiebreak + timestamp-led sort");
    assert_eq!(ord.to_string(), "timestamp@0 DESC", "one column only — all `detect_bound` reads, and the cheapest leg sort");

    let mut no_tiebreak = otel.clone();
    no_tiebreak.dedup_tiebreak = None;
    assert!(ProjectRoutingTable::keep_greatest_ordering(&no_tiebreak, &schema).is_none(), "no tiebreak ⇒ no plan change at all");

    // A sort key that isn't a dedup key breaks `detect_bound`'s contract
    // (equal keys would no longer share the bound value), so: no ordering.
    let mut unkeyed = otel.clone();
    unkeyed.dedup_keys = vec!["id".into()];
    assert!(ProjectRoutingTable::keep_greatest_ordering(&unkeyed, &schema).is_none(), "lead sort key must itself be a dedup key");
}

/// A predicate on the tombstone marker must never be handed to a scan leg —
/// applied at the source it drops the tombstone before the dedup and the
/// stale live version wins (silent resurrection).
#[test]
fn tombstone_predicates_are_never_pushed_down() {
    let deleted = col("deleted").eq(lit(true));
    assert!(ProjectRoutingTable::references_tombstone("mor_versioned", &deleted));
    assert!(!ProjectRoutingTable::references_tombstone("mor_versioned", &col("id").eq(lit("x"))));
    for t in ["otel_logs_and_spans", "otel_metrics"] {
        assert!(ProjectRoutingTable::references_tombstone(t, &deleted), "{t} ships merge-on-read — its tombstone predicate must not reach a leg");
    }
    // Tables declaring no tombstone column have no such predicate to protect
    // — there `deleted` is just an unknown column name.
    for t in ["variant_bench", "mor_dormant"] {
        assert!(!ProjectRoutingTable::references_tombstone(t, &deleted));
    }
}

/// Query-session sizing: decode buffers cost `batch_size × row width` per partition
/// per concurrent query and are invisible to the memory pool, so the bound must come
/// from config; and a background rewrite must plan with `MAINTENANCE_MAX_PARTITIONS`,
/// not the CPU quota.
#[tokio::test]
async fn query_session_sizing() -> Result<()> {
    let config = create_test_config("query-session-sizing");
    let query_partitions = config.memory.timefusion_query_partitions;
    let db = Database::with_config(config).await?;
    let exec_of = |db: Database| Arc::new(db).create_session_context().state().config().options().execution.clone();
    assert_eq!(exec_of(db.clone()).batch_size, 2048, "wide otel rows: one 8192-row batch measured 63MB");

    let query = exec_of(db.clone()).target_partitions;
    let mut maintenance = db.clone();
    maintenance.maintenance_scan = true;
    let scan = exec_of(maintenance).target_partitions;
    assert_eq!(scan, super::MAINTENANCE_MAX_PARTITIONS, "a background rewrite must plan with maintenance parallelism");
    // `timefusion_query_partitions` may be 0 in a test config, meaning DataFusion's
    // own core-count default. Either way the maintenance scan must be strictly smaller.
    assert!(scan < query, "the whole point is fewer concurrent decoders: {scan} vs {query} (configured {query_partitions})");
    Ok(())
}

/// A non-yielding maintenance poll must not prevent foreground work from being
/// scheduled — maintenance runs on its own runtime, not PGWire's workers.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_cpu_work_cannot_starve_the_foreground_runtime() -> Result<()> {
    let db = Database::with_config(create_test_config("maintenance-runtime-isolation")).await?.start_maintenance_schedulers().await?;
    let executor = db.maintenance_executor.get().expect("scheduler installs the isolated runtime").clone();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    executor.spawn(async move {
        let _ = started_tx.send(());
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(400);
        let mut value = 1_u64;
        while std::time::Instant::now() < deadline {
            value = std::hint::black_box(value.wrapping_mul(6364136223846793005).wrapping_add(1));
        }
    });
    started_rx.await.expect("maintenance hog started");

    tokio::time::timeout(std::time::Duration::from_millis(150), async {
        for _ in 0..5 {
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("foreground executor was starved by maintenance");
    db.cancel_maintenance();
    Ok(())
}

/// Reconciliation runs after preload and must be metadata-only over the handles
/// preload published: cold-resolving a source replays a large Delta log ahead of
/// foreground ingest.
#[tokio::test]
async fn maintenance_reconciliation_never_cold_loads_a_source() -> Result<()> {
    let db = Database::with_config(create_test_config("reconcile-cached-tables-only")).await?;

    assert!(db.unified_tables.read().await.is_empty());
    assert_eq!(db.reconcile_maintenance_task_cursors().await?, 0);
    assert!(db.unified_tables.read().await.is_empty(), "reconciliation must not resolve a source table");
    Ok(())
}

/// The first cursor is a baseline, not a request to turn the full retained history
/// into urgent startup work.
#[tokio::test]
async fn maintenance_reconciliation_baselines_a_new_source_cursor() -> Result<()> {
    let db = Database::with_config(create_test_config("reconcile-first-cursor-baseline")).await?;
    let table = db.get_or_create_unified_table("otel_logs_and_spans").await?;
    let version = table.read().await.version().unwrap_or_default();
    let cursor_key = ":otel_logs_and_spans";
    assert_eq!(db.maintenance_tasks.lock().unwrap().source_cursor(cursor_key), None);

    assert_eq!(db.reconcile_maintenance_task_cursors().await?, 0);
    assert_eq!(db.maintenance_tasks.lock().unwrap().source_cursor(cursor_key), Some(version));
    Ok(())
}

/// Retained Delta history may start after a durable maintenance cursor.
/// Reconstruct conservative work from the live metadata before advancing,
/// rather than failing every restart on the same expired commit.
#[test_case::test_case(true; "partition metadata without directory labels")]
#[test_case::test_case(false; "malformed source leaves healthy sources running")]
#[tokio::test]
async fn maintenance_reconciliation_recovers_expired_history(valid_partition_metadata: bool) -> Result<()> {
    use object_store::ObjectStoreExt;
    let db = Database::with_config(create_test_config("reconcile-expired-history")).await?;
    let source = "otel_logs_and_spans";
    let table = db.get_or_create_unified_table(source).await?;
    db.reconcile_maintenance_task_cursors().await?;
    // A malformed source must not prevent a healthy source establishing its cursor.
    let healthy = db.get_or_create_unified_table("otel_metrics").await?;
    let healthy_version = healthy.read().await.version().unwrap();
    let project = format!("recon_{}", uuid::Uuid::new_v4().simple());
    let ts = (Utc::now() - chrono::Duration::days(2)).timestamp_micros();
    insert_a_span(&db, &project, "a", ts).await?;
    let (version, store) = {
        let table = table.read().await;
        (table.version().unwrap(), table.log_store().object_store(None))
    };
    // An output-only partition models a missed DELETE; its deliberately unreadable
    // parquet proves this recovery uses metadata, not row scans.
    let removed_project = format!("{project}_removed");
    let date = chrono::DateTime::from_timestamp_micros(ts).unwrap().date_naive().to_string();
    let target_name = crate::schema::get_schema(source).unwrap().rollups[0].table_name(source);
    let target_ref = db.get_or_create_unified_table(&target_name).await?;
    {
        let mut target = target_ref.read().await.clone();
        let mut actions = Vec::new();
        if !valid_partition_metadata {
            use deltalake::kernel::MetadataExt;
            // A legacy layout can store date in the rows rather than partition metadata.
            let metadata = deltalake::kernel::Metadata::try_new(
                None,
                None,
                target.snapshot()?.schema(),
                vec!["project_id".into()],
                Utc::now().timestamp_millis(),
                target.snapshot()?.metadata().configuration().clone(),
            )?
            .with_table_id(target.snapshot()?.metadata().id().to_owned())?;
            actions.push(deltalake::kernel::Action::Metadata(metadata));
        }
        let mut add = deltalake::kernel::Add {
            path: "metadata-only.parquet".into(),
            partition_values: HashMap::from([("project_id".into(), Some(removed_project.clone())), ("date".into(), Some(date))]),
            size: 1,
            data_change: false,
            ..Default::default()
        };
        if !valid_partition_metadata {
            add.partition_values.remove("date");
        }
        actions.push(deltalake::kernel::Action::Add(add));
        let op = deltalake::protocol::DeltaOperation::Write { mode: deltalake::protocol::SaveMode::Append, partition_by: None, predicate: None };
        let committed = deltalake::kernel::transaction::CommitBuilder::default()
            .with_actions(actions)
            .build(Some(target.snapshot()? as &dyn deltalake::kernel::transaction::TableReference), target.log_store(), op)
            .await?;
        target.state = Some(committed.snapshot());
        *target_ref.write().await = target;
    }
    store.delete(&object_store::path::Path::from(format!("_delta_log/{version:020}.json"))).await?;
    let queued = db.reconcile_maintenance_task_cursors().await?;
    assert_eq!(db.journal().source_cursor(":otel_metrics"), Some(healthy_version));
    if !valid_partition_metadata {
        assert_eq!(queued, 0);
        assert_eq!(db.journal().source_cursor(":otel_logs_and_spans"), Some(0), "an unknown partition must never be skipped past");
        return Ok(());
    }
    assert_eq!(queued, 2 * (1 + crate::schema::get_schema(source).unwrap().rollups.len()), "one dedup and every rollup tier for both partitions");
    let day_start = midnight_micros(chrono::DateTime::from_timestamp_micros(ts).unwrap().date_naive());
    {
        let journal = crate::maintenance_coordinator::TaskJournal::load(&db.config.core.timefusion_data_dir)?;
        assert_eq!(journal.source_cursor(":otel_logs_and_spans"), Some(version));
        for project_id in [&project, &removed_project] {
            let coarse = journal
                .tasks()
                .filter(|task| {
                    &task.key.project_id == project_id && task.key.slice.start_micros == day_start && task.key.slice.end_micros == day_start + 86_400_000_000
                })
                .count();
            assert_eq!(
                coarse,
                1 + crate::schema::get_schema(source).unwrap().rollups.len(),
                "durable dedup and one task per tier, including output-only partitions"
            );
        }
    }
    assert_eq!(db.reconcile_maintenance_task_cursors().await?, 0, "recovery is durable and idempotent");
    Ok(())
}

/// `(base tiers, derived tiers)` DECLARED for `otel_logs_and_spans`. Task-count
/// expectations derive from this rather than a literal, so adding a tier is not a
/// spurious failure.
fn declared_rollup_tiers() -> (usize, usize) {
    let rollups = &crate::schema::get_schema("otel_logs_and_spans").expect("schema").rollups;
    let derived = rollups.iter().filter(|spec| spec.derive_from.is_some()).count();
    (rollups.len() - derived, derived)
}

/// Reconciling a missed commit must invalidate only the hours the commit's files
/// actually span, never the whole partition-day. This test's commit touches one hour.
#[tokio::test]
async fn reconcile_enqueues_only_the_hours_a_missed_commit_touched() -> Result<()> {
    let db = Database::with_config(create_test_config("reconcile-precise-hours")).await?;
    // The reconcile inspects cached handles only, so the table must exist
    // before the cursor baselines.
    db.get_or_create_unified_table("otel_logs_and_spans").await?;
    assert_eq!(db.reconcile_maintenance_task_cursors().await?, 0, "first reconcile baselines the cursor");

    let project = format!("recon_{}", uuid::Uuid::new_v4().simple());
    let ts = (Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    let day = chrono::DateTime::from_timestamp_micros(ts).unwrap().date_naive();
    let day_start = midnight_micros(day);
    let hour_start = ts.div_euclid(3_600_000_000) * 3_600_000_000;
    for id in ["a", "b"] {
        insert_a_span(&db, &project, id, ts).await?;
    }
    let queued = db.reconcile_maintenance_task_cursors().await?;

    let journal = db.maintenance_tasks.lock().unwrap();
    let tasks = journal.tasks().filter(|task| task.key.project_id == project).collect::<Vec<_>>();
    let (base, derived) = declared_rollup_tiers();
    assert_eq!(tasks.len(), 6 + 6 * base + derived, "one touched hour: 6 dedup + 6 per base tier + 1 per derived tier, not 312 for the whole day");
    assert!(
        tasks.iter().all(|task| task.key.slice.start_micros >= hour_start && task.key.slice.end_micros <= hour_start + 3_600_000_000),
        "every reconciled task must lie inside the touched hour; day {day} starts {day_start}"
    );
    assert_eq!(queued, base + derived, "one dirty hour x one enqueue per declared rollup spec");
    Ok(())
}

/// A self-authored DV-dedup commit must re-mint NEITHER Dedup NOR Rollup.
///
/// DV-dedup carries `data_change=true` (it masks rows), so without the tag reconcile
/// re-mints work from dedup's own output on every restart. The rebuild is redundant:
/// a DV wave moves neither the partition stats fingerprint nor `rollup_source_epochs`,
/// and the base build already reads its raw input deduped on the same keys. Untagged
/// (ingest) commits still mint both — the control half below.
#[tokio::test]
async fn reconcile_skips_dedup_and_rollup_remint_for_tagged_dv_dedup_commits() -> Result<()> {
    use crate::maintenance_coordinator::{Operation, TaskState};
    let db = Database::with_config(create_test_config("reconcile-dv-dedup-skip")).await?;
    let table_ref = db.get_or_create_unified_table("otel_logs_and_spans").await?;
    assert_eq!(db.reconcile_maintenance_task_cursors().await?, 0, "first reconcile baselines the cursor");

    let project = format!("dvskip_{}", uuid::Uuid::new_v4().simple());
    // Sealed (>2h old) so the public dedup_partition clears the sealed-chunk
    // guard; same-day so one date partition holds both files.
    let ts = (Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    // Cross-commit duplicate: identical dedup key flushed twice.
    insert_a_span(&db, &project, "dup", ts).await?;
    insert_a_span(&db, &project, "dup", ts).await?;

    // CONTROL: ingest commits are untagged, so reconcile mints Dedup.
    db.reconcile_maintenance_task_cursors().await?;
    let project_keys: Vec<_> = {
        let journal = db.maintenance_tasks.lock().unwrap();
        journal.tasks().filter(|t| t.key.project_id == project).map(|t| t.key.clone()).collect()
    };
    assert!(project_keys.iter().any(|k| k.operation == Operation::Dedup), "untagged ingest commits must still mint Dedup");
    assert!(
        project_keys.iter().any(|k| matches!(k.operation, Operation::BaseRollup | Operation::DerivedRollup)),
        "untagged ingest commits must still mint Rollup — the control the DV skip is measured against"
    );

    // Mark everything Complete: the state a restart must not undo.
    {
        let mut journal = db.maintenance_tasks.lock().unwrap();
        for key in &project_keys {
            journal.complete(key);
        }
    }

    // The DV-dedup pass masks the loser in place and commits tagged.
    let date = chrono::DateTime::from_timestamp_micros(ts).unwrap().date_naive();
    let (dropped, _) = db.dedup_partition(&table_ref, "otel_logs_and_spans", &project, date).await?;
    assert_eq!(dropped, 1, "the DV pass must mask exactly the cross-file duplicate");

    // Tag round-trip through the real Delta log.
    let (version, log_store) = {
        let table = table_ref.read().await;
        (table.version().unwrap_or_default(), table.log_store())
    };
    let bytes = log_store.read_commit_entry(version).await?.expect("dv-dedup commit entry");
    let commit_info = deltalake::logstore::get_actions(version, &bytes)?.iter().find_map(|a| match a {
        deltalake::kernel::Action::CommitInfo(ci) => Some(ci.info.clone()),
        _ => None,
    });
    let info = commit_info.expect("wave commit has commitInfo");
    assert_eq!(info.get(DV_DEDUP_COMMIT_KEY).and_then(serde_json::Value::as_bool), Some(true), "the DV-dedup wave commit must carry {DV_DEDUP_COMMIT_KEY}");
    // `with_metadata` REPLACES the map, so the lane must survive the DV tag's call.
    assert_eq!(
        info.get(super::LANE_COMMIT_KEY).and_then(serde_json::Value::as_str),
        Some("wave_commit"),
        "the lane attribution must ride the same with_metadata call as the DV tag"
    );

    db.reconcile_maintenance_task_cursors().await?;
    let journal = db.maintenance_tasks.lock().unwrap();
    let repended_for = |ops: &[Operation]| {
        journal
            .tasks()
            .filter(|t| t.key.project_id == project && ops.contains(&t.key.operation) && t.state != TaskState::Complete)
            .map(|t| t.key.clone())
            .collect::<Vec<_>>()
    };
    let repended = repended_for(&[Operation::Dedup]);
    assert!(repended.is_empty(), "a self-authored DV-dedup commit re-pended Complete Dedup slices (the floor mechanism): {repended:?}");
    let rollup_repended = repended_for(&[Operation::BaseRollup, Operation::DerivedRollup]);
    assert!(
        rollup_repended.is_empty(),
        "a self-authored DV-dedup commit re-pended Complete Rollup slices (the continuous rebuild tax) — \
             but the base build already reads raw deduped and coverage identities do not move, so the rebuild is redundant: {rollup_repended:?}"
    );
    Ok(())
}

/// One write, one durability barrier — no matter how many partitions it touches.
///
/// The maintenance journal must be `fsync`ed before a write is acknowledged, but that
/// barrier must not be paid once per (project, date) in the batch. Asserts the COST,
/// not just the outcome: a per-partition version produces the same journal.
#[tokio::test]
async fn a_multi_partition_invalidation_costs_one_journal_commit() -> Result<()> {
    use datafusion::arrow::{
        array::TimestampMicrosecondArray,
        datatypes::{DataType, Field, Schema, TimeUnit},
    };

    let db = Database::with_config(create_test_config("journal-group-commit-batches")).await?;
    const DAY: i64 = 86_400_000_000;
    // One inbound batch straddling three dates.
    let day0 = chrono::NaiveDate::from_ymd_opt(2026, 8, 16).unwrap().and_hms_opt(9, 0, 0).unwrap().and_utc().timestamp_micros();
    let days = [day0, day0 + DAY, day0 + 2 * DAY];
    let schema = Arc::new(Schema::new(vec![Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(TimestampMicrosecondArray::from(days.to_vec()).with_timezone("UTC"))])?;

    let (before, _) = db.journal_group_commit.counts();
    db.invalidate_rollup_batches("customer-a", "otel_logs_and_spans", std::slice::from_ref(&batch))?;
    let (after, _) = db.journal_group_commit.counts();

    assert_eq!(after - before, 1, "{} dates in one write must share ONE journal commit, not one each", days.len());
    // ...and every date is actually in it: the cheap version must not be cheap by
    // doing less work.
    let journal = db.maintenance_tasks.lock().unwrap();
    for day in days {
        let day_start = day - day.rem_euclid(DAY);
        assert!(
            journal.tasks().any(|task| task.key.project_id == "customer-a" && task.key.slice.overlaps(day_start, day_start + DAY)),
            "the shared commit dropped the partition at {day_start}"
        );
    }
    Ok(())
}

/// A tenant's first write must not manufacture a full day of empty and future
/// maintenance debt. File hygiene is planned by debt in `plan_compaction_debt` (one
/// day-wide unit per project, only for partitions with small or unsorted files),
/// never per slice.
#[tokio::test]
async fn first_rollup_invalidation_enqueues_only_the_touched_hour() -> Result<()> {
    use crate::maintenance_coordinator::Operation;

    let db = Database::with_config(create_test_config("first-rollup-invalidation-is-sparse")).await?;
    db.apply_rollup_hours("customer-a", "otel_logs_and_spans", "2026-08-16", 1 << 7)?;
    db.commit_journal()?;

    let journal = db.maintenance_tasks.lock().unwrap();
    let counts = journal.tasks().counts_by(|task| task.key.operation);
    let (base, derived) = declared_rollup_tiers();
    assert_eq!(counts.get(&Operation::Dedup), Some(&6));
    assert_eq!(counts.get(&Operation::BaseRollup), Some(&(6 * base)));
    assert_eq!(counts.get(&Operation::DerivedRollup), Some(&derived));
    assert_eq!(counts.get(&Operation::HotPacking), None, "ingest must not mint file-hygiene work; the debt planner owns it");
    assert_eq!(journal.tasks().count(), 6 + 6 * base + derived, "one touched hour, not 456 full-day tasks");
    Ok(())
}

/// An empty `values` slice models an action carrying no `partitionValues` (older
/// writers omit them on Remove), so the file path must still be sufficient.
#[test_case::test_case(&[("project_id", "customer-a"), ("date", "2026-08-16")], "ignored.parquet", "default" => Some(("customer-a".to_owned(), "2026-08-16".to_owned())) ; "partition values are authoritative over the path")]
#[test_case::test_case(&[], "project_id=customer-b/date=2026-08-15/part-000.parquet", "default" => Some(("customer-b".to_owned(), "2026-08-15".to_owned())) ; "a Remove without partitionValues falls back to the path")]
#[test_case::test_case(&[], "date=2026-08-14/part-000.parquet", "default" => Some(("default".to_owned(), "2026-08-14".to_owned())) ; "a path with no project takes the default project")]
#[test_case::test_case(&[], "part-000.parquet", "default" => None ; "an unpartitioned path yields no partition")]
#[test_case::test_case(&[], "date=2026-08-14/part-000.parquet", "customer-c" => Some(("customer-c".to_owned(), "2026-08-14".to_owned())) ; "default_project is a custom table's storage project, not the literal default")]
fn maintenance_reconciliation_extracts_only_the_changed_partition(values: &[(&str, &str)], path: &str, default_project: &str) -> Option<(String, String)> {
    let values: HashMap<String, Option<String>> = values.iter().map(|(key, value)| ((*key).to_owned(), Some((*value).to_owned()))).collect();
    Database::maintenance_partition_from_action(path, (!values.is_empty()).then_some(&values), default_project)
}

/// Today is swept on every tick; only the sealed tail rotates.
#[test]
fn sweep_rotation_never_rotates_today_out() {
    // [today_a, today_b, sealed0, sealed1, sealed2, sealed3]
    let sealed_from = 2;
    for cursor in 0..12 {
        let mut work = vec![100, 101, 0, 1, 2, 3];
        super::rotate_sealed_tail(&mut work, sealed_from, cursor);
        assert_eq!(&work[..2], &[100, 101], "today must stay at the front at cursor {cursor}");
        let mut tail = work[2..].to_vec();
        tail.sort_unstable();
        assert_eq!(tail, vec![0, 1, 2, 3], "rotation must preserve the sealed set at cursor {cursor}");
        assert_eq!(work[2], cursor % 4, "the sealed tail must resume at the cursor");
    }
    // Degenerate shapes must not panic.
    let mut only_today = vec![1, 2];
    super::rotate_sealed_tail(&mut only_today, 5, 3);
    assert_eq!(only_today, vec![1, 2]);
    let mut empty: Vec<i32> = vec![];
    super::rotate_sealed_tail(&mut empty, 0, 7);
    assert!(empty.is_empty());
}

/// The CPU ceiling is MEASURED, not pinned at `cores/3`.
///
/// Prod 2026-09-19 sat at 10 of 10 cpu tokens with ~2,500 units eligible, 3 of 4
/// rewrite permits idle and the box at half its CPU limit — the static
/// reservation was costing throughput it did not need to. It may now rise when
/// the runtime is not starved, and must fall back to exactly the old value when
/// it is, so this can never admit LESS than before.
#[test]
fn the_cpu_ceiling_follows_runtime_starvation() {
    use crate::maintenance_coordinator::lag_scaled_cpu_ceiling_for_test as ceiling;
    let (base, max) = (10u32, 24u32);
    assert_eq!(ceiling(base, max, 0), max, "an idle runtime gets the full ceiling");
    assert_eq!(ceiling(base, max, 25), max, "and still does at the full-cap threshold");
    assert_eq!(ceiling(base, max, 250), base, "a starved runtime falls back to the static reservation");
    assert_eq!(ceiling(base, max, 5_000), base, "and never below it, however bad the lag");
    let mid = ceiling(base, max, 137);
    assert!(mid > base && mid < max, "between the thresholds it interpolates, got {mid}");
    // A box whose max is not above its base is simply the old behaviour.
    assert_eq!(ceiling(10, 10, 0), 10);
}

/// Rollups keep a reserved share. They lose every race otherwise: compaction and
/// dedup arrive continuously, and prod ran 1,690 eligible base-rollup units with
/// `rollup_hits_full_total` at ZERO.
#[test]
fn rollups_keep_a_reserved_share_of_admission() {
    use crate::maintenance_coordinator::{AdmissionController, AdmissionLane, MAX_DECODED_BYTES, Resources, rollup_reserved_cpu};
    // Derive the expectation from the constant: pinning a literal here made this
    // test fail for the reservation being RETUNED, which is not a regression.
    let ceiling = 24;
    let admission = AdmissionController::with_cpu_ceiling(ceiling, ceiling, u64::MAX, 64, 64);
    let request = Resources { cpu: 1, decoded_bytes: MAX_DECODED_BYTES / 8, object_reads: 1, object_writes: 1 };
    // Non-rollup work fills everything EXCEPT the reservation.
    let held: Vec<_> = std::iter::repeat_with(|| admission.try_acquire_for(request, AdmissionLane::Other, crate::config::MemorySnapshot::unknown()))
        .map_while(|p| p)
        .collect();
    assert_eq!(held.len() as u32, ceiling - rollup_reserved_cpu(ceiling), "other lanes must stop short of the rollup reservation, took {}", held.len());
    assert!(admission.try_acquire_for(request, AdmissionLane::Other, crate::config::MemorySnapshot::unknown()).is_none(), "and stay stopped");
    // The reserved slots are still there for a rollup.
    assert!(
        admission.try_acquire_for(request, AdmissionLane::Rollup, crate::config::MemorySnapshot::unknown()).is_some(),
        "a rollup must reach its reserved share"
    );
}

/// Maintenance admission is bounded by the configured job count, and every token is returned
/// when a job finishes.
#[tokio::test]
async fn database_bounds_concurrent_maintenance_jobs() -> Result<()> {
    use crate::maintenance_coordinator::{MAX_DECODED_BYTES, Resources};

    let db = Database::with_config(create_test_config("bounded-maintenance-jobs")).await?;
    let jobs = db.config.derived.coordinator_jobs();

    let request = Resources { cpu: 1, decoded_bytes: MAX_DECODED_BYTES, object_reads: 1, object_writes: 1 };

    // `coordinator_jobs` is the FLOOR of the ceiling, not its cap: the ceiling is
    // lag-scaled, so an idle runtime may exceed the thread count to cover I/O wait
    // and a starved one falls back to exactly `jobs`. Asserting an exact count made
    // this test pass or fail on how busy the runner happened to be. The invariants
    // that actually matter are that admission is BOUNDED and that every token comes
    // back.
    const RUNAWAY: usize = 512;
    let mut permits = Vec::new();
    while permits.len() < RUNAWAY {
        match db.maintenance_admission.try_acquire(request) {
            Some(permit) => permits.push(permit),
            None => break,
        }
    }
    assert!(permits.len() >= jobs, "admission must reach the configured job count, got {} of {jobs}", permits.len());
    assert!(permits.len() < RUNAWAY, "admission must be bounded, not run unbounded");
    drop(permits);
    assert!(db.maintenance_admission.try_acquire(request).is_some(), "dropping the jobs returns every admission token");
    Ok(())
}

/// A maintenance scan must be charged to the maintenance pool, not the query pool.
#[tokio::test]
async fn a_maintenance_scan_is_charged_to_the_maintenance_pool() -> Result<()> {
    let db = Database::with_config(create_test_config("maintenance-scan-pool")).await?;
    let pool_of = |db: Database| Arc::new(db).create_session_context().task_ctx().runtime_env().memory_pool.clone();

    let mut maintenance = db.clone();
    maintenance.maintenance_scan = true;
    let (query_pool, maintenance_pool) = (pool_of(db.clone()), pool_of(maintenance));
    assert!(!Arc::ptr_eq(&query_pool, &maintenance_pool), "a background rewrite must not draw on the query pool");
    assert!(Arc::ptr_eq(&maintenance_pool, &db.maintenance_runtime_env().memory_pool), "it must draw on the maintenance pool");
    Ok(())
}

/// The byte cap does not bind — parquet checks it against `get_estimated_total_bytes()`, which
/// under-reports the finished row group — so the derived ROW cap is what bounds row groups, and
/// it must land far below parquet's own default. A row group is also the indivisible unit of a
/// scan, so a MEASURED row width must override the per-type model, which is far off for wide rows.
#[test]
fn row_group_row_count_binds_far_below_the_parquet_default_and_by_a_measured_row_width() {
    const PARQUET_DEFAULT: usize = 1024 * 1024;
    const TARGET: usize = 128 * 1024 * 1024;
    let otel = crate::schema::get_schema("otel_logs_and_spans").expect("otel schema");
    let rows = super::row_group_row_count(otel, TARGET, None);
    assert!(rows < PARQUET_DEFAULT / 4, "otel row groups must shrink at least 4x, got {rows}");
    // The BYTE target binds, not the row floor; the floor only protects
    // genuinely narrow tables, where the byte target allows far more rows.
    let modelled_row_bytes = TARGET / rows;
    assert!(rows * modelled_row_bytes <= TARGET, "the group must not exceed the byte target, got {rows} rows");
    let narrow = crate::schema::get_schema("otel_metrics").expect("metrics schema");
    assert!(super::row_group_row_count(narrow, TARGET, None) >= 32_768, "a narrow table still gets the floor");

    // Monotonic in the byte target, and never above the parquet default.
    assert!(super::row_group_row_count(otel, 256 * 1024 * 1024, None) > rows);
    assert_eq!(super::row_group_row_count(otel, usize::MAX / 2, None), PARQUET_DEFAULT);

    // A wide real-world width: 63 KB decoded per row.
    let measured = super::row_group_row_count(otel, TARGET, Some(63_000));
    assert!(measured < rows, "a wider-than-modelled row must yield FEWER rows per group: {measured} vs {rows}");
    assert!(measured as u64 * 63_000 <= TARGET as u64 * 2, "and the group must land near the byte target, not 6x over it");
    // A narrow row is allowed more rows, still bounded by the floor/ceiling.
    assert!(super::row_group_row_count(otel, TARGET, Some(100)) >= rows, "a narrow row must not be penalised by the model");
    // No measurement keeps the old behaviour exactly.
    assert_eq!(super::row_group_row_count(otel, TARGET, Some(0)), rows, "an unusable measurement falls back to the model");
}

/// The repair sort runs 16 partitions, so its unspillable merge is 8x the 2-partition
/// maintenance default; reserving that share up-front forces the sorter to spill instead of
/// consuming the pool and stranding the merge.
#[test]
fn repair_session_reserves_merge_memory_up_front_over_the_shared_default() {
    let env = Arc::new(datafusion::execution::runtime_env::RuntimeEnv::default());
    let repair = build_optimize_session_state_tuned(
        0,
        Arc::clone(&env),
        Some("256"),
        Some(UncappedSort { partitions: REPAIR_SORT_PARTITIONS, reservation_bytes: Some(REPAIR_SORT_RESERVATION_BYTES) }),
    );
    let exec = &repair.config().options().execution;
    assert_eq!(exec.sort_spill_reservation_bytes, REPAIR_SORT_RESERVATION_BYTES);
    assert_eq!(exec.target_partitions, REPAIR_SORT_PARTITIONS, "repair runs one bin at a time, so it is not capped at 2");

    // The reservation must scale WITH the partition count, or raising
    // partitions silently re-inflates the unspillable merge share.
    assert!(
        exec.sort_spill_reservation_bytes * exec.target_partitions >= 4 * 1024 * 1024 * 1024,
        "16 partitions need >=4 GB reserved; measured merge was 9.5 GB"
    );

    // Ordinary maintenance runs many concurrent bins, where a large
    // per-partition reservation would exhaust the bounded pool.
    let exec_of = |cpus| build_optimize_session_state(cpus, Arc::clone(&env)).config().options().execution.clone();
    let plain = exec_of(0);
    assert_eq!(plain.sort_spill_reservation_bytes, 33_554_432);
    assert_eq!(plain.batch_size, 2048, "merge memory ≈ fan-in × batch; 8192-row otel batches measured up to 145MB");
    // Parallelism capped so per-partition spill reservations fit the bounded pool.
    assert_eq!(plain.target_partitions, 2, "0 (all cores) must cap to the maintenance limit");
    assert_eq!(exec_of(64).target_partitions, 2);
}

/// Packing sorts at more than `MAINTENANCE_MAX_PARTITIONS`, but must NOT
/// take repair's inflated per-partition reservation with it: the reservation is
/// UNSPILLABLE and per-partition, so raised partitions plus repair's 256 MB would
/// reserve more of packing's pool than the bins themselves use.
#[tokio::test]
async fn packing_sorts_wider_than_the_heavy_cap_but_keeps_the_default_reservation() -> Result<()> {
    let db = Database::with_config(create_test_config("pack-partitions")).await?;
    let exec = db.light_optimize_session_state().config().options().execution.clone();
    assert_eq!(exec.sort_spill_reservation_bytes, 33_554_432, "repair's reservation must not leak onto the packing path");
    assert_eq!(exec.target_partitions, db.pack_sort_partitions(), "the session must use the derived width, not the shared cap");
    // Reservations must stay a small fraction of the pool the bins fit in.
    let reserved = exec.sort_spill_reservation_bytes * exec.target_partitions * db.config.derived.max_light_optimize_k().max(1);
    assert!(reserved * 4 <= db.pack_pool_bytes().max(4), "reservations claim {reserved} of a {} byte pack pool", db.pack_pool_bytes());
    Ok(())
}

/// A large box lifts the heavy cap; a small box falls back to it, because the
/// per-partition reservation is unspillable and would eat the pool before any row sorts.
#[test]
fn pack_sort_width_follows_the_pool_and_the_box_not_a_pinned_constant() {
    const GIB: usize = 1024 * 1024 * 1024;
    // 48 cores, 7.5 GB pool, k=3: memory affords 20, CPU (48/2/3) binds at 8.
    assert_eq!(super::pack_sort_partitions(7 * GIB + GIB / 2, 3, 48), 8);
    assert_eq!(super::pack_sort_partitions(341 * 1024 * 1024, 1, 8), MAINTENANCE_MAX_PARTITIONS, "a small pool must fall back to the cap, never below it");
    assert_eq!(super::pack_sort_partitions(64 * GIB, 1, 8), 4, "cores bound the width even when memory is abundant");

    // Neither budget may be exceeded, across two orders of magnitude of box.
    for (pool_gib, k, cores) in [(1usize, 1usize, 4usize), (2, 2, 8), (8, 3, 16), (16, 3, 48), (64, 4, 96)] {
        let parts = super::pack_sort_partitions(pool_gib * GIB, k, cores);
        assert!(parts >= MAINTENANCE_MAX_PARTITIONS, "the derivation must only ever lift the cap");
        let at_cap = parts == MAINTENANCE_MAX_PARTITIONS;
        assert!(at_cap || parts * k * 2 <= cores, "packing may take at most half the box across its {k} bins");
        assert!(at_cap || parts * k * 4 * 33_554_432 <= pool_gib * GIB, "reservations may take at most a quarter of the pack pool");
    }
}

/// A day the FINE tier already has but the COARSE tier does not must still
/// be queued — and queued for the coarse tier ALONE, because the derived tier reads
/// A tier partition the READ PATH cannot serve must count as missing.
///
/// The planner called a day covered because the tier held a partition directory
/// for it; the reader needs a coverage record. A cell whose record an
/// invalidation destroyed was invisible to one and useless to the other, so it
/// was minted into no queue at all — which is how an idle coordinator and a 2.4%
/// rollup hit rate were both true at once on 2026-09-22, with 40-49% of every
/// tier's partitions in that state.
#[test]
fn a_tier_partition_without_usable_coverage_counts_as_missing() {
    let day = |n: u32| chrono::NaiveDate::from_ymd_opt(2026, 8, n).expect("date");
    let candidates: Vec<(String, chrono::NaiveDate)> = (14..=16).map(|d| ("p".to_owned(), day(d))).collect();
    let partitions: HashSet<(String, chrono::NaiveDate)> = (14..=16).map(|d| ("p".to_owned(), day(d))).collect();
    // The tier holds all three days, but only 08-16 has a coverage record.
    let readable: HashSet<(String, String)> = std::iter::once(("p".to_owned(), day(16).to_string())).collect();

    let missing = tiers_missing_per_day(&candidates, &[(0usize, readable_cells_only(&partitions, &readable))]);
    assert_eq!(
        missing.keys().map(|(_, date)| *date).sorted().collect::<Vec<_>>(),
        vec![day(14), day(15)],
        "a partition with no coverage record is a HOLE: the reader goes raw over it, so the planner must queue it"
    );
}

/// the base tier and needs no raw source scan.
#[test]
fn a_day_missing_only_the_coarse_tier_is_queued_for_that_tier_alone() {
    let day = |n: u32| chrono::NaiveDate::from_ymd_opt(2026, 8, n).expect("date");
    let candidates: Vec<(String, chrono::NaiveDate)> = (14..=16).map(|d| ("p".to_owned(), day(d))).collect();
    let set = |days: &[u32]| days.iter().map(|d| ("p".to_owned(), day(*d))).collect::<HashSet<_>>();
    // Tier 0 (fine) has all three days; tier 1 (coarse) has only 08-16.
    let covered = vec![(0usize, set(&[14, 15, 16])), (1usize, set(&[16]))];

    let missing = tiers_missing_per_day(&candidates, &covered);
    assert_eq!(missing.get(&("p".to_owned(), day(16))), None, "a day both tiers have needs nothing");
    assert_eq!(
        missing.get(&("p".to_owned(), day(15))).map(Vec::as_slice),
        Some([1usize].as_slice()),
        "a day the fine tier has but the coarse tier lacks must be queued for the COARSE tier only — \
             queueing the fine tier too re-reads the whole raw partition to rebuild a rollup that exists"
    );
    assert_eq!(missing.get(&("p".to_owned(), day(14))).map(Vec::as_slice), Some([1usize].as_slice()));
    assert_eq!(missing.len(), 2, "only the days actually missing a tier");
}

// The gauge reports the CONTIGUOUS run back from yesterday, dragged down by the worst
// project — but a day the SOURCE never held counts as answered, else it is unreachable
// by construction.
#[test_case(&[("a", 16), ("a", 15), ("a", 14)], &["a"], None, 17, &["a"] => 3 ; "an unbroken run back from yesterday")]
#[test_case(&[("a", 16), ("a", 13), ("a", 12), ("a", 11), ("a", 10)], &["a"], None, 17, &["a"] => 1 ; "days stranded behind a hole do not count")]
#[test_case(&[("a", 17)], &["a"], None, 17, &["a"] => 0 ; "yesterday missing is zero coverage however complete today is")]
#[test_case(&[("a", 16), ("a", 15), ("b", 16)], &["a", "b"], None, 17, &["a", "b"] => 1 ; "the worst project is the number that matters")]
#[test_case(&[], &["a"], None, 17, &["a"] => 0 ; "nothing covered is zero")]
#[test_case(&[("a", 16), ("a", 15), ("dormant", 3)], &["a", "dormant"], None, 17, &["a"] => 2 ; "a project that no longer ingests must not pin the fleet number at zero")]
#[test_case(&[("q", 14), ("q", 16), ("q", 18)], &[], Some(&[("q", 14), ("q", 16), ("q", 18)]), 18, &["q"]
        => CONTIGUITY_HORIZON_DAYS ; "17 and 15 had no rows to roll up, and neither did anything older — fully answered")]
#[test_case(&[("q", 17), ("q", 15)], &[], Some(&[("q", 17), ("q", 16), ("q", 15)]), 18, &["q"] => 1 ; "16 has rows and no tier — a genuine gap")]
#[test_case(&[], &[], Some(&[]), 18, &["silent"] => CONTIGUITY_HORIZON_DAYS ; "a project that emitted nothing is fully answered")]
// All dates are in 2026-08. `dense_for` names the projects whose SOURCE held rows on EVERY day of
// the window, so an absent day in `covered` is a genuine hole rather than a day with nothing to
// roll up; `source_days` instead pins the exact days the source held.
fn contiguous_coverage_ignores_days_stranded_behind_a_hole(
    covered: &[(&str, u32)], dense_for: &[&str], source_days: Option<&[(&str, u32)]>, today: u32, active: &[&str],
) -> u64 {
    fn day(n: u32) -> chrono::NaiveDate {
        chrono::NaiveDate::from_ymd_opt(2026, 8, n).expect("date")
    }
    fn day_set(pairs: &[(&str, u32)]) -> HashSet<(String, chrono::NaiveDate)> {
        pairs.iter().map(|(p, d)| ((*p).to_owned(), day(*d))).collect()
    }
    let dense: HashSet<_> = dense_for.iter().flat_map(|p| (1u32..=31).map(move |d| ((*p).to_owned(), day(d)))).collect();
    let source = source_days.map_or(dense, day_set);
    let active: HashSet<&str> = active.iter().copied().collect();
    min_contiguous_days(&day_set(covered), &source, day(today), &active).0
}

/// GatedScanExec must release its permit BETWEEN batches: a per-stream hold
/// deadlocks any consumer that needs a batch from every input partition at
/// once (SortPreservingMerge) when permits < partitions.
#[tokio::test(flavor = "multi_thread")]
async fn gated_scan_exec_releases_permit_between_batches_no_deadlock() {
    use arrow::array::Int32Array;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    let schema = Arc::new(ArrowSchema::new(vec![Field::new("v", DataType::Int32, false)]));
    let partitions: Vec<Vec<RecordBatch>> =
        (0..8).map(|i| vec![RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![i]))]).unwrap()]).collect();
    let src: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(&partitions, schema.clone(), None).unwrap())));
    let gated = Arc::new(GatedScanExec::new(src, Arc::new(tokio::sync::Semaphore::new(2)), None, false, 2));
    let ctx = Arc::new(TaskContext::default());
    let mut streams: Vec<_> = (0..8).map(|p| gated.execute(p, ctx.clone()).unwrap()).collect();
    let firsts = tokio::time::timeout(std::time::Duration::from_secs(10), futures::future::join_all(streams.iter_mut().map(futures::StreamExt::next)))
        .await
        .expect("GatedScanExec deadlocked — per-batch permit release regressed");
    let mut vals: Vec<i32> = firsts.into_iter().map(|b| b.unwrap().unwrap().column(0).as_any().downcast_ref::<Int32Array>().unwrap().value(0)).collect();
    vals.sort();
    assert_eq!(vals, (0..8).collect::<Vec<_>>(), "every gated partition must yield its row");
}

/// Parquet decode is outside every DataFusion pool; `GatedScanExec`'s permit window is the
/// decode window, so it accounts decoded Arrow bytes there. Accounting only — it must never
/// refuse a batch.
#[tokio::test(flavor = "multi_thread")]
async fn gated_scan_exec_accounts_decoded_bytes() {
    use arrow::array::Int32Array;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use std::sync::atomic::Ordering::Relaxed;
    crate::observability::init_local_metrics_for_test();
    let schema = Arc::new(ArrowSchema::new(vec![Field::new("v", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from((0..512).collect::<Vec<i32>>()))]).unwrap();
    let want = batch.get_array_memory_size() as u64;
    let partitions = vec![vec![batch.clone(), batch.clone()]];
    let src: Arc<dyn ExecutionPlan> = Arc::new(DataSourceExec::new(Arc::new(MemorySourceConfig::try_new(&partitions, schema, None).unwrap())));

    let metrics = Arc::new(ScanMetrics::default());
    let gated = Arc::new(GatedScanExec::new(src, Arc::new(tokio::sync::Semaphore::new(4)), Some(metrics.clone()), false, 4));
    let mut stream = gated.execute(0, Arc::new(TaskContext::default())).unwrap();
    let mut rows = 0;
    while let Some(b) = futures::StreamExt::next(&mut stream).await {
        rows += b.unwrap().num_rows();
    }

    assert_eq!(rows, 1024, "gating must not drop batches");
    assert_eq!(crate::observability::counter_value(scan_metric_names::DECODE_BYTES_TOTAL), want * 2, "both decoded batches must be accounted");
    assert_eq!(metrics.decode.decode_peak_batch_bytes.load(Relaxed), want);
    assert_eq!(metrics.decode.decode_polls_inflight.load(Relaxed), 0, "in-flight gauge must return to zero");
    assert_eq!(metrics.decode.decode_polls_inflight_peak.load(Relaxed), 1, "one partition polled serially = peak 1");
}

/// The decode pressure valve. ABSOLUTE backstop tiers: full concurrency until 88% of the
/// cgroup limit, quarter pool to 95%, fully serialized past that — claims never exceed the
/// pool and never drop to zero. RATE-based tiers additionally throttle a box rushing at the
/// wall from well below the backstop, while leaving one parked at a steady level alone.
const CALM: u64 = u64::MAX; // not projected to reach the limit at all
#[test_case(0, CALM, 16 => 1 ; "well under the backstop")]
#[test_case(87, CALM, 16 => 1 ; "just under the 88% backstop")]
#[test_case(88, CALM, 16 => 4 ; "88% backstop trips to quarter pool")]
#[test_case(94, CALM, 16 => 4 ; "still quarter pool just under 95%")]
#[test_case(95, CALM, 16 => 16 ; "95% backstop fully serializes")]
#[test_case(200, CALM, 16 => 16 ; "over 100% still fully serializes")]
#[test_case(88, CALM, 2 => 1 ; "tiny pools floor at 1")]
#[test_case(95, CALM, 1 => 1 ; "single-permit pool floors at 1")]
#[test_case(70, u64::MAX, 16 => 1 ; "a steady working set parked at 70% must never be throttled")]
#[test_case(18, 65, 16 => 1 ; "a cold process filling fast from near-empty must not engage the valve")]
#[test_case(49, 10, 16 => 1 ; "below the floor, even a tiny projection is not evidence")]
#[test_case(70, 60, 16 => 4 ; "a burst projected to hit the limit within a minute engages the valve well before 88%")]
#[test_case(70, 10, 16 => 16 ; "seconds left at only 70% used still serializes")]
#[test_case(96, u64::MAX, 16 => 16 ; "a burst too fast to project still trips on level alone")]
fn pressure_permit_claim_tiers(usage_pct: u64, eta_secs: u64, total: u32) -> u32 {
    pressure_permit_claim_at(usage_pct, eta_secs, total)
}

/// The hot tier's own reads must not read as memory pressure: only `anon` + kernel is
/// unreclaimable, and the kernel drops clean file cache rather than kill the process.
#[test]
fn clean_page_cache_is_not_charged_as_memory_pressure() {
    const PROD: &str = "anon 54533181440\nfile 35626905600\nkernel 1233899520\nfile_mapped 526012416\n\
                            file_dirty 99794944\nfile_writeback 0\ninactive_file 14998753280\nactive_file 20628152320\n";
    let current: usize = 91_423_989_760;
    let limit: usize = 85_899_345_920;

    let reclaimable = super::reclaimable_file_bytes(PROD).expect("`file` is present");
    assert_eq!(reclaimable, 35_626_905_600 - 99_794_944, "dirty pages stay charged; every other file page does not");

    let charged = current - reclaimable;
    assert_eq!(charged * 100 / limit, 65, "prod read 88% while only 65% of the budget was unreclaimable");
    assert!(charged.abs_diff(54_533_181_440 + 1_233_899_520) < 200_000_000, "charged must track anon + kernel, not the page cache");

    assert_eq!(super::reclaimable_file_bytes("anon 1\n"), None, "no `file` line means charge everything");
}

/// spawn_cron_job must fire on the wall-clock schedule and stop firing once the
/// maintenance cancel token is triggered.
#[tokio::test(flavor = "multi_thread")]
async fn spawn_cron_job_fires_on_schedule_then_stops_on_cancel() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let count = Arc::new(AtomicUsize::new(0));
    let cancel = Arc::new(CancellationToken::new());
    {
        let count = count.clone();
        // "* * * * * *" = every second (6-field, seconds).
        spawn_cron_job("test", "* * * * * *", cancel.clone(), move || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
            }
        });
    }
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    let fired = count.load(Ordering::SeqCst);
    assert!(fired >= 2, "every-second cron should fire >=2x in 2.5s, got {fired}");

    cancel.cancel();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let after_cancel = count.load(Ordering::SeqCst);
    tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
    assert_eq!(count.load(Ordering::SeqCst), after_cancel, "no fires after cancel");
}

#[tokio::test(flavor = "multi_thread")]
async fn spawn_cron_job_on_runs_body_on_the_selected_runtime() {
    let isolated = tokio::runtime::Builder::new_multi_thread().worker_threads(1).thread_name("cron-isolated").enable_all().build().expect("isolated runtime");
    let cancel = Arc::new(CancellationToken::new());
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    spawn_cron_job_on("isolated-test", "* * * * * *", cancel.clone(), Some(isolated.handle().clone()), move || {
        let tx = tx.clone();
        async move {
            let name = std::thread::current().name().unwrap_or("unnamed").to_owned();
            let _ = tx.send(name);
        }
    });

    let thread_name = tokio::time::timeout(std::time::Duration::from_millis(2500), rx.recv()).await.expect("cron fired").expect("sender alive");
    cancel.cancel();
    assert!(thread_name.starts_with("cron-isolated"), "job ran on {thread_name}, not the isolated executor");
    isolated.shutdown_background();
}

/// A wedged job body must not freeze the cron loop: later ticks are skipped
/// (not queued) and the skip counter grows, so the schedule survives and the
/// wedge is visible in `timefusion_stats`. Runs alone: `cron_ticks_skipped` is a
/// process-wide counter, so a second cron job would make the skips unattributable.
#[tokio::test(flavor = "multi_thread")]
async fn spawn_cron_job_skips_ticks_while_previous_run_hangs() {
    use std::sync::atomic::Ordering::Relaxed;
    let cancel = Arc::new(CancellationToken::new());
    let skipped_before = crate::observability::maintenance_stats().cron_ticks_skipped.load(Relaxed);
    spawn_cron_job("hung-test", "* * * * * *", cancel.clone(), move || async move {
        std::future::pending::<()>().await; // never returns
    });
    // Generous window: needs >=2 wall-clock second boundaries on a loaded runner.
    tokio::time::sleep(std::time::Duration::from_millis(5200)).await;
    cancel.cancel();
    let skipped = crate::observability::maintenance_stats().cron_ticks_skipped.load(Relaxed) - skipped_before;
    assert!(skipped >= 1, "later ticks must be skipped (loop alive) while the first run hangs, got {skipped} skips");
}

/// A slow-but-healthy job must be allowed to complete across multiple skipped ticks.
#[tokio::test(flavor = "multi_thread")]
async fn spawn_cron_job_lets_slow_runs_finish() {
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    let cancel = Arc::new(CancellationToken::new());
    let completed = Arc::new(AtomicUsize::new(0));
    spawn_cron_job("slow-test", "* * * * * *", cancel.clone(), {
        let completed = completed.clone();
        move || {
            let completed = completed.clone();
            async move {
                tokio::time::sleep(std::time::Duration::from_millis(4500)).await;
                completed.fetch_add(1, Relaxed);
            }
        }
    });
    // 6.5s spans 6 one-second ticks; the first run must still finish at t≈4.5s.
    tokio::time::sleep(std::time::Duration::from_millis(6500)).await;
    cancel.cancel();
    assert_eq!(completed.load(Relaxed), 1, "slow-but-healthy run must complete, not be aborted");
}

// Warm (30-min Z-order) and cold (daily 512MB consolidate) tiers must own
// disjoint partitions, or they oscillate the same day 256MB↔512MB every cycle.
// `date_is_cold` is the single boundary both use.
#[test]
fn warm_and_cold_partition_ownership_is_disjoint() {
    use chrono::{Duration, NaiveDate};
    let today = NaiveDate::from_ymd_opt(2026, 6, 28).unwrap();

    // after_days = 1: only today is warm; yesterday and older are cold.
    assert!(!Database::date_is_cold(today, today, 1), "today must be warm (still taking writes)");
    assert!(Database::date_is_cold(today, today - Duration::days(1), 1), "yesterday must be cold");
    assert!(Database::date_is_cold(today, today - Duration::days(90), 1), "old backfill day must be cold");

    // No date is ever both tiers.
    for days_ago in 0..120 {
        let d = today - Duration::days(days_ago);
        let after = 3;
        let cold = Database::date_is_cold(today, d, after);
        let warm = !cold; // warm optimize processes exactly the complement
        assert_ne!(cold, warm, "a partition must be warm xor cold, never both");
        assert_eq!(cold, days_ago >= after as i64, "boundary off-by-one at days_ago={days_ago}");
    }
}

// Every retryable delta-rs conflict must classify as retryable, while permanent
// errors (protocol version, auth/IO) fail fast.
#[test_case("Delta transaction failed, version 58420 already exists." => true ; "VersionAlreadyExists retries")]
#[test_case("Commit failed: a concurrent transaction overlapped" => true ; "concurrent transaction overlap retries")]
#[test_case("concurrent transaction wrote to the same files" => true ; "concurrent write to the same files retries")]
#[test_case("Metadata changed since last commit." => true ; "MetadataChanged retries")]
#[test_case("Transaction failed: Error evaluating predicate" => true ; "predicate re-evaluation retries")]
#[test_case("Generic S3 error: Access Denied" => false ; "auth or IO fails fast")]
#[test_case("Unsupported reader version: requires 3, have 2" => false ; "reader protocol version fails fast")]
#[test_case("Unsupported writer version required" => false ; "writer protocol version fails fast")]
#[test_case("Arrow error: Invalid argument" => false ; "arrow error fails fast")]
fn is_occ_conflict_err_classifies_retryable_vs_permanent(msg: &str) -> bool {
    is_occ_conflict_err(msg)
}

// A single Arrow batch carrying rows for several projects must split ROW-WISE:
// routing on row 0 alone would put every row in the first row's project.
#[test]
fn test_partition_batch_by_project_row_wise() {
    use datafusion::arrow::{
        array::{ArrayRef, AsArray, Int64Array, StringArray, StringViewArray},
        datatypes::{DataType, Field, Int64Type, Schema},
    };

    let check = |pid_col: ArrayRef| {
        let schema = Arc::new(Schema::new(vec![Field::new("project_id", pid_col.data_type().clone(), true), Field::new("id", DataType::Int64, false)]));
        let ids = Int64Array::from(vec![1, 2, 3, 4]); // interleaved A/B/A + null→default
        let batch = RecordBatch::try_new(schema, vec![pid_col, Arc::new(ids)]).unwrap();

        // BTreeMap → deterministic sorted keys: A, B, default
        let parts = partition_batch_by_project(batch, "default").unwrap();
        let shape: Vec<(String, Vec<i64>)> = parts.iter().map(|(p, b)| (p.clone(), b.column(1).as_primitive::<Int64Type>().values().to_vec())).collect();
        assert_eq!(
            shape,
            vec![("A".into(), vec![1, 3]), ("B".into(), vec![2]), ("default".into(), vec![4])],
            "each project keeps exactly its own rows; null falls back to default"
        );
    };

    check(Arc::new(StringViewArray::from(vec![Some("A"), Some("B"), Some("A"), None])));
    check(Arc::new(StringArray::from(vec![Some("A"), Some("B"), Some("A"), None]))); // Utf8 path too

    // Homogeneous batch: single group, whole batch (no split).
    let schema = Arc::new(Schema::new(vec![Field::new("project_id", DataType::Utf8View, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringViewArray::from(vec!["A", "A", "A"]))]).unwrap();
    let parts = partition_batch_by_project(batch, "default").unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!((parts[0].0.as_str(), parts[0].1.num_rows()), ("A", 3));
}

const RECENCY_CUTOFF: Option<(i32, u32, u32)> = Some((2026, 6, 4));
// Only the `date=` partition decides recency; anything unclassifiable is warmed.
#[test_case("s3://b/t/date=2026-06-06/part-0.parquet", RECENCY_CUTOFF => true ; "after the cutoff is warm")]
#[test_case("s3://b/t/date=2026-06-04/part-0.parquet", RECENCY_CUTOFF => true ; "cutoff is inclusive")]
#[test_case("s3://b/t/date=2026-06-01/part-0.parquet", RECENCY_CUTOFF => false ; "older partitions are skipped")]
#[test_case("s3://b/t/part-0.parquet", RECENCY_CUTOFF => true ; "no date= segment is warm")]
#[test_case("s3://b/t/date=not-a-date/part-0.parquet", RECENCY_CUTOFF => true ; "unparseable date is warm")]
#[test_case("s3://b/t/date=2026-06", RECENCY_CUTOFF => true ; "truncated date shorter than YYYY-MM-DD is warm")]
#[test_case("s3://b/t/date=2000-01-01/part-0.parquet", None => true ; "no cutoff means no recency limit")]
#[test_case("s3://b/t/project_id=default/date=2026-05-01/part.parquet", RECENCY_CUTOFF => false ; "nested project_id partitioning still locates date=")]
fn test_within_recency(path: &str, cutoff: Option<(i32, u32, u32)>) -> bool {
    within_recency(path, cutoff.and_then(|(y, m, d)| chrono::NaiveDate::from_ymd_opt(y, m, d)))
}

/// Roundtrip the watermark through serialize → JSON → parse. Absent shards must stay
/// absent (not coerced to ORIGIN), so the per-shard MAX ignores commits that didn't
/// touch a shard.
#[test]
fn watermark_serialize_parse_roundtrip() {
    use walrus_rust::WalPosition;
    let wm = vec![Some(WalPosition { block_id: 7, offset: 1024 }), None, Some(WalPosition { block_id: 9, offset: 0 }), None];
    let json = serialize_watermark_to_json(&wm, "p", "t");
    let info = HashMap::from([(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(json))]);
    assert_eq!(parse_watermark_from_json(&info, wm.len(), "p", "t"), wm);
}

/// Landed-batch identities round-trip, are scoped to the topic that wrote
/// them (unified-table tenants share one Delta log, so an unscoped identity
/// would let one tenant's commit decline another's flush), and survive
/// sharing a commit with the watermark — `with_metadata` REPLACES the
/// metadata map, so building the two keys separately would drop one.
#[test]
fn landed_digests_roundtrip_and_stay_scoped_to_their_topic() {
    let (a, b) = ([7u8; crate::write::DIGEST_BYTES], [9u8; crate::write::DIGEST_BYTES]);
    let info: HashMap<String, serde_json::Value> = flush_commit_metadata(
        [("p".to_string(), "t".to_string(), vec![Some(walrus_rust::WalPosition { block_id: 5, offset: 64 })])],
        [("p".to_string(), "t".to_string(), a), ("p".to_string(), "t".to_string(), b), ("other".to_string(), "t".to_string(), a)],
    )
    .into_iter()
    .collect();

    assert_eq!(parse_landed_digests_from_json(&info, "p", "t"), vec![a, b]);
    assert_eq!(parse_landed_digests_from_json(&info, "other", "t"), vec![a]);
    assert!(parse_landed_digests_from_json(&info, "stranger", "t").is_empty(), "a topic that recorded nothing must match nothing");
    assert_eq!(parse_watermark_from_json(&info, 1, "p", "t"), vec![Some(walrus_rust::WalPosition { block_id: 5, offset: 64 })]);
}

/// A landed-batch identity may only DECLINE a write; it must never advance a WAL
/// cursor. A digest has no ordering meaning, so moving a cursor with one would skip
/// un-flushed entries sitting below a landed one (acked-write loss).
#[test]
fn landed_digests_never_advance_a_cursor() {
    let info: HashMap<String, serde_json::Value> =
        flush_commit_metadata([], [("p".to_string(), "t".to_string(), [3u8; crate::write::DIGEST_BYTES])]).into_iter().collect();

    assert_eq!(parse_landed_digests_from_json(&info, "p", "t"), vec![[3u8; crate::write::DIGEST_BYTES]], "the identity is recorded");
    assert_eq!(max_watermark_across_commits([&info], 4, "p", "t"), vec![None; 4], "and it advances NOTHING");
}

/// Orphaned `datafusion-*` spill dirs are reaped (a SIGKILL skips `DiskManager`'s Drop
/// cleanup); anything else in the spill dir is left alone.
#[test]
fn spill_reap_removes_only_datafusion_dirs() {
    let dir = tempfile::tempdir().unwrap();
    let orphan = dir.path().join("datafusion-AbCdEf");
    std::fs::create_dir_all(orphan.join("nested")).unwrap();
    std::fs::write(orphan.join("nested").join("0.arrow"), vec![7u8; 2048]).unwrap();
    let keep_dir = dir.path().join("something-else");
    std::fs::create_dir_all(&keep_dir).unwrap();
    let keep_file = dir.path().join("notes.txt");
    std::fs::write(&keep_file, b"keep").unwrap();

    assert_eq!(dir_size_bytes(&orphan), 2048);
    let orphans: Vec<_> =
        std::fs::read_dir(dir.path()).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().starts_with("datafusion-")).map(|e| e.path()).collect();
    // A dir created AFTER the snapshot (a live DiskManager's) must survive.
    let live = dir.path().join("datafusion-LiVe01");
    std::fs::create_dir_all(&live).unwrap();
    reap_orphaned_spill_dirs_blocking(dir.path(), orphans);

    assert!(!orphan.exists(), "orphaned spill dir survived");
    assert!(live.exists(), "reaper deleted a live (post-snapshot) spill dir");
    assert!(keep_dir.exists() && keep_file.exists(), "reaper touched unrelated entries");
    // Idempotent / tolerant of an empty or absent dir.
    reap_orphaned_spill_dirs_blocking(dir.path(), vec![]);
    reap_orphaned_spill_dirs_blocking(&dir.path().join("does-not-exist"), vec![]);
}

/// Watermark JSON format invariants. Positions are per-`topic:shard` walrus offsets and
/// NOT comparable across topics — applying a busy tenant's high block_id to a quiet
/// tenant's cursor skips that tenant's unreplayed WAL entries (acked-write loss).
#[test]
fn watermark_json_format_invariants() {
    use walrus_rust::WalPosition;

    // A commit's `commitInfo` carrying exactly one watermark object.
    let info_of = |map: serde_json::Map<String, serde_json::Value>| HashMap::from([(WAL_WATERMARK_KEY.to_string(), serde_json::Value::Object(map))]);

    // Topic scoping: only the topic that wrote a watermark may apply it, and a
    // legacy (pre-topic) commit contributes nothing rather than applying to everyone.
    let info = info_of(serialize_watermark_to_json(&vec![Some(WalPosition { block_id: 9_000, offset: 0 })], "busy_proj", "otel_logs_and_spans"));
    assert_eq!(parse_watermark_from_json(&info, 1, "busy_proj", "otel_logs_and_spans"), vec![Some(WalPosition { block_id: 9_000, offset: 0 })]);
    assert_eq!(parse_watermark_from_json(&info, 1, "quiet_proj", "otel_logs_and_spans"), vec![None], "co-tenant on the same log gets nothing");
    assert_eq!(parse_watermark_from_json(&info, 1, "busy_proj", "otel_metrics"), vec![None], "different table is a different topic");
    let old = info_of([("0".to_string(), serde_json::json!({ "block_id": 9_000, "offset": 0 }))].into_iter().collect());
    assert_eq!(parse_watermark_from_json(&old, 1, "any_proj", "otel_logs_and_spans"), vec![None], "topic-less legacy commit applies to no one");

    // All-None serializes to an empty object, so no metadata is written and recovery
    // skips the commit, the same path as pre-feature commits.
    let wm: crate::write::DeltaWatermark = vec![None, None, None];
    assert!(serialize_watermark_to_json(&wm, "p", "t").is_empty());
    assert!(parse_watermark_from_json(&info_of(serde_json::Map::new()), 3, "p", "t").iter().all(|p| p.is_none()));

    // Per-shard MAX across commits: a commit missing a shard contributes nothing and
    // must not reset the MAX.
    let mk_info = |entries: &[(usize, u64, u64)]| {
        let mut map: serde_json::Map<String, serde_json::Value> =
            entries.iter().map(|(s, b, o)| (s.to_string(), serde_json::json!({ "block_id": b, "offset": o }))).collect();
        map.insert(WATERMARK_TOPIC_KEY.to_string(), serde_json::Value::String(wal_topic("p", "t")));
        info_of(map)
    };
    let a = mk_info(&[(0, 5, 100), (1, 5, 50)]);
    let b = mk_info(&[(0, 6, 0)]); // past A on shard 0; nothing for shard 1
    let c: HashMap<String, serde_json::Value> = HashMap::new(); // replay-derived, no watermark key
    let d = mk_info(&[(1, 5, 30)]); // behind A on shard 1; must lose to A
    let max = max_watermark_across_commits([&a, &b, &c, &d], 3, "p", "t");
    assert_eq!(max[0], Some(WalPosition { block_id: 6, offset: 0 }));
    assert_eq!(max[1], Some(WalPosition { block_id: 5, offset: 50 }));
    assert_eq!(max[2], None, "shard 2 unwritten by all commits stays None");

    // Coalesced commits: ONE commit carries every included project's watermark, and each
    // project resumes from ITS OWN position; a project absent from the commit gets nothing.
    let t = "otel_logs_and_spans";
    let ca = vec![Some(WalPosition { block_id: 900, offset: 10 }), None];
    let cb = vec![None, Some(WalPosition { block_id: 4, offset: 7 })];
    let cc = vec![Some(WalPosition { block_id: 1, offset: 1 }), Some(WalPosition { block_id: 2, offset: 2 })];
    let coalesced = info_of(serialize_watermarks_to_json([
        ("proj_a".to_string(), t.to_string(), ca.clone()),
        ("proj_b".to_string(), t.to_string(), cb.clone()),
        ("proj_c".to_string(), t.to_string(), cc.clone()),
    ]));
    assert_eq!(parse_watermark_from_json(&coalesced, 2, "proj_a", t), ca, "proj_a resumes from its own position");
    assert_eq!(parse_watermark_from_json(&coalesced, 2, "proj_b", t), cb, "proj_b resumes from its own position");
    assert_eq!(parse_watermark_from_json(&coalesced, 2, "proj_c", t), cc, "proj_c resumes from its own position");
    assert_eq!(parse_watermark_from_json(&coalesced, 2, "proj_d", t), vec![None, None], "project absent from the commit gets nothing");
    assert_eq!(parse_watermark_from_json(&coalesced, 2, "proj_a", "otel_metrics"), vec![None, None]);
    let solo = info_of(serialize_watermark_to_json(&vec![Some(WalPosition { block_id: 901, offset: 0 }), None], "proj_a", t));
    // MAX across a coalesced + a per-project commit still resolves per project.
    assert_eq!(max_watermark_across_commits([&coalesced, &solo], 2, "proj_a", t), vec![Some(WalPosition { block_id: 901, offset: 0 }), None]);
    assert_eq!(max_watermark_across_commits([&coalesced, &solo], 2, "proj_b", t), cb, "proj_b unaffected by proj_a's later solo commit");

    // A one-project "coalesced" commit must serialize to the EXACT legacy flat shape,
    // so older binaries reading these commits see no format change.
    let single_wm = vec![Some(WalPosition { block_id: 3, offset: 4 }), None];
    assert_eq!(
        serialize_watermarks_to_json([("p".to_string(), "t".to_string(), single_wm.clone())]),
        serialize_watermark_to_json(&single_wm, "p", "t"),
        "single-topic coalesced commits must keep the flat legacy shape byte-for-byte"
    );
    assert!(serialize_watermarks_to_json([("p".to_string(), "t".to_string(), vec![None, None])]).is_empty());
    let one = serialize_watermarks_to_json([("p".to_string(), "t".to_string(), single_wm.clone()), ("q".to_string(), "t".to_string(), vec![None, None])]);
    assert_eq!(one, serialize_watermark_to_json(&single_wm, "p", "t"), "one populated topic of two collapses to the flat form");

    // Two units for the SAME topic in one commit must never silently drop one: the
    // survivor takes the per-shard MAX.
    let dup_info = info_of(serialize_watermarks_to_json([
        ("p".to_string(), "t".to_string(), vec![Some(WalPosition { block_id: 5, offset: 100 }), Some(WalPosition { block_id: 1, offset: 0 })]),
        ("p".to_string(), "t".to_string(), vec![Some(WalPosition { block_id: 5, offset: 40 }), Some(WalPosition { block_id: 9, offset: 0 })]),
        ("q".to_string(), "t".to_string(), vec![Some(WalPosition { block_id: 2, offset: 0 })]),
    ]));
    assert_eq!(
        parse_watermark_from_json(&dup_info, 2, "p", "t"),
        vec![Some(WalPosition { block_id: 5, offset: 100 }), Some(WalPosition { block_id: 9, offset: 0 })]
    );

    // A shard index out of range for this reader (a writer configured with more shards),
    // or a non-numeric key, is dropped silently rather than panicking on a config-skew restart.
    let skewed: HashMap<String, serde_json::Value> = HashMap::from([(
        WAL_WATERMARK_KEY.to_string(),
        serde_json::json!({
            "topic": "p:t",
            "0": {"block_id": 1, "offset": 10},
            "99": {"block_id": 1, "offset": 999},
            "garbage": {"block_id": 1, "offset": 0},
        }),
    )]);
    let parsed = parse_watermark_from_json(&skewed, 4, "p", "t");
    assert_eq!(parsed[0], Some(WalPosition { block_id: 1, offset: 10 }));
    assert!(parsed[1..].iter().all(|p| p.is_none()));
}

/// Files are written under `project_id=<id>/`, so the path IS the per-project attribution
/// for a commit that spanned projects. A single-project group returns the list unfiltered.
#[test]
fn added_files_attribute_to_their_own_project() {
    let added = vec![
        "s3://b/t/project_id=alpha/date=2026-07-29/a.parquet".to_string(),
        "s3://b/t/project_id=beta/date=2026-07-29/b.parquet".to_string(),
        "s3://b/t/project_id=alpha/date=2026-07-29/c.parquet".to_string(),
    ];
    let split = attribute_added_files(added.clone(), &["alpha", "beta", "gamma"]);
    assert_eq!(split[0], vec![added[0].clone(), added[2].clone()]);
    assert_eq!(split[1], vec![added[1].clone()]);
    assert!(split[2].is_empty(), "a project that added no files gets none of its co-tenants'");
    // Single project → unfiltered, even without a project_id partition segment.
    assert_eq!(
        attribute_added_files(vec!["s3://b/t/date=2026-07-29/x.parquet".to_string()], &["alpha"]),
        vec![vec!["s3://b/t/date=2026-07-29/x.parquet".to_string()]]
    );
}

/// `filesets_for_dates` buckets URIs by their `date=` partition and
/// pre-seeds every requested date (so the guard can tell "empty" from
/// "absent"). URIs outside the requested dates are dropped.
#[test]
fn filesets_for_dates_groups_by_partition() {
    use HashSet;
    let d0 = chrono::NaiveDate::from_ymd_opt(2026, 6, 6).unwrap();
    let d1 = chrono::NaiveDate::from_ymd_opt(2026, 6, 5).unwrap();
    let uris = vec![
        "s3://b/t/date=2026-06-06/part-a.parquet".to_string(),
        "s3://b/t/date=2026-06-06/part-b.parquet".to_string(),
        "s3://b/t/date=2026-06-05/part-c.parquet".to_string(),
        "s3://b/t/date=2026-06-01/part-x.parquet".to_string(), // outside window
    ];
    let sets = Database::filesets_for_dates(&uris, &[d0, d1]);
    assert_eq!(sets[&d0].len(), 2);
    assert_eq!(sets[&d1], HashSet::from(["s3://b/t/date=2026-06-05/part-c.parquet".to_string()]));
    // A date with no files is still present (empty), not missing.
    let d2 = chrono::NaiveDate::from_ymd_opt(2026, 6, 4).unwrap();
    let sets = Database::filesets_for_dates(&uris, &[d2]);
    assert!(sets[&d2].is_empty());
}

/// One candidate file for the tail-packer tests.
fn tail_file(path: &str, size: i64, is_sorted_run: bool, event_range: Option<(i64, i64)>, has_dv: bool, rows: Option<u64>) -> super::TailAdd {
    super::TailAdd { path: path.into(), size, is_sorted_run, event_range, rows, has_dv }
}

const MB: i64 = 1024 * 1024;

/// A candidate sized in MiB with no event-time stats — the shape the coordinator
/// selectors take (they bin by size, not by event range).
fn mb_file(path: &str, size_mb: i64, sorted: bool) -> super::TailAdd {
    tail_file(path, size_mb * MB, sorted, None, false, None)
}

/// A sorted-run candidate carrying a known row count, for the row-cap tests.
fn rows_file(path: &str, bytes: i64, rows: u64) -> super::TailAdd {
    tail_file(path, bytes, true, None, false, Some(rows))
}

/// The coordinator's own policy through the SHARED packer, so these tests
/// exercise the exact call `coordinator_compaction_files` makes.
fn coordinator_bin(files: Vec<super::TailAdd>, target: i64) -> Vec<String> {
    super::select_bin(&files, super::BinPolicy { target_size: target, max_rows: u64::MAX, order: super::BinOrder::SmallestFirst, level_unsorted_first: true })
}

/// One coordinator selection pass at the sealed byte target.
fn coordinator_pick(files: Vec<super::TailAdd>) -> Vec<String> {
    coordinator_bin(files, super::COORDINATOR_SEALED_TARGET_BYTES)
}

// The shared packing target/seal the policy tests are written against.
const BIN_TARGET: i64 = 1000;
const BIN_SEAL: i64 = 10_000;

/// One `select_tail_bin` pass at that shared target/seal.
fn tail_bin(adds: &[super::TailAdd], min_files: usize, pass: TailPass) -> Vec<String> {
    super::select_tail_bin(adds, BIN_TARGET, min_files, BIN_TARGET / 4, BIN_SEAL, pass)
}

/// One packing pass over `(path, size, is_sorted_run, min_event, max_event, has_dv)`
/// tuples, returning the selected bin as a comma-joined path list ("" = nothing selected).
fn pack_tail(files: &[(&str, i64, bool, i64, i64, bool)], min_files: usize) -> String {
    let adds: Vec<_> = files.iter().map(|&(p, size, sorted, lo, hi, dv)| tail_file(p, size, sorted, Some((lo, hi)), dv, None)).collect();
    tail_bin(&adds, min_files, TailPass::Pack).join(",")
}

/// Fan-in must scale with the bin target: a bin fills to the target, so twice the target
/// should hold twice the files. Other budgets could bind
/// first, so this drives the real `select_tail_bin` rather than restating the arithmetic.
#[test]
fn fan_in_scales_with_the_bin_target() {
    const SEAL: i64 = i64::MAX / 4;
    // 40 candidate files of 16 MiB, contiguous in event time.
    let adds: Vec<_> = (0..40).map(|i| tail_file(&format!("f{i}"), 16 * 1024 * 1024, false, Some((i, i)), false, Some(10_000))).collect();

    let fan_in = |target: i64| super::select_tail_bin(&adds, target, 5, target / 4, SEAL, TailPass::Pack).len();
    let (at256, at512, at768) = (fan_in(256 * 1024 * 1024), fan_in(512 * 1024 * 1024), fan_in(768 * 1024 * 1024));
    println!("fan-in: 256 MiB -> {at256}   512 MiB -> {at512}   768 MiB -> {at768}");

    assert!(at256 >= 2, "the fixture must produce a real bin at the current target, or this proves nothing");
    assert!(
        at512 > at256,
        "THE 10x PATH'S LOAD-BEARING ASSUMPTION: doubling the target must raise fan-in. \
             256 MiB -> {at256}, 512 MiB -> {at512}"
    );
}

/// STEADY-STATE BENCHMARK: drive the real packer over many rounds, feeding merged
/// output back in, and measure bytes written per byte ingested.
///
/// This used to compare a value floor OFF vs ON and assert the floor lowered
/// amplification. The floor is gone — it could refuse every bin, and on
/// 2026-09-15 it did, for three days. What replaces the comparison is a CEILING:
/// the unguarded packer must stay bounded and must converge, so deleting the
/// guard cannot silently trade an outage for unbounded rewrite cost.
#[test]
fn the_unguarded_packer_converges_at_bounded_write_amplification() {
    const TARGET: i64 = 256 * 1024 * 1024;
    const SEAL: i64 = i64::MAX / 4;
    const ARRIVAL: i64 = 18 * 1024 * 1024; // ~176k rows at 104 B/row
    const ROUNDS: usize = 400;

    // One partition: files arrive, the packer picks a bin, the bin becomes a single
    // larger file. Deterministic — no clock, no RNG.
    let (mut live, mut written, mut ingested) = (Vec::<i64>::new(), 0i64, 0i64);
    for _ in 0..ROUNDS {
        live.push(ARRIVAL);
        ingested += ARRIVAL;
        let adds: Vec<_> = live
            .iter()
            .enumerate()
            .map(|(i, size)| tail_file(&format!("f{i}"), *size, false, Some((i as i64, i as i64)), false, Some((*size as u64) * 12 / 104)))
            .collect();
        let picked = super::select_tail_bin(&adds, TARGET, 5, TARGET / 4, SEAL, TailPass::Pack);
        if picked.len() < 2 {
            continue;
        }
        let idx: Vec<usize> = picked.iter().filter_map(|p| p.strip_prefix('f')?.parse().ok()).collect();
        let bytes: i64 = idx.iter().map(|i| live[*i]).sum();
        let mut keep: Vec<i64> = live.iter().enumerate().filter(|(i, _)| !idx.contains(i)).map(|(_, s)| *s).collect();
        keep.push(bytes);
        live = keep;
        written += bytes;
    }
    let amplification = written as f64 / ingested as f64;
    println!("amplification {amplification:.2}x   live files {}", live.len());

    assert!(amplification > 1.0, "the packer must rewrite something, or the fixture is wrong");
    assert!(amplification <= AMPLIFICATION_CEILING, "steady-state write amplification regressed: {amplification:.2}x > {AMPLIFICATION_CEILING:.2}x");
    // CONVERGENCE: 400 arrivals must not leave 400 live files. The packer has to be
    // retiring files faster than they arrive, or readers pay the fragmentation.
    assert!(live.len() * 4 <= ROUNDS, "the packer stopped converging: {} live files after {ROUNDS} arrivals", live.len());
}

/// Measured ceiling for [`the_unguarded_packer_converges_at_bounded_write_amplification`].
/// Raising it is a real trade and must be argued, not nudged.
const AMPLIFICATION_CEILING: f64 = 9.0;

#[derive(serde::Deserialize)]
struct PackReplayArrival {
    commit_ms: i64,
    project: String,
    date: String,
    path: String,
    size: i64,
    rows: u64,
    min_event: i64,
    max_event: i64,
    sorted: bool,
    has_dv: bool,
}

#[derive(Clone)]
struct PackReplayFile {
    add: super::TailAdd,
    born_ms: i64,
    depth: u32,
}

#[derive(serde::Serialize)]
struct PackReplayResult {
    output_per_mille: i64,
    arrivals: usize,
    input_bytes: i64,
    input_rows: u64,
    pack_waves: u64,
    pack_bins: u64,
    rewrite_bytes: i64,
    rewrite_rows: u64,
    amplification: f64,
    live_files: usize,
    max_lineage_depth: u32,
    p50_fan_in: usize,
    p95_fan_in: usize,
    max_fan_in: usize,
    oldest_live_age_secs: i64,
    drain_ticks: u64,
}

fn replay_percentile(values: &mut [usize], percentile: usize) -> usize {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[((values.len() - 1) * percentile / 100).min(values.len() - 1)]
}

/// Read-only production-trace arm for the geometric packing candidate.
///
/// Generate a sanitized flush trace with `bench/delta_work_ledger.py
/// --pack-trace`, set `TIMEFUSION_PACK_REPLAY` to it, then run this ignored
/// test with `--nocapture`. It calls the real selector at real five-minute
/// ticks and feeds simulated outputs back, so the comparison exercises resume,
/// floor, seal, sorted-run, row-cap, and size-ratio behavior together. This is
/// an isolated arrival-cohort replay: it has no active-file seed at the first
/// tick, and it grants all 12 rounds without modeling staging duration.
/// Historical wave outputs are deliberately absent from the trace: inheriting
/// baseline packing decisions would make the counterfactual meaningless.
#[test]
#[ignore = "requires a locally generated production Delta trace"]
fn replay_post_retraction_pack_trace() {
    use std::{collections::BTreeMap, io::BufRead};

    const TICK_MS: i64 = 5 * 60 * 1000;
    const SEAL_LAG_MS: i64 = 15 * 60 * 1000;
    const MIN_FILES: usize = 5;
    const MAX_WAVES: usize = 12;

    let path = std::env::var("TIMEFUSION_PACK_REPLAY").expect("set TIMEFUSION_PACK_REPLAY to JSONL from delta_work_ledger.py --pack-trace");
    let file = std::fs::File::open(path).expect("open pack replay trace");
    let mut arrivals: Vec<PackReplayArrival> =
        std::io::BufReader::new(file).lines().map(|line| serde_json::from_str(&line.expect("read trace line")).expect("parse trace line")).collect();
    arrivals.sort_unstable_by_key(|arrival| arrival.commit_ms);
    assert!(!arrivals.is_empty(), "trace has no flush arrivals");

    let output_scales: Vec<i64> = std::env::var("TIMEFUSION_PACK_REPLAY_OUTPUT_PERMILLE")
        .unwrap_or_else(|_| "1000".into())
        .split(',')
        .map(|part| part.trim().parse().expect("output scale is integer permille"))
        .collect();
    let target = super::pack_target_bytes(256 * MB, std::time::Duration::from_secs(240));

    let run = |output_per_mille: i64| {
        let input_bytes = arrivals.iter().map(|arrival| arrival.size).sum::<i64>();
        let input_rows = arrivals.iter().map(|arrival| arrival.rows).sum::<u64>();
        let first_tick = arrivals[0].commit_ms.div_euclid(TICK_MS) * TICK_MS;
        let last_arrival = arrivals.last().unwrap().commit_ms;
        let mut groups: BTreeMap<(String, String), Vec<PackReplayFile>> = BTreeMap::new();
        let (mut cursor, mut tick, mut sequence) = (0usize, first_tick, 0u64);
        let (mut pack_waves, mut pack_bins, mut rewrite_bytes, mut rewrite_rows, mut drain_ticks) = (0u64, 0u64, 0i64, 0u64, 0u64);
        let mut fan_ins = Vec::new();

        loop {
            while cursor < arrivals.len() && arrivals[cursor].commit_ms <= tick {
                let arrival = &arrivals[cursor];
                groups.entry((arrival.project.clone(), arrival.date.clone())).or_default().push(PackReplayFile {
                    add: tail_file(
                        &arrival.path,
                        arrival.size,
                        arrival.sorted,
                        Some((arrival.min_event, arrival.max_event)),
                        arrival.has_dv,
                        Some(arrival.rows),
                    ),
                    born_ms: arrival.commit_ms,
                    depth: 0,
                });
                cursor += 1;
            }

            let seal = (tick - SEAL_LAG_MS).saturating_mul(1000);
            let pack_date = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(tick).expect("tick is a UTC instant").date_naive().to_string();
            let mut tick_changed = false;
            for _ in 0..MAX_WAVES {
                let mut wave_bins = 0u64;
                for ((_, date), live) in &mut groups {
                    // Production Pack selects only today's partition. Repair is
                    // the separate path that reaches into prior dates.
                    if date != &pack_date {
                        continue;
                    }
                    let candidates: Vec<_> = live.iter().map(|file| file.add.clone()).collect();
                    let picked = super::select_tail_bin(&candidates, target, MIN_FILES, target / 2, seal, TailPass::Pack);
                    // This arm measures Pack only. A singleton is today's repair
                    // gap and belongs in a separate real-I/O repair replay.
                    if picked.len() < 2 {
                        continue;
                    }
                    let selected: std::collections::HashSet<_> = picked.iter().map(String::as_str).collect();
                    let mut inputs = Vec::with_capacity(picked.len());
                    live.retain(|file| {
                        if selected.contains(file.add.path.as_str()) {
                            inputs.push(file.clone());
                            false
                        } else {
                            true
                        }
                    });
                    assert_eq!(inputs.len(), picked.len(), "selector returned a path that was not live");
                    let bytes = inputs.iter().map(|file| file.add.size).sum::<i64>();
                    let rows = inputs.iter().map(|file| file.add.rows.unwrap_or(0)).sum::<u64>();
                    let min_event = inputs.iter().filter_map(|file| file.add.event_range.map(|range| range.0)).min().unwrap();
                    let max_event = inputs.iter().filter_map(|file| file.add.event_range.map(|range| range.1)).max().unwrap();
                    let born_ms = inputs.iter().map(|file| file.born_ms).min().unwrap();
                    let depth = inputs.iter().map(|file| file.depth).max().unwrap() + 1;
                    sequence += 1;
                    live.push(PackReplayFile {
                        add: tail_file(
                            &format!("sim-{sequence}"),
                            bytes.saturating_mul(output_per_mille) / 1000,
                            true,
                            Some((min_event, max_event)),
                            false,
                            Some(rows),
                        ),
                        born_ms,
                        depth,
                    });
                    rewrite_bytes += bytes;
                    rewrite_rows += rows;
                    fan_ins.push(inputs.len());
                    pack_bins += 1;
                    wave_bins += 1;
                }
                if wave_bins == 0 {
                    break;
                }
                pack_waves += 1;
                tick_changed = true;
            }

            if cursor == arrivals.len() {
                drain_ticks += 1;
                // Once all events are sealed, a no-work tick is a fixed point:
                // no new candidate can appear without another arrival.
                if !tick_changed && tick >= last_arrival + SEAL_LAG_MS {
                    break;
                }
                assert!(drain_ticks <= 288, "replay did not quiesce within 24 hours; candidate manufactured debt");
            }
            tick += TICK_MS;
        }

        let live: Vec<_> = groups.values().flatten().collect();
        assert_eq!(live.iter().map(|file| file.add.rows.unwrap_or(0)).sum::<u64>(), input_rows, "packing changed the row count");
        let max_lineage_depth = live.iter().map(|file| file.depth).max().unwrap_or(0);
        let oldest_live_age_secs = live.iter().map(|file| (tick - file.born_ms) / 1000).max().unwrap_or(0);
        let mut p50 = fan_ins.clone();
        let mut p95 = fan_ins.clone();
        PackReplayResult {
            output_per_mille,
            arrivals: arrivals.len(),
            input_bytes,
            input_rows,
            pack_waves,
            pack_bins,
            rewrite_bytes,
            rewrite_rows,
            amplification: rewrite_bytes as f64 / input_bytes.max(1) as f64,
            live_files: live.len(),
            max_lineage_depth,
            p50_fan_in: replay_percentile(&mut p50, 50),
            p95_fan_in: replay_percentile(&mut p95, 95),
            max_fan_in: fan_ins.into_iter().max().unwrap_or(0),
            oldest_live_age_secs,
            drain_ticks,
        }
    };

    for output_per_mille in output_scales {
        println!("{}", serde_json::to_string(&run(output_per_mille)).unwrap());
    }
}

/// A bin's benefit is FILES removed; its cost is BYTES rewritten. The packer fills to
/// `target` and stops, so two nearly-converged files are a valid but very expensive bin.
/// The floor cannot be set from a unit test (it reads the global config), so the
/// ARITHMETIC the guard applies is asserted directly.
#[test]
fn a_bin_is_priced_by_files_removed_not_bytes_written() {
    // NOT sorted runs: `sorted_run_cap` (BIN_TARGET/4) would exclude the pair for an
    // unrelated reason. Priced in ROWS per file eliminated, which is what the guard compares.
    let value = |files: &[(&str, i64)]| {
        let rows: i64 = files.iter().map(|(_, s)| *s).sum();
        rows / (files.len() as i64 - 1).max(1)
    };

    // THE EXPENSIVE SHAPE: two nearly-converged files fill the target and
    // remove ONE file between them.
    assert_eq!(
        pack_tail(&[("p0", 430, false, 1, 2, false), ("p1", 430, false, 3, 4, false)], 2),
        "p0,p1",
        "unguarded, the packer takes the pair: it only asks whether the bytes fit"
    );
    assert_eq!(value(&[("p0", 430), ("p1", 430)]), 860, "860 bytes rewritten per file eliminated");

    // THE CHEAP SHAPE: ten small files, same partition, remove NINE.
    let many: Vec<_> = (0..10).map(|i| tail_file(&format!("s{i}"), 20, false, Some((i * 2 + 1, i * 2 + 2)), false, None)).collect();
    let picked = tail_bin(&many, 2, TailPass::Pack);
    assert_eq!(picked.len(), 10, "all ten fit under the target");
    assert_eq!(value(&[("s", 20); 10]), 22, "22 bytes per file eliminated — 39x better value");

    // A floor anywhere between the two separates them: it is a benefit-per-cost
    // rule, not a size rule.
    for floor in [30, 100, 500] {
        assert!(value(&[("p0", 430), ("p1", 430)]) > floor, "the pair must be refused at floor {floor}");
        assert!(value(&[("s", 20); 10]) <= floor, "the ten-file bin must survive floor {floor}");
    }
}

// Seal lag: a file whose newest event is past the seal is still filling, and
// compacting it loses every OCC race.
#[test_case(&[("a", 10, false, 1, 1, false), ("b", 10, false, 2, BIN_SEAL + 1, false)], 2 => "" ; "unsealed files leave < min_files")]
// Converged (>= 7/8 target) files are never re-selected: rewriting one alone is a
// 1→1 rewrite forever.
#[test_case(&[("big", 900, false, 1, 2, false), ("a", 10, false, 3, 4, false), ("b", 10, false, 5, 6, false)], 2 => "a,b" ; "a converged file is never re-selected")]
// REPAIR: an oversized file that is NOT a sorted run declares no `sorting_columns`,
// and one of those disables the reader's all-or-nothing footer ordering for every
// scan touching the partition. Nothing else rewrites it, so it is repaired — but
// only in the GAPS: while a project still has a packable slice, that slice wins.
#[test_case(&[("big_unsorted", 900, false, 1, 2, false), ("a", 10, false, 3, 4, false), ("b", 10, false, 5, 6, false)], 2 => "a,b" ; "normal packing must not be starved by a pending repair")]
// With no packable slice left, the tick repairs one oversized unsorted file alone,
// below min_files — so today's poisoned file is repaired even though
// `TailPass::Repair` excludes today.
#[test_case(&[("big_unsorted", 900, false, 1, 2, false)], 2 => "big_unsorted" ; "the gap rule repairs an oversized unsorted file alone")]
#[test_case(&[("big_sorted", 900, true, 1, 2, false)], 2 => "" ; "a converged sorted run is never re-selected")]
// ...but a DV disables per-file parquet pushdown, so a DV-bearing converged run
// must still be rewritten DV-free via the repair lane.
#[test_case(&[("big_dv", 900, true, 1, 2, true)], 2 => "big_dv" ; "a DV-bearing converged file is consolidated to restore pushdown")]
#[test_case(&[("run_small", 100, true, 1, 2, false), ("run_big", 300, true, 3, 4, false), ("a", 10, false, 5, 6, false)], 2 => "run_small,a" ; "only the sub-cap sorted run folds")]
// Earliest contiguous slice up to cap, ordered by EVENT time (input order is
// deliberately scrambled) — this is what makes runs disjoint.
#[test_case(&[("third", 600, false, 30, 31, false), ("first", 600, false, 10, 11, false), ("second", 300, false, 20, 21, false)], 2 => "first,second" ; "packs earliest slice, stops at cap")]
#[test_case(&[("a", 10, false, 1, 2, false), ("b", 10, false, 3, 4, false)], 3 => "" ; "min_files gate")]
// A lone over-cap-adjacent file must not wedge the pass: selection skips past it
// rather than returning a 1-file bin.
#[test_case(&[("lone", 800, false, 1, 2, false), ("a", 300, false, 10, 11, false), ("b", 300, false, 12, 13, false)], 2 => "a,b" ; "a lone over-cap-adjacent file does not wedge the pass")]
// ...and exactly one repair per bin, so a backlog drains across ticks.
#[test_case(&[("p1", 900, false, 1, 2, false), ("p2", 950, false, 3, 4, false), ("p3", 800, false, 5, 6, false)], 2
        => with |bin: String| assert!(!bin.is_empty() && !bin.contains(','), "one repair per bin, got {bin:?}") ; "one repair per bin")]
fn select_tail_bin_policy(files: &[(&str, i64, bool, i64, i64, bool)], min_files: usize) -> String {
    pack_tail(files, min_files)
}

/// Files with no event-time stats can't be binned disjointly, so they are never binned.
#[test]
fn files_without_event_stats_are_never_binned() {
    let no_stats = vec![tail_file("x", 10, false, None, false, None), tail_file("a", 10, false, Some((1, 2)), false, None)];
    assert_eq!(tail_bin(&no_stats, 2, TailPass::Pack), Vec::<String>::new());
}

/// Coordinator unit selection over `(path, bytes, is_sorted_run, rows)` tuples,
/// comma-joined ("" = nothing selected). Small L0 files are sorted before sorted
/// runs are merged; a run is never rewritten once it is at or above target (the
/// skip must NOT depend on the sorted-run tag, which only OPTIMIZE writes); and
/// the row cap must never be what reduces a bin to ONE file, since a one-file bin
/// trips the `< 2` guard, retires nothing and re-claims its cell forever.
#[test_case(&[("l0-a", 20 * MB, false, None), ("sorted", 10 * MB, true, None), ("l0-b", 20 * MB, false, None)] => "l0-a,l0-b" ; "sorted runs are excluded while any L0 file is present")]
#[test_case(&[("a", 8 * MB, false, None), ("b", 8 * MB, false, None), ("c", 8 * MB, false, None), ("d", 8 * MB, false, None)] => "a,b,c,d" ; "32 MB of small L0 files fits the unsorted-bin budget, so all share one unit")]
#[test_case(&[("a", 100 * MB, true, None), ("b", 100 * MB, true, None), ("c", 100 * MB, true, None)] => "a,b" ; "three 100 MB sorted runs stop at one committable physical run")]
#[test_case(&[("done", 100 * MB, true, None)] => "" ; "a lone sorted run is already converged")]
#[test_case(&[("legacy", 40 * MB, false, None)] => "legacy" ; "an individually oversized L0 file remains progressable and is time-sliced by staging")]
#[test_case(&[("converged-a", 520 * MB, false, None), ("converged-b", 512 * MB, false, None), ("small-a", 6 * MB, false, None), ("small-b", 6 * MB, false, None)] => "small-a,small-b" ; "a packing unit takes the small files and leaves the converged untagged ones alone")]
#[test_case(&[("converged-a", 520 * MB, false, None), ("converged-b", 512 * MB, false, None)] => "" ; "an already-packed partition produces no work, whatever its tags say")]
#[test_case(&[("a", 128 * MB, true, None), ("b", 128 * MB, true, None), ("c", 128 * MB, true, None)] => "a,b" ; "three 128 MB sorted runs fill the target exactly and still stop at one run")]
#[test_case(&[("small", 100_040_704, true, Some(915_417)), ("mid", 120_355_352, true, Some(1_108_187)), ("converged-a", 534_000_000, true, Some(4_675_365)), ("converged-b", 538_000_000, true, Some(4_701_864))] => "small,mid" ; "a pair inside the byte budget is never blocked by the row cap, even at 2,023,604 rows")]
#[test_case(&[("h0", 60 * MB, true, Some(3_000_000)), ("h1", 60 * MB, true, Some(3_000_000))] => "h0,h1" ; "a pair is admitted whatever its rows — the row cap may bound a bin, never deny it a second file")]
#[test_case(&[("h0", 100 * MB, true, Some(3_000_000)), ("h1", 100 * MB, true, Some(3_000_000)), ("h2", 100 * MB, true, Some(3_000_000))] => "h0,h1" ; "a bin is still BOUNDED, now in bytes: a third 100MB file overruns the 256MB target")]
fn coordinator_selection_bins_l0_before_runs_and_never_rewrites_a_converged_file(files: &[(&str, i64, bool, Option<u64>)]) -> String {
    coordinator_pick(files.iter().map(|&(path, size, sorted, rows)| tail_file(path, size, sorted, None, false, rows)).collect()).join(",")
}

/// The same invariant for the OTHER selector. `select_tail_bin` feeds the hot
/// lane and shares `stage_hot_bin` with the coordinator, so a veto reintroduced
/// here wedges exactly the same way — it just would not have shown up in the
/// sealed-lane symptoms that made 2026-09-15 visible.
#[test]
fn tail_selector_never_refuses_a_packable_slice() {
    const SEAL: i64 = i64::MAX / 4;
    for files in [5usize, 9, 40] {
        for size in [1, BIN_TARGET / 4, BIN_TARGET / 2] {
            for rows in [None, Some(1), Some(5_000_000)] {
                let adds: Vec<_> = (0..files).map(|i| tail_file(&format!("f{i}"), size, false, Some((i as i64, i as i64)), false, rows)).collect();
                let picked = super::select_tail_bin(&adds, BIN_TARGET, 5, BIN_TARGET / 4, SEAL, TailPass::Pack);
                assert!(picked.len() >= 2, "tail selector refused {files} sealed files of {size} B / {rows:?} rows — the pass would re-claim them forever");
            }
        }
    }
}

/// UNIFICATION, pinned. Both compaction paths must reach the same packer, so a
/// budget fixed in one is fixed in both. Until 2026-09-19 they were separate
/// functions: the coordinator's cap collapsed onto the two smallest files while
/// the off-box CLI packed to its full target, which cost a whole rewrite pass
/// per doubling and let a value floor wedge one lane and not the other.
///
/// Given the same candidates and the same byte budget, the only difference
/// either policy may produce is ORDER — never how much a bin takes.
#[test]
fn both_compaction_paths_pack_to_the_same_budget() {
    // Eight 40 MB files, contiguous in event time, well under a 256 MB target.
    let adds: Vec<_> = (0..8).map(|i| tail_file(&format!("f{i}"), 40 * MB, true, Some((i, i)), false, Some(100_000))).collect();
    let target = 256 * MB;

    let coordinator = coordinator_bin(adds.clone(), target);
    let offbox =
        super::select_bin(&adds, super::BinPolicy { target_size: target, max_rows: u64::MAX, order: super::BinOrder::EventTime, level_unsorted_first: false });

    let bytes = |bin: &[String]| bin.len() as i64 * 40 * MB;
    assert_eq!(bytes(&coordinator), bytes(&offbox), "the two paths packed different BYTES: coordinator {:?}, offbox {:?}", coordinator, offbox);
    assert!(coordinator.len() >= 6, "a 256MB target must hold six 40MB files, not a pair — got {}", coordinator.len());
}

/// The pair-collapse, gone. `coordinator_packing_cap_bytes` used to return the
/// decode margin, so a cell whose two smallest files exceeded it got a target of
/// exactly that pair and could never bin a third file. The cap is now a multiple
/// of the target file size and the SORT is what gets sliced.
#[test]
fn the_packing_cap_no_longer_collapses_onto_a_pair() {
    let pair = 97_498_284; // the exact prod value from the 2026-09-15 log line
    let cap = crate::config::coordinator_packing_cap_bytes(pair);
    assert!(cap > pair, "cap {cap} still collapses onto the pair {pair}");
    assert!(
        cap >= super::COORDINATOR_SEALED_TARGET_BYTES,
        "cap {cap} binds below the declared target {} — bins would still be shrunk instead of sliced",
        super::COORDINATOR_SEALED_TARGET_BYTES
    );
}

/// A multi-file bin must be SLICED, not shrunk. This is what makes the larger
/// cap safe: the sort is bounded per slice, so a 256 MB bin never sorts 256 MB
/// of compressed input in one pass.
#[test]
fn a_multi_file_bin_slices_its_sort_instead_of_shrinking() {
    // The slice budget carries a 3/5 margin: the decoded-bytes ratio is an
    // optimistic fixed estimate, so a slice priced at a whole sort can overrun.
    let budget = crate::config::coordinator_per_sort_decoded_bytes() * 3 / 5;
    // A full 256 MB bin of several files — the shape the new cap admits.
    let bin_bytes = super::COORDINATOR_SEALED_TARGET_BYTES;
    let target = super::coordinator_slice_target(TailPass::Pack, 6, bin_bytes).expect("multi-file Pack bins must slice");
    assert_eq!(target, budget, "a multi-file bin must slice to the sort budget");

    let want = super::repair_slice_want(bin_bytes, target);
    let decoded = crate::database::maintain::estimated_decoded_bytes(bin_bytes) as i64;
    assert!(want > 1, "a {bin_bytes}-byte bin decodes to {decoded} and must need more than one slice");
    assert!(decoded / want as i64 <= budget, "each slice must fit the sort budget: {decoded}/{want} > {budget}");

    // A bin that already fits is left alone.
    assert_eq!(super::repair_slice_want(1024, target), 1, "a small bin must not be sliced");
}

/// THE 2026-09-15 WEDGE, as a property: whenever two or more packable files
/// exist, the selector MUST return at least two. A selector that can decline all
/// work has no safe default — sealed consolidation committed nothing for three
/// days because a value floor wanted five files and the byte budget, pinned by
/// `coordinator_packing_cap_bytes` to the two smallest, could only ever give two.
///
/// Swept across the shape that actually wedged (sizes straddling the decode
/// margin, row counts far above any plausible per-file floor) and the degenerate
/// neighbours around it.
#[test]
fn coordinator_selector_never_refuses_a_packable_pair() {
    // 29 candidates, ~48 MB each, ~1M rows each: the production cell from the log
    // line `target=97498284 smallest_pair_bytes=97498284 selected=0`.
    for files in [2usize, 3, 5, 29] {
        for size in [4 * MB, 48 * MB, 97_498_284 / 2, 200 * MB] {
            for rows in [None, Some(1), Some(1_000_000), Some(5_000_000)] {
                let adds: Vec<_> = (0..files).map(|i| tail_file(&format!("f{i}"), size, true, None, false, rows)).collect();
                let picked = coordinator_pick(adds);
                assert!(picked.len() >= 2, "selector refused {files} packable files of {size} B / {rows:?} rows — the cell would be re-claimed forever");
            }
        }
    }
}

/// Where the wedge actually bit, now the other way round. This cell's files are
/// the exact prod shape whose PAIR (97,498,284 B) used to become the whole
/// target, capping every bin at two files. The cap no longer collapses, so the
/// same cell must now pack well past a pair — and must still never return one.
#[test]
fn the_prod_wedge_cell_now_packs_past_a_pair() {
    let half = 97_498_284 / 2;
    let adds: Vec<_> = (0..29).map(|i| tail_file(&format!("f{i}"), half, true, None, false, Some(1_038_000))).collect();
    let target = super::COORDINATOR_SEALED_TARGET_BYTES.min(crate::config::coordinator_packing_cap_bytes(half * 2));
    assert_eq!(target, super::COORDINATOR_SEALED_TARGET_BYTES, "the cap must no longer shrink the target onto the pair");

    let picked = coordinator_bin(adds, target);
    assert!(picked.len() > 2, "the cell that could only ever bin a pair must now pack more — got {}", picked.len());
    // ...and the bin still respects the budget it was given.
    assert!((picked.len() as i64) * half <= target, "bin of {} x {half} B exceeds target {target}", picked.len());
}

/// The wide narrow-row `otel_metrics` shape the staging measurements below are written
/// against: 4 base columns plus `extra_cols` attributes. Per-batch cost is paid PER
/// COLUMN, so a 4-column model badly understates it.
fn wide_metrics_schema(extra_cols: usize) -> Arc<arrow_schema::Schema> {
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    let mut fields = vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("series_id", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ];
    fields.extend((0..extra_cols).map(|i| Field::new(format!("attr_{i}"), if i % 3 == 0 { DataType::Utf8 } else { DataType::Int64 }, true)));
    Arc::new(Schema::new(fields))
}

/// `n` rows of `wide_metrics_schema(extra_cols)` starting at `offset`. `unique` selects
/// near-unique string values (like real otel columns) over low-cardinality ones, which
/// dictionary-encode almost for free and so understate the writer's cost.
fn wide_metrics_batch(schema: &Arc<arrow_schema::Schema>, extra_cols: usize, offset: usize, n: usize, unique: bool) -> RecordBatch {
    use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
    let s = |f: &dyn Fn(usize) -> String| -> ArrayRef { Arc::new(StringArray::from((0..n).map(|r| f(offset + r)).collect::<Vec<_>>())) };
    let ints = || -> ArrayRef { Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())) };
    let mut cols: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from((0..n as i64).map(|i| offset as i64 + i).collect::<Vec<_>>()).with_timezone("UTC")),
        s(&|r| format!("metric_{}", r % 64)),
        s(&|r| if unique { format!("series_{r}") } else { format!("series_{}", r % 512) }),
        ints(),
    ];
    cols.extend((0..extra_cols).map(|i| if i % 3 == 0 { s(&|r| if unique { format!("v{r}-{i}") } else { format!("v{}", r % 97) }) } else { ints() }));
    RecordBatch::try_new(Arc::clone(schema), cols).expect("batch")
}

/// Measures how batch size affects staging throughput for narrow rows, sorting the same
/// rows at each batch size through `execute_stream` (what staging does). `#[ignore]`d
/// because it is a measurement, not an assertion.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement, not an assertion — run explicitly with --ignored --no-capture"]
async fn batch_size_dominates_narrow_row_staging() {
    use datafusion::{
        datasource::MemTable,
        prelude::{SessionConfig, SessionContext},
    };
    use futures::StreamExt;

    const PARTS: usize = 23;
    const ROWS_PER_PART: usize = 40_000;
    let build = |extra_cols: usize| {
        let schema = wide_metrics_schema(extra_cols);
        let partitions: Vec<Vec<_>> = (0..PARTS).map(|p| vec![wide_metrics_batch(&schema, extra_cols, p * ROWS_PER_PART, ROWS_PER_PART, false)]).collect();
        (schema, partitions)
    };

    let total_rows = PARTS * ROWS_PER_PART;
    for (label, extra) in [("4 cols", 0usize), ("69 cols (metrics)", 65)] {
        let (schema, partitions) = build(extra);
        for batch_size in [256usize, 2048, 8192] {
            let cfg = SessionConfig::new().with_target_partitions(1).with_batch_size(batch_size);
            let ctx = SessionContext::new_with_config(cfg);
            ctx.register_table("bin", Arc::new(MemTable::try_new(Arc::clone(&schema), partitions.clone()).expect("memtable"))).expect("register");
            let started = std::time::Instant::now();
            let mut stream =
                ctx.sql("SELECT * FROM bin ORDER BY \"timestamp\" DESC, metric_name, series_id").await.expect("plan").execute_stream().await.expect("stream");
            let (mut rows, mut batches) = (0usize, 0usize);
            while let Some(b) = stream.next().await {
                rows += b.expect("batch").num_rows();
                batches += 1;
            }
            let elapsed = started.elapsed();
            assert_eq!(rows, total_rows, "every row must come back at {label} batch_size={batch_size}");
            println!(
                "{label:<18} batch_size={batch_size:>5}  {batches:>6} batches  {:>7.2}s  {:>9.0} rows/s",
                elapsed.as_secs_f64(),
                rows as f64 / elapsed.as_secs_f64()
            );
        }
    }
}

/// The WRITE side of staging (`cast_record_batch` plus `ArrowWriter` + zstd), which the
/// sort measurement above does not cover. `#[ignore]`d — a measurement, not an assertion.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "measurement, not an assertion — run explicitly with --run-ignored all --no-capture"]
async fn staging_write_throughput_by_batch_size() {
    use datafusion::parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};

    const ROWS: usize = 200_000;
    const COLS: usize = 65;
    let schema = wide_metrics_schema(COLS);

    for (label, unique) in [("low-cardinality", false), ("HIGH-cardinality (realistic)", true)] {
        for (zstd, batch_size) in [(1i32, 256usize), (1, 8192), (3, 8192)] {
            // Built OUTSIDE the timer, or this measures string allocation, not the writer.
            let batches: Vec<_> = (0..ROWS).step_by(batch_size).map(|off| wide_metrics_batch(&schema, COLS, off, batch_size.min(ROWS - off), unique)).collect();
            let mut buf: Vec<u8> = Vec::with_capacity(256 << 20);
            // zstd 3 is `timefusion_zstd_compression_level`, what staging uses.
            let props =
                WriterProperties::builder().set_compression(Compression::ZSTD(datafusion::parquet::basic::ZstdLevel::try_new(zstd).expect("level"))).build();
            let mut writer = ArrowWriter::try_new(&mut buf, Arc::clone(&schema), Some(props)).expect("writer");
            let started = std::time::Instant::now();
            for b in &batches {
                writer.write(b).expect("write");
            }
            writer.close().expect("close");
            let elapsed = started.elapsed();
            println!(
                "WRITE {label:<28} zstd={zstd} batch={batch_size:>5}  {:>7.2}s  {:>9.0} rows/s  {:>7.1} MB out",
                elapsed.as_secs_f64(),
                ROWS as f64 / elapsed.as_secs_f64(),
                buf.len() as f64 / 1e6
            );
        }
    }
}

#[test]
fn the_span_budget_rejects_wide_unions_and_is_off_by_default() {
    let bin = crate::database::compact::bin_micros();
    let f = |path: &str, at_bin: i64| tail_file(path, 10 * MB, true, Some((at_bin * bin, at_bin * bin + bin / 2)), false, Some(1_000));
    // Two files 100 bins apart: a merge of them spans ~101 bins.
    let far = vec![f("a", 0), f("b", 100)];

    // No config in a unit test => cap 0 => the budget is inert.
    assert_eq!(coordinator_pick(far.clone()).len(), 2, "with no span budget configured the packer must behave exactly as before");

    // The union arithmetic the budget applies, pinned directly since a unit
    // test cannot set the global config.
    let span_bins = |a: &super::TailAdd, b: &super::TailAdd| {
        let (l1, h1) = a.event_range.expect("range");
        let (l2, h2) = b.event_range.expect("range");
        (h1.max(h2) - l1.min(l2)) / bin + 1
    };
    assert_eq!(span_bins(&far[0], &far[1]), 101, "two files 100 bins apart union to ~101 bins");
    assert!(span_bins(&far[0], &far[1]) > 24, "and 101 is far past any bound the measured distribution suggests (~20-24)");
    let near = [f("c", 0), f("d", 3)];
    assert!(span_bins(&near[0], &near[1]) <= 20, "adjacent files stay inside hot packing's observed maximum of 20 bins");
}

/// A dense bin is bounded by BYTES, and its SORT by slices.
///
/// This used to assert the opposite: that a row cap clipped a dense bin before
/// the byte budget did. That cap was a second bound in a different unit, and the
/// moment the byte cap stopped collapsing onto a pair it simply became the new
/// collapse — two 1.038M-row otel_metrics files exceed 2M rows, so a row-dense
/// table was still capped at two files. Decoded-byte slicing bounds the sort and
/// already accounts for row count and row width together.
#[test]
fn a_dense_bin_is_bounded_by_bytes_and_its_sort_by_slices() {
    // Metrics shape: ~47 B/row, 23 files x 11 MB = 253 MB, ~5.5M rows.
    let dense: Vec<_> = (0..23).map(|i| rows_file(&format!("m{i:02}"), 11 * MB, 240_000)).collect();
    let picked = coordinator_pick(dense);
    assert_eq!(picked.len(), 23, "a dense cell must now fill its byte target, not stop at a pair");

    // The sort that bin feeds is what gets bounded, per slice.
    let bin_bytes = picked.len() as i64 * 11 * MB;
    let target = super::coordinator_slice_target(TailPass::Pack, picked.len(), bin_bytes).expect("multi-file bins slice");
    let want = super::repair_slice_want(bin_bytes, target);
    let decoded = crate::database::maintain::estimated_decoded_bytes(bin_bytes) as i64;
    assert!(decoded / want as i64 <= target, "each slice must fit its budget: {decoded}/{want} > {target}");

    // Logs shape: ~155 B/row — same byte bound, fewer rows, still one bin.
    let sparse: Vec<_> = (0..23).map(|i| rows_file(&format!("l{i:02}"), 11 * MB, 75_000)).collect();
    assert_eq!(coordinator_pick(sparse).len(), 23, "a sparse bin is bounded by BYTES too");
    // Absent stats must never make the cap stricter than bytes alone.
    let unknown: Vec<_> = (0..23).map(|i| mb_file(&format!("u{i:02}"), 11, true)).collect();
    assert_eq!(coordinator_pick(unknown).len(), 23, "unknown row counts do not accumulate, so the byte budget still governs");
}

/// The planner queues a cell when it holds two under-target files; the packer
/// must then bin them. There is no second predicate to drift — this pins that
/// the packer really does bin every pair the planner will queue, including the
/// shapes that used to be refused on rows or on a sum above the target.
#[test]
fn the_packer_bins_every_pair_the_planner_queues() {
    for (cell, (size_a, rows_a), (size_b, rows_b)) in [
        ("dcad860a/2026-06-17", (100_040_704, 915_417), (120_355_352, 1_108_187)),
        // 12.6M rows across the pair: the row cap may bound a bin's growth, never
        // deny it a second file.
        ("be87ebc1/2026-07-02", (103_809_024, 8_509_391), (112_918_528, 4_137_188)),
        // Two under-target files whose SUM exceeds the target. The planner queues
        // them (both are under-target), so the packer must take them.
        ("sum-over-target", (200 * MB, 1), (200 * MB, 1)),
    ] {
        let packed = coordinator_pick(vec![rows_file("a", size_a, rows_a), rows_file("b", size_b, rows_b)]).len();
        assert_eq!(packed, 2, "{cell}: the planner queues this cell, so the packer must bin both files (got {packed})");
    }
}

/// A bin holding unsorted files is budgeted in DECODED bytes — a
/// compressed-byte cap is ~12x stricter than intended, and since the
/// sorted-run tag is written only by OPTIMIZE it applies to nearly every bin
/// over fresh flush output.
#[test]
fn an_unsorted_bin_is_budgeted_in_decoded_bytes() {
    let budget = super::unsorted_bin_budget_bytes();

    // The invariant the value is chosen from: one repair slice plus two
    // concurrent unsorted-bin sorts must still fit about half the light pool.
    let decoded = crate::database::maintain::estimated_decoded_bytes(budget) as i64;
    assert!(decoded + 2 * decoded <= super::REPAIR_SLICE_DECODED_TARGET_BYTES * 3, "budget x concurrent sorts must stay within the pool share");
    assert!(decoded <= super::REPAIR_SLICE_DECODED_TARGET_BYTES, "an unsorted bin must not out-reserve a repair slice");

    // 4 MB files are typical of a fragmented sealed day; the bin must take many.
    let many: Vec<_> = (0..40).map(|i| mb_file(&format!("s{i:02}"), 4, false)).collect();
    let picked = coordinator_pick(many);
    assert!(picked.len() >= 15, "a fragmented cell must retire many files per unit, got {}", picked.len());
    assert!((picked.len() as i64) * 4 * MB <= budget, "...but never more than the decoded budget allows");
    assert!(picked.len() > 4, "the old compressed-byte cap must not still be binding");

    // An unsorted cell must still be able to bin a PAIR even when the pair
    // exceeds the (smaller) unsorted budget — a bin that holds one file
    // retires nothing, so its cell re-enqueues forever.
    let pair_cell: Vec<_> = (0..4).map(|i| tail_file(&format!("f{i}"), 54 * MB, false, None, false, Some(1_000))).collect();
    assert!(54 * MB * 2 > budget, "precondition: the pair exceeds the unsorted budget ({budget})");
    let pair_picked = coordinator_pick(pair_cell);
    assert!(pair_picked.len() >= 2, "an unsorted cell whose pair exceeds the unsorted budget still has to merge; picked {}", pair_picked.len());
}

/// A slice must bound DECODED bytes, because that is what the sort
/// allocates (and `ExternalSorterMerge` cannot spill). Sizing in compressed
/// bytes makes every slice ~12x its intent.
#[test]
fn repair_slices_bound_decoded_bytes_not_compressed() {
    use super::{REPAIR_SLICE_DECODED_TARGET_BYTES as TARGET, repair_slice_want};
    use crate::database::maintain::estimated_decoded_bytes;

    for bytes_in in [910_749_060i64, 1_717_176_058, 722_216_602, 1_317_255_463] {
        let want = repair_slice_want(bytes_in, TARGET);
        let decoded_per_slice = estimated_decoded_bytes(bytes_in) / want as u64;
        assert!(
            decoded_per_slice <= TARGET as u64,
            "bytes_in={bytes_in} cut into {want} slices leaves {decoded_per_slice} decoded bytes per slice, over the {TARGET} target"
        );
    }

    assert_eq!(repair_slice_want(910_749_060, TARGET), 11, "a 910 MB file is ~10.9 GB decoded and needs 11 slices, not the 4 that compressed sizing gave");

    // A bin small enough to sort whole is never sliced (`want > 1` gates it).
    assert_eq!(repair_slice_want(1024, TARGET), 1, "a tiny bin must not be sliced");

    // Pack's single-oversized-L0 path: the same 16 MB compressed budget,
    // expressed in decoded bytes.
    assert_eq!(
        super::coordinator_slice_target(TailPass::Pack, 1, 40 * MB),
        Some(estimated_decoded_bytes(16 * MB) as i64),
        "the L0 target is unchanged in effect, only re-denominated"
    );
    assert_eq!(
        repair_slice_want(40 * MB, estimated_decoded_bytes(16 * MB) as i64),
        40 / 16 + 1,
        "re-denominating must not change how many slices an L0 file gets"
    );
    // Repair does not slice: a slice is a whole extra pass over an input
    // nothing can prune. The sizing invariants above stay asserted so
    // re-enabling the knob cannot re-enable the wrong-unit bug.
    assert_eq!(super::coordinator_slice_target(TailPass::Repair, 1, 40 * MB), None, "repair rewrites in one pass");
    // A multi-file pack bin slices to the SORT budget. This asserted `None` until
    // 2026-09-19, and that was the whole reason a bin which could not fit its
    // sort had to be shrunk to a pair instead: with no slice to fall back on,
    // the packing cap was the only lever left.
    assert_eq!(
        super::coordinator_slice_target(TailPass::Pack, 2, 40 * MB),
        Some(crate::config::coordinator_per_sort_decoded_bytes() * 3 / 5),
        "a multi-file bin must bound its SORT by slicing, not its bin by shrinking"
    );
}

/// Candidates arrive in EVENT-TIME order, so a single budget-filling file
/// arriving first must not break the loop and leave a 1:1 rewrite that the
/// `< 2` guard then discards.
#[test]
fn packing_takes_the_small_files_even_when_a_large_one_sorts_first() {
    // Event-time order puts the 252 MB file first; the rest are tiny.
    let cell = vec![mb_file("big-252", 252, true), mb_file("t-a", 14, true), mb_file("t-b", 14, true), mb_file("t-c", 14, true)];
    let picked = coordinator_pick(cell);
    assert!(picked.len() >= 2, "a unit must merge the small files rather than retire empty, got {picked:?}");
    assert!(!picked.contains(&"big-252".to_string()), "the file that fills the budget alone must not preempt the pack: {picked:?}");
}

/// Coordinator sealed units must fit their deadline and file-count
/// contract: a physical run is at least 256 MiB, and the deadline must leave
/// the conservative rewrite floor time to commit it.
#[test]
fn coordinator_sealed_units_fit_their_deadline_and_file_count_contract() {
    assert_eq!(super::COORDINATOR_SEALED_TARGET_BYTES, 256 * MB);
    assert_eq!(super::COORDINATOR_HOT_TARGET_BYTES, 256 * MB);

    let estimated_rewrite_seconds = (super::COORDINATOR_SEALED_TARGET_BYTES + super::INPUT_BYTES_PER_SEC - 1) / super::INPUT_BYTES_PER_SEC;
    assert!(
        estimated_rewrite_seconds + 120 <= super::COORDINATOR_FILE_REWRITE_TIMEOUT.as_secs() as i64,
        "one sealed run needs {estimated_rewrite_seconds}s at the measured floor plus commit margin"
    );
    assert_eq!(
        super::coordinator_operation_timeout(crate::maintenance_coordinator::Operation::Dedup),
        super::COORDINATOR_STANDARD_UNIT_TIMEOUT,
        "a longer packing deadline must not widen dedup's fairness bound"
    );
    // Rollup builds get the longer deadline: their cost tracks the input
    // file count, which narrowing the slice cannot reduce while a sealed
    // day's files are unsorted and each spans it.
    for operation in [crate::maintenance_coordinator::Operation::BaseRollup, crate::maintenance_coordinator::Operation::DerivedRollup] {
        assert_eq!(
            super::coordinator_operation_timeout(operation),
            super::COORDINATOR_FILE_REWRITE_TIMEOUT,
            "{operation:?} must outlive the 300s bound a whale day cannot meet"
        );
    }
    assert_eq!(super::coordinator_operation_timeout(crate::maintenance_coordinator::Operation::SealedConsolidation), super::COORDINATOR_FILE_REWRITE_TIMEOUT);
}

/// A repair pass rewrites the NEWEST poisoned file (the poison is
/// all-or-nothing over a query window, so the most recent footer-less date is
/// the wall a user's time predicate hits), smallest-first within a date so one
/// un-finishable giant cannot head-of-line block everything behind it, and
/// takes ONE file rather than a bin or it spends the repair budget on the
/// packing cron's job. A SMALL sealed footer-less file is repair work too: one
/// unsorted file voids its date's scan ordering regardless of size
/// (`derive_common_ordering` is all-or-nothing) and Pack has no sealed dates in
/// scope, so nothing else would ever rewrite it.
#[test_case(&[("may", 900, 10), ("july", 900, 500), ("june", 950, 100)] => "july" ; "newest poisoned file first")]
#[test_case(&[("huge", 999, 500), ("small", 880, 500)] => "small" ; "smallest first within a date")]
#[test_case(&[("poison", 900, 500), ("a", 10, 600), ("b", 10, 700)] => "b" ; "one file, the newest — a repair pass never packs")]
#[test_case(&[("tiny_old", 10, 100), ("tiny_new", 10, 900)] => "tiny_new" ; "a small sealed footer-less file is still repair work, newest first")]
#[test_case(&[] => "" ; "nothing admitted => no work")]
fn repair_pass_takes_the_newest_poisoned_file_then_the_smallest(files: &[(&str, i64, i64)]) -> String {
    let adds: Vec<_> = files.iter().map(|&(path, size, min)| tail_file(path, size, false, Some((min, min + 1)), false, None)).collect();
    tail_bin(&adds, 2, TailPass::Repair).join(",")
}

/// Scope admission for the hot tail. Sealed-date repair exists so an
/// unsorted file that survived midnight does not force full-set dedup on
/// every query whose window crosses that date.
#[test]
fn hot_bin_admits_repairs_sealed_dates_without_rebinning_them() {
    const TARGET: i64 = 1000; // → converged at 7/8 = 875
    const REPAIR_MAX: i64 = 2000; // hot ticks repair up to here; larger is off-box CLI work
    let today = "date=2026-08-06/";
    let repair: Vec<String> = vec!["date=2026-08-05/".into()];
    let p = |d: &str| format!("timefusion/otel_logs_and_spans/project_id=abc/{d}part-x.parquet");
    let cleared: dashmap::DashSet<String> = dashmap::DashSet::new();
    let failures: dashmap::DashMap<String, u32> = dashmap::DashMap::new();
    let policy = |repair_dates: &'static [String]| super::HotBinPolicy {
        repair_dates,
        target_size: TARGET,
        min_files: 2,
        sorted_run_cap: TARGET / 2,
        repair_max_bytes: REPAIR_MAX,
        pass: TailPass::Pack,
        verified_sorted: &cleared,
        failures: &failures,
    };
    let with_repair = super::HotBinPolicy { repair_dates: &repair, ..policy(&[]) };
    let admits = |path: &str, size, sorted, repairable| super::hot_bin_admits(path, today, &repair, size, sorted, repairable, &with_repair);

    // TODAY — unchanged behaviour.
    assert!(admits(&p(today), 10, false, true), "small file today packs");
    assert!(!admits(&p(today), 900, true, true), "converged sorted run today is done");
    assert!(admits(&p(today), 900, false, true), "converged UNSORTED today is a repair candidate");

    // A candidate that keeps failing to stage is PARKED, so the queue behind
    // it drains.
    let sealed = p("date=2026-08-05/");
    assert!(admits(&sealed, 900, false, true), "sealed unsorted file is repair work");
    for n in 1..super::REPAIR_QUARANTINE_AFTER {
        failures.insert(sealed.clone(), n);
        assert!(admits(&sealed, 900, false, true), "still eligible after {n} failure(s) — transient failures must not cost eligibility");
    }
    failures.insert(sealed.clone(), super::REPAIR_QUARANTINE_AFTER);
    assert!(!admits(&sealed, 900, false, true), "parked once it has failed REPAIR_QUARANTINE_AFTER times");
    // Parking is per FILE, not per project or date — its neighbours stay eligible.
    assert!(admits(&p("date=2026-08-05/other-"), 900, false, true), "a different file on the same sealed date is unaffected");
    // A DETERMINISTIC failure (pool exhaustion) is worth the whole threshold:
    // the working set of a whole-file sort is a function of file size vs
    // pool, so it recurs every attempt, and at one repair pass per process a
    // 3-strike counter never fires.
    failures.clear();
    failures.insert(sealed.clone(), super::REPAIR_QUARANTINE_AFTER);
    assert!(!admits(&sealed, 900, false, true), "one deterministic failure parks it — a 3-strike rule never fires at the observed pass rate");
    failures.clear(); // the assertions below share these paths

    // SEALED, in the lookback window — repair only.
    let sealed = "date=2026-08-05/";
    assert!(admits(&p(sealed), 900, false, true), "the immortal file: converged + unsorted on a sealed date IS repaired");
    assert!(admits(&p(sealed), 10, false, true), "any unsorted file on a sealed date is repairable regardless of size");
    // The sort TAG grants no immunity: it records the optimizer's INTENT and
    // can disagree with the footer queries actually read.
    assert!(admits(&p(sealed), 10, true, true), "a tagged file is still only a SUSPECT; the footer decides");
    assert!(admits(&p(sealed), 900, true, true), "including a converged one");
    // Settled history still stays put — enforced by the footer read, which
    // records genuinely-sorted files so they are never offered again.
    cleared.insert(p(sealed));
    assert!(!admits(&p(sealed), 10, true, true), "a VERIFIED-sorted file is not re-binned");
    assert!(!admits(&p(sealed), 900, false, true), "verification outranks even an absent tag");
    cleared.remove(&p(sealed));
    assert!(!admits(&p(sealed), 900, false, false), "a table declaring no sort order has no footer to repair");
    // A legacy multi-GB file must NOT be dragged into a 5-minute tick; the
    // off-box CLI owns those.
    assert!(!admits(&p(sealed), REPAIR_MAX + 1, false, true), "an oversized legacy file is left to the off-box optimize CLI");
    assert!(admits(&p(sealed), REPAIR_MAX, false, true), "exactly at the ceiling is still hot-repairable");

    // OUTSIDE the lookback window — untouched at any size or tag.
    let old = "date=2026-07-01/";
    assert!(!admits(&p(old), 10, false, true), "an unsorted file outside the lookback is out of scope");
    assert!(!admits(&p(old), 900, false, true));

    // Lookback 0 restores today-only behaviour.
    assert!(!super::hot_bin_admits(&p(sealed), today, &[], 900, false, true, &policy(&[])), "empty repair window == the old today-only pass");

    // A REPAIR pass owns sealed dates and nothing else, or it spends a
    // budget sized for one whole-file rewrite on packing work.
    let repair_pass = super::HotBinPolicy { pass: TailPass::Repair, ..with_repair };
    let admits_repair = |path: &str, size, sorted| super::hot_bin_admits(path, today, &repair, size, sorted, true, &repair_pass);
    assert!(!admits_repair(&p(today), 10, false), "a repair pass never packs today");
    assert!(!admits_repair(&p(today), 900, false), "not even today's poisoned file — the Pack gap rule owns that");
    assert!(admits_repair(&p(sealed), 900, false), "sealed poisoned file is exactly its job");

    // The sort TAG is not the FOOTER: the flush path writes a correct
    // `sorting_columns` footer without stamping the tag, so an untagged file
    // is only a SUSPECT and admitting on the tag alone rewrites healthy
    // files. Once a footer read clears a suspect it is never offered again.
    cleared.insert(p(sealed));
    assert!(!admits_repair(&p(sealed), 900, false), "a suspect whose footer read came back SORTED is out of the candidate set");
}

/// Wave commit blast radius: ONE stale bin must lose only its own actions.
/// Naive batching would fail the whole wave (11 bins for one conflict).
#[test]
fn wave_drops_only_the_stale_bin() {
    let bins = vec![staged_unit("alpha", &["f1"], None), staged_unit("beta", &["f3"], None), staged_unit("gamma", &["f2", "f4"], None)];
    let (fresh, stale) = super::split_live_bins(bins, &active_without_dvs(&["f1", "f2", "f4"]));
    assert_eq!(project_ids(&fresh), vec!["alpha", "gamma"], "surviving bins still commit together");
    assert_eq!(project_ids(&stale), vec!["beta"]);
    // Surviving bins' actions concatenate in removes-then-adds order per bin.
    let actions: Vec<_> = fresh.iter().flat_map(|b| b.removes.iter().chain(&b.adds)).collect();
    assert_eq!(actions.len(), 5);
}

fn active_without_dvs(paths: &[&str]) -> super::ActiveFiles {
    super::ActiveFiles(paths.iter().map(|path| (path.to_string(), vec![None])).collect())
}

fn project_ids(bins: &[super::StagedBin]) -> Vec<&str> {
    bins.iter().map(|b| b.project_id.as_str()).collect()
}

fn test_add(path: &str) -> deltalake::kernel::Add {
    deltalake::kernel::Add { path: path.to_string(), size: 1024, modification_time: 0, data_change: true, ..Default::default() }
}

/// A snapshot that lists one file twice must still map to one target, or the
/// planners' `targets.len() != files.len()` check makes every plan mismatch.
#[test]
fn dedup_adds_by_path_collapses_a_duplicated_snapshot_entry() {
    let dup = vec![test_add("a.parquet"), test_add("b.parquet"), test_add("a.parquet")];
    let targets = super::dedup_adds_by_path(dup.into_iter(), "otel_metrics");
    assert_eq!(targets.len(), 2, "one Add per distinct path");
    let mut paths: Vec<&str> = targets.iter().map(|a| a.path.as_str()).collect();
    paths.sort();
    assert_eq!(paths, ["a.parquet", "b.parquet"]);
    // Order-preserving for the already-clean case.
    let clean = vec![test_add("b.parquet"), test_add("a.parquet")];
    let targets = super::dedup_adds_by_path(clean.into_iter(), "otel_metrics");
    assert_eq!(targets.iter().map(|a| a.path.as_str()).collect::<Vec<_>>(), ["b.parquet", "a.parquet"]);
}

fn staged_unit(project: &str, paths: &[&str], dedup: Option<super::DedupUnit>) -> super::StagedBin {
    let targets: Vec<_> = paths.iter().map(|p| test_add(p)).collect();
    let staged = vec![deltalake::kernel::Action::Add(test_add(&format!("{project}-new.parquet")))];
    let (removes, adds) = super::staged_actions(&targets, staged, dedup.is_some());
    super::StagedBin {
        sorted: false,
        discardable_paths: Vec::new(),
        project_id: project.to_string(),
        wave_id: format!("wave-{project}"),
        targets,
        removes,
        adds,
        stage_store: Arc::new(object_store::memory::InMemory::new()),
        dedup,
    }
}

#[test]
fn committed_wave_adds_map_to_exact_live_tantivy_uris() {
    let mut alpha = staged_unit("alpha", &["old-a.parquet"], None);
    let mut beta = staged_unit("beta", &["old-b.parquet"], None);
    let alpha_rel = "project_id=alpha/date=2026-08-16/alpha-new.parquet";
    let beta_rel = "project_id=beta/date=2026-08-16/beta-new.parquet";
    if let deltalake::kernel::Action::Add(add) = &mut alpha.adds[0] {
        add.path = alpha_rel.into();
    }
    if let deltalake::kernel::Action::Add(add) = &mut beta.adds[0] {
        add.path = beta_rel.into();
    }
    let alpha_uri = format!("s3://bucket/table/{alpha_rel}");
    let beta_uri = format!("s3://bucket/table/{beta_rel}");
    let unrelated = "s3://bucket/table/project_id=gamma/date=2026-08-16/other.parquet".to_owned();

    assert_eq!(
        super::wave_added_parquet(&[alpha, beta], &[unrelated, beta_uri.clone(), alpha_uri.clone()]),
        vec![("alpha".into(), alpha_rel.into(), alpha_uri), ("beta".into(), beta_rel.into(), beta_uri)],
        "only this wave's live Adds become committed-file index jobs"
    );
}

/// A wave whose commit fails must never delete parquet another instance has
/// already made LIVE — `probe_commit_landed` is all-or-nothing over the
/// wave, so a part-resumed wave reports `NotLanded`. A genuinely orphaned
/// bin must still be cleaned up, or "delete nothing" would pass.
#[tokio::test]
async fn wave_discard_spares_adds_committed_by_another_instance() {
    use object_store::ObjectStoreExt;
    let path_of = |bin: &super::StagedBin| match &bin.adds[0] {
        deltalake::kernel::Action::Add(add) => add.path.to_string(),
        other => panic!("unexpected action {other:?}"),
    };
    // [0] = resumed and committed by instance B; [1] = in no commit at all.
    let bins = vec![staged_unit("alpha", &["old-a.parquet"], None), staged_unit("beta", &["old-b.parquet"], None)];
    let stores: Vec<_> = bins.iter().map(|b| b.stage_store.clone()).collect();
    let paths: Vec<_> = bins.iter().map(|b| object_store::path::Path::from(path_of(b))).collect();
    for (store, path) in stores.iter().zip(&paths) {
        store.put(path, object_store::PutPayload::from_static(b"parquet")).await.unwrap();
    }
    super::discard_bin_parquet(&bins, &[path_of(&bins[0])].into_iter().collect()).await;
    assert!(stores[0].head(&paths[0]).await.is_ok(), "live parquet must survive a failed wave commit");
    assert!(stores[1].head(&paths[1]).await.is_err(), "parquet in no commit is still reclaimed");
}

fn dedup_unit(date: &str, before: u64, after: u64) -> super::DedupUnit {
    super::DedupUnit { key: None, date: date.to_string(), label: "chunk".into(), before, after }
}

/// The shared wave commit: hot compaction preserves rows (data_change=false
/// → snapshot-isolation downgrade → Optimize), dedup DROPS rows
/// (data_change=true → honest OCC → Write). Flipping either is a
/// correctness bug, not a tuning choice.
#[test]
fn staged_actions_carry_data_change_per_engine() {
    use deltalake::{kernel::Action, protocol::DeltaOperation};
    let flags = |bin: &super::StagedBin| -> Vec<bool> {
        bin.removes
            .iter()
            .chain(bin.adds.iter())
            .map(|a| match a {
                Action::Remove(r) => r.data_change,
                Action::Add(add) => add.data_change,
                other => panic!("unexpected action {other:?}"),
            })
            .collect()
    };
    let hot = staged_unit("alpha", &["f1", "f2"], None);
    assert_eq!(flags(&hot), vec![false; 3], "compaction Removes AND Adds must be data-preserving");
    let dedup = staged_unit("beta", &["f3"], Some(dedup_unit("2026-07-28", 10, 7)));
    assert_eq!(flags(&dedup), vec![true; 2], "a row-dropping rewrite must not claim to preserve data");
    // The operation is derived from the same flag so the two can't drift.
    assert!(matches!(super::wave_operation(false, 256, None), DeltaOperation::Optimize { .. }));
    assert!(matches!(super::wave_operation(true, 256, Some(vec!["date".into()])), DeltaOperation::Write { .. }));
}

/// A dedup wave has the same blast radius as a hot wave: the unit whose
/// target file was rewritten concurrently drops out alone. Dropped-row
/// accounting must survive that PARTIAL wave — only landed units removed
/// rows, and counting a stale unit would certify a bin that still holds
/// duplicates.
#[test]
fn dedup_wave_drops_only_the_stale_unit_and_counts_only_landed_rows() {
    let units = vec![
        staged_unit("alpha", &["f1"], Some(dedup_unit("2026-07-28", 10, 6))), // drops 4
        staged_unit("beta", &["f2"], Some(dedup_unit("2026-07-28", 100, 1))), // f2 gone: never lands
        staged_unit("gamma", &["f4"], Some(dedup_unit("2026-07-27", 8, 8))),
    ];
    let (fresh, stale) = super::split_live_bins(units, &active_without_dvs(&["f1", "f4"]));
    assert_eq!(project_ids(&fresh), vec!["alpha", "gamma"]);
    assert_eq!(project_ids(&stale), vec!["beta"]);
    assert_eq!(super::wave_dropped_rows(&fresh), 4);
    assert_eq!(super::wave_dropped_rows(&stale), 99, "the stale unit's rewrite is real but uncommitted — never added to the metric");
    // Hot bins carry no dedup accounting at all.
    assert_eq!(super::wave_dropped_rows(&[staged_unit("delta", &["f9"], None)]), 0);
}

/// "Targets gone" has two causes needing opposite handling: another writer
/// rewrote them (staged parquet is garbage → delete), or our own earlier
/// attempt landed then errored (staged parquet is live → never delete,
/// credit the bin). The bin's own Adds tell them apart.
#[test]
fn a_bin_whose_own_adds_are_live_is_self_landed_not_stale() {
    let alpha = staged_unit("alpha", &["f1"], Some(dedup_unit("2026-07-28", 10, 6)));
    let beta = staged_unit("beta", &["f2"], Some(dedup_unit("2026-07-28", 5, 4)));
    // alpha's commit LANDED: its target is gone AND its staged file is now
    // active. beta's target was rewritten by someone else.
    let live = active_without_dvs(&["alpha-new.parquet"]);
    let (fresh, stale) = super::split_live_bins(vec![alpha, beta], &live);
    assert!(fresh.is_empty(), "neither bin's targets survive");
    let (self_landed, stale): (Vec<_>, Vec<_>) = stale.into_iter().partition(|b| super::bin_adds_live(b, &live));
    assert_eq!(project_ids(&self_landed), vec!["alpha"], "a landed bin must never be discarded");
    assert_eq!(project_ids(&stale), vec!["beta"]);
    assert_eq!(super::wave_dropped_rows(&self_landed), 4);
    // A bin whose targets ARE live is never mistaken for self-landed.
    assert!(!super::bin_adds_live(&staged_unit("alpha", &["f1"], None), &active_without_dvs(&["f1"])));
}

/// A commit-lock holder must free the lock on a bounded schedule even when
/// the object store never answers, and must route the abandoned commit to
/// the UNCONFIRMED-landing branch (leave staged parquet, requeue the bins) —
/// never to `NotLanded`, which authorizes deleting files a landed commit
/// may reference.
#[tokio::test(flavor = "multi_thread")]
async fn a_timed_out_commit_frees_the_lock_and_lands_unconfirmed() {
    let lock: Arc<tokio::sync::Mutex<()>> = Arc::default();
    let started = std::time::Instant::now();
    let failure = {
        let _guard = lock.lock().await;
        assert!(lock.try_lock().is_err(), "held while the commit is in flight");
        super::bounded_commit_await(
            std::time::Duration::from_millis(50),
            "wave_commit",
            "otel_logs_and_spans",
            futures::future::pending::<std::result::Result<(), String>>(),
        )
        .await
        .expect_err("a never-answering commit must be abandoned")
    };
    assert!(failure.timed_out, "the failure must be marked as an abandoned await, not a plain error");
    assert!(started.elapsed() < std::time::Duration::from_secs(5), "the bound, not the store, decides when the lock is freed");
    assert!(lock.try_lock().is_ok(), "the next committer must find the lock free");
    // The routing decision the wave/flush paths make with `timed_out`.
    assert!(matches!(super::probe_after_timeout(super::CommitProbe::NotLanded, failure.timed_out), super::CommitProbe::Inconclusive));
    assert!(matches!(super::probe_after_timeout(super::CommitProbe::Inconclusive, true), super::CommitProbe::Inconclusive));
    // Positive evidence still passes through — a landed commit is credited.
    assert!(matches!(super::probe_after_timeout(super::CommitProbe::Landed, true), super::CommitProbe::Landed));
    // Without a timeout, a probe's "did not land" is trusted (that IS how
    // orphaned staged parquet gets reclaimed).
    assert!(matches!(super::probe_after_timeout(super::CommitProbe::NotLanded, false), super::CommitProbe::NotLanded));
}

/// The backstop must be invisible on the happy path and must not swallow
/// ordinary commit errors into the unconfirmed branch.
#[tokio::test]
async fn bounded_commit_await_passes_success_and_errors_through() {
    let ok: std::result::Result<u8, super::CommitFailure> =
        super::bounded_commit_await(std::time::Duration::from_secs(30), "flush_commit", "t", async { Ok::<_, String>(7u8) }).await;
    assert_eq!(ok.map_err(|e| e.message), Ok(7));
    let err = super::bounded_commit_await(std::time::Duration::from_secs(30), "flush_commit", "t", async { Err::<(), _>("version already exists") })
        .await
        .expect_err("errors propagate");
    assert!(!err.timed_out, "a real commit error must keep its normal (probe/OCC) classification");
    assert_eq!(err.message, "version already exists");
}

/// The staged-intent manifest is a cleanup aid, never a correctness input:
/// a torn tail from an unclean shutdown must cost entries, not a boot.
#[test]
fn staged_intent_manifest_skips_garbage_lines() {
    let contents = concat!(
        r#"{"wave_id":"w1","project_id":"a","paths":["p1","p2"]}"#,
        "\n",
        "not json at all\n",
        "\n",
        r#"{"wave_id":"w2","project_id":"b","paths":["p3"]"#, // torn tail, no newline
    );
    let entries = super::parse_staged_intents(contents);
    assert_eq!(entries.len(), 1, "only the intact line survives: {entries:?}");
    assert_eq!(entries[0].paths, vec!["p1", "p2"]);
    // Pre-resume lines carry neither resume field nor an instance id, which
    // keeps them cleanup-only and on the wall-clock gate.
    assert!(entries[0].target_paths.is_empty() && entries[0].adds.is_empty() && entries[0].instance.is_none());
    assert!(super::parse_staged_intents("").is_empty());
    assert!(super::parse_staged_intents("garbage").is_empty());
}

/// Boot reconcile deletes ONLY what the Delta log doesn't reference — a
/// referenced path belongs to a wave that committed, and deleting it would
/// destroy live data.
#[test]
fn staged_orphan_deletions_spares_committed_files_and_foreign_tables() {
    // Cleanup-only entries: `resume_intent` with no targets and no adds.
    let e = |wave: &str, table: &str, age_secs: u64, paths: &[&str]| super::StagedIntent {
        wave_id: wave.into(),
        table_name: table.into(),
        recorded_at: 100_000 - age_secs,
        paths: paths.iter().map(|s| s.to_string()).collect(),
        ..resume_intent(&[], Vec::new())
    };
    let old = super::STAGED_INTENT_MIN_AGE_SECS + 1;
    let entries = vec![
        e("w1", "logs", old, &["committed", "orphan1"]),
        e("w2", "logs", old, &["orphan2"]),
        // Another table's entry: NOT this reconcile's to judge — its paths
        // never appear in this table's snapshot and must not be deleted.
        e("w3", "metrics", old, &["metrics_staged"]),
        // Young entry: may belong to a live instance on a shared volume
        // (rolling deploy) — left alone.
        e("w4", "logs", 10, &["young_staged"]),
    ];
    let live = |paths: &[&str]| -> HashSet<String> { paths.iter().map(|s| s.to_string()).collect() };
    assert_eq!(super::staged_orphan_deletions(&entries, "logs", 100_000, &live(&["committed", "unrelated"])), vec!["orphan1", "orphan2"]);
    // Nothing to delete when every staged file landed.
    assert!(super::staged_orphan_deletions(&entries, "logs", 100_000, &live(&["committed", "orphan1", "orphan2"])).is_empty());
}

fn resume_add(path: &str, rows: i64) -> deltalake::kernel::Add {
    deltalake::kernel::Add { path: path.into(), size: 10, stats: Some(format!(r#"{{"numRecords":{rows}}}"#)), ..Default::default() }
}

fn resume_intent(targets: &[&str], adds: Vec<deltalake::kernel::Add>) -> super::StagedIntent {
    super::StagedIntent {
        wave_id: "w1".into(),
        table_name: "logs".into(),
        project_id: "p".into(),
        recorded_at: 100_000 - (super::STAGED_INTENT_MIN_AGE_SECS + 1),
        paths: adds.iter().map(|a| a.path.clone()).collect(),
        target_paths: targets.iter().map(|s| s.to_string()).collect(),
        adds,
        rollup: None,
        instance: None,
    }
}

fn resume_live<'a>(files: &[(&'a str, Option<i64>)]) -> HashMap<&'a str, Option<i64>> {
    files.iter().copied().collect()
}

/// A staged repair only commits when EVERY input is still live and the rows
/// add up. Everything else declines and leaves the work for reconcile —
/// declining costs a re-stage, committing wrongly costs rows.
#[test]
fn resume_commits_only_a_row_preserving_rewrite_of_still_live_inputs() {
    use super::ResumeVerdict::*;
    let live = resume_live(&[("in1", Some(30)), ("in2", Some(70))]);
    let ok = resume_intent(&["in1", "in2"], vec![resume_add("out1", 100)]);
    assert_eq!(super::classify_resume(&ok, "logs", 100_000, &live), Commit);
    // Another table's entry is not this reconcile's to judge.
    assert_eq!(super::classify_resume(&ok, "metrics", 100_000, &live), Skip);
    // Young and OURS: this process may still be staging it.
    let young = super::StagedIntent { recorded_at: 100_000 - 10, ..ok.clone() };
    assert_eq!(super::classify_resume(&young, "logs", 100_000, &live), Skip);
    // Pre-resume (and dedup) entries carry neither field: cleanup-only.
    let legacy = super::StagedIntent { target_paths: Vec::new(), adds: Vec::new(), ..ok.clone() };
    assert_eq!(super::classify_resume(&legacy, "logs", 100_000, &live), Skip);
    // An input was rewritten underneath us — the output is garbage.
    assert_eq!(super::classify_resume(&ok, "logs", 100_000, &resume_live(&[("in1", Some(30))])), Stale);
    // Truncated staging: the case that would silently drop 30 rows.
    let short = resume_intent(&["in1", "in2"], vec![resume_add("out1", 70)]);
    assert_eq!(super::classify_resume(&short, "logs", 100_000, &live), RowMismatch { target_rows: 100, staged_rows: 70 });
    // Unverifiable is not the same as verified: no stats ⇒ never commit.
    let no_stats = resume_intent(&["in1", "in2"], vec![deltalake::kernel::Add { path: "out1".into(), size: 10, ..Default::default() }]);
    assert_eq!(super::classify_resume(&no_stats, "logs", 100_000, &live), RowMismatch { target_rows: 100, staged_rows: -1 });
    assert_eq!(
        super::classify_resume(&ok, "logs", 100_000, &resume_live(&[("in1", None), ("in2", Some(70))])),
        RowMismatch { target_rows: -1, staged_rows: 100 }
    );
    // Staged parquet is uuid-named, so its presence in the snapshot can only
    // mean OUR commit landed — clear the intent, never re-commit.
    let landed = resume_intent(&["in1"], vec![resume_add("out1", 100)]);
    assert_eq!(super::classify_resume(&landed, "logs", 100_000, &resume_live(&[("out1", Some(100))])), AlreadyLanded);
}

/// Resume is gated on OWNERSHIP, not on age: a previous instance's intent is
/// eligible the moment it is seen, while our own (possibly still in flight)
/// and a pre-upgrade entry that cannot say whose it is keep the wall-clock gate.
#[test]
fn a_previous_instances_staged_intent_resumes_without_waiting_out_the_age_gate() {
    let ok = resume_intent(&["in1", "in2"], vec![resume_add("out1", 100)]);
    let live = resume_live(&[("in1", Some(30)), ("in2", Some(70))]);
    assert_ownership_ladder(&ok, |entry: &super::StagedIntent| super::classify_resume(entry, "logs", 100_000, &live));
}

/// The ownership ladder BOTH resume classifiers obey through
/// `resume_guarded`, asserted at a fixed `now_secs` of 100_000.
fn assert_ownership_ladder(base: &super::StagedIntent, classify: impl Fn(&super::StagedIntent) -> super::ResumeVerdict) {
    use super::ResumeVerdict::*;
    let owned_by =
        |instance: Option<&str>, age: u64| super::StagedIntent { instance: instance.map(str::to_string), recorded_at: 100_000 - age, ..base.clone() };
    let (ours, old) = (crate::observability::instance_id(), super::STAGED_INTENT_MIN_AGE_SECS + 1);
    assert_eq!(classify(&owned_by(Some("a-dead-instance"), 10)), Commit);
    assert_eq!(classify(&owned_by(Some(ours), 10)), Skip);
    assert_eq!(classify(&owned_by(None, 10)), Skip, "no id: nothing to compare, so the wall-clock proxy stands");
    assert_eq!(classify(&owned_by(None, old)), Commit);
    assert_eq!(classify(&owned_by(Some(ours), old)), Commit, "our own unit requeued by its own deadline resumes eventually, not never");
}

/// The rollup half of resume, decided WITHOUT IO. A rollup aggregates, so
/// row preservation cannot be the test; source-stillness and
/// no-other-coverage stand in for it.
#[test]
fn a_rollup_resumes_only_when_the_source_held_still_and_nothing_else_covers_the_slice() {
    use super::ResumeVerdict::*;
    let slice = crate::maintenance_coordinator::TimeSlice::new(1_000, 2_000).expect("slice");
    let rollup = |source_rows: Option<u64>| super::RollupResume {
        key: crate::maintenance_coordinator::TaskKey {
            physical_table: "logs".into(),
            source: "otel_logs_and_spans".into(),
            project_id: "p".into(),
            slice,
            operation: crate::maintenance_coordinator::Operation::BaseRollup,
        },
        publication: crate::maintenance_coordinator::Publication {
            source_fingerprint: 7,
            generation: "g1".into(),
            rows: 5,
            source_rows,
            source_rows_below: None,
        },
        source_rows,
        date: "2026-08-24".into(),
    };
    let intent = |targets: &[&str], source_rows: Option<u64>| super::StagedIntent {
        rollup: Some(rollup(source_rows)),
        ..resume_intent(targets, vec![resume_add("out1", 5)])
    };
    let live = |files: &[(&'static str, Option<(i64, i64)>)]| files.iter().copied().collect::<HashMap<&str, Option<(i64, i64)>>>();
    let inside = live(&[("in1", Some((1_000, 2_000)))]);
    let ok = intent(&["in1"], Some(100));

    assert_eq!(super::classify_rollup_resume(&ok, "logs", 100_000, &inside, Some(100)), Commit);
    // A repair/dedup entry carries no rollup evidence and must never be
    // committed by this path.
    let repair = resume_intent(&["in1"], vec![resume_add("out1", 5)]);
    assert_eq!(super::classify_rollup_resume(&repair, "logs", 100_000, &inside, Some(100)), Skip);
    // The source moved: the read path would refuse this slice as
    // `stale_coverage`, so committing it risks a wrong number for nothing.
    assert_eq!(super::classify_rollup_resume(&ok, "logs", 100_000, &inside, Some(101)), SourceMoved);
    // No witness on either side is unverifiable, which is NOT the same as
    // verified — the rule the read path applies to a witness-less slice.
    assert_eq!(super::classify_rollup_resume(&intent(&["in1"], None), "logs", 100_000, &inside, Some(100)), SourceMoved);
    assert_eq!(super::classify_rollup_resume(&ok, "logs", 100_000, &inside, None), SourceMoved);
    // A live file overlapping this slice that the staged output does NOT
    // replace would stay live beside it and be SUMMED with it. An untagged
    // file (`slice: None`) claims no range and cannot double-count.
    let overlapping = live(&[("in1", Some((1_000, 2_000))), ("other", Some((1_500, 2_500)))]);
    assert_eq!(super::classify_rollup_resume(&ok, "logs", 100_000, &overlapping, Some(100)), WouldDoubleCount);
    let adjacent = live(&[("in1", Some((1_000, 2_000))), ("later", Some((2_000, 3_000)))]);
    assert_eq!(super::classify_rollup_resume(&ok, "logs", 100_000, &adjacent, Some(100)), Commit, "slice ends are exclusive; touching is not overlapping");
    let untagged = live(&[("in1", Some((1_000, 2_000))), ("legacy", None)]);
    assert_eq!(super::classify_rollup_resume(&ok, "logs", 100_000, &untagged, Some(100)), Commit);
    // An input that left the snapshot entirely.
    assert_eq!(super::classify_rollup_resume(&ok, "logs", 100_000, &live(&[]), Some(100)), Stale);
    // Our own output is live ⇒ the commit landed and only the journal
    // publication was lost. The caller publishes; it must never re-commit.
    let landed = live(&[("out1", Some((1_000, 2_000)))]);
    assert_eq!(super::classify_rollup_resume(&ok, "logs", 100_000, &landed, Some(100)), AlreadyLanded);
    // Ownership, not age — the same ladder `classify_resume` obeys.
    assert_ownership_ladder(&ok, |entry: &super::StagedIntent| super::classify_rollup_resume(entry, "logs", 100_000, &inside, Some(100)));
}

/// The Add stats column list must be the narrow prune set, not the whole schema, and must never
/// include partition columns.
#[test]
fn stats_columns_are_the_prune_keys_only() {
    let schema = schema_or_default("otel_logs_and_spans");
    let stats_columns = super::stats_columns_for(schema);
    let cols: Vec<&str> = stats_columns.split(',').collect();
    assert!(cols.contains(&schema.time_column_name()), "the time column drives every query and the event-time binning");
    for key in &schema.dedup_keys {
        assert!(cols.contains(&key.as_str()), "dedup key {key} must keep stats");
    }
    for partition in &schema.partitions {
        assert!(!cols.contains(&partition.as_str()), "partition column {partition} is in the path, not the stats");
    }
    assert!(cols.len() < schema.fields.len(), "must be a strict subset of the schema, got {} of {}", cols.len(), schema.fields.len());
    assert_eq!(cols.len(), cols.iter().collect::<HashSet<_>>().len(), "no duplicates");
}

/// A repair pass must be bounded by its budget, not by a wave count: a wave
/// serves one bin per project, so a flat cap limits a pass to a handful of
/// files per project however much budget remains.
#[test]
fn repair_waves_are_bounded_by_budget_not_by_the_pack_cap() {
    assert_eq!(super::max_waves(super::TailPass::Pack), 12, "packing's short-cron cap must not change");
    // A bad single date holds ~120 poisoned files; one pass must clear a date.
    assert!(
        super::max_waves(super::TailPass::Repair) >= 120,
        "a repair pass capped below a single date's backlog can never clear that date, whatever its budget"
    );
}

/// Scheduling knobs for `run_bins`. The default is a far deadline, three rounds,
/// serial admission and one commit per wave, so each test names only the dimension
/// it is about.
struct Sched {
    rounds: usize,
    concurrency: usize,
    deadline: std::time::Instant,
    commit_each_bin: bool,
    /// Notified after every commit, so a bin can block until a sibling committed.
    release: Option<Arc<tokio::sync::Notify>>,
}

impl Default for Sched {
    fn default() -> Self {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        Self { rounds: 3, concurrency: 1, deadline, commit_each_bin: false, release: None }
    }
}

/// Everything one `round_robin_bins` run observes: the `(project, round)` bins it
/// admitted in order, every `on_truncate(round, remaining)`, every commit's bin list,
/// and the failure count.
struct BinRun {
    calls: Vec<(String, usize)>,
    truncated: Vec<(usize, Vec<String>)>,
    committed: Vec<Vec<String>>,
    failed: usize,
}

async fn run_bins<F, Fut>(projects: &[&str], sched: Sched, brake: impl Fn() -> Option<super::Brake>, op: F) -> BinRun
where
    F: Fn(String, usize) -> Fut,
    Fut: std::future::Future<Output = Result<super::BinOutcome<String>>>,
{
    let (calls, truncated, committed) = (std::sync::Mutex::new(Vec::new()), std::sync::Mutex::new(Vec::new()), std::sync::Mutex::new(Vec::new()));
    let failed = super::round_robin_bins(
        projects.iter().map(|p| (*p).to_string()).collect(),
        sched.rounds,
        sched.concurrency,
        sched.deadline,
        |round, remaining: &[String]| truncated.lock().unwrap().push((round, remaining.to_vec())),
        brake,
        sched.commit_each_bin,
        |project_id, round| {
            calls.lock().unwrap().push((project_id.clone(), round));
            let outcome = op(project_id.clone(), round);
            async move { (project_id, outcome.await) }
        },
        |bins: Vec<String>, _round| {
            let (committed, release) = (&committed, sched.release.clone());
            async move {
                committed.lock().unwrap().push(bins);
                if let Some(release) = release {
                    release.notify_one();
                }
                0
            }
        },
    )
    .await;
    BinRun { calls: calls.into_inner().unwrap(), truncated: truncated.into_inner().unwrap(), committed: committed.into_inner().unwrap(), failed }
}

fn bin_staged(project_id: String) -> Result<super::BinOutcome<String>> {
    Ok(super::BinOutcome::Staged(project_id))
}

/// Round-robin fairness: every project must get its Nth bin before any
/// project gets its (N+1)th, or a short cron never reaches most projects.
#[tokio::test(flavor = "multi_thread")]
async fn round_robin_bins_gives_every_project_a_bin_before_anyone_gets_a_second() {
    // Every project has unbounded work, so only the round cap stops it.
    let run = run_bins(&["a", "b", "c"], Sched::default(), || None, |project_id, _| async move { bin_staged(project_id) }).await;
    assert_eq!(run.failed, 0);
    assert!(run.truncated.is_empty(), "must not truncate: deadline is far away");
    assert_eq!(
        run.calls,
        (0..3).flat_map(|round| ["a", "b", "c"].map(|p| (p.to_string(), round))).collect::<Vec<_>>(),
        "expected round-robin, got a per-project drain"
    );
}

/// A dedup sweep cut by its deadline must still reach every
/// `(date, project)` item across ticks: a cursor that resets, or one applied
/// without wrap, re-serves the head forever and starves the tail.
#[test]
fn a_truncated_dedup_sweep_walks_the_whole_work_list_across_ticks() {
    let (total, per_tick) = (386usize, 40usize); // ~193 projects x 2 dates
    let mut seen = vec![false; total];
    let mut cursor = 0usize;
    for _ in 0..total.div_ceil(per_tick) {
        let offset = super::sweep_resume_offset(total, cursor);
        for i in 0..per_tick {
            seen[(offset + i) % total] = true;
        }
        cursor += per_tick; // the cursor only ever grows, as `fetch_add(swept)` does
    }
    assert!(seen.iter().all(|s| *s), "a bounded sweep must still cover every partition; the tail was never served");
    // An empty list must not panic on the modulo.
    assert_eq!(super::sweep_resume_offset(0, 7), 0);
}

/// The build SQL writes `date = '2026-08-01'` — a string literal against a
/// `Date32` column. The predicate handling only works if DataFusion coerces
/// the literal to `Date32` rather than casting the column to `Utf8`.
#[tokio::test]
async fn the_builds_string_date_literal_survives_as_a_date32_window() {
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        datasource::MemTable,
        logical_expr::LogicalPlan,
        prelude::SessionContext,
    };
    let schema = Arc::new(Schema::new(vec![Field::new("project_id", DataType::Utf8, true), Field::new("date", DataType::Date32, false)]));
    let ctx = SessionContext::new();
    ctx.register_table("t", Arc::new(MemTable::try_new(schema, vec![vec![]]).unwrap())).unwrap();

    // Byte-for-byte the predicate `build_partition_sql` emits.
    let state = ctx.state();
    let plan = state.optimize(&state.create_logical_plan("SELECT * FROM t WHERE project_id = 'p' AND date = '2026-08-01'").await.unwrap()).unwrap();

    // Every predicate the plan still carries: standalone Filters and the ones pushed into the scan.
    fn predicates(plan: &LogicalPlan) -> Vec<Expr> {
        let own = match plan {
            LogicalPlan::Filter(f) => vec![f.predicate.clone()],
            LogicalPlan::TableScan(scan) => scan.filters.clone(),
            _ => Vec::new(),
        };
        own.into_iter().chain(plan.inputs().iter().flat_map(|input| predicates(input))).collect()
    }
    // Split conjunctions, as the scan path does before extracting a window.
    let conjuncts: Vec<Expr> = predicates(&plan).iter().flat_map(|f| datafusion::logical_expr::utils::split_conjunction(f).into_iter().cloned()).collect();

    let window = super::date_partition_window(&conjuncts);
    assert!(window.is_some(), "the optimized plan must still expose a Date32 equality; got {conjuncts:?}");
    // 2026-08-01 is 20666 days after the epoch.
    let days = chrono::NaiveDate::from_ymd_opt(2026, 8, 1).unwrap().signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days();
    assert_eq!(window, Some((days * 86_400_000_000, (days + 1) * 86_400_000_000 - 1)));
}

/// A date equality must resolve to a one-day window, covering its day
/// EXACTLY: reaching one microsecond into the next date asks
/// `dedup_window_clean` about an uncertified date and silently costs the skip.
#[test]
fn a_date_partition_equality_is_a_one_day_window() {
    use datafusion::prelude::{col, lit};
    const DAY: i64 = 86_400_000_000;
    let date = |days: i32| lit(ScalarValue::Date32(Some(days)));

    let (start, end) = super::date_partition_window(&[col("date").eq(date(20_000))]).expect("a date equality is a window");
    assert_eq!(start, 20_000 * DAY);
    assert_eq!(end, 20_001 * DAY - 1, "the window must stop one micro short of the next date");

    // Operands either way round, since the optimizer may swap them.
    assert_eq!(super::date_partition_window(&[date(20_000).eq(col("date"))]), Some((20_000 * DAY, 20_001 * DAY - 1)));
    // Beside the project filter a real rollup build carries.
    assert_eq!(super::date_partition_window(&[col("project_id").eq(lit("p")), col("date").eq(date(1))]), Some((DAY, 2 * DAY - 1)));
    // Anything that is not a date equality yields nothing, so the skip stays
    // denied rather than being granted over an unproven window.
    assert_eq!(super::date_partition_window(&[col("project_id").eq(lit("p"))]), None);
    assert_eq!(super::date_partition_window(&[col("date").gt(date(1))]), None);
    assert_eq!(super::date_partition_window(&[col("timestamp").eq(lit(1_i64))]), None);

    // What the INCLUSIVE end buys: `window_dates` must return exactly one
    // date. An exclusive end would land on the next day's midnight and deny
    // the skip whenever that date happened to be uncertified.
    for days in [0_i32, 1, 20_666, 25_000] {
        let (lo, hi) = super::date_partition_window(&[col("date").eq(date(days))]).expect("a window");
        let dates = super::window_dates(lo, hi).expect("resolvable");
        let expected = chrono::DateTime::from_timestamp_micros(lo).expect("in range").date_naive();
        assert_eq!(dates, vec![expected], "days={days} must ask about one date, got {dates:?}");
    }
}

/// A source whose partitions all fail expensively must not starve the
/// sources behind it. Each source has its own tick budget, so only rotation
/// can fix this — bounding the budget cannot.
#[test]
fn a_failing_source_does_not_starve_the_sources_behind_it() {
    let sources = ["otel_metrics", "otel_logs_and_spans", "otel_traces"];
    // Rotating left by the offset and taking the head IS indexing by the offset.
    let served: Vec<&str> = (0..sources.len()).map(|cursor| sources[super::sweep_resume_offset(sources.len(), cursor)]).collect();
    for source in sources {
        assert!(served.contains(&source), "`{source}` never got to run first across {} runs: {served:?}", sources.len());
    }
}

/// A repair bin that times out or panics must still clear its slot, or
/// `repair_bins_in_flight` reads "busy" forever.
#[test]
fn an_in_flight_repair_slot_clears_even_when_the_bin_unwinds() {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    let counter = AtomicU64::new(0);
    {
        let _guard = super::in_flight_guard(&counter);
        assert_eq!(counter.load(Relaxed), 1, "the gauge must SHOW the sort while it runs");
    }
    assert_eq!(counter.load(Relaxed), 0);
    let _ = std::panic::catch_unwind(|| {
        let _guard = super::in_flight_guard(&counter);
        panic!("bin exploded");
    });
    assert_eq!(counter.load(Relaxed), 0, "a panicking bin left the slot held");
}

/// A finished repair bin must commit without waiting for its slowest
/// sibling. The slow bin here only finishes once the fast bin's commit has
/// run, so a collect-then-commit implementation deadlocks this test.
#[tokio::test(flavor = "multi_thread")]
async fn a_finished_repair_bin_commits_without_waiting_for_its_slow_sibling() {
    let gate = Arc::new(tokio::sync::Notify::new());
    let sched = Sched { rounds: 1, concurrency: 2, commit_each_bin: true, release: Some(gate.clone()), ..Sched::default() };
    let run = run_bins(
        &["fast", "slow"],
        sched,
        || None,
        move |project_id, _| {
            let gate = gate.clone();
            async move {
                if project_id == "slow" {
                    gate.notified().await;
                }
                bin_staged(project_id)
            }
        },
    );
    let run = tokio::time::timeout(std::time::Duration::from_secs(10), run).await.expect("a finished bin must commit while its sibling is still staging");
    assert_eq!(run.failed, 0);
    assert_eq!(run.committed, vec![vec!["fast".to_string()], vec!["slow".to_string()]], "one commit per bin, in completion order");
}

/// Packing bins are many and quick, so one commit per bin drives long OCC
/// ladders: packing must land the whole wave in a single call.
#[tokio::test(flavor = "multi_thread")]
async fn packing_still_commits_the_whole_wave_in_one_call() {
    let sched = Sched { rounds: 1, concurrency: 3, ..Sched::default() };
    let run = run_bins(&["a", "b", "c"], sched, || None, |project_id, _| async move { bin_staged(project_id) }).await;
    assert_eq!(run.failed, 0);
    assert_eq!(run.committed.len(), 1, "packing commits ONCE per wave, got {} calls", run.committed.len());
    assert_eq!(run.committed[0].len(), 3, "all three bins in the one commit");
}

/// A dedup-raced bin (`Retry`) must not silence the project for the tick; it stays in rotation and
/// gets a fresh bin next round.
#[tokio::test(flavor = "multi_thread")]
async fn round_robin_bins_keeps_a_vanished_bin_project_in_rotation() {
    let run = run_bins(
        &["raced"],
        Sched::default(),
        || None,
        // Round 0: selection went stale under a concurrent rewrite.
        |project_id, round| async move { if round == 0 { Ok(super::BinOutcome::Retry) } else { bin_staged(project_id) } },
    )
    .await;
    assert_eq!(run.failed, 0);
    assert!(run.truncated.is_empty(), "must not truncate");
    assert!(run.committed.iter().all(|bins| !bins.is_empty()), "a wave commit must never be called with an empty bin list");
    assert_eq!(run.calls.iter().filter(|(p, _)| p == "raced").count(), 3, "raced project must be retried every round, not dropped");
}

/// A project whose tail is converged (`Ok(false)`) must stop consuming
/// rounds, so the remaining budget goes to projects that still have work.
#[tokio::test(flavor = "multi_thread")]
async fn round_robin_bins_drops_converged_and_failed_projects() {
    let run = run_bins(
        &["converged", "busy", "broken"],
        Sched::default(),
        || None,
        |project_id, _| async move {
            match project_id.as_str() {
                "converged" => Ok(super::BinOutcome::Converged),
                "broken" => Err(anyhow::anyhow!("boom")),
                _ => bin_staged(project_id),
            }
        },
    )
    .await;
    assert_eq!(run.failed, 1, "the erroring project counts once, then drops out");
    assert!(run.truncated.is_empty(), "must not truncate");
    let served = |project: &str| run.calls.iter().filter(|(p, _)| p == project).count();
    assert_eq!(served("converged"), 1, "converged project must not be retried");
    assert_eq!(served("broken"), 1, "failed project must not be retried");
    assert_eq!(served("busy"), 3, "busy project keeps its rounds");
}

/// The tick must stop starting rounds past its wall-clock budget rather than
/// overrunning its own cron period.
#[tokio::test(flavor = "multi_thread")]
async fn round_robin_bins_stops_at_the_tick_deadline() {
    let sched = Sched { rounds: 12, deadline: std::time::Instant::now(), ..Sched::default() }; // already expired
    let run = run_bins(&["a", "b"], sched, || None, |project_id, _| async move { bin_staged(project_id) }).await;
    assert_eq!(run.failed, 0);
    assert!(run.calls.is_empty(), "an expired deadline must start no bin at all");
    assert_eq!(run.truncated, vec![(0, vec!["a".to_string(), "b".to_string()])], "must truncate on round 0 with both projects still pending");
}

/// A brake engaging mid-round must stop new bins from being admitted, leaving them
/// pending for the next boundary check to truncate.
#[tokio::test(flavor = "multi_thread")]
async fn round_robin_bins_stops_admitting_bins_when_the_brake_engages_mid_round() {
    // Healthy at the round boundary; trips as soon as the first bin is admitted.
    let admitted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let gate = admitted.clone();
    // Serial admission (the default concurrency), so the trip point is deterministic.
    let run = run_bins(
        &["a", "b", "c"],
        Sched::default(),
        move || (gate.load(std::sync::atomic::Ordering::SeqCst) > 0).then_some(super::Brake::Stop("mem")),
        |project_id, _| {
            let admitted = admitted.clone();
            async move {
                admitted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                bin_staged(project_id)
            }
        },
    )
    .await;

    assert_eq!(run.calls.iter().map(|(p, _)| p.clone()).collect::<Vec<_>>(), vec!["a".to_string()], "only the bin admitted before the brake tripped may run");
    assert_eq!(
        run.truncated,
        vec![(1, vec!["a".to_string(), "b".to_string(), "c".to_string()])],
        "the deferred bins stay pending and the NEXT round's boundary check truncates the tick"
    );
}

/// A permanently engaged WAL brake must degrade rather than truncate: `Degrade` serves
/// every project serially (the deadline bounds the tick); `Stop` truncates.
#[tokio::test(flavor = "multi_thread")]
async fn round_robin_bins_degrade_serves_all_projects_serially() {
    let projects: &[&str] = &["head", "b", "c"];
    let run = move |brake: super::Brake| {
        run_bins(projects, Sched { concurrency: 3, ..Sched::default() }, move || Some(brake), |project_id, _| async move { bin_staged(project_id) })
    };

    let degraded = run(super::Brake::Degrade("wal")).await;
    let serial_round = |r: usize| [("head".to_string(), r), ("b".to_string(), r), ("c".to_string(), r)];
    assert_eq!(
        degraded.calls,
        [serial_round(0), serial_round(1), serial_round(2)].concat(),
        "degrade serves every project serially — the deadline, not a cut, bounds the tick"
    );
    assert!(degraded.truncated.is_empty(), "degrade alone cuts nothing; only the deadline truncates");

    let stopped = run(super::Brake::Stop("mem")).await;
    assert!(stopped.calls.is_empty(), "stop must start no work at all");
    assert_eq!(stopped.truncated, vec![(0, vec!["head".to_string(), "b".to_string(), "c".to_string()])]);
}

/// Cache keys are bucket-relative (inserts happen below the PrefixStore), so evict/contains
/// must join the table's in-bucket path back onto the table-relative file path; probing with
/// the bare relative path never matches and evictions silently become no-ops.
#[test]
fn bucket_cache_key_restores_the_table_path_segment() {
    let prefix = super::table_cache_prefix("s3://bucket/timefusion/otel_logs_and_spans/proj-1?endpoint=x");
    assert_eq!(prefix, "s3://bucket/timefusion/otel_logs_and_spans/proj-1");
    let table_path = super::table_path_in_bucket(prefix);
    assert_eq!(table_path, "timefusion/otel_logs_and_spans/proj-1");
    let rel = super::relativize_to_prefix(prefix, "s3://bucket/timefusion/otel_logs_and_spans/proj-1/date=2026-08-03/f.parquet").unwrap();
    assert_eq!(super::bucket_cache_key(table_path, &rel), "timefusion/otel_logs_and_spans/proj-1/date=2026-08-03/f.parquet");
    assert_eq!(super::bucket_cache_key("", &rel), "date=2026-08-03/f.parquet", "bucket-rooted tables keep the bare relative key");
}

#[test]
fn hot_project_ids_prioritize_the_most_fragmented_hot_partition() {
    let date = chrono::NaiveDate::from_ymd_opt(2026, 7, 16).unwrap();
    let uris = vec![
        "s3://b/t/project_id=alpha/date=2026-07-16/a.parquet".to_string(),
        "s3://b/t/project_id=beta/date=2026-07-16/b.parquet".to_string(),
        "s3://b/t/project_id=alpha/date=2026-07-16/c.parquet".to_string(),
        "s3://b/t/project_id=beta/date=2026-07-16/d.parquet".to_string(),
        "s3://b/t/project_id=beta/date=2026-07-16/e.parquet".to_string(),
        "s3://b/t/project_id=old/date=2026-07-15/f.parquet".to_string(),
        "s3://b/t/date=2026-07-16/g.parquet".to_string(),
    ];
    // beta has 3 files today, alpha 2 → beta first; the wrong-date and
    // missing-project_id URIs are excluded.
    assert_eq!(Database::hot_project_ids(&uris, date), vec!["beta", "alpha"]);
}

/// The ZOrder idempotence guard: identical file sets compare equal (partition skipped);
/// adding a file makes them differ (partition re-optimized).
#[test]
fn filesets_equal_only_when_unchanged() {
    let d = chrono::NaiveDate::from_ymd_opt(2026, 6, 6).unwrap();
    let base = vec!["s3://b/t/date=2026-06-06/a.parquet".to_string()];
    let plus = vec!["s3://b/t/date=2026-06-06/a.parquet".to_string(), "s3://b/t/date=2026-06-06/b.parquet".to_string()];
    let a = Database::filesets_for_dates(&base, &[d]);
    let b = Database::filesets_for_dates(&base, &[d]);
    let c = Database::filesets_for_dates(&plus, &[d]);
    assert_eq!(a[&d], b[&d]);
    assert_ne!(a[&d], c[&d]);
}

#[test]
fn sorted_rewrites_sort_by_timestamp_and_consolidation_dedups_opportunistically() {
    use deltalake::operations::optimize::OptimizeType;
    let schema = get_schema("otel_logs_and_spans").unwrap();
    let (optimize_type, declare_sorted) = choose_optimize_type(schema, false, true);
    assert!(matches!(optimize_type, OptimizeType::SortBy(_)));
    assert!(declare_sorted);

    let (optimize_type, declare_sorted) = consolidate_optimize_type(schema, true);
    let OptimizeType::SortByDedup(cols, dedup) = optimize_type else { panic!("expected SortByDedup") };
    assert_eq!(cols[0].column, "timestamp");
    assert_eq!(
        dedup.columns,
        vec!["timestamp", "resource___service___name", "id"],
        "the opportunistic dedup keys ARE the schema keys, which now lead the sort"
    );
    let tb = dedup.tiebreak.expect("tiebreak from schema");
    assert!(tb.column == "updated_at" && tb.descending);
    assert!(declare_sorted);
    // Sort disabled → plain Compact, no opportunistic dedup.
    assert!(matches!(consolidate_optimize_type(schema, false), (OptimizeType::Compact, false)));
}

/// Opportunistic per-bin keep-greatest cannot certify convergence: two versions can live in
/// separate target-sized sorted runs, which are terminal and never selected again.
#[test]
fn converged_sorted_runs_are_a_counterexample_to_compaction_dedup_convergence() {
    const TARGET: i64 = 1000;
    let run = |path: &str, min: i64| super::TailAdd {
        path: path.into(),
        size: 900,
        is_sorted_run: true,
        event_range: Some((min, min + 1)),
        rows: None,
        has_dv: false,
    };
    let versions_in_different_runs = vec![run("older-version", 1), run("newer-version", 1)];

    assert!(
        super::select_tail_bin(&versions_in_different_runs, TARGET, 2, i64::MAX, 10_000, TailPass::Pack).is_empty(),
        "target-sized runs are terminal, even when their key/time domains overlap"
    );
}

/// A narrow slice of a day-spanning file must estimate a narrow share — splitting only
/// works if bisecting a unit halves its estimated cost.
#[test]
fn a_narrow_slice_estimates_a_narrow_share_of_a_day_spanning_file() {
    use crate::maintenance_coordinator::TimeSlice;
    const DAY: i64 = 86_400_000_000;
    const TEN_MIN: i64 = 600_000_000;
    let (file_min, file_max) = (0, DAY - 1);

    let day = TimeSlice::new(0, DAY).expect("day slice");
    assert_eq!(slice_share_of_file(Some(file_min), Some(file_max), day, 1), (DAY as u64, DAY as u64), "a day-wide slice takes the whole file");

    // One row group: the pruning floor is the whole file; over-estimating is the safe direction.
    let narrow = TimeSlice::new(0, TEN_MIN).expect("ten-minute slice");
    assert_eq!(slice_share_of_file(Some(file_min), Some(file_max), narrow, 1), (DAY as u64, DAY as u64), "one row group cannot be pruned below");

    // A realistically-chunked file prunes: the share collapses to the slice's fraction.
    let (share, whole) = slice_share_of_file(Some(file_min), Some(file_max), narrow, 144);
    assert!(share * 100 < whole, "a ten-minute slice must estimate well under 1% of a day-spanning file, got {share}/{whole}");

    // Unknown bounds keep full weight — unknown must never estimate cheap.
    assert_eq!(slice_share_of_file(None, None, narrow, 144), (1, 1));
    // A disjoint file contributes nothing.
    let after = TimeSlice::new(2 * DAY, 2 * DAY + TEN_MIN).expect("later slice");
    assert_eq!(slice_share_of_file(Some(file_min), Some(file_max), after, 144).0, 0);
}

fn create_test_config(test_id: &str) -> Arc<AppConfig> {
    let mut cfg = AppConfig::default();
    cfg.aws.aws_s3_bucket = Some("timefusion-tests".to_string());
    cfg.aws.aws_access_key_id = Some("minioadmin".to_string());
    cfg.aws.aws_secret_access_key = Some("minioadmin".to_string());
    cfg.aws.aws_s3_endpoint = "http://127.0.0.1:9000".to_string();
    cfg.aws.aws_default_region = Some("us-east-1".to_string());
    cfg.aws.aws_allow_http = Some("true".to_string());
    // Unique per RUN, not merely per test name: keying storage on the name alone makes a
    // test read the objects its previous runs left behind.
    let unique = format!("{test_id}-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    cfg.core.timefusion_table_prefix = format!("test-{unique}");
    cfg.core.timefusion_data_dir = PathBuf::from(format!("/tmp/timefusion-db-{unique}"));
    cfg.cache.timefusion_foyer_disabled = true;
    Arc::new(cfg)
}

/// The slice target must split large files but leave small ones unsliced: each slice costs a
/// full scan of the input. Calls the real `repair_slice_want` rather than restating its
/// arithmetic, so the test cannot drift from the production formula.
#[test]
fn a_repair_slice_splits_big_files_but_leaves_small_ones_alone() {
    const MB: i64 = 1024 * 1024;
    let slices = |input: i64| super::repair_slice_want(input, super::REPAIR_SLICE_DECODED_TARGET_BYTES);
    assert!(slices(1_088_634_971) >= 4, "the 1.04 GiB blocker must be sliced, got {}", slices(1_088_634_971));
    // ...but not shredded: slice count has to fit the 900 s deadline as well as the pool.
    assert!(slices(1_088_634_971) <= 16, "too many slices turns the rewrite into re-scans, got {}", slices(1_088_634_971));
    assert_eq!(slices(30 * MB), 1, "a small file is sorted whole");
}

/// A slice bound is the column's raw i64 micros, but the column is
/// `Timestamp(Microsecond, Some("UTC"))` and DataFusion does NOT coerce a bare integer to a
/// timestamp — the predicate must carry the column's type, rendered via `{:?}` on the Arrow
/// type (which is `arrow_cast`'s syntax).
#[tokio::test]
async fn repair_slice_predicate_plans_against_a_tz_aware_timestamp() {
    use datafusion::{datasource::MemTable, prelude::SessionContext};
    let ty = arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new("timestamp", ty.clone(), false)]));
    let ctx = SessionContext::new();
    ctx.register_table("bin", Arc::new(MemTable::try_new(Arc::clone(&schema), vec![vec![]]).expect("memtable"))).expect("register");

    let rendered = format!("{ty:?}");
    assert_eq!(rendered, r#"Timestamp(Microsecond, Some("UTC"))"#, "arrow_cast parses this exact syntax");

    // `ctx.sql()` only builds the unanalyzed plan; coercion runs on the way to a physical
    // plan, so the check has to go that far or it proves nothing.
    let plan = |sql: String| {
        let ctx = ctx.clone();
        async move { ctx.sql(&sql).await.expect("parses").create_physical_plan().await }
    };

    plan(format!("SELECT * FROM bin WHERE \"timestamp\" >= arrow_cast(1753833600000000, '{rendered}')"))
        .await
        .expect("typed literal must plan against a tz-aware timestamp");

    let Err(err) = plan("SELECT * FROM bin WHERE \"timestamp\" >= 1753833600000000".into()).await else {
        panic!("a bare integer bound must NOT silently coerce");
    };
    assert!(format!("{err}").contains("type_coercion"), "expected the prod failure mode, got: {err}");
}

/// The quantile probe must actually PLAN and return monotone cuts: it falls back to the
/// uniform split on any error, silently, so a probe that stops planning looks like success.
#[tokio::test]
async fn repair_slice_cuts_are_monotone_and_row_balanced_under_skew() {
    use datafusion::{datasource::MemTable, prelude::SessionContext};
    // 1000 rows crammed into the last 1% of the span — the shape that defeats an even
    // TIME split.
    let ts: Vec<i64> = (0..1000).map(|i| 990_000 + i * 10).chain(std::iter::once(0)).collect();
    let ty = arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()));
    let arr = arrow::array::TimestampMicrosecondArray::from(ts).with_timezone("UTC");
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new("timestamp", ty, false)]));
    let batch = arrow::array::RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)]).expect("batch");
    let ctx = SessionContext::new();
    ctx.register_table("bin", Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("memtable"))).expect("register");

    let cuts = super::repair_slice_cuts(&ctx, "bin", "timestamp", 4).await;
    assert_eq!(cuts.len(), 3, "4 slices need 3 interior cuts; empty means the probe did not plan");
    assert!(cuts.windows(2).all(|w| w[0] < w[1]), "cuts must be strictly increasing to tile: {cuts:?}");

    // Cuts follow the ROWS into the dense tail; an even TIME split would cut near 250_000.
    assert!(cuts[0] > 500_000, "cuts must track row density, not the time span: {cuts:?}");

    let bounds = super::repair_bounds_from_cuts(0, 999_990, &cuts);
    assert_eq!(bounds.len(), 4);
    assert_eq!(bounds[0].0, 0);
    assert!(bounds.last().expect("non-empty").1.is_none());
}

/// Shared tiling invariant for `repair_bounds_from_cuts` / `repair_slice_bounds`:
/// the first slice starts at `lo`, the last is open-ended so `hi` is never
/// dropped, adjacent slices abut exactly, and every value in `samples` lands
/// in exactly one slice.
fn assert_tiles(bounds: &[(i64, Option<i64>)], lo: i64, hi: i64, samples: &[i64]) {
    let last = *bounds.last().expect("non-empty");
    assert_eq!(bounds[0].0, lo, "first slice starts at lo");
    assert!(last.1.is_none(), "last slice is open so hi is never dropped");
    assert!(last.0 <= hi, "the open last slice must start at or before hi ({:?} vs {hi})", last.0);
    for w in bounds.windows(2) {
        assert_eq!(w[0].1.expect("only the last is open"), w[1].0, "{:?} must abut {:?}", w[0], w[1]);
    }
    for &v in samples {
        let hits = bounds.iter().filter(|(s, e)| v >= *s && e.is_none_or(|e| v < e)).count();
        assert_eq!(hits, 1, "value {v} must fall in exactly one slice of {bounds:?}");
    }
}

/// Quantile cuts must tile exactly like the uniform split — a gap drops rows and an overlap
/// duplicates them, hiding two versions of one key from keep-greatest.
#[test]
fn repair_cut_bounds_tile_the_range_and_survive_skew() {
    // Skewed cuts may bunch anywhere in the range.
    let bounds = super::repair_bounds_from_cuts(0, 1000, &[997, 998, 999]);
    assert_eq!(bounds.len(), 4);
    assert_tiles(&bounds, 0, 1000, &[0, 1, 500, 999, 1000]);

    // Ties collapse to FEWER slices, never to an overlap.
    assert_eq!(super::repair_bounds_from_cuts(0, 100, &[50, 50, 50]), vec![(0, Some(50)), (50, None)]);

    // Cuts outside (lo, hi] are ignored; all-degenerate falls back to one pass.
    assert_eq!(super::repair_bounds_from_cuts(10, 20, &[5, 10, 99]), vec![(10, None)]);
    assert_eq!(super::repair_bounds_from_cuts(0, 100, &[]), vec![(0, None)]);

    // A cut exactly at hi is legal: it opens a final slice holding only hi.
    assert_tiles(&super::repair_bounds_from_cuts(0, 100, &[100]), 0, 100, &[0, 1, 50, 99, 100]);

    // The uniform split obeys the same invariant over every shape of range.
    for (lo, hi, n) in [(0i64, 100i64, 4usize), (-50, 50, 3), (1_700_000_000_000_000, 1_700_000_086_400_000, 8), (0, 1, 4), (5, 5, 4)] {
        let bounds = super::repair_slice_bounds(lo, hi, n);
        assert_tiles(&bounds, lo, hi, &[lo, hi, lo + (hi - lo) / 2]);
    }
    // Degenerate inputs decline slicing rather than producing nonsense.
    assert_eq!(super::repair_slice_bounds(10, 5, 4), vec![(10, None)], "hi <= lo is a single unbounded pass");
    assert_eq!(super::repair_slice_bounds(0, 100, 1), vec![(0, None)], "one slice is the un-sliced path");
}

/// Verified-sorted paths must survive a restart and prevent re-probing. Sound to persist
/// because a Delta object path is immutable.
#[tokio::test]
async fn a_verified_sorted_footer_survives_a_restart() -> Result<()> {
    let cfg = create_test_config("verified-sorted-persist");
    let db = Database::with_config(cfg.clone()).await?;
    let paths: Vec<String> = (0..3).map(|i| format!("timefusion/t/project_id=p/date=2026-07-30/part-{i}.parquet")).collect();
    db.persist_verified_sorted(&paths);

    // A fresh Database over the SAME data dir is the restart.
    let restarted = Database::with_config(cfg).await?;
    assert!(restarted.repair_verified_sorted.is_empty(), "nothing is adopted until the load runs");
    restarted.load_verified_sorted();
    for path in &paths {
        assert!(restarted.repair_verified_sorted.contains(path), "re-adopted after restart: {path}");
    }

    restarted.load_verified_sorted();
    assert_eq!(restarted.repair_verified_sorted.len(), paths.len(), "idempotent");
    Ok(())
}

fn add_action(path: &str) -> deltalake::kernel::Action {
    deltalake::kernel::Action::Add(deltalake::kernel::Add { path: path.to_string(), size: 1, ..Default::default() })
}

/// Delta stores `Add.path` URL-ENCODED, but `LogicalFile::path()` — what repair admission and
/// the footer probe key on — returns it DECODED. Marking the raw form is a silent no-op.
#[tokio::test]
async fn a_marked_path_is_stored_decoded_so_admission_can_find_it() -> Result<()> {
    let db = Database::with_config(create_test_config("sortmark-decode")).await?;
    let schema = crate::schema::get_schema("otel_logs_and_spans").expect("the default schema declares a sort order");
    assert!(!schema.sorting_columns().is_empty(), "this test is vacuous unless the parquet conversion is non-empty");

    let encoded = "project_id=acme%20corp/date=2026-07-30/part-0.parquet";
    let decoded = "project_id=acme corp/date=2026-07-30/part-0.parquet";
    db.mark_written_sorted(schema, true, &[add_action(encoded)]);

    assert!(db.repair_verified_sorted.contains(decoded), "admission keys on the decoded path, so that is what must be stored");
    assert!(!db.repair_verified_sorted.contains(encoded), "storing the raw form is the silent no-op this test exists to catch");
    Ok(())
}

/// The two ways a write must NOT be credited, both of which would make a file
/// permanently invisible to the repair that exists to fix it.
#[tokio::test]
async fn marking_declines_an_unsorted_write_and_an_empty_parquet_conversion() -> Result<()> {
    let db = Database::with_config(create_test_config("sortmark-declines")).await?;
    let schema = crate::schema::get_schema("otel_logs_and_spans").expect("schema");

    // (a) The sort degraded, so no footer was declared.
    db.mark_written_sorted(schema, false, &[add_action("project_id=p/date=2026-07-30/a.parquet")]);
    assert!(db.repair_verified_sorted.is_empty(), "an unsorted write must stay a suspect");

    // (b) The schema DECLARES an order whose parquet conversion is empty — `sorting_columns()`
    // drops names it cannot map to a physical leaf index, so no footer is stamped. Testing the
    // field instead of the conversion would exonerate such a file forever.
    let mut drifted = schema.clone();
    drifted.sorting_columns.truncate(1);
    drifted.sorting_columns[0].name = "a_column_that_does_not_exist".to_string();
    assert!(drifted.sorting_columns().is_empty(), "the conversion drops unmappable names");
    assert!(!drifted.sorting_columns.is_empty(), "while the declaration still looks non-empty — that IS the drift");
    db.mark_written_sorted(&drifted, true, &[add_action("project_id=p/date=2026-07-30/b.parquet")]);
    assert!(db.repair_verified_sorted.is_empty(), "a declared-but-unstamped order must not exonerate anything");
    Ok(())
}

/// Degradation is forced by an unmergeable schema (`id` Utf8 vs Int64), one of the ways
/// `sort_batches_by_schema` gives up, so the guard runs on the in-process path too.
#[tokio::test]
async fn rewrite_aborts_rather_than_writing_an_unsorted_file() -> Result<()> {
    use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray};
    let db = Database::with_config(create_test_config("rewrite-no-unsorted")).await?;
    let table = get_schema("otel_logs_and_spans").expect("registered");
    let batch = |id: ArrayRef| {
        let ts: TimestampMicrosecondArray = (0..4i64).map(Some).collect();
        let schema = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("timestamp", arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), false),
            arrow_schema::Field::new("id", id.data_type().clone(), false),
        ]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(ts) as ArrayRef, id]).unwrap()
    };
    let conflicting =
        || vec![batch(Arc::new(StringArray::from(vec!["a", "b", "c", "d"])) as ArrayRef), batch(Arc::new(Int64Array::from(vec![1i64, 2, 3, 4])) as ArrayRef)];

    // `FlushBatches` is a lazy iterator, not `Debug` — destructure rather than `expect_err`.
    let Err(err) = db.sort_flush_group(table, conflicting(), UnsortedFallback::Forbid).await else {
        panic!("a rewrite must fail, not silently degrade to an unsorted file")
    };
    assert!(format!("{err}").contains("keeping the committed inputs"), "the error must say the inputs were kept, got: {err}");

    // Ingest degrades rather than losing rows — and says so honestly.
    let (_, sorted) = db.sort_flush_group(table, conflicting(), UnsortedFallback::Allow).await?;
    assert!(!sorted, "ingest writes the group unsorted and must NOT claim a sorted footer");

    // An empty group has no footer to lose: not a degradation, never an abort.
    let (_, sorted) = db.sort_flush_group(table, vec![], UnsortedFallback::Forbid).await?;
    assert!(!sorted, "empty group is trivially unsorted but must not abort a rewrite");
    Ok(())
}

/// Scrambled-event-time batches of `batches_n × rows_per_batch` wide rows (`id_width` bytes
/// each), so an append-ordered pass-through cannot masquerade as a sort.
fn wide_row_batches(rows_per_batch: i64, batches_n: i64, id_width: usize) -> Vec<RecordBatch> {
    use arrow::array::{StringArray, TimestampMicrosecondArray};
    let arrow_schema = Arc::new(arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("timestamp", arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), false),
        arrow_schema::Field::new("id", arrow_schema::DataType::Utf8, false),
    ]));
    (0..batches_n)
        .map(|b| {
            let ts: TimestampMicrosecondArray =
                (0..rows_per_batch).map(|i| Some((b * rows_per_batch + i).wrapping_mul(2_654_435_761) % 1_000_000_000)).collect();
            let ids = StringArray::from_iter_values((0..rows_per_batch).map(|i| format!("{b:04}-{i:06}-{}", "x".repeat(id_width))));
            RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(ts), Arc::new(ids)]).unwrap()
        })
        .collect()
}

/// The escalated sort must be a single-partition plan with no merge exec, so it survives the
/// minimum pool floor (64 MB) with data far larger than the pool. A batch is the sort's
/// indivisible admission unit: a pool that cannot hold one batch fails rather than spilling,
/// and wide rows can make one default-sized batch larger than a small floor pool.
#[test_case(10_000, 16, 800, 160_000 ; "128MB across 16 batches, 2x the pool, starves the sort into escalation")]
#[test_case(1_000, 16, 8_000, 16_000 ; "8KB rows: one old-sized batch would be 65MB, over the 64MB pool floor")]
#[tokio::test]
async fn escalated_flush_sort_is_one_pool_consumer_and_admits_wide_row_batches(
    rows_per_batch: i64, batches_n: i64, id_width: usize, expected_rows: i64,
) -> Result<()> {
    use arrow::array::TimestampMicrosecondArray;
    let mut cfg = (*create_test_config("flush-sort-floor")).clone();
    cfg.maintenance.timefusion_sort_skip_bytes = 0; // every group escalates
    cfg.maintenance.timefusion_flush_sort_pool_mb = 64; // the config floor
    let db = Database::with_config(Arc::new(cfg)).await?;
    let table = get_schema("otel_logs_and_spans").expect("registered");
    let batches = wide_row_batches(rows_per_batch, batches_n, id_width);

    let (out, escalated) = db.sort_flush_group(table, batches, UnsortedFallback::Allow).await?;
    assert!(escalated, "the pool could not sustain the sort and fell back to writing unsorted");
    let FlushBatches::Ready(it) = out else { panic!("escalated path yields Ready batches") };
    let stamps: Vec<i64> =
        it.flat_map(|b| b.column_by_name("timestamp").unwrap().as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().values().to_vec()).collect();
    assert_eq!(stamps.len() as i64, expected_rows, "the sort must not lose or duplicate rows");
    assert!(stamps.windows(2).all(|w| w[0] >= w[1]), "output must honor the schema's timestamp DESC ordering");
    Ok(())
}

/// The DML footer declaration must track the path that actually sorts: only the DV-merge path
/// sorts, while `UpdateBuilder`/`DeleteBuilder` write in scan order. A false footer makes
/// `DedupExec`'s bounded mode advance its bound over unordered rows.
#[tokio::test]
async fn dml_declares_a_sorted_footer_only_on_the_path_that_sorts() -> Result<()> {
    let db = Database::with_config(create_test_config("dml-footer-honesty")).await?;
    let schema = get_schema("otel_logs_and_spans").expect("registered");
    assert!(!schema.sorting_columns.is_empty(), "fixture needs a table with sort keys or this asserts nothing");

    // The merge/DV path sorts its appended rows, so it may declare them.
    let sorted = db.dml_writer_properties("otel_logs_and_spans", true);
    assert!(sorted.sorting_columns().is_some_and(|c| !c.is_empty()), "the DV-merge path sorts (append_sort_by) and must declare its footer");

    // Update/Delete do not sort. Their footer must claim nothing.
    let unsorted = db.dml_writer_properties("otel_logs_and_spans", false);
    assert!(
        unsorted.sorting_columns().is_none_or(|c| c.is_empty()),
        "UpdateBuilder/DeleteBuilder write in SCAN order — declaring the schema's sort order is a footer that lies"
    );
    Ok(())
}

/// Every node of a physical plan, so a test can assert on the scans it holds.
fn plan_nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let (mut pending, mut nodes) = (vec![Arc::clone(plan)], Vec::new());
    while let Some(node) = pending.pop() {
        pending.extend(node.children().into_iter().cloned());
        nodes.push(node);
    }
    nodes
}

/// A query session wired exactly as the server wires one.
fn session_for(db: &Database) -> Result<SessionContext> {
    let mut ctx = Arc::new(db.clone()).create_session_context();
    datafusion_functions_json::register_all(&mut ctx)?;
    db.setup_session_context(&mut ctx)?;
    Ok(ctx)
}

async fn setup_test_database() -> Result<(Database, SessionContext, String)> {
    let test_prefix = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let db = Database::with_config(create_test_config(&test_prefix)).await?;
    let ctx = session_for(&db)?;
    Ok((db, ctx, test_prefix))
}

/// The narrow maintenance provider must not throw away the parquet footer ordering: that
/// ordering is what lets a dedup rewrite's schema-order sort be satisfied by merging
/// pre-sorted file groups instead of by a blocking, spilling `SortExec`.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn the_maintenance_scan_keeps_the_footer_ordering_it_was_written_with() -> Result<()> {
    let (db, _ctx, prefix) = setup_test_database().await?;
    let project_id = format!("scan_order_{prefix}");
    let base = chrono::Utc::now().timestamp_micros() - 3_600_000_000;
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(base).unwrap().date_naive();
    let rows: Vec<_> = (0..64)
        .map(|i| {
            serde_json::json!({
                "timestamp": base + i * 1_000_000,
                "id": format!("id-{i:03}"),
                "name": "n",
                "project_id": project_id,
                "date": date.to_string(),
                "resource___service___name": format!("svc-{}", i % 4),
                "summary": [],
            })
        })
        .collect();
    let batch = json_to_batch_for("otel_logs_and_spans", rows)?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;

    let table_ref = db.resolve_table(&project_id, "otel_logs_and_spans").await?;
    let (snapshot, log_store, files) = {
        let table = table_ref.read().await;
        let snapshot = Arc::new(table.snapshot()?.snapshot().clone());
        let files: Vec<String> = snapshot.log_data().iter().map(|f| f.path().to_string()).collect();
        (snapshot, table.log_store(), files)
    };
    assert!(!files.is_empty(), "the insert must have committed a file, or this test proves nothing");

    let provider = Database::narrow_provider(log_store, snapshot, files, None, None).await.map_err(|e| anyhow::anyhow!("{e}"))?;
    let ctx = SessionContext::new();
    ctx.register_table("scan", provider)?;
    let schema = get_schema("otel_logs_and_spans").expect("shipped schema");
    let sql = format!("SELECT \"timestamp\", \"id\", \"resource___service___name\" FROM scan{}", schema_order_by_clause(schema));
    let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
    let rendered = datafusion::physical_plan::displayable(plan.as_ref()).indent(false).to_string();
    println!("--- maintenance scan plan:\n{rendered}");

    // A partial footer ordering is not merely weaker, it is FALSE: data sorted by
    // (timestamp, service, id) is not sorted by (timestamp, id), since within one timestamp
    // ids do not ascend across services. So every key must be declared, in order.
    let expected: Vec<_> = schema.sorting_columns.iter().map(|column| (column.name.as_str(), column.descending, column.nulls_first)).collect();
    let nodes = plan_nodes(&plan);
    let scans: Vec<_> = nodes.iter().filter(|node| node.downcast_ref::<DataSourceExec>().is_some()).collect();
    assert!(!scans.is_empty(), "must inspect the actual Parquet scan");
    for node in scans {
        let declared = node.properties().output_ordering().expect("the Parquet scan retains its footer ordering");
        let actual: Vec<_> = declared
            .iter()
            .map(|expr| {
                let column = expr.expr.downcast_ref::<datafusion::physical_expr::expressions::Column>().expect("plain sort column");
                (column.name(), expr.options.descending, expr.options.nulls_first)
            })
            .collect();
        assert_eq!(actual, expected, "the scan must retain all footer keys in order, including direction and null placement");
    }
    assert!(!rendered.contains("SortExec"), "the declared ordering satisfies the schema sort, so nothing should sort:\n{rendered}");
    Ok(())
}

/// The logical-count fast path over the full merge-on-read lifecycle: build an exact snapshot
/// base, resolve a newly appended tombstone as a narrow overlay, and replace DedupExec in the
/// physical COUNT plan. The `sum(1)` leg is the control — the pushdown gate declines it, so it
/// scans the identical window and the two answers must agree.
///
/// `timefusion_count_pushdown` is enabled here explicitly; its default is false. This fixture
/// is a guard, NOT a reproducer of the known undercount, which needs a fragmented partition
/// where a partial or stale index still satisfies the gate.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn logical_count_build_and_append_overlay_are_exact_end_to_end() -> Result<()> {
    let prefix = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let mut cfg = (*create_test_config(&prefix)).clone();
    cfg.maintenance.timefusion_count_pushdown = true;
    let db = Database::with_config(Arc::new(cfg)).await?;
    let ctx = session_for(&db)?;
    let project_id = format!("logical_count_{prefix}");
    let timestamp = chrono::Utc::now().timestamp_micros() - 60_000_000;
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(timestamp).unwrap().date_naive();
    let row = |id: &str, deleted: Option<bool>| {
        serde_json::json!({
            "timestamp": timestamp,
            "id": id,
            "name": id,
            "project_id": project_id,
            "date": date.to_string(),
            "deleted": deleted,
        })
    };

    let base = json_to_batch_for("mor_versioned", vec![row("live", None), row("gone", None)])?;
    db.insert_records_batch(&project_id, "mor_versioned", vec![base], true, None).await?;
    let key = crate::read::CountPartition { project_id: project_id.clone(), table_name: "mor_versioned".to_string(), date: date.to_string() };
    db.build_logical_count_partition(&key, false).await?;

    let table_ref = db.resolve_table(&project_id, "mor_versioned").await?;
    let (index, added) = {
        let table = table_ref.read().await;
        let (_, files) = Database::logical_count_partition_snapshot(&table, &project_id, &date.to_string())?;
        db.logical_count_memory_for_files(&project_id, "mor_versioned", &date.to_string(), &files).expect("built index must be memory-resident")
    };
    assert!(added.is_empty());
    assert_eq!(index.count(timestamp, timestamp + 1), 2);

    let tombstone = json_to_batch_for("mor_versioned", vec![row("gone", Some(true))])?;
    db.insert_records_batch(&project_id, "mor_versioned", vec![tombstone], true, None).await?;

    let window = format!(
        "FROM mor_versioned WHERE project_id = '{project_id}' AND timestamp >= to_timestamp_micros({timestamp}) AND timestamp < to_timestamp_micros({})",
        timestamp + 1
    );
    let answer = |sql: String| {
        let ctx = &ctx;
        async move {
            let plan = ctx.sql(&sql).await?.create_physical_plan().await?;
            let rendered = datafusion::physical_plan::displayable(plan.as_ref()).indent(true).to_string();
            let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
            let value = batches[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().expect("an aggregate over Int64").value(0);
            Ok::<_, anyhow::Error>((value, rendered))
        }
    };

    let (count, rendered) = answer(format!("SELECT COUNT(*) {window}")).await?;
    assert!(!rendered.contains("DedupExec"), "logical-count pushdown did not fire:\n{rendered}");
    assert_eq!(count, 1, "the appended tombstone must retire its base winner");

    // The scan of the SAME window, over the same data, in the same process.
    let (scanned, rendered) = answer(format!("SELECT SUM(1) {window}")).await?;
    assert!(rendered.contains("DedupExec"), "`sum(1)` must decline the pushdown, or it is not a control:\n{rendered}");
    assert_eq!(count, scanned, "the pushdown and a scan of the same window must agree");
    Ok(())
}

/// The memory pool must be process-wide, including across `Database` clones (bootstrap clones
/// the db); per-context RuntimeEnvs would each grant the full budget and oversubscribe N×.
#[tokio::test]
async fn session_contexts_share_one_memory_pool() -> Result<()> {
    let cfg = create_test_config("pool-share");
    let db = Database::with_config(cfg).await?;
    let ctx1 = Arc::new(db.clone()).create_session_context();
    let ctx2 = Arc::new(db.clone()).create_session_context();
    assert!(Arc::ptr_eq(&ctx1.runtime_env(), &ctx2.runtime_env()), "contexts must share one RuntimeEnv/memory pool");
    Ok(())
}

/// A deploy handoff must be able to stop the maintenance lane GROWING, and the
/// lane must come back if the handoff fails.
///
/// The drain waits up to four minutes for in-flight writers; under a saturated
/// lane ordinary ingest writes take minutes and it never finishes. Three
/// consecutive rollouts failed that way on 2026-09-21 — each one then leaving a
/// mutating lease that blocked the next deploy for two hours.
///
/// The resume-on-drop half is the part worth pinning: a handoff that times out
/// reopens write admission, and a lane left quiesced after that would be a
/// permanent stall dressed as a safety feature.
#[tokio::test]
async fn a_deploy_handoff_can_quiesce_maintenance_and_the_lane_returns() -> Result<()> {
    let db = Database::with_config(create_test_config("handoff-quiesce")).await?;
    assert!(!db.maintenance_quiesced.load(std::sync::atomic::Ordering::Acquire), "a fresh database claims normally");

    let quiesced = db.quiesce_maintenance();
    assert!(db.maintenance_quiesced.load(std::sync::atomic::Ordering::Acquire), "the guard suspends claiming");
    assert!(!db.run_maintenance_coordinator_once().await?, "a quiesced coordinator must report IDLE, so its workers park instead of spinning");

    drop(quiesced);
    assert!(
        !db.maintenance_quiesced.load(std::sync::atomic::Ordering::Acquire),
        "a failed handoff must resume the lane, exactly as it reopens write admission"
    );

    // And the success path keeps it suspended: the replacement owns writes from
    // that point, so this process must not compact partitions it has handed over.
    db.quiesce_maintenance().hold_until_exit();
    assert!(db.maintenance_quiesced.load(std::sync::atomic::Ordering::Acquire), "a completed handoff holds the lane down until exit");
    Ok(())
}

/// A packing unit must not CLAIM a slot it has no permit to start: claiming stamps the unit
/// `Running` and starts its 900 s deadline, which would then be spent waiting in the permit
/// queue. The permit is taken BEFORE the claim, and a turn that cannot get one leaves the task
/// untouched for whoever can.
#[tokio::test]
async fn a_packing_unit_never_claims_a_slot_it_cannot_start() -> Result<()> {
    use crate::maintenance_coordinator::{Operation, TaskKey, TaskState, TimeSlice};
    let db = Database::with_config(create_test_config("light-permit-preclaim")).await?;
    let project = format!("permit_{}", uuid::Uuid::new_v4().simple());
    let day_start = midnight_micros((Utc::now() - chrono::Duration::days(3)).date_naive());
    let key = TaskKey {
        physical_table: "otel_logs_and_spans".to_owned(),
        source: "otel_logs_and_spans".to_owned(),
        project_id: project.clone(),
        slice: TimeSlice::new(day_start, day_start + crate::maintenance_coordinator::DAY_MICROS)?,
        operation: Operation::SealedConsolidation,
    };
    db.maintenance_tasks.lock().unwrap().enqueue(key.clone(), 0, 1024, 0);

    // Every light-rewrite permit taken.
    let held = Arc::clone(&db.light_rewrite_sem).acquire_many_owned(db.light_rewrite_sem.available_permits() as u32).await?;

    let claimed = db.run_coordinator_compaction_once(Operation::SealedConsolidation).await?;
    let (state, attempts) = {
        let journal = db.maintenance_tasks.lock().unwrap();
        (journal.state(&key), journal.tasks().find(|task| task.key == key).map(|task| task.attempts))
    };
    assert!(!claimed, "a turn with no permit must report no work done, so the cycle moves this worker on");
    assert_eq!(state, Some(TaskState::Pending), "the unit must stay claimable rather than burn a deadline waiting for a permit");
    assert_eq!(attempts, Some(0), "queueing is not an attempt — inflating it feeds the unit to abandon_running's bisect and its >=900s floor");

    // And with a permit free, the same turn does claim it.
    drop(held);
    assert!(db.run_coordinator_compaction_once(Operation::SealedConsolidation).await?, "the refusal must be the permit, not the queue being empty");
    Ok(())
}

/// Current-day packing may not occupy every light lane while sealed debt is
/// waiting. The reservation is taken before the claim, so a declined hot turn
/// neither burns an attempt nor starts a unit deadline.
#[serial]
#[tokio::test]
async fn pending_sealed_debt_reserves_light_lanes_before_hot_claims() -> Result<()> {
    use crate::maintenance_coordinator::{Operation, TaskKey, TaskState, TimeSlice};
    use std::sync::atomic::Ordering::Relaxed;

    let db = Database::with_config(create_test_config("sealed-light-reserve")).await?;
    let day_start = midnight_micros(Utc::now().date_naive());
    let key = TaskKey {
        physical_table: "otel_logs_and_spans".to_owned(),
        source: "otel_logs_and_spans".to_owned(),
        project_id: "hot-reserve-project".to_owned(),
        slice: TimeSlice::new(day_start, day_start + crate::maintenance_coordinator::DAY_MICROS)?,
        operation: Operation::HotPacking,
    };
    db.maintenance_tasks.lock().unwrap().enqueue(key.clone(), 0, 1024, 0);

    let hot_capacity = db.hot_packing_sem.available_permits();
    let held = Arc::clone(&db.hot_packing_sem).acquire_many_owned(u32::try_from(hot_capacity).unwrap()).await?;
    let stats = crate::observability::maintenance_stats();
    let prior_pending = stats.pending_sealed_consolidation.swap(1, Relaxed);

    let ran = db.run_coordinator_compaction_once(Operation::HotPacking).await?;

    stats.pending_sealed_consolidation.store(prior_pending, Relaxed);
    drop(held);
    let journal = db.maintenance_tasks.lock().unwrap();
    assert!(!ran, "a capped hot turn must fall through so the worker can serve sealed work");
    assert_eq!(journal.state(&key), Some(TaskState::Pending), "the hot task must remain claimable");
    assert_eq!(journal.attempts(&key), 0, "lane contention before a claim is not a task attempt");
    Ok(())
}

/// Hot-tail wave staging must hold its own permits, not share the heavy maintenance rewrite
/// semaphore: a long dedup drain holding every heavy permit must not starve hot compaction.
#[tokio::test]
async fn wave_staging_permits_are_independent_of_heavy_rewrite_permits() -> Result<()> {
    let db = Database::with_config(create_test_config("rewrite-sem-split")).await?;
    assert!(!Arc::ptr_eq(&db.maintenance_rewrite_sem, &db.light_rewrite_sem), "wave staging must not share the heavy rewrite semaphore");
    assert_eq!(db.light_rewrite_sem.available_permits(), db.config.derived.max_light_optimize_k().max(1));
    // Dedup/optimize/recompress take every heavy permit…
    let heavy = db.maintenance_rewrite_sem.clone().acquire_many_owned(db.maintenance_rewrite_sem.available_permits() as u32).await?;
    assert_eq!(db.maintenance_rewrite_sem.available_permits(), 0);
    // …and a wave still stages immediately.
    assert!(db.light_rewrite_sem.try_acquire().is_ok(), "hot-compact waves must not wait on a dedup drain");
    drop(heavy);
    Ok(())
}

/// Packing and footer repair must use disjoint memory pools, and the split must be
/// memory-neutral: the two slices PARTITION the light share rather than adding to it, or it
/// trades a compaction stall for an OOM.
#[tokio::test]
async fn packing_and_repair_hold_disjoint_pools_that_partition_the_light_share() -> Result<()> {
    let db = Database::with_config(create_test_config("pool-split")).await?;
    assert_eq!(db.pack_pool_bytes() + db.repair_pool_bytes(), db.light_optimize_pool_bytes(), "the split must not grow the maintenance budget");
    assert!(db.pack_pool_bytes() > 0 && db.repair_pool_bytes() > 0, "neither pass may be sized to zero");
    // Coordinator, light and heavy pools must TILE the maintenance budget. Independent
    // definitions silently over-commit it, which is an OOM.
    assert_eq!(
        db.config.derived.coordinator_share_bytes() + db.light_optimize_pool_bytes() + db.heavy_pool_bytes(),
        db.config.derived.maintenance_pool_bytes(),
        "coordinator + light + heavy must tile the pool, never over-commit it"
    );
    assert_eq!(db.heavy_pool_bytes(), db.config.derived.heavy_share_bytes(), "the heavy pool must be the budget tree's heavy share, not a second opinion");
    assert!(!Arc::ptr_eq(&db.light_optimize_runtime_env(), &db.repair_runtime_env()), "sharing one RuntimeEnv is sharing one pool");
    // The coordinator's Repair units must stage under repair's OWN pool, not the shared
    // coordinator pool where a large sort competes with dedup/pack.
    use crate::maintenance_coordinator::Operation;
    assert!(
        Arc::ptr_eq(&db.coordinator_compaction_runtime_env(Operation::Repair), &db.repair_runtime_env()),
        "coordinator Repair must use the dedicated repair pool"
    );
    assert!(
        Arc::ptr_eq(&db.coordinator_compaction_runtime_env(Operation::HotPacking), &db.coordinator_runtime_env()),
        "non-repair coordinator work keeps the shared coordinator pool"
    );

    // The brake must be blind to repair: an in-flight repair bin must not stop packing.
    let _repair_bin = in_flight_guard(&crate::observability::maintenance_stats().repair_bins_in_flight);
    assert!(db.light_optimize_brake().is_none(), "a repair bin in flight must no longer stop packing");
    Ok(())
}

/// The Delta commit lock must be per physical table, not process-wide, or flush commits to
/// independent tables needlessly serialize. Two default projects share the unified table's
/// single log → one lock; commit and DML locks are distinct critical sections.
#[tokio::test]
async fn commit_lock_is_per_physical_table() -> Result<()> {
    let db = Database::with_config(create_test_config("commit-lock-key")).await?;
    let a = db.commit_lock("proj_a", "otel_logs_and_spans").await;
    let b = db.commit_lock("proj_b", "otel_logs_and_spans").await;
    let c = db.commit_lock("proj_a", "metrics").await;
    assert!(Arc::ptr_eq(&a, &b), "default projects on a unified table must share one commit lock");
    assert!(!Arc::ptr_eq(&a, &c), "different tables must get independent commit locks");
    assert!(!Arc::ptr_eq(&a, &db.dml_lock("proj_a", "otel_logs_and_spans").await), "commit and DML locks must be distinct");
    Ok(())
}

/// A builder over an existing Delta root; every builder from the same `(store, url)` sees the
/// same bytes, so a test can rebuild a second handle over a table it just wrote.
fn mem_backend(store: Arc<dyn object_store::ObjectStore>, url: &Url) -> DeltaTableBuilder {
    DeltaTableBuilder::from_url(url.clone()).expect("a memory:/// root parses").with_storage_backend(store, url.clone())
}

/// The single nullable `id` column the memory-backed fixtures below are built on.
fn int_id_cols() -> Vec<deltalake::kernel::StructField> {
    use deltalake::kernel::{DataType, PrimitiveType, StructField};
    vec![StructField::new("id", DataType::Primitive(PrimitiveType::Integer), true)]
}

/// One `id` batch matching `int_id_cols`.
fn int_id_batch(ids: Vec<i32>) -> RecordBatch {
    use arrow::array::Int32Array;
    let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new("id", arrow_schema::DataType::Int32, true)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids)) as _]).expect("one-column batch")
}

/// A fresh `memory:///{name}` Delta table, returned with the store and URL behind it so a test
/// can build a second handle (throttled, stale) over the same bytes.
async fn mem_table(name: &str, cols: Vec<deltalake::kernel::StructField>) -> Result<(Arc<object_store::memory::InMemory>, Url, DeltaTable)> {
    let store = Arc::new(object_store::memory::InMemory::new());
    let url = Url::parse(&format!("memory:///{name}"))?;
    let table = mem_backend(store.clone(), &url).build()?.create().with_columns(cols).await?;
    Ok((store, url, table))
}

/// A one-column Int32 Delta table with deletion vectors enabled, holding `1..=4` in a
/// single Add.
async fn dv_int_table(name: &str) -> Result<DeltaTable> {
    let store = Arc::new(object_store::memory::InMemory::new());
    let url = Url::parse(&format!("memory:///{name}"))?;
    let table = mem_backend(store, &url)
        .build()?
        .create()
        .with_columns(int_id_cols())
        .with_configuration(HashMap::from([("delta.enableDeletionVectors".to_string(), Some("true".to_string()))]))
        .await?;
    Ok(table.write(vec![int_id_batch(vec![1, 2, 3, 4])]).await?)
}

/// One `test_span` row for `project`. `skip_queue` writes straight to Delta;
/// `false` routes through the buffered layer (WAL → MemBuffer) so a flush
/// tick owns the row.
async fn insert_span(db: &Database, project: &str, table: &str, id: &str, skip_queue: bool) -> Result<()> {
    db.insert_records_batch(project, table, vec![json_to_batch(vec![test_span(id, "span", project)])?], skip_queue, None).await?;
    Ok(())
}

/// The unified table's current Delta version.
async fn unified_version(db: &Database, table: &str) -> u64 {
    get_unified_delta_table(db.unified_tables(), table).await.expect("table created").read().await.version().unwrap_or(0)
}

/// The single Int64 value a `COUNT(*)`-shaped query returns.
async fn count_of(ctx: &SessionContext, sql: &str) -> Result<i64> {
    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(batches[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().expect("an Int64 aggregate").value(0))
}

/// One `CoalescedWriteUnit` carrying `batch` plus its own watermark.
fn coalesced_unit(project: &str, table: &str, batch: RecordBatch, block_id: u64, offset: u64) -> CoalescedWriteUnit {
    use walrus_rust::WalPosition;
    CoalescedWriteUnit {
        project_id: project.to_owned(),
        table_name: table.to_owned(),
        batches: vec![batch],
        watermark: vec![Some(WalPosition { block_id, offset })],
    }
}

#[tokio::test]
async fn dv_scan_preserves_physical_positions_under_file_repartitioning() -> Result<()> {
    use deltalake::delta_datafusion::TableProviderBuilder;
    use deltalake::kernel::transaction::CommitBuilder;
    use deltalake::operations::deletion_vectors::{FileDeletion, write_deletion_vectors};

    let mut table = dv_int_table("dv_physical_positions").await?;
    let table_ref = Arc::new(RwLock::new(table.clone()));
    let targets = live_adds(&table_ref).await;
    assert_eq!(targets.len(), 1);
    let log_store = table.log_store();
    let actions =
        write_deletion_vectors(log_store.as_ref(), log_store.root_url(), vec![FileDeletion { add: targets[0].clone(), deleted_indexes: vec![1] }]).await?;
    let committed = CommitBuilder::default()
        .with_actions(actions)
        .build(Some(table.snapshot()?), log_store.clone(), deltalake::protocol::DeltaOperation::Delete { predicate: Some("id = 2".into()) })
        .await?;
    table.state = Some(committed.snapshot().clone());
    let provider = TableProviderBuilder::default()
        .with_log_store(log_store)
        .with_eager_snapshot(Arc::new(table.snapshot()?.snapshot().clone()))
        .with_file_column("file_id")
        .with_row_index_column("row_ordinal")
        .build()
        .await?;
    // Force the optimizer's splitting threshold below this tiny fixture.
    let mut config = datafusion::prelude::SessionConfig::new().with_batch_size(1).with_target_partitions(4);
    config.options_mut().optimizer.repartition_file_min_size = 0;
    let ctx = datafusion::prelude::SessionContext::new_with_config(config);
    ctx.register_table("dv", Arc::new(provider))?;
    for (query, expected) in [
        ("SELECT id FROM dv", "+----+\n| id |\n+----+\n| 1  |\n| 3  |\n| 4  |\n+----+"),
        (
            "SELECT id, row_ordinal FROM dv",
            "+----+-------------+\n| id | row_ordinal |\n+----+-------------+\n| 1  | 1           |\n| 3  | 3           |\n| 4  | 4           |\n+----+-------------+",
        ),
    ] {
        let plan = ctx.sql(query).await?.create_physical_plan().await?;
        let nodes = plan_nodes(&plan);
        let files: Vec<_> = nodes
            .iter()
            .filter_map(|node| node.downcast_ref::<DataSourceExec>()?.data_source().downcast_ref::<FileScanConfig>())
            .flat_map(|scan| scan.file_groups.iter().flat_map(|group| group.iter()))
            .collect();
        assert!(!files.is_empty(), "must inspect the actual Parquet scan");
        assert!(files.iter().all(|file| file.range.is_none()), "deletion masks and physical ordinals require whole-file scans");
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx()).await?;
        assert_eq!(datafusion::arrow::util::pretty::pretty_format_batches(&batches)?.to_string(), expected);
    }
    assert_eq!(count_of(&ctx, "SELECT COUNT(*) FROM dv WHERE id >= 2").await?, 2, "filtered count must exclude the deleted row");
    Ok(())
}

#[tokio::test]
async fn wave_rejects_a_superseded_deletion_vector() -> Result<()> {
    use deltalake::kernel::{Action, transaction::CommitBuilder};
    use deltalake::operations::deletion_vectors::{FileDeletion, dv_object_store_relative_path, write_deletion_vectors};
    use object_store::ObjectStoreExt;

    let db = Database::with_config(create_test_config("wave-dv-liveness")).await?;
    let table_ref = Arc::new(RwLock::new(dv_int_table("wave_dv_liveness").await?));
    let target = live_adds(&table_ref).await.into_iter().next().expect("one written file");
    let mut table = table_ref.read().await.clone();
    let log_store = table.log_store();
    let stage_store = log_store.object_store(None);
    let stale_actions =
        write_deletion_vectors(log_store.as_ref(), log_store.root_url(), vec![FileDeletion { add: target.clone(), deleted_indexes: vec![0] }]).await?;
    let committed_actions =
        write_deletion_vectors(log_store.as_ref(), log_store.root_url(), vec![FileDeletion { add: target.clone(), deleted_indexes: vec![1] }]).await?;
    let committed = CommitBuilder::default()
        .with_actions(committed_actions.clone())
        .build(Some(table.snapshot()?), log_store.clone(), super::wave_operation(true, 256, None))
        .await?;
    table.state = Some(committed.snapshot().clone());
    let version = table.version();
    let table_ref = Arc::new(RwLock::new(table));
    let bin = |actions: Vec<Action>| {
        let discardable_paths = actions
            .iter()
            .filter_map(|action| match action {
                Action::Add(add) => add.deletion_vector.as_ref().and_then(dv_object_store_relative_path),
                _ => None,
            })
            .collect();
        let (removes, adds) = actions.into_iter().partition(|a| matches!(a, Action::Remove(_)));
        super::StagedBin {
            project_id: "alpha".into(),
            wave_id: "dv-liveness".into(),
            targets: vec![target.clone()],
            removes,
            adds,
            stage_store: stage_store.clone(),
            discardable_paths,
            sorted: false,
            dedup: Some(dedup_unit("2026-09-07", 4, 3)),
        }
    };
    // A survivor read can change without appearing in our Remove actions.
    let mut read_only = bin(stale_actions.clone());
    read_only.removes.clear();
    let active = super::ActiveFiles::from_snapshot(table_ref.read().await.snapshot()?);
    assert!(super::split_live_bins(vec![read_only], &active).0.is_empty(), "changed read-only inputs also invalidate staging");
    let stale = bin(stale_actions);
    let stale_sidecar = object_store::path::Path::from(stale.discardable_paths[0].as_str());
    assert!(stage_store.head(&stale_sidecar).await.is_ok(), "staging wrote the orphan candidate");
    let probe = db.probe_commit_landed(&table_ref, &stale.adds).await;
    assert!(matches!(probe, super::CommitProbe::Inconclusive), "a changed DV cannot authorize deleting live parquet");
    let reported_landed = matches!(probe, super::CommitProbe::Landed);
    let result = db.commit_wave(&table_ref, "otel_logs_and_spans", &[], true, vec![stale], 0).await;
    assert_eq!(
        (reported_landed, result.landed.len(), result.failed.len(), table_ref.read().await.version()),
        (false, 0, 1, version),
        "same path with a different DV is a stale target, not a landed commit"
    );
    assert!(stage_store.head(&stale_sidecar).await.is_err(), "uncommitted sidecar is reclaimed");
    let committed_bin = bin(committed_actions);
    let committed_sidecar = object_store::path::Path::from(committed_bin.discardable_paths[0].as_str());
    assert!(matches!(db.probe_commit_landed(&table_ref, &committed_bin.adds).await, super::CommitProbe::Landed));
    assert!(stage_store.head(&committed_sidecar).await.is_ok(), "committed sidecar exists before cleanup");
    // Cleanup can see a partially landed wave: preserve the exact live DV.
    db.discard_bins(&table_ref, std::slice::from_ref(&committed_bin), None).await;
    assert!(stage_store.head(&committed_sidecar).await.is_ok(), "committed DV must survive cleanup");
    assert!(stage_store.head(&object_store::path::Path::from(target.path.as_str())).await.is_ok(), "same-path parquet remains live");
    let landed = db.commit_wave(&table_ref, "otel_logs_and_spans", &[], true, vec![committed_bin], 0).await;
    assert_eq!(
        (landed.landed.len(), landed.failed.len(), table_ref.read().await.version()),
        (1, 0, version),
        "an exact DV match recognizes our prior commit without repeating it"
    );
    Ok(())
}

/// The commit lock is FIFO, so long wave commits queued ahead of a flush can starve it past
/// its watchdog without any holder hanging. Durability outranks maintenance: a wave with a
/// flush already queued must requeue its bins and count the yield.
#[tokio::test]
async fn wave_commit_yields_to_a_waiting_flush() -> Result<()> {
    use deltalake::protocol::SaveMode;
    use std::sync::atomic::Ordering::Relaxed;

    let db = Database::with_config(create_test_config("wave-flush-priority")).await?;
    let (_, _, table) = mem_table("wave_flush_priority", int_id_cols()).await?;
    let table = table.write(vec![int_id_batch(vec![1, 2])]).with_save_mode(SaveMode::Append).await?;
    let target = table.snapshot()?.log_data().iter().map(|f| f.path().into_owned()).next().expect("the written file is live");
    let version = table.version();
    let table_ref = Arc::new(RwLock::new(table));
    // The wave keys on ("", table) — the same key every flush committer uses.
    let bin = || vec![staged_unit("alpha", &[target.as_str()], None)];

    // A flush queued on the lock ⇒ the wave stands down without committing.
    let waiter = flush_waiter(&db.flush_waiters("", "otel_logs_and_spans").await);
    let yields = mstats().wave_commits_yielded_to_flush.load(Relaxed);
    let deferred = db.commit_wave(&table_ref, "otel_logs_and_spans", &[], false, bin(), 0).await;
    assert_eq!(mstats().wave_commits_yielded_to_flush.load(Relaxed), yields + 1, "the yield is counted, not silent");
    assert!(deferred.landed.is_empty(), "nothing may land while a flush waits");
    assert_eq!(deferred.failed.len(), 1, "the bin is requeued (dedup's dirty bin must not be certified clean)");
    assert_eq!(table_ref.read().await.version(), version, "no commit was attempted");

    // Flush done ⇒ the very same wave commits.
    drop(waiter);
    let landed = db.commit_wave(&table_ref, "otel_logs_and_spans", &[], false, bin(), 0).await;
    assert_eq!(mstats().wave_commits_yielded_to_flush.load(Relaxed), yields + 1, "no yield without a waiter");
    assert_eq!(landed.landed.len(), 1);
    assert!(landed.failed.is_empty());
    assert_eq!(table_ref.read().await.version(), version.map(|v| v + 1), "the wave commits as before once no flush is queued");
    Ok(())
}

/// A stuck coalescer drain must not overrun the stop grace: `shutdown_by` must honor its
/// deadline rather than hang, which would hold wal.lock until the orchestrator SIGKILLs us.
#[tokio::test]
async fn shutdown_by_bounds_a_blocked_dml_drain() -> Result<()> {
    let db = Database::with_config(create_test_config("shutdown-drain-bound")).await?;
    let coalescer = Arc::new(crate::dml::DmlCoalescer::new(600, true));
    let _ = db.dml_coalescer.set(coalescer.clone());
    let _held = coalescer.lock_drain_for_test().await; // drain() blocks on this
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(300);
    let res = tokio::time::timeout(std::time::Duration::from_secs(5), db.shutdown_by(deadline)).await;
    assert!(res.is_ok(), "shutdown_by hung on a blocked drain instead of honoring the deadline");
    Ok(())
}

/// Snapshot refresh must not hold the table write lock across `update_state()` (full log
/// replay + object-store IO), or concurrent readers convoy behind it. With a deliberately slow
/// object store, read-lock acquisition must stay fast.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_table_snapshot_does_not_block_readers() -> Result<()> {
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    let (mem, url, table) = mem_table("convoy_tbl", get_default_schema().columns().unwrap_or_default()).await?;
    assert_eq!(table.version(), Some(0));

    // Same store, but every list/get pays a delay, making update_state measurably slow.
    let wait = std::time::Duration::from_millis(100);
    let throttled = ThrottledStore::new(
        mem,
        ThrottleConfig { wait_get_per_call: wait, wait_list_per_call: wait, wait_list_with_delimiter_per_call: wait, ..Default::default() },
    );
    let slow = mem_backend(Arc::new(throttled), &url).build()?;
    let shared = Arc::new(RwLock::new(slow));

    let refresher = {
        let shared = Arc::clone(&shared);
        tokio::spawn(async move { refresh_table_snapshot(&shared, true).await })
    };

    // Sample read-lock acquisition latency while the refresh is in flight.
    let mut max_wait = std::time::Duration::ZERO;
    let started = std::time::Instant::now();
    while !refresher.is_finished() && started.elapsed() < std::time::Duration::from_secs(30) {
        let t0 = std::time::Instant::now();
        drop(shared.read().await);
        max_wait = max_wait.max(t0.elapsed());
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let refresh_took = started.elapsed();
    let version = refresher.await?.map_err(|e| anyhow::anyhow!(e))?;

    assert_eq!(version, Some(0), "refresh resolved the table snapshot");
    assert!(refresh_took >= wait, "throttle must make the refresh measurably slow (took {refresh_took:?})");
    assert!(
        max_wait < wait / 2,
        "readers stalled {max_wait:?} behind an in-flight refresh (refresh took {refresh_took:?}) — write lock is being held across update_state"
    );
    Ok(())
}

/// Advancing a materialized snapshot incrementally across a `replace_where` (Add + Remove)
/// must yield exactly the active file set a full re-materialize produces. Drift here silently
/// corrupts query results by keeping a tombstoned file or dropping a live one.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_incremental_matches_full_across_removes() -> Result<()> {
    use datafusion::arrow::{
        array::{Int32Array, RecordBatch, StringArray},
        datatypes::{DataType as ArrowDataType, Field, Schema},
    };
    use deltalake::{
        kernel::{DataType, PrimitiveType, StructField},
        protocol::SaveMode,
    };

    let mem = Arc::new(object_store::memory::InMemory::new());
    let url = Url::parse("memory:///tierc_removes")?;
    let backend = || mem_backend(mem.clone(), &url);

    let cols = vec![
        StructField::new("id", DataType::Primitive(PrimitiveType::Integer), true),
        StructField::new("p", DataType::Primitive(PrimitiveType::String), true),
    ];
    let table = backend().build()?.create().with_columns(cols).with_partition_columns(["p".to_string()]).await?;

    let schema = Arc::new(Schema::new(vec![Field::new("id", ArrowDataType::Int32, true), Field::new("p", ArrowDataType::Utf8, true)]));
    let batch = |ids: Vec<i32>, ps: Vec<&str>| {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(ids)) as _, Arc::new(StringArray::from(ps)) as _]).unwrap()
    };

    // Plain writes set no incremental flag, so the returned `table` is the authoritative full
    // re-materialize at every step.
    let table = table.write(vec![batch(vec![1, 2], vec!["a", "a"])]).with_save_mode(SaveMode::Append).await?;
    let table = table.write(vec![batch(vec![3], vec!["b"])]).with_save_mode(SaveMode::Append).await?;
    assert_eq!(table.version(), Some(2));

    // v3: replace_where p=a → tombstones v1's file, adds a new one (Add + Remove).
    let table = table.write(vec![batch(vec![10, 11], vec!["a", "a"])]).with_save_mode(SaveMode::Overwrite).with_replace_where("p = 'a'").await?;
    assert_eq!(table.version(), Some(3));

    let uris = |t: &DeltaTable| t.get_file_uris().map(|it| it.collect::<HashSet<String>>()).unwrap_or_default();
    let truth = uris(&table); // authoritative v3 set (full re-materialize)
    assert_eq!(truth.len(), 2, "v3 active set = p=b file + replaced p=a file");

    // Stale handle pinned at v2. Assert catch-up RETURNED TRUE — a silent fallback to a full
    // update_state also produces a correct set and so would hide a broken incremental path.
    let mut stale = backend().with_version(2).load().await?;
    assert!(stale.state.as_ref().is_some_and(|s| s.has_materialized_files()), "stale handle must be materialized to exercise the fast path");
    let log_store = stale.log_store();
    let took_fast_path = stale.state.as_mut().unwrap().advance_catchup(log_store.as_ref(), REFRESH_APPEND_CATCHUP_MAX_GAP).await?;
    assert!(took_fast_path, "advance_catchup must take the incremental path over the replace_where, not fall back to a full update");
    assert_eq!(stale.version(), Some(3), "incremental catch-up reached the latest version");
    assert_eq!(uris(&stale), truth, "incremental advance across replace_where must equal the full re-materialize");
    Ok(())
}

/// Tables predating the `delta.deletedFileRetentionDuration` property get it set once at
/// load, idempotently — otherwise delta's 7-day default accumulates Remove tombstones.
#[tokio::test(flavor = "multi_thread")]
async fn ensure_deleted_file_retention_sets_property_once() -> Result<()> {
    const KEY: &str = "delta.deletedFileRetentionDuration";
    const CP_KEY: &str = "delta.checkpointInterval";
    let props = |hours: u64| HashMap::from([(KEY.to_string(), format!("interval {hours} hours")), (CP_KEY.to_string(), "1".to_string())]);
    let (_, _, table) = mem_table("retention_tbl", get_default_schema().columns().unwrap_or_default()).await?;
    assert!(!table.snapshot()?.metadata().configuration().contains_key(KEY), "fresh table has no retention property");

    let table = ensure_table_properties(table, props(24)).await;
    let config = table.snapshot()?.metadata().configuration().clone();
    assert_eq!(config.get(KEY).map(String::as_str), Some("interval 24 hours"));
    assert_eq!(config.get(CP_KEY).map(String::as_str), Some("1"), "checkpoint interval retrofitted alongside");
    assert_eq!(table.version(), Some(1), "properties set in one commit");

    let table = ensure_table_properties(table, props(24)).await;
    assert_eq!(table.version(), Some(1), "matching properties must not commit again");

    let table = ensure_table_properties(table, props(48)).await;
    assert_eq!(table.snapshot()?.metadata().configuration().get(KEY).map(String::as_str), Some("interval 48 hours"));
    assert_eq!(table.version(), Some(2));
    let files: Vec<_> = table.log_store().object_store(None).list(None).try_collect().await?;
    assert!(files.iter().all(|file| !file.location.as_ref().contains("checkpoint")), "table property reconciliation must leave checkpointing out of band");
    Ok(())
}

/// `refresh_table_snapshot` on an already-current table must not pay a `_delta_log` LIST;
/// the immutable-commit probe (GET version+1 → 404) short-circuits it.
#[tokio::test(flavor = "multi_thread")]
async fn refresh_table_snapshot_probes_instead_of_listing() -> Result<()> {
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    let (mem, url, table) = mem_table("probe_tbl", get_default_schema().columns().unwrap_or_default()).await?;

    let list_wait = std::time::Duration::from_secs(2);
    let throttled =
        ThrottledStore::new(mem, ThrottleConfig { wait_list_per_call: list_wait, wait_list_with_delimiter_per_call: list_wait, ..Default::default() });
    let mut slow = mem_backend(Arc::new(throttled), &url).build()?;
    slow.update_state().await?; // initial load pays the LIST
    let shared = Arc::new(RwLock::new(slow));

    let t0 = std::time::Instant::now();
    assert_eq!(refresh_table_snapshot(&shared, true).await.map_err(|e| anyhow::anyhow!(e))?, Some(0));
    assert!(t0.elapsed() < list_wait, "current-table refresh paid a LIST ({:?})", t0.elapsed());

    // External commit → the probe finds {v+1}.json and runs the full update.
    let _ = ensure_table_properties(table, HashMap::from([("delta.checkpointInterval".to_string(), "50".to_string())])).await;
    assert_eq!(refresh_table_snapshot(&shared, true).await.map_err(|e| anyhow::anyhow!(e))?, Some(1));
    Ok(())
}

/// `scoped_file_uris` must produce URIs byte-identical to `get_file_uris()`
/// (warm/evict diffs compare them), and partition markers must select exactly
/// the matching files.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn scoped_file_uris_matches_get_file_uris_and_filters_by_partition() -> Result<()> {
    let (db, _ctx, prefix) = setup_test_database().await?;
    let (p1, p2) = (format!("sfu_a_{prefix}"), format!("sfu_b_{prefix}"));
    for pid in [&p1, &p2] {
        insert_span(&db, pid, "otel_logs_and_spans", "sfu1", true).await?;
    }
    let table_ref = get_unified_delta_table(db.unified_tables(), "otel_logs_and_spans").await.expect("table created");
    let table = table_ref.read().await;

    let expected: Vec<String> = table.get_file_uris()?.collect();
    assert!(!expected.is_empty(), "expected active files");
    assert_eq!(scoped_file_uris(&table, &[]), expected, "unscoped walk must be byte-identical to get_file_uris()");

    let marker = format!("project_id={p1}/");
    let scoped = scoped_file_uris(&table, &[marker.as_str()]);
    assert!(!scoped.is_empty() && scoped.len() < expected.len(), "scope must select a proper non-empty subset");
    assert!(scoped.iter().all(|u| u.contains(&marker)), "every scoped URI is in the scoped partition");
    assert_eq!(scoped, expected.iter().filter(|u| u.contains(&marker)).cloned().collect::<Vec<_>>(), "scoped walk must equal the filtered full walk");
    assert!(scoped_file_uris(&table, &["project_id=no_such_project"]).is_empty(), "a non-matching scope selects nothing");
    Ok(())
}

/// `--project` scoping is refused (it deadlocks the write) rather than silently
/// ignored, which would rewrite every tenant on the date behind the caller's back.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn recompress_refuses_project_scope_and_leaves_data_intact() -> Result<()> {
    within(180, async {
        let (db, ctx, prefix) = setup_test_database().await?;
        let (target, other) = (format!("target_{prefix}"), format!("other_{prefix}"));
        let today = chrono::Utc::now().date_naive();
        for (pid, ids) in [(&target, ["t1", "t2"]), (&other, ["o1", "o2"])] {
            for id in ids {
                insert_span(&db, pid, "otel_logs_and_spans", id, true).await?;
            }
        }
        let table_ref = get_unified_delta_table(db.unified_tables(), "otel_logs_and_spans").await.expect("table created");
        let files =
            |m: String| -> Result<Vec<String>> { Ok(futures::executor::block_on(table_ref.read()).get_file_uris()?.filter(|u| u.contains(&m)).collect()) };
        let (t_before, o_before) = (files(format!("project_id={target}/"))?, files(format!("project_id={other}/"))?);
        assert!(t_before.len() > 1 && o_before.len() > 1, "both tenants need >1 file for the rewrite to be non-trivial");

        let err = db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9, Some(target.as_str())).await.unwrap_err();
        assert!(err.to_string().contains("--project is disabled"), "must refuse, got: {err}");
        assert_eq!(files(format!("project_id={target}/"))?, t_before, "a refused scope must not have written anything");
        assert_eq!(files(format!("project_id={other}/"))?, o_before, "every other project's files must be untouched");

        // Unscoped still works and preserves every tenant's rows.
        db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9, None).await?;
        let rows = ctx.sql(&format!("SELECT id FROM otel_logs_and_spans WHERE project_id = '{other}'")).await?.collect().await?;
        assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 2, "other project's rows must survive the rewrite");

        db.shutdown().await?;
        Ok(())
    })
    .await
}

/// End-to-end `recompress_partition`. Skip behaviour is the load-bearing
/// property: if the footer-tier probe breaks, the daily cron rewrites every
/// partition every night. Asserted via file-set comparison.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_recompress_partition_skip_idempotency() -> Result<()> {
    within(180, async {
        let (db, ctx, prefix) = setup_test_database().await?;
        let project_id = format!("project_{}", prefix);
        let today = chrono::Utc::now().date_naive();

        // Two rows across two commits → >1 file, so the rewrite genuinely merges.
        for (id, name) in [("rc1", "span1"), ("rc2", "span2")] {
            let batch = json_to_batch(vec![test_span(id, name, &project_id)])?;
            db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
        }

        let table_ref = get_unified_delta_table(db.unified_tables(), "otel_logs_and_spans").await.expect("table created");

        // Baseline: the replace_where rewrite must preserve every row verbatim.
        let rows_sql = format!("SELECT id, name FROM otel_logs_and_spans WHERE project_id = '{project_id}' ORDER BY id");
        let live = async || -> Result<Vec<String>> { Ok(table_ref.read().await.get_file_uris()?.collect()) };
        let ids_before = ctx.sql(&rows_sql).await?.collect().await?;
        assert_eq!(ids_before.iter().map(|b| b.num_rows()).sum::<usize>(), 2, "baseline must have both rows");

        let files_before = live().await?;
        assert!(!files_before.is_empty(), "expected files in today's partition");
        db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9, None).await?;
        let files_after = live().await?;
        assert_ne!(files_before, files_after, "first recompress must rewrite files");

        let ids_after = ctx.sql(&rows_sql).await?.collect().await?;
        assert_eq!(ids_after.iter().map(|b| b.num_rows()).sum::<usize>(), 2, "recompress must preserve all rows");
        assert_eq!(format!("{ids_before:?}"), format!("{ids_after:?}"), "recompress must preserve row contents verbatim");

        // Re-run at the same tier — the footer probe must detect tier=9 and skip.
        let rerun = db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 9, None).await?;
        assert!(matches!(rerun, RecompressOutcome::Skipped(_)), "rerun at same tier must report a SKIP, not a rewrite");
        assert_eq!(files_after, live().await?, "rerun at same tier must skip");

        db.recompress_partition(&table_ref, "otel_logs_and_spans", today, 3, None).await?;
        assert_eq!(files_after, live().await?, "downgrade target must skip");

        db.shutdown().await?;
        Ok(())
    })
    .await
}

/// The Delta-empty short-circuit: `delta_scan_can_be_skipped` defaults to
/// `false` (run the full scan), and the has-files bit is per-(project,table)
/// and STICKY — a resolve observing `version() == 0` must never downgrade it.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_delta_has_files_sticky_bit() -> Result<()> {
    let (db, _ctx, prefix) = setup_test_database().await?;
    let t = "otel_logs_and_spans";
    let p1 = format!("proj-marked-{prefix}");
    let p2 = format!("proj-unmarked-{prefix}");

    assert!(!db.delta_scan_can_be_skipped(&p1, t), "unknown projects must default to false so callers don't skip Delta");
    assert!(!db.delta_scan_can_be_skipped(&p2, t), "second unknown project also defaults to false");

    db.mark_delta_has_files(&p1, t);
    assert!(!db.delta_scan_can_be_skipped(&p1, t), "after mark_delta_has_files, table has files → can't skip");

    assert!(!db.delta_scan_can_be_skipped(&p2, t), "marking p1 must not affect p2's bit");

    db.mark_delta_has_files(&p1, t);
    assert!(!db.delta_scan_can_be_skipped(&p1, t), "re-mark is idempotent — still has files");

    // A fresh handle reports version() == 0; the sticky-true bit must survive it.
    let _t = db.resolve_table(&p1, t).await?;
    assert!(
        !db.delta_scan_can_be_skipped(&p1, t),
        "STICKY-TRUE: resolve_table observing version==0 must NOT downgrade a previously-marked bit. \
             A regression here means post-flush rows get hidden from queries."
    );

    // Same invariant on the SELECT path (try_fast_resolve → fast_resolve_cache).
    let _ = db.try_fast_resolve(&p1, t);
    assert!(!db.delta_scan_can_be_skipped(&p1, t), "STICKY-TRUE preserved across try_fast_resolve too");
    Ok(())
}

/// The write MARKS a path; admission LOOKS ONE UP — if the two strings differ the
/// feature fails silently. Asserted against what `plan_compaction_debt` reads, the
/// snapshot's `LogicalFile::path()` (delta-rs stores `Add.path` URL-encoded and
/// decodes it there), for both the solo and the coalesced commit path.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn write_time_marks_use_the_same_path_strings_admission_reads() -> Result<()> {
    let (db, _ctx, prefix) = setup_test_database().await?;
    let t = "otel_logs_and_spans";
    let solo = format!("mark-solo-{prefix}");
    let group: Vec<String> = (0..2).map(|i| format!("mark-grp{i}-{prefix}")).collect();

    insert_span(&db, &solo, t, "s", true).await?;
    let units: Vec<CoalescedWriteUnit> = group
        .iter()
        .enumerate()
        .map(|(i, p)| coalesced_unit(p, t, json_to_batch(vec![test_span(&format!("g{i}"), "span", p)]).unwrap(), 700 + i as u64, i as u64))
        .collect();
    for result in db.insert_records_batches_coalesced(units).await {
        result.expect("coalesced commit failed");
    }

    let table_ref = get_unified_delta_table(db.unified_tables(), t).await.expect("table created");
    let live: HashSet<String> = table_ref.read().await.snapshot()?.log_data().iter().map(|f| f.path().into_owned()).collect();
    let marked: HashSet<String> = db.repair_verified_sorted.iter().map(|entry| entry.key().clone()).collect();

    // The falsifier: an empty marked set is a wiring bug that looks like "nothing to do".
    assert!(!marked.is_empty(), "no file was marked at write time — the marking is not reaching the commit path");
    assert_eq!(marked, live, "every marked path must be a path admission will look up, byte for byte");
    Ok(())
}

/// Files that predate write-time marking must be seeded from their footers, not
/// left as repair suspects. Clearing the set after a real write stands in for a
/// fleet of correctly-sorted files this process was never told about.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn the_seeding_sweep_reaches_files_that_predate_write_time_marking() -> Result<()> {
    let (db, _ctx, prefix) = setup_test_database().await?;
    let t = "otel_logs_and_spans";
    let project = format!("seed-{prefix}");
    insert_span(&db, &project, t, "s", true).await?;

    let written: HashSet<String> = db.repair_verified_sorted.iter().map(|entry| entry.key().clone()).collect();
    assert!(!written.is_empty(), "the write must have marked something for this test to mean anything");
    db.repair_verified_sorted.clear();

    let (tables_read, seeded) = db.seed_verified_sorted(REPAIR_VERIFY_SEED_LIMIT).await;
    assert!(tables_read > 0, "the sweep must have READ a table — reading none is the boot race, not a clean fleet");
    assert!(seeded > 0, "the sweep must re-derive sortedness from the footers already on storage");
    let after: HashSet<String> = db.repair_verified_sorted.iter().map(|entry| entry.key().clone()).collect();
    assert!(written.is_subset(&after), "every file the write knew was sorted must be recovered by the probe");

    // Idempotent and IO-free the second time — nothing is unknown any more.
    assert_eq!(db.seed_verified_sorted(REPAIR_VERIFY_SEED_LIMIT).await.1, 0, "a second sweep must find nothing unknown");
    Ok(())
}

/// N default-storage projects flushed in one tick must produce EXACTLY ONE Delta
/// commit carrying every project's files and watermark, with each project's result
/// listing only its own files, and every project's rows queryable from that commit.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn coalesced_commit_spans_projects_in_one_delta_version() -> Result<()> {
    use walrus_rust::WalPosition;
    let (db, _ctx, prefix) = setup_test_database().await?;
    let t = "otel_logs_and_spans";
    let projects: Vec<String> = (0..3).map(|i| format!("coal{i}-{prefix}")).collect();

    // Create the table first so the version delta measures the coalesced commit alone.
    db.insert_records_batch(&projects[0], t, vec![json_to_batch(vec![test_span("warm", "warm", &projects[0])])?], true, None).await?;
    let table_ref = get_unified_delta_table(db.unified_tables(), t).await.expect("table created");
    let before = table_ref.read().await.version().unwrap_or(0);

    let units: Vec<CoalescedWriteUnit> = projects
        .iter()
        .enumerate()
        .map(|(i, p)| coalesced_unit(p, t, json_to_batch(vec![test_span(&format!("c{i}"), "span", p)]).unwrap(), 100 + i as u64, i as u64))
        .collect();
    let results = db.insert_records_batches_coalesced(units).await;

    assert_eq!(results.len(), projects.len(), "one result per unit, in input order");
    let added: Vec<Vec<String>> = results.into_iter().map(|r| r.expect("coalesced commit failed")).collect();

    let after = table_ref.read().await.version().unwrap_or(0);
    assert_eq!(after, before + 1, "N default-storage projects must land in ONE Delta commit, got {} commits", after - before);

    // Files are attributed to their own partition path (tantivy/warming inputs stay per project).
    for (i, project) in projects.iter().enumerate() {
        assert!(!added[i].is_empty(), "project {project} contributed no files to the coalesced commit");
        assert!(added[i].iter().all(|u| u.contains(&format!("project_id={project}/"))), "project {project} was handed a co-tenant's files: {:?}", added[i]);
    }

    // The single commit carries EVERY project's watermark (crash-recovery invariant).
    let history: Vec<_> = table_ref.read().await.history(Some(1)).await?.collect();
    assert_eq!(history.len(), 1);
    let shards = 8;
    for (i, project) in projects.iter().enumerate() {
        let parsed = parse_watermark_from_json(&history[0].info, shards, project, t);
        assert_eq!(parsed[0], Some(WalPosition { block_id: 100 + i as u64, offset: i as u64 }), "project {project} lost/mixed up its watermark");
    }
    // A project not in the commit inherits nothing.
    assert!(parse_watermark_from_json(&history[0].info, shards, "outsider", t).iter().all(Option::is_none));

    let files = table_ref.read().await.get_file_uris().map(|it| it.collect::<Vec<_>>()).unwrap_or_default();
    for project in &projects {
        assert!(files.iter().any(|u| u.contains(&format!("project_id={project}/"))), "project {project} has no active file after the coalesced commit");
    }
    Ok(())
}

/// A project whose batches need a schema merge must be split OUT of the coalesced
/// group and committed on its own, not drag every co-tenant through the slow path:
/// two default projects + one evolving project ⇒ exactly TWO commits.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn schema_evolution_project_splits_out_of_the_coalesced_group() -> Result<()> {
    use datafusion::arrow::{
        array::{Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    let (db, _ctx, prefix) = setup_test_database().await?;
    let t = "otel_logs_and_spans";
    let (p1, p2, evolving) = (format!("se1-{prefix}"), format!("se2-{prefix}"), format!("se3-{prefix}"));

    db.insert_records_batch(&p1, t, vec![json_to_batch(vec![test_span("warm", "warm", &p1)])?], true, None).await?;
    let before = unified_version(&db, t).await;

    // delta-rs' Default-mode RecordBatchWriter cannot evolve schema on a partitioned
    // table, so a batch with an unknown column has no staged writer and goes solo.
    let base = json_to_batch(vec![test_span("e1", "span", &evolving)])?;
    let mut fields: Vec<Field> = base.schema().fields().iter().map(|f| f.as_ref().clone()).collect();
    fields.push(Field::new("c3_brand_new_column", DataType::Utf8, true));
    let mut columns: Vec<Arc<dyn Array>> = base.columns().to_vec();
    columns.push(Arc::new(StringArray::from(vec![Some("evolved")])));
    let evolved_batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;

    let units = vec![
        coalesced_unit(&p1, t, json_to_batch(vec![test_span("s1", "span", &p1)])?, 1, 0),
        coalesced_unit(&evolving, t, evolved_batch, 2, 0),
        coalesced_unit(&p2, t, json_to_batch(vec![test_span("s2", "span", &p2)])?, 3, 0),
    ];
    let results = db.insert_records_batches_coalesced(units).await;
    for (i, r) in results.iter().enumerate() {
        assert!(r.is_ok(), "unit {i} failed: {:?}", r.as_ref().err());
    }
    // Results stay in INPUT order even though the evolving unit committed last.
    assert!(results[0].as_ref().unwrap().iter().all(|u| u.contains(&format!("project_id={p1}/"))));
    assert!(results[1].as_ref().unwrap().iter().all(|u| u.contains(&format!("project_id={evolving}/"))));
    assert!(results[2].as_ref().unwrap().iter().all(|u| u.contains(&format!("project_id={p2}/"))));

    // Three commits would mean no coalescing; one, that the merge path swallowed the co-tenants.
    let after = unified_version(&db, t).await;
    assert_eq!(after, before + 2, "expected 1 coalesced + 1 solo schema-evolution commit");

    let table = get_unified_delta_table(db.unified_tables(), t).await.expect("table");
    let guard = table.read().await;
    assert!(guard.snapshot()?.schema().fields().any(|f| f.name() == "c3_brand_new_column"), "schema merge never landed");
    Ok(())
}

/// A custom-storage project has its OWN `_delta_log` and must never be
/// coalesced into the shared unified-table commit. The grouping key IS
/// `table_lock_key`, so isolation is structural: same key ⇒ same physical
/// log ⇒ safe to share a commit; different key ⇒ separate commit.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn custom_storage_project_is_not_coalesced_with_default_storage() -> Result<()> {
    let (db, _ctx, prefix) = setup_test_database().await?;
    let t = "otel_logs_and_spans";
    let custom = format!("cust-{prefix}");
    register_custom_storage(&db, &custom, t, &format!("custom-{prefix}")).await;

    let (a, b) = (db.table_lock_key("proj_a", t).await, db.table_lock_key("proj_b", t).await);
    assert_eq!(a, b, "default-storage projects share a physical log → one coalesced commit");
    assert_ne!(db.table_lock_key(&custom, t).await, a, "custom-storage project must group (and commit) separately");
    // Same project on a different table is also a different physical log.
    assert_ne!(db.table_lock_key("proj_a", "otel_metrics").await, a);
    Ok(())
}

/// The provider cache must rebuild when `table.version()` advances: query, commit,
/// query again — the second query must see the new row.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_delta_provider_cache_invalidates_on_version_change() -> Result<()> {
    let (db, ctx, prefix) = setup_test_database().await?;
    let project_id = format!("proj-inv-{prefix}");
    let t = "otel_logs_and_spans";
    const ENTRIES_SQL: &str = "SELECT value FROM timefusion_stats WHERE component = 'scan' AND key = 'provider_cache_entries'";

    db.insert_records_batch(&project_id, t, vec![json_to_batch(vec![test_span("v1", "span1", &project_id)])?], true, None).await?;
    let v1 = unified_version(&db, t).await;
    assert!(v1 > 0, "first commit must bump version above zero");
    let count_sql = format!("SELECT count(*) AS c FROM {t} WHERE project_id = '{project_id}'");
    assert_eq!(count_of(&ctx, &count_sql).await?, 1, "first query sees the v=1 row");
    assert_eq!(db.delta_provider_cache.len(), 1, "provider cache must retain the resolved provider for the warm query");
    assert_eq!(strings_of(&ctx, ENTRIES_SQL, 0).await?, ["1"], "timefusion_stats must observe the live provider cache, not a cloned startup snapshot");

    db.insert_records_batch(&project_id, t, vec![json_to_batch(vec![test_span("v2", "span2", &project_id)])?], true, None).await?;
    let v2 = unified_version(&db, t).await;
    assert!(v2 > v1, "second commit must advance version");

    let c2 = count_of(&ctx, &count_sql).await?;
    assert_eq!(
        c2, 2,
        "STALE CACHE REGRESSION: second query must see the row added at v=v{v2}. \
             Got {c2}/2 — the delta_provider_cache version-mismatch branch is broken."
    );
    assert_eq!(db.delta_provider_cache.len(), 1, "version invalidation adds a version to the key's ring, it does not add a key");
    // The v1 provider stays cached alongside v2 so an in-flight v1 query still hits.
    {
        let ring = db.delta_provider_cache.get(&(project_id.clone(), t.to_string())).expect("ring for the queried key");
        assert_eq!(ring.len(), 2, "both v{v1} and v{v2} providers must be retained");
        let ttl = db.config.cache.provider_cache_ttl();
        let old = ring.get(v1, ttl).expect("previous version still retrievable — no rebuild for in-flight queries");
        assert!(old.initialized(), "the retained v{v1} cell must still hold its built provider");
        assert!(ring.get(v2, ttl).is_some(), "latest version cached");
        assert!(ring.get(v2 + 99, ttl).is_none(), "lookup is exact-version: an unseen version must miss");
    }
    assert_eq!(strings_of(&ctx, ENTRIES_SQL, 0).await?, ["2"], "provider_cache_entries counts retained providers across the version ring");
    Ok(())
}

/// Retention semantics of the per-(project,table) version ring, without IO.
#[test]
fn provider_versions_retains_recent_and_expires() {
    let ttl = std::time::Duration::from_secs(300);
    let mut ring = ProviderVersions::default();
    let cells: Vec<_> = (1..=4).map(|v| ring.install(v, ttl)).collect();
    assert_eq!(ring.len(), PROVIDER_VERSION_RETENTION, "ring is bounded at the retention window");
    assert!(ring.get(1, ttl).is_none(), "the oldest version falls out once the window is full");
    for (i, v) in [4u64, 3, 2].iter().enumerate() {
        let got = ring.get(*v, ttl).expect("recent version retained");
        assert!(Arc::ptr_eq(&got, &cells[3 - i]), "the SAME cell Arc comes back — no rebuild");
    }
    // Re-installing an existing version replaces it in place (no duplicate).
    let fresh = ring.install(4, ttl);
    assert_eq!(ring.len(), PROVIDER_VERSION_RETENTION);
    assert!(Arc::ptr_eq(&ring.get(4, ttl).unwrap(), &fresh));
    let zero = std::time::Duration::ZERO;
    assert!(ring.get(4, zero).is_none(), "expired versions are not served");
    assert_eq!(ring.prune(zero), PROVIDER_VERSION_RETENTION);
    assert_eq!(ring.len(), 0);
}

/// Commit coalescing end-to-end through the real stack (bootstrap → WAL → MemBuffer
/// → flush → Delta → SQL): one commit per tick, every project queryable immediately.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn coalesced_flush_e2e_keeps_every_project_queryable() -> Result<()> {
    // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
    let prefix = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let mut cfg = (*create_test_config(&prefix)).clone();
    cfg.buffer.timefusion_flush_coalesce_commits = true;
    let cfg = Arc::new(cfg);
    within(50, async {
        let b = crate::server::bootstrap(Arc::clone(&cfg)).await?;
        let t = "otel_logs_and_spans";
        let projects: Vec<String> = (0..3).map(|i| format!("e2e{i}_{prefix}")).collect();

        // Create the table first so the version delta measures only the flush commit.
        b.db.insert_records_batch(&projects[0], t, vec![json_to_batch(vec![test_span("warm", "warm", &projects[0])])?], true, None).await?;
        let before = unified_version(&b.db, t).await;

        // skip_queue=false → WAL + MemBuffer, so the flush tick owns these rows.
        for (i, project) in projects.iter().enumerate() {
            insert_span(&b.db, project, t, &format!("row{i}"), false).await?;
        }
        let stats = b.buffered_layer.flush_all_now().await?;
        assert_eq!(stats.buckets_failed, 0, "coalesced e2e flush failed");
        assert_eq!(stats.buckets_flushed, projects.len() as u64);

        let after = unified_version(&b.db, t).await;
        assert_eq!(after, before + 1, "three projects flushed in one tick must produce ONE Delta commit");

        for project in &projects {
            let n = count_of(&b.session_ctx, &format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{project}'")).await?;
            let expected = if project == &projects[0] { 2 } else { 1 }; // p0 also has its warm-up row
            assert_eq!(n, expected, "{project} rows are not queryable after the coalesced commit");
        }

        b.shutdown.cancel();
        Ok(())
    })
    .await
}

/// When `force_flush_current_buckets` commits the open bucket to Delta and later
/// inserts repopulate the same bucket_id, the query path must still return the
/// force-flushed rows (the per-bucket exclusion must not mask the whole range).
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn force_flushed_current_bucket_rows_stay_queryable() -> Result<()> {
    force_flush_visibility("ffq", false).await
}

use crate::observability::maintenance_stats as mstats;

/// Runs a test body under a wall-clock cap, so a regression fails the test
/// instead of hanging the suite.
async fn within<T>(secs: u64, body: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    tokio::time::timeout(std::time::Duration::from_secs(secs), body).await.map_err(|_| anyhow::anyhow!("Test timed out after {secs} seconds"))?
}

const TEN_MIN: i64 = 10 * 60 * 1_000_000;

/// Noon of the day 26h ago, with its date. Sealed far beyond the seal lag,
/// and ±20 min around it can never cross a date boundary, so a test's bins
/// stay on one date whatever the wall clock says.
fn sealed_noon() -> (chrono::NaiveDate, i64) {
    let day = (Utc::now() - chrono::Duration::hours(26)).date_naive();
    (day, day.and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros())
}

/// The unified `otel_logs_and_spans` Delta table handle.
async fn otel_unified_table(db: &Database) -> Arc<RwLock<DeltaTable>> {
    db.unified_tables().read().await.get("otel_logs_and_spans").expect("the unified otel table").clone()
}

/// One `test_span_ts` row into `otel_logs_and_spans`. `skip_queue` writes
/// straight to Delta; `false` routes through the buffered layer.
async fn insert_otel_ts(db: &Database, project: &str, id: &str, name: &str, ts: i64, skip_queue: bool) -> Result<()> {
    db.insert_records_batch(project, "otel_logs_and_spans", vec![json_to_batch(vec![test_span_ts(id, name, project, ts)])?], skip_queue, None).await?;
    Ok(())
}

/// Rows force-flushed out of an open bucket must stay queryable once later inserts
/// repopulate the same bucket_id, and — with `seal` — once that bucket also seals.
/// The clock is frozen mid-window so every insert lands in the one bucket.
async fn force_flush_visibility(tag: &str, seal: bool) -> Result<()> {
    // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
    let prefix = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let cfg = create_test_config(&prefix);
    let res = within(50, async {
        // Needs the real buffered layer (force_flush path), hence full bootstrap.
        let b = crate::server::bootstrap(Arc::clone(&cfg)).await?;
        let project_id = format!("{tag}_{prefix}");
        let dur = crate::write::mem_buffer::bucket_duration_micros();
        let t0 = crate::support::set_micros((crate::support::now_micros() / dur) * dur + dur / 2);

        // 3 rows into the open bucket (skip_queue=false ⇒ WAL → MemBuffer),
        // then force-flush: bucket drained, its range still "current".
        for i in 0..3 {
            insert_otel_ts(&b.db, &project_id, &format!("flushed_{i}"), "span", t0, false).await?;
        }
        b.buffered_layer.force_flush_current_buckets().await?;
        // 2 more rows repopulate the same current bucket_id in MemBuffer.
        for i in 0..2 {
            insert_otel_ts(&b.db, &project_id, &format!("buffered_{i}"), "span", t0 + 1_000_000, false).await?;
        }
        if seal {
            // Roll past the boundary: the bucket is now sealed but unflushed.
            crate::support::advance_micros(dur);
        }

        // All 5 visible: 3 from Delta (force-flushed), 2 from MemBuffer.
        let sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{project_id}'");
        let n = count_of(&b.session_ctx, &sql).await?;
        anyhow::ensure!(n == 5, "force-flushed rows must stay queryable alongside repopulated MemBuffer rows (sealed={seal}); got {n} of 5");
        b.shutdown.cancel();
        Ok(())
    })
    .await;
    crate::support::unfreeze();
    res
}

/// A refused drain leaves its bin queued with both copies intact, and the next
/// pass that does have budget collapses the pair.
async fn assert_requeued_then_dedups(db: &Database, table: &Arc<RwLock<DeltaTable>>, refusal: &str) -> Result<()> {
    assert_eq!(db.dedup_dirty_bins.len(), 1, "a bin refused by {refusal} stays queued for the next tick, not lost");
    assert_eq!(delta_physical_row_count(table).await?, 2, "no partial rewrite landed; read-side dedup keeps results correct");
    drain_dirty_bins_ok(db, table).await?;
    assert_eq!(delta_physical_row_count(table).await?, 1, "the requeued bin dedups on a pass that has budget");
    assert!(db.dedup_dirty_bins.is_empty());
    Ok(())
}

/// A `Database` on its own storage prefix plus a unique project id — the
/// isolation every dirty-bin test needs.
async fn dirty_bin_db(tag: &str) -> Result<(Database, String)> {
    let db = Database::with_config(create_test_config(&format!("{tag}-{}", uuid::Uuid::new_v4().simple()))).await?;
    Ok((db, format!("dirty_{}", uuid::Uuid::new_v4().simple())))
}

/// A `Database` holding ONE sealed duplicate pair — `id` observed twice at
/// the same 26h-old timestamp, so exactly one dirty bin is queued — plus the
/// unified table handle. 26h is well beyond the seal lag and keeps the test
/// deterministic around midnight.
async fn sealed_dup_pair(tag: &str, id: &str) -> Result<(Database, Arc<RwLock<DeltaTable>>)> {
    let (db, project) = dirty_bin_db(tag).await?;
    let old = (Utc::now() - chrono::Duration::hours(26)).timestamp_micros();
    for observed in ["first", "second"] {
        insert_otel_ts(&db, &project, id, observed, old, true).await?;
    }
    assert_eq!(db.dedup_dirty_bins.len(), 1);
    let table = otel_unified_table(&db).await;
    Ok((db, table))
}

/// A dedup drain with the whole budget: healthy flush, no per-bin or pass deadline.
async fn drain_dirty_bins_ok(db: &Database, table: &Arc<RwLock<DeltaTable>>) -> Result<()> {
    db.dedup_dirty_bins_for_table(table, "otel_logs_and_spans", &|| true, std::time::Duration::MAX, far_future()).await
}

/// The strings in one column of a query's result, in row order.
async fn strings_of(ctx: &SessionContext, sql: &str, column: usize) -> Result<Vec<String>> {
    let batches = ctx.sql(sql).await?.collect().await?;
    Ok(batches.iter().flat_map(|b| (0..b.num_rows()).map(|row| array_get_str(b.column(column).as_ref(), row)).collect::<Vec<_>>()).collect())
}

/// `writers` concurrent single-row inserts through one `Database`, each into
/// `project_of(i)`; every task must land its row.
async fn concurrent_inserts_all_land(tag: &str, writers: usize, project_of: impl Fn(usize) -> String) -> Result<()> {
    let db = Arc::new(Database::with_config(create_test_config(tag)).await?);
    let tasks = (0..writers).map(|i| {
        let (db, project) = (Arc::clone(&db), project_of(i));
        tokio::spawn(async move {
            let batch_id = format!("batch_{i}");
            let batch = json_to_batch(vec![test_span(&batch_id, &format!("test_{batch_id}"), &project)])?;
            db.insert_records_batch(&project, "otel_logs_and_spans", vec![batch], true, None).await.map(|_| batch_id)
        })
    });
    let results: Vec<Result<String, _>> =
        futures::future::join_all(tasks).await.into_iter().map(|r| r.map_err(|e| anyhow::anyhow!("Task failed: {}", e))?).collect();
    let landed: Vec<String> = results.into_iter().collect::<Result<Vec<_>>>()?;
    assert_eq!(landed.len(), writers, "all {writers} concurrent writes should succeed");
    db.shutdown().await?;
    Ok(())
}

/// Rows force-flushed from an open bucket must stay visible after the bucket seals:
/// force-flushed buckets are exempt from the exclusion for their whole lifetime,
/// not just while current.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn force_flushed_bucket_rows_stay_queryable_after_seal() -> Result<()> {
    force_flush_visibility("ffs", true).await
}

/// A late-arriving row can pull MemBuffer's oldest timestamp to/below the query's
/// lower bound while newer rows live only in Delta; the skip-Delta rule must be
/// the flushed watermark, not `query_min >= mem_oldest`, or those rows are hidden.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn delta_skip_must_not_hide_force_flushed_rows_from_bounded_query() -> Result<()> {
    // SAFETY: walrus reads WALRUS_DATA_DIR from process env; #[serial] protects it.
    let prefix = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let cfg = create_test_config(&prefix);
    let res = within(50, async {
        let b = crate::server::bootstrap(Arc::clone(&cfg)).await?;
        let project_id = format!("ffw_{}", prefix);
        let dur = crate::write::mem_buffer::bucket_duration_micros();
        let t0 = crate::support::set_micros((crate::support::now_micros() / dur) * dur + dur / 2);

        // Newer row first → force-flushed, lives only in Delta.
        insert_otel_ts(&b.db, &project_id, "newer", "span", t0 + 2_000_000, false).await?;
        b.buffered_layer.force_flush_current_buckets().await?;
        // Late arrival with an older timestamp lands in MemBuffer.
        insert_otel_ts(&b.db, &project_id, "older", "span", t0 + 1_000_000, false).await?;

        let bound = chrono::DateTime::from_timestamp_micros(t0 + 1_000_000).unwrap().to_rfc3339();
        let sql = format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{}' AND timestamp >= TIMESTAMP '{}'", project_id, bound);
        let n = count_of(&b.session_ctx, &sql).await?;
        anyhow::ensure!(n == 2, "Delta-only rows inside the bound must not be skipped; got {n} of 2");
        b.shutdown.cancel();
        Ok(())
    })
    .await;
    crate::support::unfreeze();
    res
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
// A mixed-project batch must route per ROW, not by row 0's project. Only custom
// storage exposes it: for all-unified projects Delta's project_id partitioning
// masks the misrouting.
async fn test_fast_insert_mixed_custom_storage_routing() -> Result<()> {
    within(60, async {
        let (db, ctx, prefix) = setup_test_database().await?;
        let (pa, pb, table) = (format!("csA_{prefix}"), format!("csB_{prefix}"), "otel_logs_and_spans".to_string());

        // pb is a BYO-bucket tenant: same MinIO, distinct prefix → its own Delta table.
        register_custom_storage(&db, &pb, &table, &format!("custom-{prefix}")).await;

        // One batch, interleaved A/B/A so row 0 (pa) is not the only project.
        let batch = json_to_batch(vec![test_span("a1", "n", &pa), test_span("b1", "n", &pb), test_span("a2", "n", &pa)])?;
        let provider = ctx.table_provider(table.as_str()).await?;
        // Upcast to &dyn Any (TableProvider: Any) — `use super::*` pulls arrow's
        // Array::as_any into scope, which would otherwise shadow the right method.
        let any: &dyn std::any::Any = provider.as_ref();
        let rt = any.downcast_ref::<ProjectRoutingTable>().ok_or_else(|| anyhow::anyhow!("otel_logs_and_spans is not a ProjectRoutingTable"))?;
        assert_eq!(rt.fast_insert_batch(batch).await?, 3);

        let count = async |p: &str| count_of(&ctx, &format!("SELECT COUNT(*) c FROM otel_logs_and_spans WHERE project_id = '{p}'")).await;
        assert_eq!(count(&pb).await?, 1, "pb's row must reach pb's BYO bucket, not leak into pa's unified table");
        assert_eq!(count(&pa).await?, 2, "pa keeps exactly its 2 rows");

        db.shutdown().await?;
        Ok(())
    })
    .await
}

/// SQL surface smoke test, one shared DB/session: insert+count+select+project isolation,
/// level/duration/compound filtering, literal SQL `INSERT` (single- and multi-row) alongside
/// an API insert, and timestamp filtering + `to_char` formatting.
#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn sql_surface_smoke() -> Result<()> {
    within(30, async {
            let (db, ctx, prefix) = setup_test_database().await?;
            let pcount = async |p: &str| count_of(&ctx, &format!("SELECT COUNT(*) as cnt FROM otel_logs_and_spans WHERE project_id = '{p}'")).await;
            let sel = async |cols: &str, rest: &str, idx: usize| strings_of(&ctx, &format!("SELECT {cols} FROM otel_logs_and_spans WHERE {rest}"), idx).await;
            let insert = async |p: &str, records: Vec<serde_json::Value>| -> Result<()> {
                db.insert_records_batch(p, "otel_logs_and_spans", vec![json_to_batch(records)?], true, None).await?;
                Ok(())
            };

            let solo = format!("project_{prefix}");
            insert(&solo, vec![test_span("test1", "span1", &solo)]).await?;
            assert_eq!(pcount(&solo).await?, 1);
            assert_eq!(sel("id, name", &format!("project_id = '{solo}'"), 0).await?, ["test1"]);
            assert_eq!(sel("id, name", &format!("project_id = '{solo}'"), 1).await?, ["span1"]);

            let projects: Vec<String> = (1..=3).map(|i| format!("proj{i}_{prefix}")).collect();
            for project in &projects {
                insert(project, vec![test_span(&format!("id_{project}"), &format!("span_{project}"), project)]).await?;
            }
            let mut total_count = 0;
            for project in &projects {
                assert_eq!(sel("id", &format!("project_id = '{project}'"), 0).await?, [format!("id_{project}")]);
                total_count += pcount(project).await?;
            }
            assert_eq!(total_count, 3, "each project keeps exactly its own row, unseen by the others");

            let filter_project = format!("filter_proj_{prefix}");
            use chrono::Utc;
            use serde_json::json;
            let now = Utc::now();
            insert(&filter_project, vec![
                json!({
                    "timestamp": now.timestamp_micros(), "id": "span1", "name": "test_span_1", "project_id": &filter_project,
                    "level": "INFO", "status_code": "OK", "duration": 100_000_000, "date": now.date_naive().to_string(),
                    "hashes": [], "summary": ["Test span 1 - INFO level"]
                }),
                json!({
                    "timestamp": (now + chrono::Duration::minutes(10)).timestamp_micros(), "id": "span2", "name": "test_span_2",
                    "project_id": &filter_project, "level": "ERROR", "status_code": "ERROR", "status_message": "Error occurred",
                    "duration": 200_000_000, "date": now.date_naive().to_string(), "hashes": [], "summary": ["Test span 2 - ERROR level"]
                }),
            ])
            .await?;
            let errors = format!("project_id = '{filter_project}' AND level = 'ERROR'");
            assert_eq!(sel("id", &errors, 0).await?, ["span2"]);
            assert_eq!(sel("id", &format!("project_id = '{filter_project}' AND duration > 150000000"), 0).await?, ["span2"]);
            assert_eq!(sel("id, status_message", &errors, 1).await?, ["Error occurred"]);

            let sql_proj1 = format!("default_{prefix}");
            let sql_proj2 = format!("sqlins_proj2_{prefix}");
            insert(&sql_proj1, vec![test_span("id1", "name1", &sql_proj1)]).await?;
            let sql = format!("INSERT INTO otel_logs_and_spans (
                       project_id, date, timestamp, id, hashes, name, level, status_code, summary
                     ) VALUES (
                       '{sql_proj2}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T10:00:00Z',
                       'sql_id', ARRAY[], 'sql_name', 'INFO', 'OK', ARRAY['SQL inserted test span']
                     )");
            let result = ctx.sql(&sql).await?.collect().await?;
            assert_eq!(result[0].num_rows(), 1);
            assert_eq!(pcount(&sql_proj1).await? + pcount(&sql_proj2).await?, 2);
            assert_eq!(sel("id, name", &format!("project_id = '{sql_proj2}' AND id = 'sql_id'"), 1).await?, ["sql_name"]);
            // A multi-row INSERT returns the row count and preserves insertion order.
            let multirow_id = format!("multirow_{prefix}");
            let sql = format!("INSERT INTO otel_logs_and_spans (
                       project_id, date, timestamp, id, hashes, name, level, status_code, summary
                     ) VALUES
                     ('{multirow_id}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T10:00:00Z', 'id1', ARRAY[], 'name1', 'INFO', 'OK', ARRAY['Multi-row insert test 1']),
                     ('{multirow_id}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T11:00:00Z', 'id2', ARRAY[], 'name2', 'INFO', 'OK', ARRAY['Multi-row insert test 2']),
                     ('{multirow_id}', TIMESTAMP '2023-01-01', TIMESTAMP '2023-01-01T12:00:00Z', 'id3', ARRAY[], 'name3', 'ERROR', 'ERROR', ARRAY['Multi-row insert test 3 - ERROR'])");
            let result = ctx.sql(&sql).await?.collect().await?;
            use datafusion::arrow::array::AsArray;
            assert_eq!(result[0].column(0).as_primitive::<arrow::datatypes::UInt64Type>().value(0), 3);
            assert_eq!(pcount(&multirow_id).await?, 3);
            assert_eq!(sel("id, name", &format!("project_id = '{multirow_id}' ORDER BY id"), 0).await?, ["id1", "id2", "id3"]);

            let ts_project = format!("ts_test_{prefix}");
            let base_time = chrono::DateTime::parse_from_rfc3339("2023-01-01T10:00:00Z").unwrap().with_timezone(&Utc);
            insert(&ts_project, vec![
                json!({
                    "timestamp": base_time.timestamp_micros(), "id": "early", "name": "early_span", "project_id": &ts_project,
                    "date": base_time.date_naive().to_string(), "hashes": [], "summary": ["Early span for timestamp test"]
                }),
                json!({
                    "timestamp": (base_time + chrono::Duration::hours(2)).timestamp_micros(), "id": "late", "name": "late_span",
                    "project_id": &ts_project, "date": base_time.date_naive().to_string(), "hashes": [], "summary": ["Late span for timestamp test"]
                }),
            ])
            .await?;
            assert_eq!(sel("id", &format!("project_id = '{ts_project}' AND timestamp > '2023-01-01T11:00:00Z'"), 0).await?, ["late"]);
            assert_eq!(
                sel("id, to_char(timestamp, 'YYYY-MM-DD HH24:MI') as ts", &format!("project_id = '{ts_project}' ORDER BY timestamp"), 1).await?,
                ["2023-01-01 10:00", "2023-01-01 12:00"]
            );

            db.shutdown().await?;
            Ok(())
        })
        .await
}

// The #[ignore]'d tests below stress real Delta commit contention against S3. They
// wedge in a shared test process because `config::init_config()` is a OnceLock, so
// every test inherits the FIRST test's table prefix and commit retries never settle.
#[serial]
#[ignore = "wedges under shared-state CI; see comment above. Run with cargo test -- --ignored"]
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_writes_same_project() -> Result<()> {
    // 3 concurrent writers into ONE project: Delta commit contention is the point.
    let project_id = format!("concurrent_test_{}", uuid::Uuid::new_v4());
    within(180, concurrent_inserts_all_land("concurrent-writes-same-project", 3, move |_| project_id.clone())).await
}

#[serial]
#[ignore = "wedges under shared-state CI; see test_concurrent_writes_same_project comment"]
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_table_creation() -> Result<()> {
    within(180, concurrent_inserts_all_land("concurrent-table-creation", 5, |i| format!("project_create_test_{i}"))).await
}

#[serial]
#[tokio::test(flavor = "multi_thread")]
async fn test_batch_queue_under_load() -> Result<()> {
    within(30, async {
        use crate::write::BatchQueue;
        let db = Arc::new(Database::with_config(create_test_config("batch-queue-under-load")).await?);
        let queue = BatchQueue::new(Arc::clone(&db), 100, 50); // 100ms interval, 50 rows max
        let project_id = format!("queue_test_{}", uuid::Uuid::new_v4());

        for i in 0..100 {
            let batch_id = format!("queued_batch_{i}");
            let batch = json_to_batch(vec![test_span(&batch_id, &format!("test_{batch_id}"), &project_id)])?;
            match queue.queue(batch) {
                Ok(_) => {}
                Err(e) if e.to_string().contains("batch queue full") => break,
                Err(e) => return Err(e),
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        queue.shutdown().await;
        assert_eq!(Arc::strong_count(&db), 1, "queue shutdown must release its database worker before the runtime can stop");
        db.shutdown().await?;
        Ok(())
    })
    .await
}

#[serial]
#[ignore = "wedges under shared-state CI; see test_concurrent_writes_same_project comment"]
#[tokio::test(flavor = "multi_thread")]
async fn test_concurrent_mixed_operations() -> Result<()> {
    within(180, async {
        let db = Arc::new(Database::with_config(create_test_config("concurrent-mixed-operations")).await?);

        // Concurrent writes to DIFFERENT projects, then concurrent reads across them.
        let spawn_each = |body: fn(Arc<Database>, usize) -> tokio::task::JoinHandle<Result<()>>| (0..3).map(|i| body(Arc::clone(&db), i)).collect::<Vec<_>>();
        let writes = spawn_each(|db, i| {
            tokio::spawn(async move {
                let project_id = format!("project_{i}");
                let batch = json_to_batch(vec![test_span(&format!("id_{i}"), &format!("span_{i}"), &project_id)])?;
                db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![batch], true, None).await?;
                Ok(())
            })
        });
        for handle in writes {
            handle.await??;
        }

        let reads = spawn_each(|db, i| {
            tokio::spawn(async move {
                let ctx = db.create_session_context();
                let _ = ctx.sql(&format!("SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'project_{i}'")).await;
                Ok(())
            })
        });
        for handle in reads {
            handle.await??;
        }

        db.shutdown().await?;
        Ok(())
    })
    .await
}

#[serial]
#[tokio::test]
async fn dirty_dedup_bins_survive_restart() -> Result<()> {
    let cfg = create_test_config(&format!("dirty-dedup-restart-{}", uuid::Uuid::new_v4().simple()));
    let project = format!("dirty_{}", uuid::Uuid::new_v4().simple());
    let old = (Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    let db = Database::with_config(Arc::clone(&cfg)).await?;
    insert_otel_ts(&db, &project, "restart", "first", old, true).await?;
    assert_eq!(db.dedup_dirty_bins.len(), 1);
    drop(db);

    let restored = Database::with_config(cfg).await?;
    assert_eq!(restored.dedup_dirty_bins.len(), 1, "restart restores the sealed late-event bin");
    Ok(())
}

#[serial]
#[tokio::test]
async fn dirty_dedup_bins_enqueue_seal_and_requeue() -> Result<()> {
    let (db, project) = dirty_bin_db("dirty-dedup-bins").await?;
    assert!(!db.config.maintenance.timefusion_dedup_sweep_fallback, "the broad fallback sweep must default off");
    // 26h is well beyond the seal lag and keeps the test deterministic around midnight.
    let old = (Utc::now() - chrono::Duration::hours(26)).timestamp_micros();

    // The duplicate shares a parquet file with a row from an adjacent 10-minute bin:
    // a targeted rewrite must carry that neighbour through rather than drop it.
    let neighbour = old - 60 * 60 * 1_000_000;
    let first_file = json_to_batch(vec![test_span_ts("sealed", "first", &project, old), test_span_ts("neighbour", "keep", &project, neighbour)])?;
    db.insert_records_batch(&project, "otel_logs_and_spans", vec![first_file], true, None).await?;
    insert_otel_ts(&db, &project, "sealed", "second", old, true).await?;
    assert_eq!(db.dedup_dirty_bins.len(), 2, "successful commits enqueue both timestamp bins");
    let table = otel_unified_table(&db).await;
    let selected = {
        let table = table.read().await;
        let snapshot = table.snapshot()?.snapshot();
        let old_date = chrono::DateTime::<Utc>::from_timestamp_micros(old).unwrap().date_naive().to_string();
        dedup_partition_paths(snapshot.log_data().iter().map(|f| f.path().to_string()), &project, &old_date)
    };
    assert_eq!(selected.len(), 2, "snapshot selection must retain both duplicate-bearing files: {selected:?}");
    drain_dirty_bins_ok(&db, &table).await?;
    assert_eq!(delta_physical_row_count(&table).await?, 2, "sealed bin is deduplicated without dropping an adjacent-bin row from the same file");
    assert!(db.dedup_dirty_bins.is_empty(), "completed sealed bin is consumed");

    insert_otel_ts(&db, &project, "sealed", "later", old, true).await?;
    assert_eq!(db.dedup_dirty_bins.len(), 1, "late retry requeues the previously consumed bin");
    drain_dirty_bins_ok(&db, &table).await?;
    assert_eq!(delta_physical_row_count(&table).await?, 2, "later observed timestamp survives the requeue rewrite and the neighbour remains");

    let fresh = Utc::now().timestamp_micros();
    insert_otel_ts(&db, &project, "unsealed", "a", fresh, true).await?;
    insert_otel_ts(&db, &project, "unsealed", "b", fresh, true).await?;
    drain_dirty_bins_ok(&db, &table).await?;
    assert_eq!(db.dedup_dirty_bins.len(), 1, "unsealed bin remains queued without rewrite");
    assert_eq!(delta_physical_row_count(&table).await?, 4, "unsealed copies remain for read-side dedup");
    Ok(())
}

/// One transient flush-unhealthy sample mid-pass must not discard the batch's
/// staged work; a wave whose commit finds flush unhealthy waits for recovery.
#[tokio::test]
async fn dirty_dedup_drain_survives_transient_flush_unhealthy() -> Result<()> {
    let (db, table) = sealed_dup_pair("dirty-dedup-transient", "sealed").await?;

    // Healthy at pass start, unhealthy for exactly one mid-pass sample.
    let calls = std::sync::atomic::AtomicUsize::new(0);
    let flaky = || calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed) != 1;
    db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &flaky, std::time::Duration::MAX, far_future()).await?;
    assert!(calls.load(std::sync::atomic::Ordering::Relaxed) >= 2, "the unhealthy sample must have been consumed");
    assert_eq!(delta_physical_row_count(&table).await?, 1, "a transient unhealthy flush sample must not forfeit the staged dedup work");
    assert!(db.dedup_dirty_bins.is_empty(), "bin is consumed, not requeued");
    Ok(())
}

/// One whole-date probe classifies every queued bin of a (project, date):
/// probe-clean bins are consumed WITHOUT per-bin staging scans, while
/// dup-bearing bins still dedup through the per-bin path.
#[tokio::test]
async fn dirty_dedup_batch_probe_consumes_clean_bins() -> Result<()> {
    let (db, project) = dirty_bin_db("dirty-dedup-batchprobe").await?;
    let (_day, base) = sealed_noon();
    // Three bins on one date: one duplicate pair, two clean singles.
    insert_otel_ts(&db, &project, "dup", "first", base, true).await?;
    insert_otel_ts(&db, &project, "dup", "second", base, true).await?;
    insert_otel_ts(&db, &project, "clean1", "only", base - TEN_MIN, true).await?;
    insert_otel_ts(&db, &project, "clean2", "only", base - 2 * TEN_MIN, true).await?;
    assert_eq!(db.dedup_dirty_bins.len(), 3);
    let table = otel_unified_table(&db).await;

    drain_dirty_bins_ok(&db, &table).await?;
    use std::sync::atomic::Ordering::Relaxed;
    assert_eq!(mstats().dirty_bin_batch_probe_clean.load(Relaxed), 2, "both clean bins are consumed by the batch probe alone");
    assert!(db.dedup_dirty_bins.is_empty(), "clean and dup bins are all consumed");
    assert_eq!(delta_physical_row_count(&table).await?, 3, "the duplicate collapsed; clean rows untouched");
    Ok(())
}

/// A batch probe that finds no duplicates CERTIFIES the date it proved: the probe
/// already GROUP BYs the dedup keys over the whole (project, date), so an empty
/// result is the same predicate a zero-drop rewrite establishes, at key-only cost.
#[tokio::test]
async fn a_clean_batch_probe_certifies_the_whole_date() -> Result<()> {
    let (db, project) = dirty_bin_db("batchprobe-certify").await?;
    let (day, base) = sealed_noon();
    insert_otel_ts(&db, &project, "clean1", "only", base, true).await?;
    insert_otel_ts(&db, &project, "clean2", "only", base - TEN_MIN, true).await?;
    let table = otel_unified_table(&db).await;

    let key = (project.clone(), "otel_logs_and_spans".to_owned(), day.to_string());
    assert!(db.dedup_clean_fp.get(&key).is_none(), "nothing is certified before the probe runs");

    drain_dirty_bins_ok(&db, &table).await?;

    let cert = db.dedup_clean_fp.get(&key).map(|entry| entry.value().clone()).expect("a clean probe certifies the date it proved");
    assert!(!cert.stale, "a whole-date grant is live evidence, not the stale per-file kind a partial slice banks");
    assert!(!cert.files.is_empty(), "and it names the files it proved, so the per-file skip stays reachable");
    // The grant is what removes DedupExec: certified files union ABOVE it, so
    // a partial grant only routes files around the operator.
    assert!(db.dedup_window_clean(&*table.read().await, &project, "otel_logs_and_spans", (base - TEN_MIN, base + TEN_MIN)).granted());
    Ok(())
}

/// A sealed date with NO queued dirty bins still gets certified — a scan only sheds
/// `DedupExec` when EVERY date it reads is granted, so an empty queue must not make
/// a date permanently unprovable.
#[tokio::test]
async fn a_sealed_date_with_no_queued_bins_is_still_certified() -> Result<()> {
    let (db, project) = dirty_bin_db("certify-sealed").await?;
    let (day, base) = sealed_noon();
    insert_otel_ts(&db, &project, "solo", "only", base, true).await?;
    let table = otel_unified_table(&db).await;

    // THE precondition: nothing is queued.
    db.dedup_dirty_bins.clear();
    let key = (project.clone(), "otel_logs_and_spans".to_owned(), day.to_string());
    assert!(db.dedup_clean_fp.get(&key).is_none(), "and nothing is certified yet");

    drain_dirty_bins_ok(&db, &table).await?;

    assert!(db.dedup_clean_fp.get(&key).is_some_and(|entry| !entry.value().stale), "an empty queue must not stop a sealed date from being proved");
    // Today is excluded on purpose: a live partition gains files under ingest,
    // so its fingerprint moves and the grant is refused by construction.
    let today = Utc::now().date_naive().to_string();
    assert!(
        !db.uncertified_window_dates(&*table.read().await, "otel_logs_and_spans").iter().any(|(_, date)| *date == today),
        "today is never a certification candidate"
    );
    Ok(())
}

/// A date probed dirty is not re-probed until a commit moves its fingerprint —
/// otherwise a never-certifiable date stays a candidate forever and crowds out the
/// ones never examined. The memo is keyed by file-set fingerprint, as certification is.
#[tokio::test]
async fn a_date_probed_dirty_is_not_reprobed_until_its_files_change() -> Result<()> {
    let (db, project) = dirty_bin_db("certify-memo").await?;
    let (day, base) = sealed_noon();
    for observed in ["first", "second"] {
        insert_otel_ts(&db, &project, "dup", observed, base, true).await?;
    }
    let table = otel_unified_table(&db).await;
    let key = (project.clone(), "otel_logs_and_spans".to_owned(), day.to_string());

    let want = (project.clone(), day.to_string());
    let candidate = async || db.uncertified_window_dates(&*table.read().await, "otel_logs_and_spans").contains(&want);
    assert!(candidate().await, "an unexamined dup-bearing date is a candidate");

    let files = {
        let guard = table.read().await;
        Database::partition_files_by_pid(&guard, &format!("date={day}"))?.remove(&key.0).unwrap_or_default()
    };
    db.dedup_probe_declined.insert(key.clone(), partition_file_fp(&files));
    assert!(!candidate().await, "and is skipped once declined at that exact file set");

    // A commit moves the fingerprint, so it must be examined again.
    db.dedup_probe_declined.insert(key, partition_file_fp(&["some-other-file.parquet".to_owned()]));
    assert!(candidate().await, "a changed file set makes it a candidate again");
    Ok(())
}

/// Candidates come out PROJECT-MAJOR (a scan sheds `DedupExec` only when every date
/// in its window is granted, so grants scattered one-per-project buy nothing) and
/// BUSIEST first (fewest-remaining-dates ordering never reaches the slow tenants).
#[tokio::test]
async fn certification_candidates_start_with_the_busiest_project() -> Result<()> {
    let (db, _) = dirty_bin_db("certify-order").await?;
    let (busy, quiet) = (format!("busy_{}", uuid::Uuid::new_v4().simple()), format!("quiet_{}", uuid::Uuid::new_v4().simple()));
    // `quiet` has ONE date (nearest to done); `busy` has three and more files on each.
    for (project, days) in [(&quiet, vec![2i64]), (&busy, vec![1, 3, 4])] {
        for back in days {
            let ts = (Utc::now() - chrono::Duration::days(back)).date_naive().and_hms_opt(12, 0, 0).unwrap().and_utc().timestamp_micros();
            // Two inserts on the busy project's dates ⇒ strictly more FILES, the
            // proxy the ordering sorts on.
            for row in 0..if *project == busy { 2 } else { 1 } {
                insert_otel_ts(&db, project, &format!("r{back}_{row}"), "only", ts + row, true).await?;
            }
        }
    }
    let table = otel_unified_table(&db).await;
    let got = db.uncertified_window_dates(&*table.read().await, "otel_logs_and_spans");

    let ours: Vec<&String> = got.iter().map(|(project, _)| project).filter(|p| **p == busy || **p == quiet).collect();
    let first_quiet = ours.iter().position(|p| ***p == quiet);
    let last_busy = ours.iter().rposition(|p| ***p == busy);
    assert!(last_busy.is_some() && first_quiet.is_some(), "both projects have uncertified dates in the window");
    assert!(last_busy < first_quiet, "the BUSIEST project must be finished before a quiet one is started: {ours:?}");
    Ok(())
}

/// The probe phase's deadline is an `Instant` shared by every group, not a per-group
/// `Duration`. A group reached with nothing left must leave its bins queued, or it
/// spends the cheap classification path's entry ticket on a probe that cannot run.
#[tokio::test]
async fn dedup_batch_probe_leaves_bins_queued_when_the_phase_budget_is_gone() -> Result<()> {
    let (db, project) = dirty_bin_db("dirty-dedup-probedeadline").await?;
    let (_day, base) = sealed_noon();
    // Two bins on one date, so the group clears the `bins.len() >= 2` filter.
    insert_otel_ts(&db, &project, "a", "only", base, true).await?;
    insert_otel_ts(&db, &project, "b", "only", base - TEN_MIN, true).await?;
    let table = otel_unified_table(&db).await;

    let ready: Vec<(String, String, i64)> = db.dedup_dirty_bins.iter().map(|e| (e.key().0.clone(), e.key().2.clone(), e.key().3)).collect();
    let queued_before = db.dedup_dirty_bins.len();
    assert_eq!(queued_before, 2, "both bins are queued to begin with");

    let out = db.batch_probe_classify(&table, "otel_logs_and_spans", ready, Vec::new(), std::time::Instant::now()).await;

    assert_eq!(db.dedup_dirty_bins.len(), queued_before, "an exhausted phase budget must not dequeue bins it never probed");
    assert_eq!(out.len(), queued_before, "and every bin still fails OPEN to the per-bin staging path");
    Ok(())
}

/// A drain pass out of budget must stop admitting bins and leave them queued: an
/// over-running pass holds `maintenance_job_sem` and drops every overlapping tick.
#[tokio::test]
async fn dedup_drain_out_of_budget_leaves_its_bins_queued() -> Result<()> {
    let (db, table) = sealed_dup_pair("dirty-dedup-passbudget", "sealed").await?;

    // Elapsed pass deadline + generous per-bin ceiling: only the pass budget can stop this.
    let started = std::time::Instant::now();
    db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::from_secs(3600), std::time::Instant::now()).await?;
    assert!(started.elapsed() < std::time::Duration::from_secs(30), "the pass must return on its budget, not run the per-bin ceiling");
    assert_requeued_then_dedups(&db, &table, "an exhausted pass budget").await
}

/// A hung staging read must not wedge the drain: the per-bin deadline converts the
/// hang into an ordinary requeue and the pass moves on.
#[tokio::test]
async fn dirty_dedup_bin_staging_deadline_requeues_instead_of_wedging() -> Result<()> {
    let (db, table) = sealed_dup_pair("dirty-dedup-deadline", "sealed").await?;

    // A 1ms deadline fires before any real staging read completes — the hung GET.
    use std::sync::atomic::Ordering::Relaxed;
    db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| true, std::time::Duration::from_millis(1), far_future()).await?;
    assert!(mstats().dedup_bin_stage_timeouts.load(Relaxed) >= 1, "the per-bin deadline must fire");
    assert_requeued_then_dedups(&db, &table, "the per-bin staging deadline").await
}

/// Hot bins drain newest-first, and cold-owned dates sink below every hot bin
/// instead of monopolising the batch.
#[test]
fn drain_bins_order_newest_first_and_sink_cold() {
    let today = chrono::NaiveDate::from_ymd_opt(2026, 7, 30).unwrap();
    let bin = |date: &str, bin: i64| ("p".to_string(), date.to_string(), bin);
    let candidates = vec![bin("2026-07-20", 5), bin("2026-07-29", 1), bin("2026-07-29", 7), bin("2026-07-21", 3), bin("2026-07-28", 2)];

    // after_days=3 ⇒ 07-20/07-21 are cold-owned, 07-28/07-29 are hot.
    let (ready, deferred) = Database::select_drain_bins(candidates.clone(), today, 3, 10);
    assert_eq!(
        ready,
        vec![bin("2026-07-29", 7), bin("2026-07-29", 1), bin("2026-07-28", 2), bin("2026-07-21", 3), bin("2026-07-20", 5)],
        "newest-first within each tier, cold tier last"
    );
    assert!(deferred.is_empty(), "a batch with room serves the cold tail too");

    // A batch smaller than the hot tier still RESERVES half for cold, so the cold
    // backlog drains instead of being deferred forever behind continuous hot work.
    let (ready, deferred) = Database::select_drain_bins(candidates.clone(), today, 3, 2);
    assert_eq!(ready, vec![bin("2026-07-29", 7), bin("2026-07-21", 3)], "hot keeps priority, cold gets its reserved slot");
    assert_eq!(deferred, vec![bin("2026-07-20", 5)], "cold bins are deferred, not dropped");
    assert_eq!(deferred.last().unwrap().1, "2026-07-20", "summary line reports the oldest deferred date");

    // With no cold work, hot gets the WHOLE batch — the reserve must never idle a slot.
    let hot_only: Vec<_> = candidates.iter().filter(|(_, d, _)| d.as_str() >= "2026-07-28").cloned().collect();
    let (ready, deferred) = Database::select_drain_bins(hot_only, today, 3, 2);
    assert_eq!(ready, vec![bin("2026-07-29", 7), bin("2026-07-29", 1)], "no cold work ⇒ hot uses the full batch");
    assert!(deferred.is_empty());
}

#[test]
fn dedup_shards_bound_oversized_rewrites() {
    // Shard concurrency is funded by SHRINKING the shard: `shards_in_flight x
    // per-shard budget` must never exceed the per-bin Arrow budget.
    const GIB: u64 = 1024 * 1024 * 1024;
    for budget in [GIB / 8, GIB / 4, GIB / 2, GIB, 2 * GIB] {
        let k = dedup_shard_concurrency(budget, 48) as u64;
        assert!(k >= 1, "a bin must always make progress");
        assert!(k * budget <= super::DEDUP_BIN_ARROW_BUDGET, "{k} shards x {budget} B exceeds the per-bin Arrow budget");
    }
    assert_eq!(dedup_shard_concurrency(GIB / 2, 48), 4, "prod: 512 MiB shards, four in flight, same 2 GiB peak as one old shard");
    assert_eq!(dedup_shard_concurrency(GIB / 2, 8), 2, "cores/4 bounds it on a small box");
    assert_eq!(dedup_shard_concurrency(0, 48), 1, "no configured ceiling means one shard already holds everything");

    assert_eq!(dedup_shard_count(false, 100, 100, 100, 100), 1);
    assert_eq!(dedup_shard_count(false, 101, 100, 100, 100), 2);
    assert_eq!(dedup_shard_count(false, u64::MAX, 1, 1, 0), DEDUP_BUCKET_COUNT);
    // The streaming branch never collects, so no budget can shard it.
    assert_eq!(dedup_shard_count(true, u64::MAX, u64::MAX, 1, 1), 1, "a ROW_NUMBER dedup is bounded by its execution shape, not by the decoded budget");
}

#[test]
fn dedup_rewrite_rejects_the_production_partial_file_loss_shape() {
    assert!(dedup_rewrite_counts_match(5_795_641, 5_795_641, 2_100_000, 2_100_000));
    assert!(!dedup_rewrite_counts_match(63_786, 5_795_641, 63_786, 2_100_000), "a bin-scoped re-read must never remove files whose adjacent rows were omitted");
    assert!(!dedup_rewrite_counts_match(5_795_641, 5_795_641, 2_100_001, 2_100_000), "winner-count drift must also fail closed");
}

/// The cold cutoff must never DROP bins: the nightly consolidate bin-packs those
/// partitions but does not collapse duplicates, so this drain is their only dedup.
#[serial]
#[tokio::test]
async fn dirty_dedup_cold_bins_are_deferred_not_dropped() -> Result<()> {
    let (db, project) = dirty_bin_db("dirty-dedup-cold").await?;
    let after_days = db.config.parquet.cold_optimize_after_days();
    let ancient = (Utc::now() - chrono::Duration::days(after_days as i64 + 2)).timestamp_micros();
    for observed in ["first", "second"] {
        insert_otel_ts(&db, &project, "cold", observed, ancient, true).await?;
    }
    assert_eq!(db.dedup_dirty_bins.len(), 1);

    let deferred_before = mstats().dedup_bins_deferred_cold.load(std::sync::atomic::Ordering::Relaxed);
    let table = otel_unified_table(&db).await;
    // A cold-only queue is still served: lowest priority ≠ never.
    drain_dirty_bins_ok(&db, &table).await?;
    assert_eq!(mstats().dedup_bins_deferred_cold.load(std::sync::atomic::Ordering::Relaxed), deferred_before, "nothing to defer when the batch has room");
    assert_eq!(delta_physical_row_count(&table).await?, 1, "cold duplicates are physically collapsed, never silently abandoned");
    assert!(db.dedup_dirty_bins.is_empty());
    Ok(())
}

/// Waiting for the repair permit must not hold a coordinator worker — every worker
/// parked on that wait means nothing completes and the planning pass never runs.
#[tokio::test]
async fn a_busy_repair_permit_requeues_instead_of_parking_a_worker() -> Result<()> {
    let (db, _) = dirty_bin_db("repair-park").await?;
    let schema = crate::schema::get_schema("otel_logs_and_spans").expect("otel schema");

    // A REAL committed file, so the bin survives the vanished-selection check and
    // actually reaches the permit; a nonexistent path exits at the earlier branch.
    let project = format!("repair_busy_{}", uuid::Uuid::new_v4().simple());
    let ts = (Utc::now() - chrono::Duration::hours(3)).timestamp_micros();
    insert_otel_ts(&db, &project, "r1", "op", ts, true).await?;
    let table = otel_unified_table(&db).await;
    let file = {
        let t = table.read().await;
        t.snapshot()?.log_data().iter().map(|f| f.path().to_string()).find(|p| p.contains(&project)).expect("the committed file")
    };

    // Hold the WHOLE byte budget, as a live oversized rewrite does.
    let budget = db.repair_rewrite_sem.available_permits();
    let held = Arc::clone(&db.repair_rewrite_sem).try_acquire_many_owned(u32::try_from(budget).unwrap()).expect("the budget starts free");

    let stage = async |files: Vec<String>| {
        let options = HotStageOptions {
            pass: TailPass::Repair,
            operation: Some(crate::maintenance_coordinator::Operation::Repair),
            runtime_env: Some(db.coordinator_runtime_env()),
            light_permit: None,
        };
        db.stage_hot_bin(&table, "otel_logs_and_spans", schema, &project, files, options).await
    };

    let started = std::time::Instant::now();
    let outcome = stage(vec![file]).await;
    assert!(started.elapsed() < std::time::Duration::from_secs(5), "it must return immediately, not wait for the permit");
    // `BudgetBusy`, not `Retry`: the coordinator paces the two differently
    // (+300s on the holder's clock vs +30s); folding them together spins the claim.
    assert!(matches!(outcome, Ok(BinOutcome::BudgetBusy)), "a held budget must report BudgetBusy, and hand the worker back");
    drop(held);

    // A vanished file still reports `Retry` (re-servable immediately).
    let outcome = stage(vec!["nonexistent.parquet".to_owned()]).await;
    assert!(matches!(outcome, Ok(BinOutcome::Retry)), "a vanished selection stays Retry");
    Ok(())
}

/// The flush path fills a dirty-bin queue that nothing drains for rollup-declared
/// tables (the dedup cron skips exactly those), so those bins must be retired.
#[serial]
#[tokio::test]
async fn undrainable_dirty_bins_are_retired_not_left_to_grow() -> Result<()> {
    let (db, project) = dirty_bin_db("dirty-retire").await?;
    let old = (Utc::now() - chrono::Duration::hours(26)).timestamp_micros();

    // Coordinator-owned (declares rollups) and a table the cron still serves.
    let date = chrono::DateTime::from_timestamp_micros(old).expect("timestamp").date_naive().to_string();
    let bin = old.div_euclid(10 * 60 * 1_000_000);
    db.enqueue_dirty_bin(&project, "otel_logs_and_spans", &date, bin);
    db.enqueue_dirty_bin(&project, "__cron_owned_table__", &date, bin);
    assert_eq!(db.dedup_dirty_bins.len(), 2);

    assert_eq!(db.retire_undrainable_dirty_bins(), 1, "only the queue with no consumer is dropped");
    let survivors: Vec<String> = db.dedup_dirty_bins.iter().map(|entry| entry.key().1.clone()).collect();
    assert_eq!(survivors, vec!["__cron_owned_table__".to_owned()], "the cron's own queue must be untouched");
    assert_eq!(db.retire_undrainable_dirty_bins(), 0, "and a second pass finds nothing");
    Ok(())
}

/// Admission contention follows capacity, not task history. An old task must
/// re-approach promptly after a transient lag spike releases the CPU ceiling.
#[tokio::test]
async fn admission_backoff_does_not_grow_with_task_attempts() -> Result<()> {
    let (db, _) = dirty_bin_db("admission-backoff").await?;
    let key = crate::maintenance_coordinator::TaskKey {
        physical_table: "otel_logs_and_spans".to_owned(),
        source: "otel_logs_and_spans".to_owned(),
        project_id: "p".to_owned(),
        slice: crate::maintenance_coordinator::TimeSlice { start_micros: 0, end_micros: 60_000_000 },
        operation: crate::maintenance_coordinator::Operation::BaseRollup,
    };
    {
        let mut journal = db.journal();
        journal.enqueue(key.clone(), 0, 1, 0);
        for _ in 0..10 {
            journal.claim_exact(&key, 0, false).expect("the retry is immediately due");
            assert!(journal.retry(&key, "admission_busy".to_owned(), 0));
        }
        assert_eq!(journal.attempts(&key), 10, "precondition: this is an old, repeatedly contended task");
    }
    let delay = db.admission_backoff_for(&key);
    assert_eq!(delay, std::time::Duration::from_secs(5), "busy capacity must be revisited promptly and without a task-history multiplier");
    Ok(())
}

/// Repair must not queue on the hygiene permits, and oversized repair rewrites must
/// not overlap each other — they share one sort pool and exhaust it.
#[tokio::test]
async fn repair_serialises_on_its_own_permit() -> Result<()> {
    let (db, _) = dirty_bin_db("repair-permit").await?;
    let budget = db.config.derived.repair_rewrite_budget_mib();
    assert_eq!(db.repair_rewrite_sem.available_permits(), budget, "repair's budget is decoded MiB, not a count of rewrites");
    assert!(db.light_rewrite_sem.available_permits() >= 1, "and repair must not spend a hygiene permit it would then starve");

    // A bin bigger than the whole budget still runs alone: its request is CLAMPED to
    // the budget and therefore takes all of it.
    let take = |mib: u32| Arc::clone(&db.repair_rewrite_sem).try_acquire_many_owned(mib);
    let huge = u32::try_from(budget).expect("budget fits u32");
    let held = take(huge).expect("an oversized bin takes the whole budget");
    assert!(take(1).is_err(), "nothing may overlap a bin that filled the budget — this is what the count of 1 was for");
    drop(held);
    let small = u32::try_from(budget / 4).expect("quarter fits u32").max(1);
    let a = take(small).expect("first small bin");
    assert!(take(small).is_ok(), "two small repair bins must share the budget");
    drop(a);

    // A repair unit is one whole file, and compaction deliberately produces
    // `COORDINATOR_HOT_TARGET_BYTES`-sized ones. Two must share, or repair
    // serializes on its ordinary input.
    let target_sized = u32::try_from(maintain::estimated_decoded_bytes(COORDINATOR_HOT_TARGET_BYTES) / (1024 * 1024)).expect("fits u32");
    let first = take(target_sized).expect("a target-sized file must fit the budget at all");
    assert!(take(target_sized).is_ok(), "two target-sized repair rewrites must share the budget ({target_sized} MiB each of {budget} MiB)");
    drop(first);
    Ok(())
}

#[serial]
#[tokio::test]
async fn dirty_dedup_drain_yields_to_unhealthy_flush() -> Result<()> {
    let (db, table) = sealed_dup_pair("dirty-dedup-gate", "gated").await?;
    let yields_before = mstats().dedup_passes_flush_yields.load(std::sync::atomic::Ordering::Relaxed);
    db.dedup_dirty_bins_for_table(&table, "otel_logs_and_spans", &|| false, std::time::Duration::MAX, far_future()).await?;
    assert_eq!(mstats().dedup_passes_flush_yields.load(std::sync::atomic::Ordering::Relaxed), yields_before + 1, "the skipped pass is counted, not silent");
    assert_requeued_then_dedups(&db, &table, "an unhealthy flush").await
}

/// `date = '…'` and `BETWEEN` bound a scan even though
/// `extract_time_range_from_filters` cannot see them, so enforce mode must accept them.
#[test_case(col("date").eq(lit("2026-08-10")) => true ; "a date partition equality bounds the scan")]
#[test_case(col("timestamp").between(lit(1_i64), lit(2_i64)) => true ; "BETWEEN on timestamp bounds the scan")]
#[test_case(col("timestamp").not_between(lit(1_i64), lit(2_i64)) => false ; "NOT BETWEEN reaches outside the range")]
#[test_case(col("name").between(lit(1_i64), lit(2_i64)) => false ; "BETWEEN on a non-time column bounds nothing")]
#[test_case(col("project_id").eq(lit("project")) => false ; "the project filter is not itself a bound")]
fn a_date_partition_or_between_predicate_counts_as_bounded(predicate: Expr) -> bool {
    ProjectRoutingTable::is_bounding_predicate(&predicate)
}

#[test_case("otel_logs_and_spans", vec![], None, false => Some("missing exact project_id filter") ; "no filters at all")]
#[test_case("otel_logs_and_spans", vec![col("project_id").eq(lit("project"))], None, false
        => Some("missing timestamp lower bound or scan limit") ; "a project alone is unbounded")]
#[test_case("otel_logs_and_spans", vec![col("project_id").eq(lit("project"))], Some(1), false => None ; "a limit bounds it")]
#[test_case("otel_logs_and_spans", vec![col("project_id").eq(lit("project"))], None, true => None ; "a timestamp lower bound bounds it")]
#[test_case("otel_logs_and_spans", vec![col("project_id").eq(lit("one")).or(col("project_id").eq(lit("two")))], None, true
        => Some("missing exact project_id filter") ; "a project disjunction is not an exact filter")]
#[test_case("timefusion_stats", vec![], None, false => None ; "only the otel table is gated")]
fn bounded_otel_scan_requires_a_project_and_bound(table: &str, filters: Vec<Expr>, limit: Option<usize>, lower_bound: bool) -> Option<&'static str> {
    ProjectRoutingTable::raw_otel_scan_reason(table, &filters, limit, lower_bound)
}
