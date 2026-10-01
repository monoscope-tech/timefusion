//! Integration tests for file-level needle pruning (bloom sidecars).

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use datafusion::arrow::{array::AsArray, datatypes::Int64Type};
use object_store::memory::InMemory;
use serde_json::json;
use timefusion::{
    database::Database,
    read::bloom_prune::BloomPruneRegistry,
    support::test_helpers::{BufferMode, TestConfigBuilder, json_to_batch},
};

/// A minimal otel_logs_and_spans row carrying a known `context___trace_id`.
fn row(id: &str, project_id: &str, ts: i64, trace_id: &str) -> serde_json::Value {
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string();
    json!({
        "timestamp": ts, "id": id, "name": "n", "project_id": project_id, "date": date,
        "hashes": [], "summary": [], "context___trace_id": trace_id,
    })
}

/// 3h ago: keeps the ±1h window in `count_by_trace_id` on the row's partition day.
fn ts() -> i64 {
    (chrono::Utc::now() - chrono::Duration::hours(3)).timestamp_micros()
}

/// One Delta commit of `rows` into otel_logs_and_spans.
async fn insert(db: &Arc<Database>, project_id: &str, rows: Vec<serde_json::Value>) -> Result<()> {
    db.insert_records_batch(project_id, "otel_logs_and_spans", vec![json_to_batch(rows)?], true, None).await?;
    Ok(())
}

/// Database + bloom registry + a fresh project id. The registry's sidecar store is
/// deliberately separate from the table's store (reconcile reads one, writes the other).
async fn setup(name: &str) -> Result<(Arc<Database>, String)> {
    let cfg = TestConfigBuilder::new(name).with_buffer_mode(BufferMode::Enabled).build();
    let reg = Arc::new(BloomPruneRegistry::new(Arc::new(InMemory::new()), 64 << 20, Duration::from_secs(300)));
    let db = Arc::new(Database::with_config(Arc::clone(&cfg)).await?.with_bloom_prune(reg));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    Ok((db, project_id))
}

/// `COUNT(*)` for a project/trace_id; the time bound is required for the scan to
/// consult the bloom registry.
async fn count_by_trace_id(db: &Arc<Database>, project_id: &str, trace_id: &str, ts: i64) -> Result<i64> {
    let mut ctx = Arc::clone(db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let lo = ts - 3_600_000_000;
    let hi = ts + 3_600_000_000;
    let sql = format!(
        "SELECT COUNT(*) AS cnt FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND context___trace_id = '{trace_id}' \
         AND timestamp >= to_timestamp_micros({lo}) AND timestamp <= to_timestamp_micros({hi})"
    );
    let res = ctx.sql(&sql).await?.collect().await?;
    Ok(res[0].column(0).as_primitive::<Int64Type>().value(0))
}

#[tokio::test]
async fn bloom_sidecar_build_has_no_false_negatives() -> Result<()> {
    let (db, project_id) = setup("bloom_no_fn").await?;
    let ts = ts();

    let rows: Vec<_> = (0..100).map(|i| row(&format!("id-{i}"), &project_id, ts, &format!("trace-{i}"))).collect();
    insert(&db, &project_id, rows).await?;

    let (built, errors) = db.bloom_sidecar_reconcile().await?;
    assert!(built >= 1, "reconcile should have built at least one file's sidecar, got {built}");
    assert_eq!(errors, 0);

    assert_eq!(count_by_trace_id(&db, &project_id, "trace-42", ts).await?, 1, "present trace_id must be returned");
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-does-not-exist", ts).await?, 0, "absent trace_id must return zero rows");
    Ok(())
}

#[tokio::test]
async fn bloom_pruning_never_drops_updated_or_deleted_versions() -> Result<()> {
    let (db, project_id) = setup("bloom_mor").await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let ts = ts();
    let trace_id = "trace-mor";

    insert(&db, &project_id, vec![row("mor-1", &project_id, ts, trace_id)]).await?;
    db.bloom_sidecar_reconcile().await?;
    assert_eq!(count_by_trace_id(&db, &project_id, trace_id, ts).await?, 1);

    // UPDATE appends a new version; both versions carry the needle, so neither file
    // may be bloom-rejected out from under the fresher version.
    let sql = format!("UPDATE otel_logs_and_spans SET hashes = make_array('v2') WHERE project_id = '{project_id}' AND context___trace_id = '{trace_id}'");
    ctx.sql(&sql).await?.collect().await?;
    db.bloom_sidecar_reconcile().await?;
    assert_eq!(count_by_trace_id(&db, &project_id, trace_id, ts).await?, 1, "must still resolve to exactly the latest version, not 0 or 2");

    // DELETE appends a tombstone; an older physical copy must not resurrect the row.
    let del = format!("DELETE FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND context___trace_id = '{trace_id}'");
    ctx.sql(&del).await?.collect().await?;
    db.bloom_sidecar_reconcile().await?;
    assert_eq!(count_by_trace_id(&db, &project_id, trace_id, ts).await?, 0, "tombstoned row must not be resurrected");
    Ok(())
}

#[tokio::test]
async fn bloom_pruning_excludes_files_and_empty_needle_scans_zero_files() -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let (db, project_id) = setup("bloom_exclude").await?;
    let ts = ts();

    // Two commits => two files, same project/date, disjoint trace_id sets.
    insert(&db, &project_id, vec![row("a1", &project_id, ts, "trace-A")]).await?;
    insert(&db, &project_id, vec![row("b1", &project_id, ts, "trace-B")]).await?;
    db.bloom_sidecar_reconcile().await?;

    let reg = db.bloom_prune().expect("registry attached");
    let before = reg.stats.files_rejected.load(Relaxed);
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-A", ts).await?, 1);
    assert!(reg.stats.files_rejected.load(Relaxed) > before, "querying a needle present in only one file must reject the other");

    let before2 = reg.stats.files_rejected.load(Relaxed);
    // Needle in neither file: exercises the empty-include-selection path.
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-nowhere", ts).await?, 0);
    assert!(reg.stats.files_rejected.load(Relaxed) >= before2 + 2, "an all-rejected needle must reject every in-window file");
    Ok(())
}

/// Split path: tantivy covers some files while others are unindexed. Bloom rejection
/// must reach both legs without dropping rows, and an all-rejected needle must take
/// the empty-include arm rather than falling back to an unrestricted scan.
#[tokio::test]
async fn split_path_bloom_prunes_indexed_and_raw_legs() -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    use timefusion::tantivy::search::{TantivyIndexService, TantivySearchService};

    let cfg = TestConfigBuilder::new("bloom_split").with_buffer_mode(BufferMode::Enabled).build();
    let reg = Arc::new(BloomPruneRegistry::new(Arc::new(InMemory::new()), 64 << 20, Duration::from_secs(300)));
    let db = Database::with_config(Arc::clone(&cfg)).await?;
    let storage_uri = format!("s3://{}/{}/tantivy", cfg.aws.aws_s3_bucket.clone().unwrap(), cfg.core.timefusion_table_prefix);
    let tstore = db.create_object_store(&storage_uri, &cfg.aws.build_storage_options(None)).await?;
    // A cold index cache skips the prefilter, so seed the reader on publish as prod does.
    let tcfg = Arc::new(timefusion::config::TantivyConfig { timefusion_tantivy_seed_cache_on_publish: true, ..cfg.tantivy.clone() });
    let svc = Arc::new(TantivyIndexService::new(tstore.clone(), tcfg.clone(), std::env::temp_dir().join(format!("tf-scratch-{}", uuid::Uuid::new_v4()))));
    let search = Arc::new(TantivySearchService::new(tstore, cfg.core.timefusion_data_dir.clone(), tcfg));
    svc.with_reader(&search);
    let db = Arc::new(db.with_tantivy_search(search.clone()).with_tantivy_indexer(svc.clone()).with_bloom_prune(reg));
    let project_id = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = ts();

    // File 1 is tantivy-covered via the indexer callback (what the flush hook does);
    // file 2 is never indexed.
    let b1 = json_to_batch(vec![row("a1", &project_id, ts, "trace-covered")])?;
    db.insert_records_batch(&project_id, "otel_logs_and_spans", vec![b1.clone()], true, None).await?;
    let file1: Vec<String> = db.list_file_uris(&project_id, "otel_logs_and_spans").await?;
    svc.clone().batch_callback()(project_id.clone(), "otel_logs_and_spans".into(), vec![b1], file1.clone()).await?;
    insert(&db, &project_id, vec![row("b1", &project_id, ts, "trace-raw")]).await?;
    let all: Vec<String> = db.list_file_uris(&project_id, "otel_logs_and_spans").await?;
    assert!(all.len() > file1.len(), "second insert must add an uncovered file");
    db.bloom_sidecar_reconcile().await?;

    let stats = &db.bloom_prune().unwrap().stats;
    let before = stats.files_rejected.load(Relaxed);
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-raw", ts).await?, 1, "raw-leg row must survive the split");
    assert!(stats.files_rejected.load(Relaxed) > before, "covered file must be bloom-rejected for the raw needle");

    let before = stats.files_rejected.load(Relaxed);
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-covered", ts).await?, 1, "indexed-leg row must survive");
    assert!(stats.files_rejected.load(Relaxed) > before, "raw file must be bloom-rejected for the covered needle");

    // Proves the split branch ran rather than the no-coverage fallback.
    assert!(search.stats.queries.load(Relaxed) > 0, "equality routing must have engaged the tantivy prefilter");
    assert_eq!(search.stats.cold_warms_spawned.load(Relaxed), 0, "every query must find the published index already local");

    // Needle in neither file: the split path's empty-include arm.
    let before = stats.files_rejected.load(Relaxed);
    assert_eq!(count_by_trace_id(&db, &project_id, "trace-nowhere", ts).await?, 0);
    assert!(stats.files_rejected.load(Relaxed) >= before + 2, "an all-absent needle must reject every file on the split path");
    Ok(())
}

/// monoscope's session cross-lookup binds `col = ANY($1)`. DataFusion lowers an array or IN list
/// of up to 3 items to an OR chain of equalities, which the needle extractor did not read, so a
/// 3-id lookup scanned the whole window. It must prune like the union of three single-id lookups.
#[tokio::test(flavor = "multi_thread")]
async fn pgwire_bound_any_array_prunes_like_in_list() -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let (db, project_id) = setup("bloom_any").await?;
    let ts = ts();
    for i in 0..6 {
        insert(&db, &project_id, vec![row(&format!("r{i}"), &project_id, ts, &format!("trace-{i}"))]).await?;
    }
    db.bloom_sidecar_reconcile().await?;

    let server = crate::pgwire_harness::TestServer::serve(Arc::clone(&db)).await?;
    let client = server.client().await?;
    let (lo, hi) = (ts - 3_600_000_000, ts + 3_600_000_000);
    let sql = format!(
        "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{project_id}' AND context___trace_id = ANY($1) \
         AND timestamp >= to_timestamp_micros({lo}) AND timestamp <= to_timestamp_micros({hi})"
    );
    let stats = &db.bloom_prune().unwrap().stats;
    let before = stats.files_rejected.load(Relaxed);
    let n: i64 = client.query_one(&sql.replace("= ANY($1)", "IN ('trace-0', 'trace-2', 'trace-4')"), &[]).await?.get(0);
    assert_eq!((n, stats.files_rejected.load(Relaxed) - before), (3, 3), "control: the IN-list form prunes");
    let before = stats.files_rejected.load(Relaxed);
    let ids = vec!["trace-0", "trace-2", "trace-4"];
    // Typed like hasql's `#{ids}` bind: the client declares text[].
    let stmt = client.prepare_typed(&sql, &[tokio_postgres::types::Type::TEXT_ARRAY]).await?;
    let n: i64 = client.query_one(&stmt, &[&ids]).await?.get(0);
    assert_eq!(n, 3, "every bound id must be found");
    assert_eq!(stats.files_rejected.load(Relaxed) - before, 3, "the three files holding none of the ids must be bloom-rejected");
    Ok(())
}

/// A file whose blooms over all columns exceeded the per-file cap was recorded `no_bloom` —
/// every compacted file, so a sealed day never pruned. Such stubs in a sidecar written before
/// the per-column fit must be re-lifted, after which a session lookup rejects the other file.
#[tokio::test(flavor = "multi_thread")]
async fn legacy_no_bloom_stubs_are_relifted_and_prune() -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    use timefusion::read::bloom_prune::{DateSidecar, FileBlooms, encode_sidecar, sidecar_path};
    let cfg = TestConfigBuilder::new("bloom_relift").with_buffer_mode(BufferMode::Enabled).build();
    let store = Arc::new(InMemory::new());
    let reg = Arc::new(BloomPruneRegistry::new(store.clone(), 64 << 20, Duration::from_secs(300)));
    let db = Arc::new(Database::with_config(cfg).await?.with_bloom_prune(reg));
    let pid = format!("proj_{}", &uuid::Uuid::new_v4().to_string()[..8]);
    let ts = ts();
    for (id, session) in [("a", "sess-A"), ("b", "sess-B")] {
        let mut r = row(id, &pid, ts, &format!("trace-{id}"));
        r["attributes___session___id"] = json!(session);
        insert(&db, &pid, vec![r]).await?;
    }
    // Seeded before any reconcile or query, so no resident entry masks the stored stub.
    let date = chrono::DateTime::<chrono::Utc>::from_timestamp_micros(ts).unwrap().date_naive().to_string();
    let files = db.list_file_uris(&pid, "otel_logs_and_spans").await?;
    let stubs = files.iter().map(|uri| FileBlooms { rel: uri[uri.find("project_id=").unwrap()..].to_string(), no_bloom: true, columns: vec![] }).collect();
    let mut legacy = encode_sidecar(&DateSidecar { files: stubs })?;
    legacy[0] = 1;
    object_store::ObjectStoreExt::put(store.as_ref(), &sidecar_path("otel_logs_and_spans", &pid, &date), legacy.into()).await?;
    db.bloom_sidecar_reconcile().await?;

    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let (lo, hi) = (ts - 3_600_000_000, ts + 3_600_000_000);
    let sql = format!(
        "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = '{pid}' AND attributes___session___id IN ('sess-A', 'sess-X') \
         AND timestamp >= to_timestamp_micros({lo}) AND timestamp <= to_timestamp_micros({hi})"
    );
    let stats = &db.bloom_prune().unwrap().stats;
    let before = stats.files_rejected.load(Relaxed);
    let n = ctx.sql(&sql).await?.collect().await?[0].column(0).as_primitive::<Int64Type>().value(0);
    assert_eq!((n, stats.files_rejected.load(Relaxed) - before), (1, 1), "sess-B's file must be bloom-rejected once its stub is re-lifted");
    Ok(())
}

/// `attributes___session___id` is `enrich_only`: monoscope's backfill only fills it, so a
/// session lookup may prune files lacking the id even though an UPDATE appends a new version
/// in a new file and leaves the empty version behind. The lookup must still resolve every row
/// to its winning version, exactly like the unpruned formulation.
#[tokio::test(flavor = "multi_thread")]
async fn enrich_only_session_lookup_prunes_and_resolves_winners() -> Result<()> {
    use std::sync::atomic::Ordering::Relaxed;
    let (db, pid) = setup("bloom_enrich").await?;
    let mut ctx = Arc::clone(&db).create_session_context();
    db.setup_session_context(&mut ctx)?;
    let ts = ts();
    // One commit per row => one file each.
    for (id, session) in [("s1", None), ("s2", Some("sess-B")), ("s3", Some("")), ("s4", None)] {
        let mut r = row(id, &pid, ts, &format!("trace-{id}"));
        r["attributes___session___id"] = json!(session);
        insert(&db, &pid, vec![r]).await?;
    }
    let exec = async |sql: String| ctx.sql(&sql).await?.collect().await;
    // monoscope's backfill shape fills s1; a direct fill takes s3 from '' to a value. Each
    // appends a version in its own file; the empty versions stay in theirs.
    exec(format!(
        "UPDATE otel_logs_and_spans SET attributes___session___id = COALESCE(attributes___session___id, 'sess-A') WHERE project_id = '{pid}' AND id = 's1'"
    ))
    .await?;
    exec(format!("UPDATE otel_logs_and_spans SET attributes___session___id = 'sess-C' WHERE project_id = '{pid}' AND id = 's3'")).await?;
    db.bloom_sidecar_reconcile().await?;

    let (lo, hi) = (ts - 3_600_000_000, ts + 3_600_000_000);
    let rows = async |pred: &str| -> Result<Vec<String>> {
        let sql = format!(
            "SELECT id || ':' || COALESCE(attributes___session___id, 'null') AS v FROM otel_logs_and_spans WHERE project_id = '{pid}' AND {pred} \
             AND timestamp >= to_timestamp_micros({lo}) AND timestamp <= to_timestamp_micros({hi}) ORDER BY 1"
        );
        let batches = ctx.sql(&sql).await?.collect().await?;
        let mut out = vec![];
        for b in &batches {
            let col = datafusion::arrow::compute::cast(b.column(0), &datafusion::arrow::datatypes::DataType::Utf8)?;
            out.extend(col.as_string::<i32>().iter().flatten().map(String::from));
        }
        Ok(out)
    };
    let stats = &db.bloom_prune().unwrap().stats;
    let pruned = async |pred: &str| -> Result<(Vec<String>, u64)> {
        let before = stats.files_rejected.load(Relaxed);
        let got = rows(pred).await?;
        Ok((got, stats.files_rejected.load(Relaxed) - before))
    };

    let ids = "attributes___session___id IN ('sess-A', 'sess-B', 'sess-C')";
    let (got, rejected) = pruned(ids).await?;
    assert_eq!(got, ["s1:sess-A", "s2:sess-B", "s3:sess-C"], "enriched winners found, their empty versions not returned");
    assert_eq!(rejected, 3, "the three files holding only empty versions (s1, s3, s4) must be bloom-rejected");
    // The same predicate in a shape no pruning path reads.
    assert_eq!(rows(&format!("COALESCE({ids}, false)")).await?, got, "pruned lookup must equal the unpruned one");

    // Predicates the pre-enrichment versions satisfy must not prune or run below the dedup.
    assert_eq!(pruned("attributes___session___id = ''").await?, (vec![], 0), "s3's stale '' version must not win");
    assert_eq!(rows("attributes___session___id IS NULL").await?, ["s4:null"], "s1's stale NULL version must not win");

    // Enrich-only: a set value can be neither changed nor cleared; refilling it is a no-op.
    for set in ["'other'", "NULL", "''"] {
        let res = exec(format!("UPDATE otel_logs_and_spans SET attributes___session___id = {set} WHERE project_id = '{pid}' AND id = 's2'")).await;
        assert!(res.is_err_and(|e| e.to_string().contains("enrich_only")), "SET {set} over a set value must be refused");
    }
    exec(format!("UPDATE otel_logs_and_spans SET attributes___session___id = 'sess-B' WHERE project_id = '{pid}' AND id = 's2'")).await?;
    assert_eq!(rows("attributes___session___id = 'sess-B'").await?, ["s2:sess-B"]);

    // A tombstone carries the enriched value, so it is read and hides the row.
    exec(format!("DELETE FROM otel_logs_and_spans WHERE project_id = '{pid}' AND id = 's1'")).await?;
    db.bloom_sidecar_reconcile().await?;
    assert_eq!(pruned(ids).await?.0, ["s2:sess-B", "s3:sess-C"], "deleted row must not resurrect");
    Ok(())
}

/// Parquet's row-group bloom check reads only a bare column, and the Delta leg casts the
/// Utf8 file column to the table's Utf8View. DataFusion unwraps that cast for `=` and for
/// IN lists of <=3 (lowered to `=`), not for longer IN lists, so monoscope's 5-id
/// `session_id = ANY($1)` lookup decoded every row group of every file it opened.
#[tokio::test(flavor = "multi_thread")]
async fn parquet_row_group_bloom_prunes_absent_ids_in_long_in_lists() -> Result<()> {
    let (db, pid) = setup("pq_bloom_in_list").await?;
    let ts = ts();
    let rows = (0..50)
        .map(|i| {
            let mut r = row(&format!("id-{i}"), &pid, ts, &format!("trace-{i}"));
            r["attributes___session___id"] = json!(format!("sess-{i}"));
            r
        })
        .collect();
    insert(&db, &pid, rows).await?;
    let server = crate::pgwire_harness::TestServer::serve(Arc::clone(&db)).await?;
    let client = server.client().await?;
    let (lo, hi) = (ts - 3_600_000_000, ts + 3_600_000_000);
    let explain = |pred: &str| {
        format!(
            "EXPLAIN ANALYZE SELECT id FROM otel_logs_and_spans WHERE project_id = '{pid}' AND {pred} \
             AND timestamp >= to_timestamp_micros({lo}) AND timestamp <= to_timestamp_micros({hi})"
        )
    };
    let re = regex::Regex::new(r"row_groups_pruned_bloom_filter=(\d+) total \u{2192} (\d+) matched")?;
    let assert_pruned = |pred: &str, rows: Vec<tokio_postgres::Row>| {
        let plan = rows.iter().map(|r| r.get::<_, String>(1)).collect::<Vec<_>>().join("\n");
        let pruned: u64 = re.captures_iter(&plan).map(|c| c[1].parse::<u64>().unwrap() - c[2].parse::<u64>().unwrap()).sum();
        assert!(pruned > 0, "`{pred}` must bloom-prune its row group:\n{plan}");
    };
    // Absent but inside each column's min/max, so only the bloom filter can skip them.
    let bound = "attributes___session___id = ANY($1)";
    let stmt = client.prepare_typed(&explain(bound), &[tokio_postgres::types::Type::TEXT_ARRAY]).await?;
    assert_pruned(bound, client.query(&stmt, &[&vec!["sess-1x", "sess-2x", "sess-3x", "sess-4x", "sess-5x"]]).await?);
    for pred in [
        "attributes___session___id IN ('sess-1x', 'sess-2x', 'sess-3x', 'sess-4x', 'sess-5x')",
        "attributes___session___id IN ('sess-1x', 'sess-2x')",
        "id = 'id-1x'",
    ] {
        assert_pruned(pred, client.query(&explain(pred), &[]).await?);
    }
    Ok(())
}
