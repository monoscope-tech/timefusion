use std::{collections::BTreeMap, sync::Arc, time::Instant};
use arrow::{array::{Array, ArrayRef, ListBuilder, StringBuilder, StringArray, TimestampMicrosecondArray}, datatypes::{DataType, Field, Schema, TimeUnit}, record_batch::RecordBatch};
use timefusion::tantivy::{MergeMode, build_to_dir, open_index, histogram::{HistogramWindow, Membership}};

fn main() {
    const DAY: i64 = 86_400_000_000;
    const HOUR: i64 = DAY / 24;
    let rows = 300_000usize;
    let timestamp = |row: usize| (row % 30) as i64 * DAY + (row / 30) as i64 * 1_000_000;
    let mut hashes = ListBuilder::new(StringBuilder::new());
    for row in 0..rows {
        hashes.values().append_value("common");
        hashes.values().append_value("common");
        if row % 100 == 0 { hashes.values().append_value("rare"); }
        hashes.append(true);
    }
    let hashes: ArrayRef = Arc::new(hashes.finish());
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("id", DataType::Utf8, false),
        Field::new("hashes", hashes.data_type().clone(), true),
    ]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(TimestampMicrosecondArray::from_iter_values((0..rows).map(timestamp)).with_timezone("UTC")),
        Arc::new(StringArray::from_iter_values((0..rows).map(|row| format!("row-{row}")))),
        hashes,
    ]).unwrap();
    let table = timefusion::schema::get_schema("otel_logs_and_spans").unwrap();
    let window = HistogramWindow::new(0, 30 * DAY, HOUR, 0, 720).unwrap();
    for trial in 0..2 {
        let modes = if trial == 0 { [MergeMode::Now, MergeMode::Deferred] } else { [MergeMode::Deferred, MergeMode::Now] };
        for mode in modes {
            let dir = tempfile::tempdir().unwrap();
            let started = Instant::now();
            let (_, stats) = build_to_dir(table, std::slice::from_ref(&batch), dir.path(), mode).unwrap();
            let build_ms = started.elapsed().as_secs_f64() * 1000.0;
            let index = open_index(dir.path()).unwrap();
            let reader = index.reader().unwrap();
            println!("build trial={trial} mode={mode:?} rows={} segments={} elapsed_ms={build_ms:.3}", stats.rows, stats.segments);
            for term in ["rare", "common"] {
                let expected = (0..rows).filter(|row| row % 7 != 0 && (term == "common" || row % 100 == 0))
                    .fold(BTreeMap::new(), |mut counts, row| { *counts.entry(timestamp(row) / HOUR * HOUR).or_insert(0u64) += 1; counts });
                let membership = Membership::Contains { column: "hashes".into(), value: term.into() };
                for repeat in 0..8 {
                    let query = membership.query(&index.schema(), &stats.element_fields).unwrap();
                    let started = Instant::now();
                    let counts = window.search(&reader.searcher(), query, |ordinal| ordinal % 7 != 0).unwrap();
                    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
                    assert_eq!(counts, expected, "{mode:?} {term} {repeat}");
                    println!("query trial={trial} mode={mode:?} term={term} repeat={repeat} elapsed_ms={elapsed_ms:.3}");
                }
            }
        }
    }
}
