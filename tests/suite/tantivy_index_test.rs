//! Unit tests for `tantivy_index`: schema build, batch indexing, query roundtrip.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, ListBuilder, RecordBatch, StringArray, StringBuilder, StructArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit},
};
use parquet_variant_compute::VariantArrayBuilder;
use parquet_variant_json::JsonToVariant;
use tantivy::{
    Term,
    query::{BooleanQuery, Occur, QueryParser, RangeQuery, TermQuery},
    schema::IndexRecordOption,
};
use test_case::test_case;
use timefusion::{
    schema::TableSchema,
    tantivy::{
        build_for_table, build_in_memory,
        search::{Hit, query_index},
    },
};

use super::tantivy_search_test::{TS_TYPE, field, table_schema, tantivy_cfg};

fn small_table() -> TableSchema {
    table_schema(
        "t",
        vec![
            field("timestamp", TS_TYPE, false, None),
            field("id", "Utf8", false, None),
            field("level", "Utf8", true, Some(tantivy_cfg("raw", None))),
            field("message", "Utf8", true, Some(tantivy_cfg("default", None))),
            field("summary", "List(Utf8)", false, Some(tantivy_cfg("default", None))),
            field("body", "Variant", true, Some(tantivy_cfg("default", Some("json")))),
            field("attributes", "Variant", true, Some(tantivy_cfg("default", Some("kv")))),
        ],
    )
}

/// (timestamp, id, level, message, summary, body_json, attrs_json) — an empty
/// JSON string means a null variant.
#[allow(clippy::type_complexity)]
type Row<'a> = (i64, &'a str, &'a str, &'a str, Vec<&'a str>, &'a str, &'a str);

fn batch(rows: &[Row<'_>]) -> RecordBatch {
    let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>()).with_timezone("UTC"));
    let id: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()));
    let level: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>()));
    let msg: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|r| r.3).collect::<Vec<_>>()));

    // Every summary list is non-null; an empty Vec yields an empty (not null) list.
    let mut lists = ListBuilder::new(StringBuilder::new());
    for r in rows {
        r.4.iter().for_each(|s| lists.values().append_value(s));
        lists.append(true);
    }
    let summary: ArrayRef = Arc::new(lists.finish());

    let body = build_variant(rows.iter().map(|r| r.5).collect());
    let attrs = build_variant(rows.iter().map(|r| r.6).collect());

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("id", DataType::Utf8, false),
        Field::new("level", DataType::Utf8, true),
        Field::new("message", DataType::Utf8, true),
        Field::new("summary", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), false),
        Field::new(
            "body",
            DataType::Struct(vec![Arc::new(Field::new("metadata", DataType::Binary, false)), Arc::new(Field::new("value", DataType::Binary, false))].into()),
            true,
        ),
        Field::new(
            "attributes",
            DataType::Struct(vec![Arc::new(Field::new("metadata", DataType::Binary, false)), Arc::new(Field::new("value", DataType::Binary, false))].into()),
            true,
        ),
    ]));
    RecordBatch::try_new(schema, vec![ts, id, level, msg, summary, body, attrs]).unwrap()
}

fn build_variant(jsons: Vec<&str>) -> ArrayRef {
    let mut b = VariantArrayBuilder::new(jsons.len());
    for j in jsons {
        if j.is_empty() {
            b.append_null();
        } else {
            b.append_json(j).expect("append_json");
        }
    }
    // The builder emits BinaryView; cast metadata/value to Binary to match what delta_kernel produces.
    let struct_arr: StructArray = b.build().into();
    let (fields, columns, nulls) = struct_arr.into_parts();
    let new_cols: Vec<ArrayRef> = columns.iter().map(|c| arrow::compute::cast(c, &DataType::Binary).expect("variant column must cast to Binary")).collect();
    let new_fields: Vec<_> = fields.iter().map(|f| Arc::new(Field::new(f.name(), DataType::Binary, f.is_nullable()))).collect();
    Arc::new(StructArray::new(new_fields.into(), new_cols, nulls)) as ArrayRef
}

/// Row c has an empty summary list and null variants; its message says "timeout"
/// so a body query must not match it — text in one field must not match another.
fn corpus() -> Vec<Row<'static>> {
    vec![
        (1_000_000, "a", "INFO", "hello world", vec!["alpha", "beta", "greeting"], r#"{"msg":"timeout occurred"}"#, r#"{"http":{"status":"200"}}"#),
        (2_000_000, "b", "ERROR", "panic on shutdown", vec!["gamma", "fatal", "shutdown"], r#"{"msg":"db connection lost"}"#, r#"{"http":{"status":"500"}}"#),
        (3_000_000, "c", "INFO", "goodbye world timeout", vec![], "", ""),
    ]
}

/// Two sliced batches (non-zero offset into shared buffers) plus one whose
/// variant columns are entirely null.
fn corpus_batches() -> Vec<RecordBatch> {
    let rows = corpus();
    let ab = batch(&rows[0..2]);
    vec![ab.slice(0, 1), ab.slice(1, 1), batch(&rows[2..3])]
}

#[test]
fn schema_build_emits_reserved_and_user_fields() {
    let table = small_table();
    let built = build_for_table(&table);
    assert!(built.schema.get_field("_timestamp").is_ok());
    assert!(built.schema.get_field("_id").is_ok());
    for name in ["level", "message", "summary", "body", "attributes"] {
        assert!(built.user_fields.contains_key(name), "missing user field {name}");
    }
}

#[test]
fn build_and_query_term_range_and_boolean() {
    let table = small_table();
    let b = batch(&corpus());
    let (idx, built, stats) = build_in_memory(&table, std::slice::from_ref(&b)).unwrap();
    assert_eq!((stats.batches, stats.rows), (1, 3), "single batch, all rows indexed");
    assert_eq!(stats.min_timestamp_micros, Some(1_000_000));
    assert_eq!(stats.max_timestamp_micros, Some(3_000_000));

    let level = built.user_fields.get("level").unwrap().field;
    let q = TermQuery::new(Term::from_field_text(level, "ERROR"), IndexRecordOption::Basic);
    let hits = query_index(&idx, &q, None).unwrap();
    assert_eq!(hits, vec![Hit { timestamp_micros: 2_000_000, id: "b".into(), row_ordinal: Some(1) }]);

    let range = RangeQuery::new_i64("_timestamp".to_string(), 1_500_000..3_500_000);
    let info = TermQuery::new(Term::from_field_text(level, "INFO"), IndexRecordOption::Basic);
    let combined = BooleanQuery::new(vec![(Occur::Must, Box::new(range)), (Occur::Must, Box::new(info))]);
    let hits = query_index(&idx, &combined, None).unwrap();
    assert_eq!(hits.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(), vec!["c"]);
}

#[test]
fn multi_batch_indexes_sliced_and_null_variant_batches() {
    let (_, _, stats) = build_in_memory(&small_table(), &corpus_batches()).unwrap();
    assert_eq!((stats.batches, stats.rows), (3, 3));
}

// Returns matching ids, sorted and comma-joined. `summary` (List(Utf8)) is joined into one
// text field; `attributes` uses kv flatten (emits "http.status:500"); `body` uses json flatten.
#[test_case("message", "\"panic on shutdown\"" => "b" ; "phrase on default tokenizer")]
#[test_case("message", "world" => "a,c" ; "term matching two rows")]
#[test_case("body", "timeout" => "a" ; "variant json flatten full text")]
#[test_case("attributes", "500" => "b" ; "variant kv flatten indexes status value")]
#[test_case("attributes", "200" => "a" ; "variant kv flatten other status")]
#[test_case("summary", "beta" => "a" ; "list utf8 joined and searchable")]
#[test_case("summary", "gamma" => "b" ; "list utf8 second row")]
fn parsed_query_matches(field: &str, query: &str) -> String {
    let table = small_table();
    let (idx, built, _) = build_in_memory(&table, &corpus_batches()).unwrap();
    let f = built.user_fields.get(field).unwrap().field;
    let q = QueryParser::for_index(&idx, vec![f]).parse_query(query).unwrap();
    let mut ids: Vec<String> = query_index(&idx, &*q, None).unwrap().iter().map(|h| h.id.to_string()).collect();
    ids.sort();
    ids.join(",")
}
