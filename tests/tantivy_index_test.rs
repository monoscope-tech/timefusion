//! Tier-1 unit tests for `tantivy_index`: schema build, batch indexing,
//! and query roundtrip. Pure-Rust, no S3, no DataFusion plumbing.

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayBuilder, ArrayRef, ListArray, RecordBatch, StringArray, StringBuilder, StructArray, TimestampMicrosecondArray},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit},
};
use parquet_variant_compute::VariantArrayBuilder;
use parquet_variant_json::JsonToVariant;
use tantivy::{
    Term,
    query::{BooleanQuery, Occur, QueryParser, RangeQuery, TermQuery},
    schema::IndexRecordOption,
};
use timefusion::{
    schema_loader::{FieldDef, SortingColumnDef, TableSchema, TantivyFieldConfig},
    tantivy_index::{Hit, build_for_table, build_in_memory, query_index},
};

fn ts_field(name: &str, nullable: bool) -> FieldDef {
    FieldDef {
        name: name.into(),
        data_type: "Timestamp(Microsecond, Some(\"UTC\"))".into(),
        nullable,
        tantivy: None,
        dictionary: None,
        bloom_filter: false,
    }
}
fn utf8(name: &str, indexed: bool, tokenizer: &str) -> FieldDef {
    FieldDef {
        name: name.into(),
        data_type: "Utf8".into(),
        nullable: true,
        tantivy: indexed.then(|| TantivyFieldConfig {
            indexed: true,
            tokenizer: Some(tokenizer.into()),
            flatten: None,
        }),
        dictionary: None,
        bloom_filter: false,
    }
}
fn list_utf8(name: &str, tokenizer: &str) -> FieldDef {
    FieldDef {
        name: name.into(),
        data_type: "List(Utf8)".into(),
        nullable: false,
        tantivy: Some(TantivyFieldConfig {
            indexed: true,
            tokenizer: Some(tokenizer.into()),
            flatten: None,
        }),
        dictionary: None,
        bloom_filter: false,
    }
}
fn variant(name: &str, flatten: &str) -> FieldDef {
    FieldDef {
        name: name.into(),
        data_type: "Variant".into(),
        nullable: true,
        tantivy: Some(TantivyFieldConfig {
            indexed: true,
            tokenizer: Some("default".into()),
            flatten: Some(flatten.into()),
        }),
        dictionary: None,
        bloom_filter: false,
    }
}

fn small_table() -> TableSchema {
    TableSchema {
        table_name: "t".into(),
        partitions: vec![],
        sorting_columns: vec![SortingColumnDef {
            name: "timestamp".into(),
            descending: false,
            nulls_first: false,
        }],
        z_order_columns: vec![],
        time_column: None,
        dedup_keys: vec![],
        dedup_tiebreak: None,
        fields: vec![
            ts_field("timestamp", false),
            FieldDef {
                name: "id".into(),
                data_type: "Utf8".into(),
                nullable: false,
                tantivy: None,
                dictionary: None,
                bloom_filter: false,
            },
            utf8("level", true, "raw"),
            utf8("message", true, "default"),
            list_utf8("summary", "default"),
            variant("body", "json"),
            variant("attributes", "kv"),
        ],
    }
}

#[allow(clippy::type_complexity)]
fn batch(rows: &[(i64, &str, &str, &str, Vec<&str>, &str, &str)]) -> RecordBatch {
    // (timestamp, id, level, message, summary, body_json, attrs_json)
    let ts: ArrayRef = Arc::new(TimestampMicrosecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>()).with_timezone("UTC"));
    let id: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>()));
    let level: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|r| r.2).collect::<Vec<_>>()));
    let msg: ArrayRef = Arc::new(StringArray::from(rows.iter().map(|r| r.3).collect::<Vec<_>>()));

    // Summary: List(Utf8)
    let mut sb = StringBuilder::new();
    let mut offsets = vec![0i32];
    for r in rows {
        for s in &r.4 {
            sb.append_value(s);
        }
        offsets.push(sb.len() as i32);
    }
    let values = sb.finish();
    let summary: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )
        .unwrap(),
    );

    // Variant columns built from JSON literals.
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
    let arr = b.build();
    // The builder yields BinaryView; Tantivy code path uses VariantArray::try_new(StructArray)
    // which works with either Binary or BinaryView for our test purposes — but the builder
    // currently emits BinaryView, so cast metadata/value down to Binary for parity with what
    // delta_kernel produces in production.
    let struct_arr: StructArray = arr.into();
    let (fields, columns, nulls) = struct_arr.into_parts();
    use arrow::array::{BinaryArray, BinaryViewArray};
    let mut new_cols: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut new_fields = Vec::with_capacity(fields.len());
    for (i, c) in columns.into_iter().enumerate() {
        if let Some(view) = c.as_any().downcast_ref::<BinaryViewArray>() {
            let mut b = arrow::array::BinaryBuilder::new();
            for r in 0..view.len() {
                if view.is_null(r) {
                    b.append_null();
                } else {
                    b.append_value(view.value(r));
                }
            }
            new_cols.push(Arc::new(b.finish()) as ArrayRef);
            new_fields.push(Arc::new(Field::new(fields[i].name(), DataType::Binary, fields[i].is_nullable())));
        } else if c.as_any().downcast_ref::<BinaryArray>().is_some() {
            new_cols.push(c);
            new_fields.push(Arc::new(Field::new(fields[i].name(), DataType::Binary, fields[i].is_nullable())));
        } else {
            panic!("unexpected variant column: {:?}", c.data_type());
        }
    }
    Arc::new(StructArray::new(new_fields.into(), new_cols, nulls)) as ArrayRef
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
fn build_and_query_term_and_phrase() {
    let table = small_table();
    let b = batch(&[
        (
            1_000_000,
            "a",
            "INFO",
            "hello world",
            vec!["greeting"],
            r#"{"msg":"timeout occurred"}"#,
            r#"{"http":{"status":"200"}}"#,
        ),
        (
            2_000_000,
            "b",
            "ERROR",
            "panic on shutdown",
            vec!["fatal", "shutdown"],
            r#"{"msg":"db connection lost"}"#,
            r#"{"http":{"status":"500"}}"#,
        ),
        (
            3_000_000,
            "c",
            "INFO",
            "goodbye world",
            vec!["greeting"],
            r#"{"msg":"clean exit"}"#,
            r#"{"http":{"status":"200"}}"#,
        ),
    ]);
    let (idx, built, stats) = build_in_memory(&table, std::slice::from_ref(&b)).unwrap();
    assert_eq!(stats.rows, 3);
    assert_eq!(stats.min_timestamp_micros, Some(1_000_000));
    assert_eq!(stats.max_timestamp_micros, Some(3_000_000));

    // Term query on raw-tokenizer field (level = ERROR)
    let level_field = built.user_fields.get("level").unwrap().field;
    let q = TermQuery::new(Term::from_field_text(level_field, "ERROR"), IndexRecordOption::Basic);
    let hits = query_index(&idx, &q, None).unwrap();
    assert_eq!(
        hits,
        vec![Hit {
            timestamp_micros: 2_000_000,
            id: "b".into(),
            row_ordinal: Some(1),
        }]
    );

    // Phrase via QueryParser on default-tokenizer field (message)
    let msg_field = built.user_fields.get("message").unwrap().field;
    let qp = QueryParser::for_index(&idx, vec![msg_field]);
    let q = qp.parse_query("\"panic on shutdown\"").unwrap();
    let hits = query_index(&idx, &*q, None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "b");
}

#[test]
fn query_timestamp_range_and_boolean() {
    let table = small_table();
    let b = batch(&[
        (1_000_000, "a", "INFO", "x", vec![], "", ""),
        (2_000_000, "b", "ERROR", "y", vec![], "", ""),
        (3_000_000, "c", "INFO", "z", vec![], "", ""),
    ]);
    let (idx, built, _) = build_in_memory(&table, std::slice::from_ref(&b)).unwrap();
    let ts = built.timestamp;
    let level = built.user_fields.get("level").unwrap().field;

    let range = RangeQuery::new_i64("_timestamp".to_string(), 1_500_000..3_500_000);
    let _ = ts;
    let info = TermQuery::new(Term::from_field_text(level, "INFO"), IndexRecordOption::Basic);
    let combined = BooleanQuery::new(vec![(Occur::Must, Box::new(range)), (Occur::Must, Box::new(info))]);
    let hits = query_index(&idx, &combined, None).unwrap();
    let ids: Vec<_> = hits.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, vec!["c"]);
}

#[test]
fn variant_kv_flatten_indexes_status_value() {
    let table = small_table();
    let b = batch(&[
        (1_000_000, "a", "INFO", "x", vec![], r#"{"msg":"hello"}"#, r#"{"http":{"status":"200"}}"#),
        (2_000_000, "b", "ERROR", "y", vec![], r#"{"msg":"oops"}"#, r#"{"http":{"status":"500"}}"#),
    ]);
    let (idx, built, _) = build_in_memory(&table, std::slice::from_ref(&b)).unwrap();
    let attrs = built.user_fields.get("attributes").unwrap().field;
    // kv flatten emits "http.status:500" — query for "500" should match the second row.
    let qp = QueryParser::for_index(&idx, vec![attrs]);
    let q = qp.parse_query("500").unwrap();
    let hits = query_index(&idx, &*q, None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "b");
}

#[test]
fn variant_json_flatten_full_text() {
    let table = small_table();
    let b = batch(&[
        (1_000_000, "a", "INFO", "x", vec![], r#"{"msg":"timeout occurred"}"#, ""),
        (2_000_000, "b", "ERROR", "y", vec![], r#"{"msg":"db connection lost"}"#, ""),
    ]);
    let (idx, built, _) = build_in_memory(&table, std::slice::from_ref(&b)).unwrap();
    let body = built.user_fields.get("body").unwrap().field;
    let qp = QueryParser::for_index(&idx, vec![body]);
    let q = qp.parse_query("timeout").unwrap();
    let hits = query_index(&idx, &*q, None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "a");
}

#[test]
fn list_utf8_is_joined_and_searchable() {
    let table = small_table();
    let b = batch(&[(1_000_000, "a", "INFO", "x", vec!["alpha", "beta"], "", ""), (2_000_000, "b", "INFO", "y", vec!["gamma"], "", "")]);
    let (idx, built, _) = build_in_memory(&table, std::slice::from_ref(&b)).unwrap();
    let summary = built.user_fields.get("summary").unwrap().field;
    let qp = QueryParser::for_index(&idx, vec![summary]);
    let q = qp.parse_query("beta").unwrap();
    let hits = query_index(&idx, &*q, None).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "a");
}
