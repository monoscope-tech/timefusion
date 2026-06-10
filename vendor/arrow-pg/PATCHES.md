# TimeFusion patches against arrow-pg 0.13.0

## 1. UUID parameter decoder
(Pre-existing, predates this file. See git history for the original patch.)

## 2. Surface PG JSON/JSONB OIDs from Arrow field metadata

DataFusion has no native jsonb type, so TF UDFs that should look like jsonb
(e.g. `jsonb_build_array`, `to_jsonb`) return `Utf8View`. Upstream arrow-pg
maps `Utf8View → Type::TEXT` (OID 25), which breaks strict drivers — hasql's
jsonb decoder rejects text-typed columns.

### Changes

- `src/datatypes.rs::field_into_pg_type` — if the field carries metadata
  `tf.pg_type = jsonb` (or `json`), return `Type::JSONB` / `Type::JSON`.
- `src/encoder.rs` — introduces a `JsonbStr<'a>` newtype that implements
  `ToSql`/`ToSqlText` for `Type::JSON | Type::JSONB`. For binary JSONB it
  prepends the mandatory `0x01` version byte; for binary JSON and text-format
  output it writes the raw UTF-8 bytes. The `Utf8`/`Utf8View` branches in
  `encode_value` route through `JsonbStr` whenever the pg_field datatype is
  JSON/JSONB.

### TF-side usage

`src/functions.rs` defines `JsonbBuildArrayUDF` and `ToJsonbUDF` whose
`return_field_from_args` attaches `{"tf.pg_type": "jsonb"}` to the output
Field. They wrap the existing `JsonBuildArrayUDF`/`ToJsonUDF` implementations
to reuse the body but emit a jsonb-tagged Field. The previous
`jsonb_build_array` / `to_jsonb` aliases on the json UDFs are removed.
