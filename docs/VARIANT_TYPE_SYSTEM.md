# Variant Type System

TimeFusion supports Snowflake-style Variant columns for semi-structured JSON data. This document explains how Variant types are implemented and used.

## Overview

Variant columns allow storing arbitrary JSON structures without a predefined schema. They're useful for:
- Dynamic attributes that vary between records
- Nested JSON objects from external APIs
- Schema-less data that evolves over time

## Representation

Variant is represented as an Arrow Struct with two BinaryView fields:

```
Struct {
    metadata: BinaryView,  // Type information
    value: BinaryView,     // Serialized data
}
```

### Detection

The `is_variant_type()` function in `schema_loader.rs` identifies Variant columns:

```rust
pub fn is_variant_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Struct(fields)
        if fields.len() == 2
        && fields.iter().any(|f| f.name() == "metadata")
        && fields.iter().any(|f| f.name() == "value"))
}
```

## Schema Definition

In YAML schema files, Variant columns are defined with type `Variant`:

```yaml
# schemas/otel_logs_and_spans.yaml
fields:
  - name: attributes
    type: Variant
    nullable: true
  - name: resource_attributes
    type: Variant
    nullable: true
```

## Query Transformations

### INSERT: Automatic Utf8 → Variant Conversion

When inserting JSON strings into Variant columns, the `VariantInsertRewriter` automatically wraps them with `json_to_variant()`:

**Before rewrite:**
```sql
INSERT INTO otel_logs_and_spans (project_id, attributes)
VALUES ('proj-1', '{"user": "alice", "action": "login"}');
```

**After rewrite (internal):**
```sql
INSERT INTO otel_logs_and_spans (project_id, attributes)
VALUES ('proj-1', json_to_variant('{"user": "alice", "action": "login"}'));
```

The rewriter:
1. Intercepts INSERT DML statements
2. Identifies columns with Variant target type
3. Wraps Utf8/Utf8View literals with `json_to_variant()` UDF
4. Applies recursively to Values and Projection nodes

### SELECT: Automatic Variant → JSON Conversion

When selecting Variant columns over pgwire, `VariantPgwireRootWrap` wraps them with `variant_to_json()` for wire-protocol compatibility (companion to `VariantTableScanSchemaPatch`, which restores the real Variant type on the scan side). Internal SQL contexts skip the wrap and receive binary Variant end-to-end.

**Before rewrite:**
```sql
SELECT attributes FROM otel_logs_and_spans WHERE project_id = 'proj-1';
```

**After rewrite (internal):**
```sql
SELECT variant_to_json(attributes) AS attributes FROM otel_logs_and_spans WHERE project_id = 'proj-1';
```

The rewriter:
1. Intercepts Projection nodes
2. Checks if expression result type is Variant
3. Wraps with `variant_to_json()` to output JSON string
4. Preserves original column aliases

## JSON Path Access Operators

TimeFusion supports PostgreSQL-style JSON operators for accessing nested values:

| Operator | Description | Example |
|----------|-------------|---------|
| `->` | Get JSON object at key | `attributes->'user'` |
| `->>` | Get JSON value as text | `attributes->>'user_id'` |

### Implementation

The `VariantAwareExprPlanner` (in `functions.rs`) intercepts these operators:

```rust
// Example: attributes->'user'->'id' becomes:
variant_get(attributes, "user.id")

// Example: attributes->>'user_id' becomes:
variant_to_json(variant_get(attributes, "user_id"))
```

### Usage Examples

```sql
-- Get nested object
SELECT attributes->'http'->'request'
FROM otel_logs_and_spans
WHERE project_id = 'proj-1';

-- Get text value for filtering
SELECT * FROM otel_logs_and_spans
WHERE project_id = 'proj-1'
  AND attributes->>'user_id' = 'u_123';

-- Access array elements
SELECT attributes->'items'->0
FROM otel_logs_and_spans
WHERE project_id = 'proj-1';
```

## Variant UDFs

### json_to_variant(utf8) → Variant

Converts a JSON string to Variant type:

```sql
SELECT json_to_variant('{"key": "value"}');
```

### variant_to_json(variant) → Utf8

Converts Variant back to JSON string:

```sql
SELECT variant_to_json(attributes) FROM otel_logs_and_spans;
```

### variant_get(variant, path) → Variant

Extracts a sub-value using dot-notation path:

```sql
-- Get nested value
SELECT variant_get(attributes, 'user.profile.name');

-- Get array element
SELECT variant_get(attributes, 'items[0]');
```

## WAL and Recovery

Variant data is stored in WAL entries as serialized Arrow data:
- INSERT: `CompactBatch` contains the raw Variant struct data
- No special handling needed - Variant is just a Struct type to Arrow

On recovery, Variant columns are reconstructed from the WAL entry's schema.

## Schema Evolution

Variant columns naturally support schema evolution:
- New JSON fields can be added without schema changes
- Old fields can be removed from new records
- Different records can have different JSON structures

## Performance Considerations

### Storage
- Variant data is stored as binary, typically more compact than string JSON
- Parquet compression applies to the underlying BinaryView

### Query Performance
- `->` and `->>` operators are converted to `variant_get()` calls
- Path access involves parsing and traversing the Variant structure
- For frequently-accessed fields, consider promoting to top-level columns

### Best Practices
1. Use Variant for truly dynamic data
2. Promote frequently-queried fields to dedicated columns
3. Use `->>` for text comparisons in WHERE clauses
4. Index on top-level columns, not Variant paths

## Files

| File | Purpose |
|------|---------|
| `src/schema_loader.rs` | `is_variant_type()` detection, schema parsing |
| `src/optimizers/variant_insert_rewriter.rs` | INSERT Utf8 → Variant rewriting |
| `src/optimizers/variant_select_rewriter.rs` | SELECT Variant → JSON rewriting |
| `src/functions.rs` | `VariantAwareExprPlanner` for `->` and `->>` |
| `datafusion-variant` crate | UDF implementations |

## Example Session

```sql
-- Create data with Variant attributes
INSERT INTO otel_logs_and_spans (
    project_id, name, id, timestamp, date, hashes,
    attributes
) VALUES (
    'proj-1',
    'api.request',
    '550e8400-e29b-41d4-a716-446655440000',
    '2025-01-17 14:25:00',
    '2025-01-17',
    ARRAY[]::text[],
    '{"http": {"method": "POST", "status": 200}, "user": {"id": "u_123", "role": "admin"}}'
);

-- Query with path access
SELECT
    name,
    attributes->>'http'->>'method' as http_method,
    attributes->>'user'->>'id' as user_id
FROM otel_logs_and_spans
WHERE project_id = 'proj-1'
  AND attributes->'http'->>'status' = '200';

-- Filter on nested values
SELECT * FROM otel_logs_and_spans
WHERE project_id = 'proj-1'
  AND attributes->'user'->>'role' = 'admin';
```
