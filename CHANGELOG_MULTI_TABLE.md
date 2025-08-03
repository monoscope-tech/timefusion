# Multi-Table Support Implementation

## Summary of Changes

### Core Architecture Changes

1. **Updated ProjectConfigs Type** (`src/database.rs`)
   - Changed from `HashMap<String, Arc<RwLock<DeltaTable>>>` to `HashMap<(String, String), Arc<RwLock<DeltaTable>>>`
   - Key is now `(project_id, table_name)` tuple instead of just `project_id`

2. **Modified Database Methods** (`src/database.rs`)
   - `resolve_table()`: Now accepts both `project_id` and `table_name` parameters
   - `insert_records_batch()`: Added `table_name` parameter
   - `register_project()`: Reordered parameters to include `table_name` as required parameter
   - Added `list_registered_tables()`: Returns all registered project-table combinations

3. **Updated ProjectRoutingTable** (`src/database.rs`)
   - Changed `_table_name` field to `table_name` (no longer unused)
   - `scan()` method now passes `table_name` to `resolve_table()`
   - `write_all()` method now passes `table_name` to `insert_records_batch()`

4. **Enhanced Session Context Setup** (`src/database.rs`)
   - `setup_session_context()` now registers all available table schemas from the registry
   - Each table type gets its own `ProjectRoutingTable` instance

### Schema Management

1. **Added New Schemas** (`schemas/`)
   - `metrics.yaml`: Schema for time-series metrics data
   - `events.yaml`: Schema for application and system events
   - Both follow the same structure as `otel_logs_and_spans.yaml`

2. **Updated Schema Loader** (`src/schema_loader.rs`)
   - Added new schemas to the `include_schemas!` macro
   - Registry now contains three table types

### API Changes

1. **Updated Registration Endpoint** (`src/main.rs`)
   - `/register_project` now includes table name in the S3 path
   - Path structure: `s3://{bucket}/{prefix}/projects/{project_id}/{table_name}/`
   - Defaults to `otel_logs_and_spans` if no table_name provided

2. **Added List Tables Endpoint** (`src/main.rs`)
   - New GET endpoint: `/list_tables`
   - Returns all registered project-table combinations

### Storage Structure

- **Old**: `s3://{bucket}/{prefix}/projects/{project_id}/`
- **New**: `s3://{bucket}/{prefix}/projects/{project_id}/{table_name}/`

### Maintenance Operations

- Updated optimize and vacuum schedulers to handle multiple tables per project
- Each table is maintained independently

### Test Updates

- Updated all test cases to use the new `insert_records_batch()` signature
- Tests still pass with the new architecture

## Benefits

1. **Multiple Table Types Per Project**: Projects can now have separate tables for logs, metrics, events, etc.
2. **Schema Flexibility**: Each table type has its own optimized schema
3. **Better Query Performance**: Queries only scan relevant table types
4. **BYOB Support**: Better support for customers with custom S3 buckets
5. **Backward Compatibility**: Existing single-table projects continue to work

## Files Modified

- `src/database.rs`: Core database logic and routing
- `src/main.rs`: API endpoints
- `src/batch_queue.rs`: Batch processing
- `src/schema_loader.rs`: Schema registry
- `schemas/metrics.yaml`: New metrics schema (created)
- `schemas/events.yaml`: New events schema (created)
- `docs/MULTI_TABLE_ARCHITECTURE.md`: Architecture documentation (created)
- `examples/multi_table_demo.sh`: Demo script (created)

## Migration Notes

- Existing deployments will continue to work with the default `otel_logs_and_spans` table
- The default project registration path has been updated to include the table name
- Projects can incrementally add new table types without affecting existing data