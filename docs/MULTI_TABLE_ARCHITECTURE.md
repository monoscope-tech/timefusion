# Multi-Table Architecture in TimeFusion

## Overview

TimeFusion now supports multiple table types per project, allowing each project to have different schemas for different types of data (logs, metrics, events, etc.). This enables better data organization and query performance.

## Key Changes

### 1. Data Structure Changes

The project configuration has been updated from:
```rust
HashMap<String, Arc<RwLock<DeltaTable>>>  // project_id -> table
```

To:
```rust
HashMap<(String, String), Arc<RwLock<DeltaTable>>>  // (project_id, table_name) -> table
```

### 2. Storage Structure

Delta Lake tables are now organized with the following S3 path structure:
```
s3://{bucket}/{prefix}/projects/{project_id}/{table_name}/
```

For example:
- `s3://my-bucket/timefusion/projects/acme-corp/otel_logs_and_spans/`
- `s3://my-bucket/timefusion/projects/acme-corp/metrics/`
- `s3://my-bucket/timefusion/projects/acme-corp/events/`

### 3. Available Table Types

Currently, three table schemas are available:

1. **otel_logs_and_spans**: OpenTelemetry logs and spans data
2. **metrics**: Time-series metrics data
3. **events**: Application and system events

### 4. Query Routing

The `ProjectRoutingTable` now routes queries based on both:
- The table being queried (from the SQL FROM clause)
- The project_id (extracted from WHERE clause filters)

Example queries:
```sql
-- Query logs for a specific project
SELECT * FROM otel_logs_and_spans WHERE project_id = 'acme-corp';

-- Query metrics for a specific project
SELECT * FROM metrics WHERE project_id = 'acme-corp' AND timestamp > '2024-01-01';

-- Query events for a specific project
SELECT * FROM events WHERE project_id = 'acme-corp' AND event_type = 'error';
```

## API Usage

### Registering Tables

To register a table for a project, use the `/register_project` endpoint:

```bash
curl -X POST "http://localhost:80/register_project" \
  -H "Content-Type: application/json" \
  -d '{
    "project_id": "acme-corp",
    "bucket": "my-data-bucket",
    "access_key": "ACCESS_KEY",
    "secret_key": "SECRET_KEY",
    "endpoint": "https://s3.amazonaws.com",
    "table_name": "metrics"
  }'
```

### Listing Registered Tables

To see all registered project-table combinations:

```bash
curl -X GET "http://localhost:80/list_tables"
```

Response:
```json
{
  "tables": [
    {"project_id": "project-uuid-1", "table_name": "otel_logs_and_spans"},
    {"project_id": "acme-corp", "table_name": "otel_logs_and_spans"},
    {"project_id": "acme-corp", "table_name": "metrics"},
    {"project_id": "acme-corp", "table_name": "events"}
  ]
}
```

## Benefits

1. **Data Isolation**: Different data types are stored in separate Delta tables
2. **Schema Flexibility**: Each table type can have its own optimized schema
3. **Query Performance**: Queries only scan relevant table types
4. **Storage Optimization**: Different partitioning and optimization strategies per table type
5. **BYOB Support**: Customers can bring their own S3 buckets with proper table organization

## Migration Guide

For existing deployments:

1. Each project must have a valid UUID for project_id
2. Existing data paths remain unchanged for backward compatibility
3. New table types can be added incrementally without affecting existing data

## Adding New Table Types

To add a new table type:

1. Create a new schema YAML file in `schemas/` directory
2. Update `schema_loader.rs` to include the new schema
3. Register the table for projects that need it using the API

## Maintenance

Each table is independently:
- Optimized (with Z-ordering on table-specific columns)
- Vacuumed (to clean up old files)
- Checkpointed (for query performance)

The maintenance schedulers automatically handle all registered tables.