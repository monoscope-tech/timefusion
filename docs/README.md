# Documentation

The root [README](../README.md) contains the local setup and common SQL examples.
The [runbook](../RUNBOOK.md) contains production procedures.

## Reference documents

| Document | Purpose |
|---|---|
| [Architecture](ARCHITECTURE.md) | Current components, data paths, and design rules |
| [Configuration](../DELTA_CONFIG.md) | Environment variables and defaults |
| [PostgreSQL configuration](CONFIG_POSTGRES.md) | Optional project configuration database |
| [PostgreSQL compatibility](pg-client-compat.md) | Supported client behavior and known limits |
| [WAL](WAL.md) | WAL format, ownership, recovery, and durability |
| [Buffered writes](buffered-write-layer.md) | Buffer and flush internals |
| [Caching](CACHING.md) | Object-store cache behavior |
| [Delta checkpoints](DELTA_CHECKPOINT_HANDLING.md) | Delta metadata cache behavior |
| [Multiple tables](MULTI_TABLE_ARCHITECTURE.md) | Tenant and table routing |
| [Variant type](VARIANT_TYPE_SYSTEM.md) | Variant storage and SQL conversion |
| [Tracing](TRACING.md) | Logs, metrics, and traces |

## Plans

Keep only active implementation plans in [`plans/`](plans/README.md). Delete a
plan after the implementation ships or the direction changes. Git history keeps
the investigation record.
