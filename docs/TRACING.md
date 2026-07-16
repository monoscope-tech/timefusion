# DataFusion Tracing Integration

TimeFusion now includes [datafusion-tracing](https://github.com/datafusion-contrib/datafusion-tracing) for detailed query execution insights.

## Overview

DataFusion-tracing automatically instruments your query execution with detailed spans, providing visibility into:
- Query planning phases
- Physical plan execution
- Operator execution times
- Row counts and metrics
- Memory usage

## Configuration

### Environment Variables

```bash
# Basic tracing configuration
RUST_LOG=info,datafusion=debug,timefusion=debug

# For structured JSON logs (recommended for production)
RUST_LOG=info

# OpenTelemetry configuration (for future OTLP export)
OTEL_SERVICE_NAME=timefusion
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_TRACES_SAMPLER_RATIO=1.0
```

### How It Works

1. **Automatic Integration**: When you create a `SessionContext` using `Database::create_session_context()`, datafusion-tracing is automatically configured.

2. **Instrumentation**: The tracing extension adds spans around physical plan execution, capturing:
   - Execution time for each operator
   - Row counts processed
   - Memory allocation
   - Plan optimization steps

3. **Output Format**: Traces are output as structured JSON logs by default, making them easy to parse and send to observability platforms.

## Usage Example

```rust
use timefusion::database::Database;
use timefusion::telemetry;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize telemetry
    telemetry::init_telemetry()?;
    
    // Create database and session
    let db = Database::new().await?;
    let ctx = db.create_session_context();
    
    // Execute queries - they will be automatically traced
    let df = ctx.sql("SELECT * FROM my_table WHERE timestamp > now() - interval '1 hour'")
        .await?;
    
    df.show().await?;
    
    Ok(())
}
```

## Viewing Traces

### Local Development

Run with debug logging to see traces in your console:
```bash
RUST_LOG=debug cargo run
```

### Production

The JSON-formatted logs can be:
1. Collected by log aggregators (e.g., Fluentd, Logstash)
2. Sent to observability platforms (e.g., Datadog, New Relic)
3. Stored in time-series databases for analysis

### Example Trace Output

```json
{
  "timestamp": "2024-01-15T10:30:45.123Z",
  "level": "INFO",
  "target": "datafusion_tracing",
  "span": {
    "name": "execute_plan",
    "phase": "physical_plan",
    "operator": "FilterExec",
    "rows_produced": 1523,
    "elapsed_ms": 45.6
  }
}
```

## Slow-query diagnosis recipe

PGWire emits `pgwire.slow_statement` for every successful or failed statement
that takes at least one second. The event includes:

- `query.fingerprint`: SHA-256 of the complete normalized query shape;
- `query.template`: lower-cased SQL with literals and comments replaced by `?`,
  capped at 512 characters for logs. The fingerprint is computed before that
  cap, so long statements do not collapse into the same query identity;
- `query.class`, up to three `query.tables`, optional literal `project.id`,
  `protocol`, `duration_us`, and `success`.

The `postgres.query.*` trace span's `query.text` uses the same template. Raw
SQL must not be sent to normal logs or metrics: it may contain customer data,
credentials, or unbounded bulk values.

### Investigation workflow

1. Aggregate slow-statement events by `query.fingerprint`; rank first by total
   time consumed (`count × average duration`), then by p99 and count. Use
   `query.template` to identify the predicate, join, sort, grouping, and limit
   shape behind each fingerprint.
2. Narrow the candidate with `query.tables`, `project.id`, and `protocol`.
   Treat project IDs as log dimensions, not metric labels, to avoid
   high-cardinality metrics.
3. Compare the time window with `timefusion_stats` PGWire and scan percentiles,
   buffer/WAL state, flush/checkpoint logs, and object-store cache signals.
   A normal scan tail with a slow PGWire tail points to planning, response
   encoding, client backpressure, or maintenance contention; a Delta-only scan
   tail points to Delta metadata, file fan-out, pruning, or object-store work.
4. Inspect the physical-plan trace for the specific fingerprint before changing
   storage layout or cache settings. Apply the narrowest measured fix: improve
   partition predicates, reduce result size, compact affected files, correct
   pruning, or isolate maintenance contention.
5. Verify the same fingerprint's count, p99, and total time after the change.
   Do not use broad production scans as a diagnostic shortcut.

If a normalized template is insufficient, capture raw SQL only through a
separate, access-controlled, sampled diagnostic sink with an explicit retention
policy. Do not enable raw SQL in the standard tracing/log pipeline.

## Performance Impact

DataFusion-tracing is designed to have minimal overhead:
- Instrumentation points are strategically placed
- Metrics collection is lightweight
- Can be completely disabled by setting `RUST_LOG` to exclude datafusion spans

## Future Enhancements

1. **OpenTelemetry Export**: Direct OTLP export to observability backends (Jaeger, Tempo, etc.)
2. **Custom Spans**: Add business-specific tracing around your queries
3. **Metrics Integration**: Combine with Prometheus metrics for comprehensive observability

## Troubleshooting

### No Traces Appearing
- Ensure `RUST_LOG` includes appropriate levels
- Check that telemetry is initialized before creating session contexts

### Performance Degradation
- Reduce sampling ratio: `OTEL_TRACES_SAMPLER_RATIO=0.1`
- Use more selective log levels: `RUST_LOG=info,datafusion::physical_plan=warn`

### Too Many Traces
- Filter by operator: `RUST_LOG=info,datafusion::physical_plan::filter=debug`
- Adjust span verbosity in the InstrumentationOptions