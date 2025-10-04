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