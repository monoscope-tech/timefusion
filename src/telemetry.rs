use std::{sync::OnceLock, time::Duration};

use opentelemetry::{KeyValue, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    logs::SdkLoggerProvider,
    propagation::TraceContextPropagator,
    trace::{RandomIdGenerator, Sampler},
};
use tracing::info;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{EnvFilter, Layer, Registry, layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::TelemetryConfig;

/// Kept for `shutdown_telemetry` to flush buffered log batches at exit.
static LOGGER_PROVIDER: OnceLock<SdkLoggerProvider> = OnceLock::new();

/// Max spans/logs per OTLP export message. TF's spans/logs embed full query
/// text, so the SDK default (512) overflowed the collector's 4MB gRPC limit
/// (messages up to 39MB → every export failed). 32 keeps a typical message
/// ~2-3MB. See init_telemetry.
const EXPORT_BATCH: usize = 32;

pub fn init_telemetry(config: &TelemetryConfig) -> anyhow::Result<()> {
    // Set global propagator for trace context
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    let otlp_endpoint = &config.otel_exporter_otlp_endpoint;
    info!("Initializing OpenTelemetry with OTLP endpoint: {}", otlp_endpoint);

    // Configure service resource
    let service_name = &config.otel_service_name;
    let service_version = &config.otel_service_version;

    let resource = Resource::builder()
        .with_attributes([KeyValue::new("service.name", service_name.clone()), KeyValue::new("service.version", service_version.clone())])
        .build();

    // Span export honors the standard OTEL_TRACES_EXPORTER=none switch. When on
    // (prod has OTEL_TRACES_EXPORTER=otlp), TF's spans carry full query text +
    // attributes, so the DEFAULT batch of 512 produced export messages up to
    // 39MB — far over the collector's 4MB gRPC receive limit — and every export
    // failed ("resource exhausted"), silently losing TF's self-observability.
    // opentelemetry-otlp 0.31 can't raise the message-size limit via the public
    // API, so we cap the batch instead: EXPORT_BATCH keeps a typical message
    // well under 4MB (≈76KB/span observed → ~2.4MB at 32). A single span larger
    // than 4MB still can't be sent, but those are rare vs the batch-size overflow.
    let telemetry_layer = if config.otel_traces_exporter.as_deref() == Some("none") {
        None
    } else {
        use opentelemetry_sdk::trace::{BatchConfigBuilder, BatchSpanProcessor};
        let span_exporter =
            opentelemetry_otlp::SpanExporter::builder().with_tonic().with_endpoint(otlp_endpoint).with_timeout(Duration::from_secs(10)).build()?;
        let span_processor = BatchSpanProcessor::builder(span_exporter)
            .with_batch_config(BatchConfigBuilder::default().with_max_export_batch_size(EXPORT_BATCH).build())
            .build();
        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_span_processor(span_processor)
            .with_sampler(Sampler::AlwaysOn)
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(resource.clone())
            .build();
        opentelemetry::global::set_tracer_provider(tracer_provider.clone());
        Some(OpenTelemetryLayer::new(tracer_provider.tracer("timefusion")))
    };

    // OTLP logs: bridge tracing events to the collector so TF shows up as a
    // service in monoscope (the 2026-06-11 OOM loop was diagnosed entirely
    // from client-side error strings because TF only logged to stdout).
    // The bridge must not observe the exporter's own tracing output —
    // tonic/hyper events inside an export would recurse into another export.
    let log_exporter = opentelemetry_otlp::LogExporter::builder().with_tonic().with_endpoint(otlp_endpoint).with_timeout(Duration::from_secs(10)).build()?;
    // Slow-statement logs also carry full SQL text, so cap the log batch too
    // (same 4MB-limit reasoning as spans above).
    let log_processor = opentelemetry_sdk::logs::BatchLogProcessor::builder(log_exporter)
        .with_batch_config(opentelemetry_sdk::logs::BatchConfigBuilder::default().with_max_export_batch_size(EXPORT_BATCH).build())
        .build();
    let logger_provider = SdkLoggerProvider::builder().with_log_processor(log_processor).with_resource(resource).build();
    let log_bridge =
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider).with_filter(tracing_subscriber::filter::filter_fn(|meta| {
            let t = meta.target();
            !(t.starts_with("opentelemetry") || t.starts_with("tonic") || t.starts_with("h2") || t.starts_with("hyper"))
        }));
    let _ = LOGGER_PROVIDER.set(logger_provider);

    // Get log filter from environment
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Initialize tracing subscriber with telemetry and formatting layers
    let is_json = config.is_json_logging();

    let subscriber = Registry::default().with(env_filter).with(telemetry_layer).with(log_bridge);

    if is_json {
        subscriber.with(tracing_subscriber::fmt::layer().json().with_target(true).with_thread_ids(true).with_thread_names(true)).try_init()
    } else {
        subscriber.with(tracing_subscriber::fmt::layer().with_target(true).with_thread_ids(true).with_thread_names(true)).try_init()
    }
    .map_err(|e| anyhow::anyhow!("Failed to set tracing subscriber: {}", e))?;

    info!("OpenTelemetry initialized successfully with service name: {}", service_name);

    Ok(())
}

pub fn shutdown_telemetry() {
    info!("Shutting down OpenTelemetry");
    // Tracer/meter providers shut down when dropped; flush buffered logs
    // explicitly so the final shutdown lines reach the collector.
    if let Some(p) = LOGGER_PROVIDER.get() {
        let _ = p.shutdown();
    }
}

/// Cell-capped preview formatter for datafusion-tracing spans, replacing the
/// crate's `default_preview_fn` (comfy_table over WHOLE cell values). Cells
/// here are unbounded — Variant/JSON bodies on SELECTs, and on an
/// INSERT…unnest input node ONE cell holds an entire bind array — so the
/// default burned 86–93% CPU and drove the 85GiB OOM loop of 2026-07-06.
/// The capped writer aborts each cell's `Display` after `PREVIEW_CELL_CAP`
/// bytes, so oversized values are never materialized, only their prefix.
pub fn capped_preview_fn(batch: &arrow::record_batch::RecordBatch) -> Result<String, arrow::error::ArrowError> {
    use std::fmt::Write;

    use arrow::util::display::{ArrayFormatter, FormatOptions};

    const PREVIEW_CELL_CAP: usize = 256;

    /// `fmt::Write` that stops accepting bytes after `left` is exhausted; the
    /// resulting `fmt::Error` aborts the value's `Display` mid-render.
    struct Capped<'a> {
        buf: &'a mut String,
        left: usize,
    }
    impl std::fmt::Write for Capped<'_> {
        fn write_str(&mut self, s: &str) -> std::fmt::Result {
            let take = (0..=s.len().min(self.left)).rev().find(|&i| s.is_char_boundary(i)).unwrap_or(0);
            self.buf.push_str(&s[..take]);
            self.left -= take;
            if take < s.len() { Err(std::fmt::Error) } else { Ok(()) }
        }
    }

    let opts = FormatOptions::default();
    let formatters = batch.columns().iter().map(|c| ArrayFormatter::try_new(c.as_ref(), &opts)).collect::<Result<Vec<_>, _>>()?;
    let mut out = String::new();
    for row in 0..batch.num_rows() {
        for (formatter, field) in formatters.iter().zip(batch.schema().fields()) {
            let _ = write!(out, "{}=", field.name());
            let mut w = Capped { buf: &mut out, left: PREVIEW_CELL_CAP };
            if write!(w, "{}", formatter.value(row)).is_err() {
                out.push('…');
            }
            out.push_str("  ");
        }
        out.push('\n');
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ListBuilder, StringArray, StringBuilder},
        record_batch::RecordBatch,
    };

    use super::*;

    /// Regression guard for the 2026-07-06 OOM: a cell holding a huge value
    /// (like an INSERT…unnest bind array) must preview as a bounded prefix,
    /// not render in full.
    #[test]
    fn capped_preview_bounds_giant_cells() {
        let mut list = ListBuilder::new(StringBuilder::new());
        for _ in 0..10_000 {
            list.values().append_value("x".repeat(100));
        }
        list.append(true);
        let names = StringArray::from(vec!["row1"]);
        let batch = RecordBatch::try_from_iter([
            ("name", Arc::new(names) as arrow::array::ArrayRef),
            ("bind_array", Arc::new(list.finish()) as arrow::array::ArrayRef),
        ])
        .unwrap();

        let out = capped_preview_fn(&batch).unwrap();
        assert!(out.len() < 1024, "1MB cell must not render in full, got {} bytes", out.len());
        assert!(out.contains('…'), "oversized cell must be marked truncated");
        assert!(out.contains("name=row1"), "small cells render whole");
    }
}
