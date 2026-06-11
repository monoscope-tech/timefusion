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

    // Span export honors the standard OTEL_TRACES_EXPORTER=none switch —
    // prod runs with spans off (per-query span volume) but logs/metrics on.
    // Note: In opentelemetry-otlp 0.31, message size limits cannot be directly configured
    // through the public API. The default limit is 4MB for incoming messages.
    // Batch configuration is handled automatically (batch size 512, delay 5s,
    // queue 2048).
    let telemetry_layer = if config.otel_traces_exporter.as_deref() == Some("none") {
        None
    } else {
        let span_exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(otlp_endpoint)
            .with_timeout(Duration::from_secs(10))
            .build()?;
        let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter)
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
    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()?;
    let logger_provider = SdkLoggerProvider::builder().with_batch_exporter(log_exporter).with_resource(resource).build();
    let log_bridge = opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider).with_filter(
        tracing_subscriber::filter::filter_fn(|meta| {
            let t = meta.target();
            !(t.starts_with("opentelemetry") || t.starts_with("tonic") || t.starts_with("h2") || t.starts_with("hyper"))
        }),
    );
    let _ = LOGGER_PROVIDER.set(logger_provider);

    // Get log filter from environment
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Initialize tracing subscriber with telemetry and formatting layers
    let is_json = config.is_json_logging();

    let subscriber = Registry::default().with(env_filter).with(telemetry_layer).with(log_bridge);

    if is_json {
        subscriber
            .with(tracing_subscriber::fmt::layer().json().with_target(true).with_thread_ids(true).with_thread_names(true))
            .try_init()
    } else {
        subscriber
            .with(tracing_subscriber::fmt::layer().with_target(true).with_thread_ids(true).with_thread_names(true))
            .try_init()
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
