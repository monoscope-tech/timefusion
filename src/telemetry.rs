use crate::config::TelemetryConfig;
use opentelemetry::{KeyValue, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    propagation::TraceContextPropagator,
    trace::{RandomIdGenerator, Sampler},
};
use std::time::Duration;
use tracing::info;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

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

    // Create OTLP span exporter
    // Note: In opentelemetry-otlp 0.31, message size limits cannot be directly configured
    // through the public API. The default limit is 4MB for incoming messages.
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()?;

    // Build the tracer provider
    // Note: In opentelemetry-sdk 0.31, batch configuration is handled automatically
    // by the batch exporter. The default settings include:
    // - Max export batch size: 512
    // - Scheduled delay: 5 seconds
    // - Max queue size: 2048
    // These defaults work well for most use cases and help prevent hitting the 4MB limit
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_sampler(Sampler::AlwaysOn)
        .with_id_generator(RandomIdGenerator::default())
        .with_resource(resource)
        .build();

    // Set global tracer provider
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    // Create tracer
    let tracer = tracer_provider.tracer("timefusion");

    // Create telemetry layer
    let telemetry_layer = OpenTelemetryLayer::new(tracer);

    // Get log filter from environment
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Initialize tracing subscriber with telemetry and formatting layers
    let is_json = config.is_json_logging();

    let subscriber = Registry::default().with(env_filter).with(telemetry_layer);

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
    // Note: In OpenTelemetry 0.31, there's no global shutdown function
    // The tracer provider will be shut down when dropped
}
