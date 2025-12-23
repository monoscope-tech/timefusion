use opentelemetry::{KeyValue, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    propagation::TraceContextPropagator,
    trace::{RandomIdGenerator, Sampler},
};
use std::env;
use std::time::Duration;
use tracing::info;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_telemetry() -> anyhow::Result<()> {
    // Set global propagator for trace context
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    // Get OTLP endpoint from environment or use default
    let otlp_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT").unwrap_or_else(|_| "http://localhost:4317".to_string());

    info!("Initializing OpenTelemetry with OTLP endpoint: {}", otlp_endpoint);

    // Configure service resource
    let service_name = env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "timefusion".to_string());
    let service_version = env::var("OTEL_SERVICE_VERSION").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string());

    let resource = Resource::builder()
        .with_attributes([KeyValue::new("service.name", service_name.clone()), KeyValue::new("service.version", service_version)])
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
    let is_json = env::var("LOG_FORMAT").unwrap_or_default() == "json";

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
