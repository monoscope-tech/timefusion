use opentelemetry::{trace::TracerProvider, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    propagation::TraceContextPropagator,
    trace::{RandomIdGenerator, Sampler},
    Resource,
};
use std::env;
use std::time::Duration;
use tracing::info;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry};

pub fn init_telemetry() -> anyhow::Result<()> {
    // Set global propagator for trace context
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    // Get OTLP endpoint from environment or use default
    let otlp_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".to_string());

    info!("Initializing OpenTelemetry with OTLP endpoint: {}", otlp_endpoint);

    // Configure service resource
    let service_name = env::var("OTEL_SERVICE_NAME").unwrap_or_else(|_| "timefusion".to_string());
    let service_version = env::var("OTEL_SERVICE_VERSION").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string());
    
    let resource = Resource::builder()
        .with_attributes([
            KeyValue::new("service.name", service_name.clone()),
            KeyValue::new("service.version", service_version),
        ])
        .build();

    // Create OTLP span exporter
    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_endpoint)
        .with_timeout(Duration::from_secs(10))
        .build()?;

    // Build the tracer provider
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
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // Initialize tracing subscriber with telemetry and formatting layers
    let subscriber = Registry::default()
        .with(env_filter)
        .with(telemetry_layer)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_target(true)
                .with_thread_ids(true)
                .with_thread_names(true)
        );

    subscriber.try_init().map_err(|e| anyhow::anyhow!("Failed to set tracing subscriber: {}", e))?;

    info!("OpenTelemetry initialized successfully with service name: {}", service_name);

    Ok(())
}

pub fn shutdown_telemetry() {
    info!("Shutting down OpenTelemetry");
    // Note: In OpenTelemetry 0.31, there's no global shutdown function
    // The tracer provider will be shut down when dropped
}