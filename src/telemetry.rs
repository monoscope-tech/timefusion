// src/telemetry.rs

use opentelemetry_sdk::{
    trace::SdkTracerProvider,
    Resource,
};
use opentelemetry::trace::TracerProvider; // for `.tracer()`
use opentelemetry_otlp::{Protocol, WithExportConfig};
use std::{env, time::Duration};
use opentelemetry::KeyValue;
use tracing_subscriber::{fmt, layer::SubscriberExt, Registry};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};

pub fn init_telemetry() {
    // Read configuration from environment variables.
    let otlp_trace_endpoint = env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:4317".into());
    let otlp_metrics_endpoint = env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
        .unwrap_or_else(|_| otlp_trace_endpoint.clone());
    let service_name = env::var("OTEL_SERVICE_NAME")
        .unwrap_or_else(|_| "timefusion".into());
    let resource_attrs = env::var("OTEL_RESOURCE_ATTRIBUTES").unwrap_or_default();

    // Build resource using the public builder API.
    let resource = Resource::builder()
        .with_attributes(vec![
            KeyValue::new("service.name", service_name),
            KeyValue::new("at-project-key", resource_attrs),
        ])
        .build();

    // --- Setup OTLP Tracing ---
    let trace_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_trace_endpoint)
        .with_protocol(Protocol::Grpc)
        .build()
        .expect("Failed to create OTLP trace exporter");

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(trace_exporter)
        .with_resource(resource.clone())
        .build();

    let sdk_tracer = tracer_provider.tracer("timefusion_tracer");

    opentelemetry::global::set_tracer_provider(tracer_provider);

    let otel_layer = tracing_opentelemetry::layer().with_tracer(sdk_tracer);
    let fmt_layer = fmt::layer();
    let subscriber = Registry::default()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(fmt_layer)
        .with(otel_layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set global tracing subscriber");

    // --- Setup OTLP Metrics ---
    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(otlp_metrics_endpoint)
        .with_protocol(Protocol::Grpc)
        .build()
        .expect("Failed to create OTLP metric exporter");

    let reader = PeriodicReader::builder(metric_exporter)
        .with_interval(Duration::from_secs(60))
        .build();

    let meter_provider = SdkMeterProvider::builder()
        .with_reader(reader)
        .with_resource(resource)
        .build();

    opentelemetry::global::set_meter_provider(meter_provider);
}
