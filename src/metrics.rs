// src/metrics.rs
use prometheus::{Counter, CounterVec, Gauge, Registry};
use lazy_static::lazy_static;
use prometheus::{register_counter_vec, register_counter, register_gauge};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
    pub static ref HTTP_REQUEST_COUNTER: CounterVec = register_counter_vec!(
        "http_requests_total",
        "Total number of HTTP requests",
        &["method", "path", "status"]
    ).unwrap();
    pub static ref UPTIME_GAUGE: Gauge = register_gauge!(
        "uptime_seconds",
        "Application uptime in seconds"
    ).unwrap();
    pub static ref COMPACTION_COUNTER: Counter = register_counter!(
        "compactions_total",
        "Total number of compactions performed"
    ).unwrap();
    pub static ref INGESTION_COUNTER: Counter = register_counter!(
        "ingestion_total",
        "Total number of successful ingestions"
    ).unwrap();
    pub static ref ERROR_COUNTER: Counter = register_counter!(
        "error_total",
        "Total number of ingestion errors"
    ).unwrap();
    pub static ref HTTP_REQUEST_DURATION: prometheus::Histogram = prometheus::Histogram::with_opts(
        prometheus::HistogramOpts::new(
            "http_request_duration_seconds",
            "Duration of HTTP requests in seconds"
        ).buckets(vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
    ).unwrap();
}

pub fn gather_metrics() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}