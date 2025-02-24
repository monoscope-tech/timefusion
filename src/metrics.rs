use lazy_static::lazy_static;
use prometheus::{
    register_counter_vec, register_histogram_vec, register_gauge,
    CounterVec, HistogramVec, Gauge, Encoder, TextEncoder,
};

lazy_static! {
    pub static ref HTTP_REQUEST_COUNTER: CounterVec = register_counter_vec!(
        "http_requests_total",
        "Total number of HTTP requests",
        &["endpoint", "method"]
    ).unwrap();

    pub static ref HTTP_REQUEST_DURATION: HistogramVec = register_histogram_vec!(
        "http_request_duration_seconds",
        "HTTP request latencies in seconds",
        &["endpoint", "method"],
        vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
    ).unwrap();

    pub static ref UPTIME_GAUGE: Gauge = register_gauge!(
        "app_uptime_seconds",
        "Application uptime in seconds"
    ).unwrap();

    pub static ref COMPACTION_COUNTER: Gauge = register_gauge!(
        "compaction_total",
        "Total number of compactions performed"
    ).unwrap();
}

/// Gathers all metrics into a Prometheus text format string.
pub fn gather_metrics() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}
