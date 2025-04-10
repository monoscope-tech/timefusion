// src/config.rs
use std::env;

use crate::error::{Result, TimeFusionError};

#[derive(Clone)]
pub struct Config {
    pub pg_port:               u16,
    pub http_port:             String,
    pub s3_bucket:             String,
    pub s3_endpoint:           String,
    pub table_prefix:          String,
    pub aws_access_key_id:     String,
    pub aws_secret_access_key: String,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let pg_port = env::var("PGWIRE_PORT")
            .unwrap_or_else(|_| "5432".to_string())
            .parse::<u16>()
            .map_err(|e| TimeFusionError::Config(format!("Invalid PGWIRE_PORT: {}", e)))?;

        let http_port = env::var("PORT").unwrap_or_else(|_| "80".to_string());
        if http_port.parse::<u16>().is_err() {
            return Err(TimeFusionError::Config(format!("Invalid PORT: {} must be a valid u16", http_port)));
        }

        let s3_bucket = env::var("AWS_S3_BUCKET").map_err(|_| TimeFusionError::Config("AWS_S3_BUCKET environment variable not set".to_string()))?;

        let s3_endpoint = env::var("AWS_S3_ENDPOINT").unwrap_or_else(|_| "https://s3.amazonaws.com".to_string());
        if s3_endpoint.is_empty() || (!s3_endpoint.starts_with("http://") && !s3_endpoint.starts_with("https://")) {
            return Err(TimeFusionError::Config(format!("Invalid AWS_S3_ENDPOINT: {} must be a valid URL", s3_endpoint)));
        }

        let table_prefix = env::var("TIMEFUSION_TABLE_PREFIX").unwrap_or_else(|_| "timefusion".to_string());
        if table_prefix.is_empty() {
            return Err(TimeFusionError::Config("TIMEFUSION_TABLE_PREFIX cannot be empty".to_string()));
        }

        // Load AWS credentials, required
        let aws_access_key_id =
            env::var("AWS_ACCESS_KEY_ID").map_err(|_| TimeFusionError::Config("AWS_ACCESS_KEY_ID environment variable not set".to_string()))?;
        let aws_secret_access_key =
            env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| TimeFusionError::Config("AWS_SECRET_ACCESS_KEY environment variable not set".to_string()))?;

        Ok(Config {
            pg_port,
            http_port,
            s3_bucket,
            s3_endpoint,
            table_prefix,
            aws_access_key_id,
            aws_secret_access_key,
        })
    }
}
