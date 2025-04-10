// error.rs
use thiserror::Error;
use datafusion::error::DataFusionError;
use actix_web::error::Error as ActixError;
use deltalake::DeltaTableError;
use std::io;

#[derive(Error, Debug)]
pub enum TimeFusionError {
    #[error("Database error: {0}")]
    Database(#[from] DeltaTableError),

    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),

    #[error("HTTP error: {0}")]
    Http(#[from] ActixError),

    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Generic error: {0}")]
    Generic(#[from] anyhow::Error),
}

impl actix_web::ResponseError for TimeFusionError {
    fn error_response(&self) -> actix_web::HttpResponse {
        match self {
            TimeFusionError::Http(err) => err.error_response(),
            _ => actix_web::HttpResponse::InternalServerError().json(serde_json::json!({
                "error": self.to_string()
            })),
        }
    }
}

pub type Result<T> = std::result::Result<T, TimeFusionError>;