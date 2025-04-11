use std::io;

use actix_web::error::Error as ActixError;
use datafusion::error::DataFusionError;
use deltalake::DeltaTableError;
use regex::Regex;
use thiserror::Error;

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
    // #[error("Validation error: {0}")]
    // Validation(String),
}

/// Extracts key details (such as error code and message) from an XML error string.
/// Returns a simplified error message.
fn extract_error_details(error: &str) -> String {
    let code_re = Regex::new(r"<Code>(.*?)</Code>").unwrap();
    let message_re = Regex::new(r"<Message>(.*?)</Message>").unwrap();

    let code = code_re.captures(error).and_then(|caps| caps.get(1)).map(|m| m.as_str()).unwrap_or("UnknownErrorCode");

    let message = message_re.captures(error).and_then(|caps| caps.get(1)).map(|m| m.as_str()).unwrap_or("Unknown error message");

    format!("Error [{}]: {}", code, message)
}

impl actix_web::ResponseError for TimeFusionError {
    fn error_response(&self) -> actix_web::HttpResponse {
        // For database errors, try to extract relevant XML details.
        let error_message = match self {
            TimeFusionError::Database(err) => {
                let err_str = err.to_string();
                if err_str.contains("<Error>") { extract_error_details(&err_str) } else { err_str }
            }
            _ => self.to_string(),
        };

        actix_web::HttpResponse::InternalServerError().json(serde_json::json!({
            "error": error_message,
        }))
    }
}

pub type Result<T> = std::result::Result<T, TimeFusionError>;
