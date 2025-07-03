use actix_web::HttpResponse;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize)]
pub struct ValidationError {
    pub field: String,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct ValidationErrors {
    pub errors: Vec<ValidationError>,
}

impl ValidationErrors {
    pub fn new() -> Self {
        Self { errors: Vec::new() }
    }

    pub fn add(&mut self, field: &str, message: &str) {
        self.errors.push(ValidationError {
            field: field.to_string(),
            message: message.to_string(),
        });
    }

    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }
}

pub trait Validate {
    fn validate(&self) -> ValidationErrors;
}

/// Validation for project registration requests
#[derive(Deserialize)]
pub struct RegisterProjectRequest {
    pub project_id: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub endpoint: Option<String>,
}

impl Validate for RegisterProjectRequest {
    fn validate(&self) -> ValidationErrors {
        let mut errors = ValidationErrors::new();

        // Validate project_id
        if self.project_id.is_empty() {
            errors.add("project_id", "Project ID cannot be empty");
        } else if self.project_id.len() > 128 {
            errors.add("project_id", "Project ID cannot be longer than 128 characters");
        } else if !self.project_id.chars().all(|c| c.is_alphanumeric() || c == '-' || c == '_') {
            errors.add("project_id", "Project ID can only contain alphanumeric characters, hyphens, and underscores");
        }

        // Validate bucket name (AWS S3 bucket naming rules)
        if self.bucket.is_empty() {
            errors.add("bucket", "Bucket name cannot be empty");
        } else if self.bucket.len() < 3 || self.bucket.len() > 63 {
            errors.add("bucket", "Bucket name must be between 3 and 63 characters");
        } else if !self.bucket.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.') {
            errors.add("bucket", "Bucket name can only contain lowercase letters, numbers, hyphens, and periods");
        } else if self.bucket.starts_with('-') || self.bucket.ends_with('-') {
            errors.add("bucket", "Bucket name cannot start or end with a hyphen");
        }

        // Validate access_key
        if self.access_key.is_empty() {
            errors.add("access_key", "Access key cannot be empty");
        } else if self.access_key.len() < 16 || self.access_key.len() > 128 {
            errors.add("access_key", "Access key must be between 16 and 128 characters");
        }

        // Validate secret_key
        if self.secret_key.is_empty() {
            errors.add("secret_key", "Secret key cannot be empty");
        } else if self.secret_key.len() < 16 || self.secret_key.len() > 128 {
            errors.add("secret_key", "Secret key must be between 16 and 128 characters");
        }

        // Validate endpoint if provided
        if let Some(endpoint) = &self.endpoint {
            if !endpoint.is_empty() {
                if let Err(_) = url::Url::parse(endpoint) {
                    errors.add("endpoint", "Endpoint must be a valid URL");
                } else if !endpoint.starts_with("https://") && !endpoint.starts_with("http://") {
                    errors.add("endpoint", "Endpoint must use HTTP or HTTPS protocol");
                }
            }
        }

        errors
    }
}

/// Validation for SQL queries
pub struct SqlQueryValidator;

impl SqlQueryValidator {
    pub fn validate_query(query: &str) -> ValidationErrors {
        let mut errors = ValidationErrors::new();

        if query.is_empty() {
            errors.add("query", "Query cannot be empty");
            return errors;
        }

        if query.len() > 100_000 {
            errors.add("query", "Query cannot exceed 100,000 characters");
        }

        // Basic SQL injection protection - check for dangerous patterns
        let dangerous_patterns = [
            "xp_cmdshell",
            "sp_oacreate", 
            "exec(",
            "execute(",
            "--",
            "/*",
            "*/",
            "union.*select",
            "drop.*table",
            "drop.*database",
            "truncate.*table",
            "delete.*from.*where.*1.*=.*1",
        ];

        let query_lower = query.to_lowercase();
        for pattern in &dangerous_patterns {
            if query_lower.contains(pattern) {
                errors.add("query", &format!("Query contains potentially dangerous pattern: {}", pattern));
            }
        }

        // Check for balanced parentheses
        let mut paren_count = 0;
        for ch in query.chars() {
            match ch {
                '(' => paren_count += 1,
                ')' => paren_count -= 1,
                _ => {}
            }
            if paren_count < 0 {
                errors.add("query", "Unbalanced parentheses in query");
                break;
            }
        }
        if paren_count != 0 {
            errors.add("query", "Unbalanced parentheses in query");
        }

        errors
    }
}

/// Create a validation error response
pub fn validation_error_response(errors: ValidationErrors) -> HttpResponse {
    HttpResponse::BadRequest().json(serde_json::json!({
        "error": "Validation failed",
        "details": errors
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_register_project_request() {
        let req = RegisterProjectRequest {
            project_id: "test-project-123".to_string(),
            bucket: "my-test-bucket".to_string(),
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            endpoint: Some("https://s3.amazonaws.com".to_string()),
        };

        let errors = req.validate();
        assert!(errors.is_empty(), "Valid request should have no errors");
    }

    #[test]
    fn test_invalid_project_id() {
        let req = RegisterProjectRequest {
            project_id: "".to_string(),
            bucket: "my-test-bucket".to_string(),
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            endpoint: None,
        };

        let errors = req.validate();
        assert!(!errors.is_empty());
        assert!(errors.errors.iter().any(|e| e.field == "project_id"));
    }

    #[test]
    fn test_sql_query_validation() {
        // Valid query
        let errors = SqlQueryValidator::validate_query("SELECT * FROM otel_logs_and_spans WHERE project_id = 'test'");
        assert!(errors.is_empty());

        // Empty query
        let errors = SqlQueryValidator::validate_query("");
        assert!(!errors.is_empty());

        // Dangerous pattern
        let errors = SqlQueryValidator::validate_query("SELECT * FROM users; DROP TABLE users; --");
        assert!(!errors.is_empty());
    }
}
