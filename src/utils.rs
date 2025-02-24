use anyhow::{Context, Result};
use regex::Regex;
use chrono::{Utc, LocalResult, TimeZone};
use datafusion::arrow::array::{Array, StringArray, TimestampMicrosecondArray};
use std::collections::HashMap;

pub fn parse_insert_query(query: &str) -> Result<HashMap<String, String>> {
    let re = Regex::new(r#"(?i)insert\s+into\s+"table"\s*\(([^)]+)\)\s+values\s*\(([^)]+)\)"#)
        .context("Error preparing SQL: Failed to compile regex for INSERT query")?;
    if let Some(caps) = re.captures(query) {
        let columns_str = caps.get(1).unwrap().as_str();
        let values_str = caps.get(2).unwrap().as_str();
        let columns: Vec<&str> = columns_str.split(',')
            .map(|s| s.trim().trim_matches('"'))
            .collect();
        let values: Vec<&str> = values_str.split(',')
            .map(|s| s.trim().trim_matches('\''))
            .collect();
        if columns.len() != values.len() {
            return Err(anyhow::anyhow!("Column count does not match value count"));
        }
        let mut map = HashMap::new();
        for (col, val) in columns.into_iter().zip(values.into_iter()) {
            map.insert(col.to_string(), val.to_string());
        }
        Ok(map)
    } else {
        Err(anyhow::anyhow!("Could not parse INSERT query"))
    }
}

pub fn prepare_sql(query: &str) -> Result<String> {
    let query_lower = query.trim().to_lowercase();
    if query_lower.starts_with("insert") {
        let re = Regex::new(r#"(?i)insert\s+into\s+"table"\s*\(([^)]+)\)\s+values\s*\(([^)]+)\)"#)
            .context("Error preparing SQL: Failed to compile regex for INSERT query")?;
        if let Some(caps) = re.captures(query) {
            let columns_str = caps.get(1).unwrap().as_str();
            let columns: Vec<&str> = columns_str.split(',')
                .map(|s| s.trim().trim_matches('"'))
                .collect();
            if let Some(idx) = columns.iter().position(|&col| col.eq_ignore_ascii_case("project_id")) {
                let values_str = caps.get(2).unwrap().as_str();
                let values: Vec<&str> = values_str.split(',')
                    .map(|s| s.trim())
                    .collect();
                if let Some(project_value) = values.get(idx) {
                    let project_id = project_value.trim_matches('\'');
                    let unique_table_name = format!("table_{}", project_id);
                    return Ok(query.replace("\"table\"", &format!("\"{}\"", unique_table_name)));
                }
            }
        }
        Err(anyhow::anyhow!("Could not extract project_id from INSERT query"))
    } else if query_lower.starts_with("select") {
        if let Ok(project_id) = extract_project_id_from_sql(query) {
            let unique_table_name = format!("table_{}", project_id);
            Ok(query.replace("\"table\"", &format!("\"{}\"", unique_table_name)))
        } else {
            Err(anyhow::anyhow!("Project ID not found in SQL"))
        }
    } else {
        Ok(query.to_string())
    }
}

pub fn extract_project_id_from_sql(sql: &str) -> Result<String> {
    sql.to_lowercase()
        .find("where project_id = '")
        .map(|start| {
            let idx = start + "where project_id = '".len();
            let end = sql[idx..].find('\'').unwrap();
            sql[idx..idx + end].to_string()
        })
        .ok_or_else(|| anyhow::anyhow!("Project ID not found in SQL"))
}

pub fn value_to_string(array: &dyn Array, index: usize) -> String {
    if array.is_null(index) {
        return "NULL".to_string();
    }
    if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
        return string_array.value(index).to_string();
    }
    if let Some(ts_array) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
        let micros = ts_array.value(index);
        match Utc.timestamp_micros(micros) {
            LocalResult::Single(dt) => dt.to_rfc3339(),
            _ => "Invalid timestamp".to_string(),
        }
    } else {
        format!("{:?}", array.to_data().buffers()[0].as_slice())
    }
}