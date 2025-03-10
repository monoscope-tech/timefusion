// tests/sqllogictest.rs

use sqllogictest::Runner;
use timefusion::database::Database;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[test]
fn run_sqllogictest() {
    // Create a Tokio runtime.
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let db = Arc::new(Database::new().await.expect("Failed to initialize database"));
        
        // Define a closure to run SQL statements.
        let run_sql = |sql: &str| async {
            match db.query(sql).await {
                Ok(df) => {
                    let batches = df.collect().await.map_err(|e| e.to_string())?;
                    // TODO: Implement a proper formatter matching sqllogictest expectations.
                    let output = format!("{:?}", batches);
                    Ok(output)
                }
                Err(e) => Err(e.to_string()),
            }
        };

        let mut runner = Runner::new("tests/logic.test");
        let result = runner.run(|sql| rt.block_on(run_sql(sql))).await;
        if !result.passed() {
            panic!("sqllogictest failures:\n{}", result.failure_summary());
        }
    });
}
