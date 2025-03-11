use sqllogictest::{AsyncDB, Runner, ColumnType};
use timefusion::database::Database;
use std::sync::Arc;
use tokio::runtime::Runtime;
use futures::Future;
use std::pin::Pin;

struct Connection {
    db: Arc<Database>,
}

impl Connection {
    fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

impl AsyncDB for Connection {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type ColumnType = sqllogictest::ColumnType;

    async fn run<'a>(&'a self, sql: &'a str) -> Result<String, Self::Error> {
        let df = self.db.query(sql).await.map_err(|e| Box::new(e) as Self::Error)?;
        let batches = df.collect().await.map_err(|e| Box::new(e) as Self::Error)?;
        let output = batches
            .iter()
            .flat_map(|batch| {
                // Adjust this based on the actual RecordBatch API
                batch.rows().iter().map(|row| {
                    row.iter().map(|value| value.to_string()).collect::<Vec<_>>().join(" ")
                })
            })
            .collect::<Vec<_>>()
            .join("\n");
        Ok(output)
    }

    fn shutdown(&mut self) -> Pin<Box<(dyn Future<Output = ()> + Send + 'async_trait)>> {
        Box::pin(async {})
    }
}

#[test]
fn run_sqllogictest() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let db = Arc::new(Database::new().await.expect("Failed to initialize database"));
        let make_conn = || async { Ok(Connection::new(db.clone())) };
        let mut runner = Runner::new(make_conn);

        let result = runner.run_file("tests/logic.test").await;
        match result {
            Ok(()) => println!("SQL logic tests passed"),
            Err(e) => panic!("SQL logic test failed: {}", e),
        }
    });
}