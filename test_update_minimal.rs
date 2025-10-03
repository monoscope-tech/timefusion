use datafusion::prelude::*;
use tokio;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    // Create a simple context
    let ctx = SessionContext::new();
    
    // Create a simple table
    ctx.sql("CREATE TABLE test (id INT, name VARCHAR)")
        .await?
        .collect()
        .await?;
    
    // Insert some data
    ctx.sql("INSERT INTO test VALUES (1, 'test')")
        .await?
        .collect()
        .await?;
    
    // Try UPDATE - this should show the error
    match ctx.sql("UPDATE test SET name = 'updated' WHERE id = 1").await {
        Ok(_) => println!("UPDATE succeeded"),
        Err(e) => println!("UPDATE failed with error: {:?}", e),
    }
    
    Ok(())
}