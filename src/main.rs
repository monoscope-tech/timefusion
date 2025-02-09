use arrow::array::record_batch;
use color_eyre::eyre::Result;
use iceberg::{
    io::FileIO, spec::{NestedField, PrimitiveType, Schema, Type}, transaction::Transaction, Catalog, NamespaceIdent, TableIdent
};
use iceberg_catalog_memory::MemoryCatalog;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    println!("TEST");

    let catalog_storage =
        FileIO::from_path("file:///Users/tonyalaribe/Projects/apitoolkit/timefusion")?.build()?;
    let catalog = MemoryCatalog::new(catalog_storage, None);

    let namespace_id = NamespaceIdent::new("apitoolkit".to_string());
    // Create namespace
    let ns = catalog
        .create_namespace(
            &namespace_id,
            HashMap::from([("key1".to_string(), "value1".to_string())]),
        )
        .await
        .unwrap();

    let table_id = TableIdent::from_strs(["apitoolkit", "events"]).unwrap();
    let table_schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "project_id", Type::Primitive(PrimitiveType::String)).into(),
            // NestedField::required(2, "timestamp", Type::Time).into(),
            // NestedField::required(2, "start_time", Type::Time).into(),
            // NestedField::required(2, "end_time", Type::Time).into(),
            NestedField::required(2, "event_type", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "payload", Type::Primitive(PrimitiveType::Boolean)).into(),
        ])
        .with_schema_id(1)
        .with_identifier_field_ids(vec![2])
        .build()
        .unwrap();

    let table_creation = iceberg::TableCreation::builder()
        .name(table_id.name.clone())
        .schema(table_schema.clone())
        .properties(HashMap::from([("owner".to_string(), "testx".to_string())]))
        .build();

    let table = match catalog.load_table(&table_id).await {
        Ok(table) => table,
        Err(_err) => {
            println!(
                "Table not found. Creating table with identifier: {:?}",
                table_id
            );
            let table = catalog.create_table(&namespace_id, table_creation).await?;
            println!("Created table: {:?}", table);
            table
        }
    };

    let batch = record_batch!(
        ("project_id", Utf8, ["project1", "project2"]),
        ("event_type", Utf8, ["start", "stop"]),
        ("payload", Utf8, ["alpha", "beta"])
    );


    // Write the RecordBatch to the Iceberg table
    let mut transaction = Transaction::new(&table); // Start a transaction
    let append_action = transaction.fast_append(None, vec![])?;
    append_action.add_data_files(batch)?;

    // transaction.append_file(batch).await?; // Append the data file

    transaction.commit(&catalog).await?; // Commit the transaction

    println!("Namespace created: {:?}", ns);
    Ok(())
}

