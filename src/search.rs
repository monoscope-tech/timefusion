use anyhow::{Context, Result};
use object_store::{ObjectStore, ObjectStoreBuilder};
use std::path::Path;
use tantivy::{
    collector::TopDocs,
    directory::MmapDirectory,
    doc,
    query::QueryParser,
    schema::{Schema, SchemaBuilder, STORED, TEXT},
    Index, IndexReader, IndexWriter, TantivyDocument,
};
use tantivy_object_store::ObjectStoreDirectory;

pub struct SearchIndex {
    index: Index,
    reader: IndexReader,
    writer: IndexWriter,
    schema: Schema,
}

impl SearchIndex {
    pub async fn new(
        object_store: ObjectStore,
        index_path: &Path,
    ) -> Result<Self> {
        // Define the schema
        let mut schema_builder = SchemaBuilder::new();
        let id_field = schema_builder.add_text_field("id", STORED);
        let title_field = schema_builder.add_text_field("title", TEXT | STORED);
        let content_field = schema_builder.add_text_field("content", TEXT | STORED);
        let schema = schema_builder.build();

        // Create the object store directory
        let directory = ObjectStoreDirectory::new(object_store, index_path.to_string_lossy().to_string())
            .await
            .context("Failed to create object store directory")?;

        // Create or open the index
        let index = Index::open_or_create(directory, schema.clone())
            .context("Failed to open or create index")?;

        // Create reader and writer
        let reader = index
            .reader()
            .context("Failed to create index reader")?;
        let writer = index
            .writer(50_000_000)
            .context("Failed to create index writer")?;

        Ok(Self {
            index,
            reader,
            writer,
            schema,
        })
    }

    pub fn add_document(&mut self, id: &str, title: &str, content: &str) -> Result<()> {
        let id_field = self.schema.get_field("id").unwrap();
        let title_field = self.schema.get_field("title").unwrap();
        let content_field = self.schema.get_field("content").unwrap();

        let doc = doc!(
            id_field => id,
            title_field => title,
            content_field => content
        );

        self.writer.add_document(doc)?;
        self.writer.commit()?;
        Ok(())
    }

    pub fn search(&self, query_str: &str, limit: usize) -> Result<Vec<TantivyDocument>> {
        let searcher = self.reader.searcher();
        let title_field = self.schema.get_field("title").unwrap();
        let content_field = self.schema.get_field("content").unwrap();

        let query_parser = QueryParser::for_index(&self.index, vec![title_field, content_field]);
        let query = query_parser.parse_query(query_str)?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        let mut results = Vec::new();
        for (_, doc_address) in top_docs {
            let doc = searcher.doc(doc_address)?;
            results.push(doc);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_search_index() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let index_path = temp_dir.path().join("test_index");
        
        // Create an in-memory object store for testing
        let object_store = ObjectStoreBuilder::new()
            .with_backend(InMemory::new())
            .build()?;

        let mut search_index = SearchIndex::new(object_store, &index_path).await?;

        // Add some test documents
        search_index.add_document("1", "Rust Programming", "Rust is a systems programming language")?;
        search_index.add_document("2", "Python Basics", "Python is a high-level programming language")?;
        search_index.add_document("3", "Rust vs Python", "Comparison between Rust and Python programming languages")?;

        // Search for documents
        let results = search_index.search("Rust", 10)?;
        assert_eq!(results.len(), 2);

        Ok(())
    }
}