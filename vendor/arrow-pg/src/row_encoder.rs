use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::array::RecordBatch;
#[cfg(feature = "datafusion")]
use datafusion::arrow::array::RecordBatch;

use pgwire::{
    api::results::{DataRowEncoder, FieldInfo},
    error::PgWireResult,
    messages::data::DataRow,
};

use crate::encoder::encode_value;

pub struct RowEncoder {
    rb: RecordBatch,
    curr_idx: usize,
    fields: Arc<Vec<FieldInfo>>,
    row_encoder: DataRowEncoder,
}

impl RowEncoder {
    pub fn new(rb: RecordBatch, fields: Arc<Vec<FieldInfo>>) -> Self {
        assert_eq!(rb.num_columns(), fields.len());
        Self {
            rb,
            fields: fields.clone(),
            curr_idx: 0,
            row_encoder: DataRowEncoder::new(fields),
        }
    }

    pub fn next_row(&mut self) -> Option<PgWireResult<DataRow>> {
        if self.curr_idx == self.rb.num_rows() {
            return None;
        }

        let arrow_schema = self.rb.schema_ref();
        for col in 0..self.rb.num_columns() {
            let array = self.rb.column(col);
            let arrow_field = arrow_schema.field(col);
            let pg_field = &self.fields[col];

            if let Err(e) = encode_value(
                &mut self.row_encoder,
                array,
                self.curr_idx,
                arrow_field,
                pg_field,
            ) {
                return Some(Err(e));
            };
        }
        self.curr_idx += 1;
        Some(Ok(self.row_encoder.take_row()))
    }
}
