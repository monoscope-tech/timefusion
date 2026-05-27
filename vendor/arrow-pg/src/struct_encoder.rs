use std::error::Error;
use std::io::Write;
use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::array::{Array, StructArray};
use arrow_schema::Fields;
#[cfg(feature = "datafusion")]
use datafusion::arrow::array::{Array, StructArray};

use bytes::{BufMut, BytesMut};
use pgwire::api::results::{FieldFormat, FieldInfo};
use pgwire::error::PgWireResult;
use pgwire::types::format::FormatOptions;
use pgwire::types::{ToSqlText, QUOTE_CHECK, QUOTE_ESCAPE};
use postgres_types::{Field, IsNull, ToSql, Type};

use crate::datatypes::field_into_pg_type;
use crate::encoder::{encode_value, Encoder};

#[derive(Debug)]
struct BytesWrapper(BytesMut, bool);

impl ToSql for BytesWrapper {
    fn to_sql(&self, _ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        out.writer().write_all(&self.0)?;
        Ok(IsNull::No)
    }

    fn accepts(_ty: &Type) -> bool
    where
        Self: Sized,
    {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        self.to_sql(ty, out)
    }
}

impl ToSqlText for BytesWrapper {
    fn to_sql_text(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
        _format_options: &FormatOptions,
    ) -> Result<IsNull, Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
    {
        if self.1 {
            out.put_u8(b'"');
            out.put_slice(
                QUOTE_ESCAPE
                    .replace_all(&String::from_utf8_lossy(&self.0), r#"\$1"#)
                    .as_bytes(),
            );
            out.put_u8(b'"');
        } else {
            out.put_slice(&self.0);
        }
        Ok(IsNull::No)
    }
}

pub(crate) fn encode_structs<T: Encoder>(
    encoder: &mut T,
    arr: &Arc<dyn Array>,
    arrow_fields: &Fields,
    parent_pg_field_info: &FieldInfo,
) -> PgWireResult<()> {
    let arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    let quote_wrapper = matches!(parent_pg_field_info.format(), FieldFormat::Text);

    let fields = arrow_fields
        .iter()
        .map(|f| field_into_pg_type(f).map(|t| Field::new(f.name().to_owned(), t)))
        .collect::<PgWireResult<Vec<_>>>()?;

    let values: PgWireResult<Vec<_>> = (0..arr.len())
        .map(|row| {
            if arr.is_null(row) {
                Ok(None)
            } else {
                let mut row_encoder = StructEncoder::new(arrow_fields.len());

                for (i, arr) in arr.columns().iter().enumerate() {
                    let field = &fields[i];
                    let type_ = field.type_();
                    let arrow_field = &arrow_fields[i];

                    let format = parent_pg_field_info.format();
                    let format_options = parent_pg_field_info.format_options().clone();
                    let mut pg_field =
                        FieldInfo::new(field.name().to_string(), None, None, type_.clone(), format);
                    pg_field = pg_field.with_format_options(format_options);

                    encode_value(&mut row_encoder, arr, row, arrow_field, &pg_field).unwrap();
                }

                Ok(Some(BytesWrapper(row_encoder.take_buffer(), quote_wrapper)))
            }
        })
        .collect();
    encoder.encode_field(&values?, parent_pg_field_info)
}

pub(crate) fn encode_struct<T: Encoder>(
    encoder: &mut T,
    arr: &Arc<dyn Array>,
    idx: usize,
    arrow_fields: &Fields,
    parent_pg_field_info: &FieldInfo,
) -> PgWireResult<()> {
    let arr = arr.as_any().downcast_ref::<StructArray>().unwrap();
    if arr.is_null(idx) {
        return encoder.encode_field(&None::<&[i8]>, parent_pg_field_info);
    }

    let fields = arrow_fields
        .iter()
        .map(|f| field_into_pg_type(f).map(|t| Field::new(f.name().to_owned(), t)))
        .collect::<PgWireResult<Vec<_>>>()?;

    let mut row_encoder = StructEncoder::new(arrow_fields.len());

    for (i, arr) in arr.columns().iter().enumerate() {
        let field = &fields[i];
        let type_ = field.type_();

        let arrow_field = &arrow_fields[i];

        let mut pg_field = FieldInfo::new(
            field.name().to_string(),
            None,
            None,
            type_.clone(),
            parent_pg_field_info.format(),
        );
        pg_field = pg_field.with_format_options(parent_pg_field_info.format_options().clone());

        encode_value(&mut row_encoder, arr, idx, arrow_field, &pg_field).unwrap();
    }
    let encoded_value = BytesWrapper(row_encoder.row_buffer, false);
    encoder.encode_field(&encoded_value, parent_pg_field_info)
}

pub(crate) struct StructEncoder {
    num_cols: usize,
    curr_col: usize,
    row_buffer: BytesMut,
}

impl StructEncoder {
    pub(crate) fn new(num_cols: usize) -> Self {
        Self {
            num_cols,
            curr_col: 0,
            row_buffer: BytesMut::new(),
        }
    }

    pub(crate) fn take_buffer(self) -> BytesMut {
        self.row_buffer
    }
}

impl Encoder for StructEncoder {
    type Item = BytesMut;

    fn encode_field<T>(&mut self, value: &T, pg_field: &FieldInfo) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized,
    {
        let datatype = pg_field.datatype();
        let format = pg_field.format();

        if format == FieldFormat::Text {
            if self.curr_col == 0 {
                self.row_buffer.put_slice(b"(");
            }
            // encode value in an intermediate buf
            let mut buf = BytesMut::new();
            value.to_sql_text(datatype, &mut buf, pg_field.format_options().as_ref())?;
            let encoded_value_as_str = String::from_utf8_lossy(&buf);
            if QUOTE_CHECK.is_match(&encoded_value_as_str) {
                self.row_buffer.put_u8(b'"');
                self.row_buffer.put_slice(
                    QUOTE_ESCAPE
                        .replace_all(&encoded_value_as_str, r#"\$1"#)
                        .as_bytes(),
                );
                self.row_buffer.put_u8(b'"');
            } else {
                self.row_buffer.put_slice(&buf);
            }
            if self.curr_col == self.num_cols - 1 {
                self.row_buffer.put_slice(b")");
            } else {
                self.row_buffer.put_slice(b",");
            }
        } else {
            if self.curr_col == 0 && format == FieldFormat::Binary {
                // Place Number of fields
                self.row_buffer.put_i32(self.num_cols as i32);
            }

            self.row_buffer.put_u32(datatype.oid());
            // remember the position of the 4-byte length field
            let prev_index = self.row_buffer.len();
            // write value length as -1 ahead of time
            self.row_buffer.put_i32(-1);
            let is_null = value.to_sql(datatype, &mut self.row_buffer)?;
            if let IsNull::No = is_null {
                let value_length = self.row_buffer.len() - prev_index - 4;
                let mut length_bytes = &mut self.row_buffer[prev_index..(prev_index + 4)];
                length_bytes.put_i32(value_length as i32);
            }
        }
        self.curr_col += 1;
        Ok(())
    }

    fn take_row(&mut self) -> Self::Item {
        std::mem::take(&mut self.row_buffer)
    }
}
