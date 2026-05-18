use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::{datatypes::*, record_batch::RecordBatch};
#[cfg(feature = "postgis")]
use arrow_schema::extension::ExtensionType;
#[cfg(feature = "datafusion")]
use datafusion::arrow::{datatypes::*, record_batch::RecordBatch};

use pgwire::api::portal::Format;
use pgwire::api::results::FieldInfo;
use pgwire::api::Type;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::types::format::FormatOptions;
use postgres_types::Kind;

use crate::row_encoder::RowEncoder;

#[cfg(feature = "datafusion")]
pub mod df;

pub fn into_pg_type(arrow_type: &DataType) -> PgWireResult<Type> {
    let datatype = match arrow_type {
        DataType::Null => Type::UNKNOWN,
        DataType::Boolean => Type::BOOL,
        DataType::Int8 => Type::INT2,
        DataType::Int16 | DataType::UInt8 => Type::INT2,
        DataType::Int32 | DataType::UInt16 => Type::INT4,
        DataType::Int64 | DataType::UInt32 => Type::INT8,
        DataType::UInt64 => Type::NUMERIC,
        DataType::Timestamp(_, tz) => {
            if tz.is_some() {
                Type::TIMESTAMPTZ
            } else {
                Type::TIMESTAMP
            }
        }
        DataType::Time32(_) | DataType::Time64(_) => Type::TIME,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Interval(_) | DataType::Duration(_) => Type::INTERVAL,
        DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView => Type::BYTEA,
        DataType::Float16 | DataType::Float32 => Type::FLOAT4,
        DataType::Float64 => Type::FLOAT8,
        DataType::Decimal128(_, _) => Type::NUMERIC,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Type::TEXT,
        DataType::List(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field) => match field.data_type() {
            DataType::Boolean => Type::BOOL_ARRAY,
            DataType::Int8 => Type::INT2_ARRAY,
            DataType::Int16 | DataType::UInt8 => Type::INT2_ARRAY,
            DataType::Int32 | DataType::UInt16 => Type::INT4_ARRAY,
            DataType::Int64 | DataType::UInt32 => Type::INT8_ARRAY,
            DataType::UInt64 | DataType::Decimal128(_, _) => Type::NUMERIC_ARRAY,
            DataType::Timestamp(_, tz) => {
                if tz.is_some() {
                    Type::TIMESTAMPTZ_ARRAY
                } else {
                    Type::TIMESTAMP_ARRAY
                }
            }
            DataType::Time32(_) | DataType::Time64(_) => Type::TIME_ARRAY,
            DataType::Date32 | DataType::Date64 => Type::DATE_ARRAY,
            DataType::Interval(_) | DataType::Duration(_) => Type::INTERVAL_ARRAY,
            DataType::FixedSizeBinary(_)
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView => Type::BYTEA_ARRAY,
            DataType::Float16 | DataType::Float32 => Type::FLOAT4_ARRAY,
            DataType::Float64 => Type::FLOAT8_ARRAY,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Type::TEXT_ARRAY,
            DataType::Struct(_) => Type::new(
                Type::RECORD_ARRAY.name().into(),
                Type::RECORD_ARRAY.oid(),
                Kind::Array(field_into_pg_type(field)?),
                Type::RECORD_ARRAY.schema().into(),
            ),
            list_type => {
                return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    format!("Unsupported List Datatype {list_type}"),
                ))));
            }
        },
        DataType::Dictionary(_, value_type) => into_pg_type(value_type.as_ref())?,
        DataType::Struct(fields) => {
            let name: String = fields
                .iter()
                .map(|x| x.name().clone())
                .reduce(|a, b| a + ", " + &b)
                .map(|x| format!("({x})"))
                .unwrap_or("()".to_string());
            let kind = Kind::Composite(
                fields
                    .iter()
                    .map(|x| {
                        field_into_pg_type(x)
                            .map(|_type| postgres_types::Field::new(x.name().clone(), _type))
                    })
                    .collect::<Result<Vec<_>, PgWireError>>()?,
            );
            Type::new(name, Type::RECORD.oid(), kind, Type::RECORD.schema().into())
        }
        _ => {
            return Err(PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("Unsupported Datatype {arrow_type}"),
            ))));
        }
    };

    Ok(datatype)
}

pub fn field_into_pg_type(field: &Arc<Field>) -> PgWireResult<Type> {
    let arrow_type = field.data_type();

    match field.extension_type_name() {
        // As of arrow 56, there are additional extension logical type that is
        // defined using field metadata, for instance, json or geo.
        //
        // TODO: there is no fixed Geometry/Geography type id, here we use text
        // for placeholder.
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::PointType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::LineStringType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::PolygonType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::MultiPointType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::MultiLineStringType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::MultiPolygonType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::GeometryCollectionType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::GeometryType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::RectType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::WktType::NAME) => Ok(Type::TEXT),
        #[cfg(feature = "postgis")]
        Some(geoarrow_schema::WkbType::NAME) => Ok(Type::TEXT),

        _ => into_pg_type(arrow_type),
    }
}

pub fn arrow_schema_to_pg_fields(
    schema: &Schema,
    format: &Format,
    data_format_options: Option<Arc<FormatOptions>>,
) -> PgWireResult<Vec<FieldInfo>> {
    let _ = data_format_options;
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let pg_type = field_into_pg_type(f)?;
            let mut field_info =
                FieldInfo::new(f.name().into(), None, None, pg_type, format.format_for(idx));
            if let Some(data_format_options) = &data_format_options {
                field_info = field_info.with_format_options(data_format_options.clone());
            }

            Ok(field_info)
        })
        .collect::<PgWireResult<Vec<FieldInfo>>>()
}

pub fn encode_recordbatch(
    fields: Arc<Vec<FieldInfo>>,
    record_batch: RecordBatch,
) -> Box<impl Iterator<Item = PgWireResult<DataRow>>> {
    let mut row_stream = RowEncoder::new(record_batch, fields);
    Box::new(std::iter::from_fn(move || row_stream.next_row()))
}
