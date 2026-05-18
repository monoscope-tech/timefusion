use std::iter;
use std::sync::Arc;

use arrow_schema::IntervalUnit;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use datafusion::arrow::datatypes::{DataType, Date32Type, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ParamValues;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use futures::{stream, StreamExt};
use pg_interval::Interval;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::results::QueryResponse;
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::types::format::FormatOptions;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use super::{arrow_schema_to_pg_fields, encode_recordbatch, into_pg_type};

pub async fn encode_dataframe(
    df: DataFrame,
    format: &Format,
    data_format_options: Option<Arc<FormatOptions>>,
) -> PgWireResult<QueryResponse> {
    let fields = Arc::new(arrow_schema_to_pg_fields(
        df.schema().as_arrow(),
        format,
        data_format_options,
    )?);

    let recordbatch_stream = df
        .execute_stream()
        .await
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    let fields_ref = fields.clone();
    let pg_row_stream = recordbatch_stream
        .map(move |rb: datafusion::error::Result<RecordBatch>| {
            let row_stream: Box<dyn Iterator<Item = PgWireResult<DataRow>> + Send + Sync> = match rb
            {
                Ok(rb) => encode_recordbatch(fields_ref.clone(), rb),
                Err(e) => Box::new(iter::once(Err(PgWireError::ApiError(e.into())))),
            };
            stream::iter(row_stream)
        })
        .flatten();
    Ok(QueryResponse::new(fields, pg_row_stream))
}

/// Deserialize client provided parameter data.
///
/// First we try to use the type information from `pg_type_hint`, which is
/// provided by the client.
/// If the type is empty or unknown, we fallback to datafusion inferenced type
/// from `inferenced_types`.
/// An error will be raised when neither sources can provide type information.
pub fn deserialize_parameters<S>(
    portal: &Portal<S>,
    inferenced_types: &[Option<&DataType>],
) -> PgWireResult<ParamValues>
where
    S: Clone,
{
    fn get_pg_type(
        pg_type_hint: Option<Type>,
        inferenced_type: Option<&DataType>,
    ) -> PgWireResult<Type> {
        if let Some(ty) = pg_type_hint {
            Ok(ty.clone())
        } else if let Some(infer_type) = inferenced_type {
            into_pg_type(infer_type)
        } else {
            Ok(Type::UNKNOWN)
        }
    }

    let param_len = portal.parameter_len();
    let mut deserialized_params = Vec::with_capacity(param_len);
    for i in 0..param_len {
        let inferenced_type = inferenced_types.get(i).and_then(|v| v.to_owned());
        let pg_type = get_pg_type(
            portal
                .statement
                .parameter_types
                .get(i)
                .and_then(|f| f.clone()),
            inferenced_type,
        )?;
        match pg_type {
            // enumerate all supported parameter types and deserialize the
            // type to ScalarValue
            Type::BOOL => {
                let value = portal.parameter::<bool>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Boolean(value));
            }
            Type::CHAR => {
                let value = portal.parameter::<i8>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Int8(value));
            }
            Type::INT2 => {
                let value = portal.parameter::<i16>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Int16(value));
            }
            Type::INT4 => {
                let value = portal.parameter::<i32>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Int32(value));
            }
            Type::INT8 => {
                let value = portal.parameter::<i64>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Int64(value));
            }
            Type::TEXT | Type::VARCHAR => {
                let value = portal.parameter::<String>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Utf8(value));
            }
            Type::BYTEA => {
                let value = portal.parameter::<Vec<u8>>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Binary(value));
            }

            Type::FLOAT4 => {
                let value = portal.parameter::<f32>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Float32(value));
            }
            Type::FLOAT8 => {
                let value = portal.parameter::<f64>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::Float64(value));
            }
            Type::NUMERIC => {
                let value = match portal.parameter::<Decimal>(i, &pg_type)? {
                    None => ScalarValue::Decimal128(None, 0, 0),
                    Some(value) => {
                        let precision = match value.mantissa() {
                            0 => 1,
                            m => (m.abs() as f64).log10().floor() as u8 + 1,
                        };
                        let scale = value.scale() as i8;
                        ScalarValue::Decimal128(value.to_i128(), precision, scale)
                    }
                };
                deserialized_params.push(value);
            }
            Type::TIMESTAMP => {
                let value = portal.parameter::<NaiveDateTime>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::TimestampMicrosecond(
                    value.map(|t| t.and_utc().timestamp_micros()),
                    None,
                ));
            }
            Type::TIMESTAMPTZ => {
                let value = portal.parameter::<DateTime<FixedOffset>>(i, &pg_type)?;
                deserialized_params.push(ScalarValue::TimestampMicrosecond(
                    value.map(|t| t.timestamp_micros()),
                    value.map(|t| t.offset().to_string().into()),
                ));
            }
            Type::DATE => {
                let value = portal.parameter::<NaiveDate>(i, &pg_type)?;
                deserialized_params
                    .push(ScalarValue::Date32(value.map(Date32Type::from_naive_date)));
            }
            Type::TIME => {
                let value = portal.parameter::<NaiveTime>(i, &pg_type)?;

                let ns = value.map(|t| {
                    t.num_seconds_from_midnight() as i64 * 1_000_000_000 + t.nanosecond() as i64
                });

                let scalar_value = match inferenced_type {
                    Some(DataType::Time64(TimeUnit::Nanosecond)) => {
                        ScalarValue::Time64Nanosecond(ns)
                    }
                    Some(DataType::Time64(TimeUnit::Microsecond)) => {
                        ScalarValue::Time64Microsecond(ns.map(|ns| (ns / 1_000) as _))
                    }
                    Some(DataType::Time32(TimeUnit::Millisecond)) => {
                        ScalarValue::Time32Millisecond(ns.map(|ns| (ns / 1_000_000) as _))
                    }
                    Some(DataType::Time32(TimeUnit::Second)) => {
                        ScalarValue::Time32Second(ns.map(|ns| (ns / 1_000_000_000) as _))
                    }
                    _ => {
                        return Err(PgWireError::ApiError(
                            format!(
                                "Unable to deserialise time parameter type {:?} to type {:?}",
                                value, inferenced_type
                            )
                            .into(),
                        ))
                    }
                };

                deserialized_params.push(scalar_value);
            }
            Type::UUID => {
                // pgwire's FromSql<String> rejects the UUID OID, and uuid::Uuid
                // doesn't implement FromSqlText, so neither works through
                // portal.parameter<T>. Read raw bytes and decode by protocol
                // format: 16-byte binary or text representation.
                let raw = portal.parameters.get(i).and_then(|o| o.as_ref());
                let value = match raw {
                    None => None,
                    Some(bytes) if portal.parameter_format.is_binary(i) => Some(
                        uuid::Uuid::from_slice(bytes)
                            .map_err(|e| PgWireError::ApiError(format!("uuid binary: {e}").into()))?
                            .to_string(),
                    ),
                    Some(bytes) => Some(
                        std::str::from_utf8(bytes)
                            .map_err(|e| PgWireError::ApiError(format!("uuid utf8: {e}").into()))?
                            .to_string(),
                    ),
                };
                deserialized_params.push(ScalarValue::Utf8(value));
            }
            Type::JSON | Type::JSONB => {
                // Same issue as Type::UUID — FromSql<String> rejects JSONB OID.
                // Binary JSONB framing is [version=0x01][utf8 json]; binary JSON is
                // raw utf8; text protocol is utf8 directly.
                let raw = portal.parameters.get(i).and_then(|o| o.as_ref());
                let is_binary = portal.parameter_format.is_binary(i);
                let value = match raw {
                    None => None,
                    Some(bytes) if is_binary && pg_type == Type::JSONB => {
                        let body = bytes.get(1..).unwrap_or(&[]);
                        Some(
                            std::str::from_utf8(body)
                                .map_err(|e| PgWireError::ApiError(format!("jsonb utf8: {e}").into()))?
                                .to_string(),
                        )
                    }
                    Some(bytes) => Some(
                        std::str::from_utf8(bytes)
                            .map_err(|e| PgWireError::ApiError(format!("json utf8: {e}").into()))?
                            .to_string(),
                    ),
                };
                deserialized_params.push(ScalarValue::Utf8(value));
            }
            Type::INTERVAL => {
                let value = portal.parameter::<Interval>(i, &pg_type)?;
                let scalar_value = if let Some(i) = value {
                    ScalarValue::new_interval_mdn(i.months, i.days, i.microseconds * 1_000i64)
                } else {
                    ScalarValue::IntervalMonthDayNano(None)
                };

                deserialized_params.push(scalar_value);
            }
            // Array types support
            Type::BOOL_ARRAY => {
                let value = portal.parameter::<Vec<Option<bool>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Boolean).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Boolean,
                )));
            }
            Type::INT2_ARRAY => {
                let value = portal.parameter::<Vec<Option<i16>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Int16).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Int16,
                )));
            }
            Type::INT4_ARRAY => {
                let value = portal.parameter::<Vec<Option<i32>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Int32).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Int32,
                )));
            }
            Type::INT8_ARRAY => {
                let value = portal.parameter::<Vec<Option<i64>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Int64).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Int64,
                )));
            }
            Type::FLOAT4_ARRAY => {
                let value = portal.parameter::<Vec<Option<f32>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Float32).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Float32,
                )));
            }
            Type::FLOAT8_ARRAY => {
                let value = portal.parameter::<Vec<Option<f64>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Float64).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Float64,
                )));
            }
            Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => {
                let value = portal.parameter::<Vec<Option<String>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter().map(ScalarValue::Utf8).collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Utf8,
                )));
            }
            Type::INTERVAL_ARRAY => {
                let value = portal.parameter::<Vec<Option<Interval>>>(i, &pg_type)?;
                let scalar_values: Vec<ScalarValue> = value.map_or(Vec::new(), |v| {
                    v.into_iter()
                        .map(|i| {
                            if let Some(i) = i {
                                ScalarValue::new_interval_mdn(
                                    i.months,
                                    i.days,
                                    i.microseconds * 1_000i64,
                                )
                            } else {
                                ScalarValue::IntervalMonthDayNano(None)
                            }
                        })
                        .collect()
                });
                deserialized_params.push(ScalarValue::List(ScalarValue::new_list_nullable(
                    &scalar_values,
                    &DataType::Interval(IntervalUnit::MonthDayNano),
                )));
            }
            // Advanced types
            Type::MONEY => {
                let value = portal.parameter::<i64>(i, &pg_type)?;
                // Store money as int64 (cents)
                deserialized_params.push(ScalarValue::Int64(value));
            }
            Type::INET => {
                let value = portal.parameter::<String>(i, &pg_type)?;
                // Store IP addresses as strings for now
                deserialized_params.push(ScalarValue::Utf8(value));
            }
            Type::MACADDR => {
                let value = portal.parameter::<String>(i, &pg_type)?;
                // Store MAC addresses as strings for now
                deserialized_params.push(ScalarValue::Utf8(value));
            }
            // TODO: add more advanced types (composite types, ranges, etc.)
            _ => {
                // the client didn't provide type information and we are also
                // unable to inference the type, or it's a type that we haven't
                // supported:
                //
                // In this case we retry to resolve it as String or StringArray
                let value = portal.parameter::<String>(i, &pg_type)?;
                if let Some(value) = value {
                    if value.starts_with('{') && value.ends_with('}') {
                        // Looks like an array
                        let items = value.trim_matches(|c| c == '{' || c == '}' || c == ' ');
                        let items = items.split(',').map(|s| s.trim());
                        let scalar_values: Vec<ScalarValue> = items
                            .map(|s| ScalarValue::Utf8(Some(s.to_string())))
                            .collect();

                        deserialized_params.push(ScalarValue::List(
                            ScalarValue::new_list_nullable(&scalar_values, &DataType::Utf8),
                        ));
                    } else {
                        deserialized_params.push(ScalarValue::Utf8(Some(value)));
                    }
                }
            }
        }
    }

    Ok(ParamValues::List(
        deserialized_params.into_iter().map(|p| p.into()).collect(),
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use bytes::Bytes;
    use datafusion::{common::ParamValues, scalar::ScalarValue};
    use pgwire::{
        api::{portal::Portal, stmt::StoredStatement},
        messages::{data::FORMAT_CODE_BINARY, extendedquery::Bind},
    };
    use postgres_types::Type;

    use crate::datatypes::df::deserialize_parameters;

    #[test]
    fn test_deserialise_time_params() {
        let postgres_types = vec![Some(Type::TIME)];

        let us: i64 = 1_000_000; // 1 second

        let bind = Bind::new(
            None,
            None,
            vec![FORMAT_CODE_BINARY],
            vec![Some(Bytes::from(i64::to_be_bytes(us).to_vec()))],
            vec![],
        );

        let stmt = StoredStatement::new("statement_id".into(), "statement", postgres_types);
        let portal = Portal::try_new(&bind, Arc::new(stmt)).unwrap();

        for (arrow_type, expected) in [
            (
                DataType::Time32(arrow::datatypes::TimeUnit::Second),
                ScalarValue::Time32Second(Some(1)),
            ),
            (
                DataType::Time32(arrow::datatypes::TimeUnit::Millisecond),
                ScalarValue::Time32Millisecond(Some(1000)),
            ),
            (
                DataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
                ScalarValue::Time64Microsecond(Some(1000000)),
            ),
            (
                DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond),
                ScalarValue::Time64Nanosecond(Some(1000000000)),
            ),
        ] {
            let result = deserialize_parameters(&portal, &[Some(&arrow_type)]).unwrap();
            let ParamValues::List(list) = result else {
                panic!("expected list");
            };

            assert_eq!(list.len(), 1);
            assert_eq!(list[0].value(), &expected)
        }
    }
}
