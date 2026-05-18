use std::str::FromStr;
use std::sync::Arc;

#[cfg(not(feature = "datafusion"))]
use arrow::{array::*, datatypes::*};
use chrono::NaiveTime;
use chrono::{NaiveDate, NaiveDateTime};
#[cfg(feature = "datafusion")]
use datafusion::arrow::{array::*, datatypes::*};
use pg_interval::Interval as PgInterval;
use pgwire::api::results::{CopyEncoder, DataRowEncoder, FieldInfo};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::copy::CopyData;
use pgwire::messages::data::DataRow;
use pgwire::types::ToSqlText;
use postgres_types::ToSql;
use rust_decimal::Decimal;
use timezone::Tz;

use crate::error::ToSqlError;
#[cfg(feature = "postgis")]
use crate::geo_encoder::encode_geo;
use crate::list_encoder::encode_list;
use crate::struct_encoder::encode_struct;

pub trait Encoder {
    type Item;

    fn encode_field<T>(&mut self, value: &T, pg_field: &FieldInfo) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized;

    fn take_row(&mut self) -> Self::Item;
}

impl Encoder for DataRowEncoder {
    type Item = DataRow;

    fn encode_field<T>(&mut self, value: &T, pg_field: &FieldInfo) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized,
    {
        self.encode_field_with_type_and_format(
            value,
            pg_field.datatype(),
            pg_field.format(),
            pg_field.format_options(),
        )
    }

    fn take_row(&mut self) -> Self::Item {
        self.take_row()
    }
}

impl Encoder for CopyEncoder {
    type Item = CopyData;

    fn encode_field<T>(&mut self, value: &T, _pg_field: &FieldInfo) -> PgWireResult<()>
    where
        T: ToSql + ToSqlText + Sized,
    {
        self.encode_field(value)
    }

    fn take_row(&mut self) -> Self::Item {
        self.take_copy()
    }
}

fn get_bool_value(arr: &Arc<dyn Array>, idx: usize) -> Option<bool> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(idx)
    })
}

macro_rules! get_primitive_value {
    ($name:ident, $t:ty, $pt:ty) => {
        fn $name(arr: &Arc<dyn Array>, idx: usize) -> Option<$pt> {
            (!arr.is_null(idx)).then(|| {
                arr.as_any()
                    .downcast_ref::<PrimitiveArray<$t>>()
                    .unwrap()
                    .value(idx)
            })
        }
    };
}

get_primitive_value!(get_i8_value, Int8Type, i8);
get_primitive_value!(get_i16_value, Int16Type, i16);
get_primitive_value!(get_i32_value, Int32Type, i32);
get_primitive_value!(get_i64_value, Int64Type, i64);
get_primitive_value!(get_u8_value, UInt8Type, u8);
get_primitive_value!(get_u16_value, UInt16Type, u16);
get_primitive_value!(get_u32_value, UInt32Type, u32);
get_primitive_value!(get_u64_value, UInt64Type, u64);

fn get_u64_as_decimal_value(arr: &Arc<dyn Array>, idx: usize) -> Option<Decimal> {
    get_u64_value(arr, idx).map(Decimal::from)
}
get_primitive_value!(get_f32_value, Float32Type, f32);
get_primitive_value!(get_f64_value, Float64Type, f64);

fn get_utf8_view_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&str> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_binary_view_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&[u8]> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<BinaryViewArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_utf8_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&str> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_large_utf8_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&str> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<LargeStringArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_binary_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&[u8]> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_large_binary_value(arr: &Arc<dyn Array>, idx: usize) -> Option<&[u8]> {
    (!arr.is_null(idx)).then(|| {
        arr.as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap()
            .value(idx)
    })
}

fn get_date32_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDate> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Date32Array>()
        .unwrap()
        .value_as_date(idx)
}

fn get_date64_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveDate> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Date64Array>()
        .unwrap()
        .value_as_date(idx)
}

fn get_time32_second_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time32SecondArray>()
        .unwrap()
        .value_as_time(idx)
}

fn get_time32_millisecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time32MillisecondArray>()
        .unwrap()
        .value_as_time(idx)
}

fn get_time64_microsecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time64MicrosecondArray>()
        .unwrap()
        .value_as_time(idx)
}
fn get_time64_nanosecond_value(arr: &Arc<dyn Array>, idx: usize) -> Option<NaiveTime> {
    if arr.is_null(idx) {
        return None;
    }
    arr.as_any()
        .downcast_ref::<Time64NanosecondArray>()
        .unwrap()
        .value_as_time(idx)
}

fn get_numeric_128_value(
    arr: &Arc<dyn Array>,
    idx: usize,
    scale: u32,
) -> PgWireResult<Option<Decimal>> {
    if arr.is_null(idx) {
        return Ok(None);
    }

    let array = arr.as_any().downcast_ref::<Decimal128Array>().unwrap();
    let value = array.value(idx);
    Decimal::try_from_i128_with_scale(value, scale)
        .map_err(|e| {
            let error_code = match e {
                rust_decimal::Error::ExceedsMaximumPossibleValue => {
                    "22003" // numeric_value_out_of_range
                }
                rust_decimal::Error::LessThanMinimumPossibleValue => {
                    "22003" // numeric_value_out_of_range
                }
                rust_decimal::Error::ScaleExceedsMaximumPrecision(scale) => {
                    return PgWireError::UserError(Box::new(ErrorInfo::new(
                        "ERROR".to_string(),
                        "22003".to_string(),
                        format!("Scale {scale} exceeds maximum precision for numeric type"),
                    )));
                }
                _ => "22003", // generic numeric_value_out_of_range
            };
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_string(),
                error_code.to_string(),
                format!("Numeric value conversion failed: {e}"),
            )))
        })
        .map(Some)
}

pub fn encode_value<T: Encoder>(
    encoder: &mut T,
    arr: &Arc<dyn Array>,
    idx: usize,
    arrow_field: &Field,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    let arrow_type = arrow_field.data_type();

    #[cfg(feature = "postgis")]
    if let Some(geoarrow_type) = geoarrow_schema::GeoArrowType::from_extension_field(arrow_field)
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?
    {
        let geoarrow_array: Arc<dyn geoarrow::array::GeoArrowArray> =
            geoarrow::array::from_arrow_array(arr, arrow_field)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        return encode_geo(
            encoder,
            geoarrow_type,
            &geoarrow_array,
            idx,
            arrow_field,
            pg_field,
        );
    }

    match arrow_type {
        DataType::Null => encoder.encode_field(&None::<i8>, pg_field)?,
        DataType::Boolean => encoder.encode_field(&get_bool_value(arr, idx), pg_field)?,
        DataType::Int8 => encoder.encode_field(&get_i8_value(arr, idx), pg_field)?,
        DataType::Int16 => encoder.encode_field(&get_i16_value(arr, idx), pg_field)?,
        DataType::Int32 => encoder.encode_field(&get_i32_value(arr, idx), pg_field)?,
        DataType::Int64 => encoder.encode_field(&get_i64_value(arr, idx), pg_field)?,
        DataType::UInt8 => {
            encoder.encode_field(&(get_u8_value(arr, idx).map(|x| x as i16)), pg_field)?
        }
        DataType::UInt16 => {
            encoder.encode_field(&(get_u16_value(arr, idx).map(|x| x as i32)), pg_field)?
        }
        DataType::UInt32 => {
            encoder.encode_field(&get_u32_value(arr, idx).map(|x| x as i64), pg_field)?
        }
        DataType::UInt64 => encoder.encode_field(&get_u64_as_decimal_value(arr, idx), pg_field)?,
        DataType::Float32 => encoder.encode_field(&get_f32_value(arr, idx), pg_field)?,
        DataType::Float64 => encoder.encode_field(&get_f64_value(arr, idx), pg_field)?,
        DataType::Decimal128(_, s) => {
            encoder.encode_field(&get_numeric_128_value(arr, idx, *s as u32)?, pg_field)?
        }
        DataType::Utf8 => encoder.encode_field(&get_utf8_value(arr, idx), pg_field)?,
        DataType::Utf8View => encoder.encode_field(&get_utf8_view_value(arr, idx), pg_field)?,
        DataType::BinaryView => encoder.encode_field(&get_binary_view_value(arr, idx), pg_field)?,
        DataType::LargeUtf8 => encoder.encode_field(&get_large_utf8_value(arr, idx), pg_field)?,
        DataType::Binary => encoder.encode_field(&get_binary_value(arr, idx), pg_field)?,
        DataType::LargeBinary => {
            encoder.encode_field(&get_large_binary_value(arr, idx), pg_field)?
        }
        DataType::Date32 => encoder.encode_field(&get_date32_value(arr, idx), pg_field)?,
        DataType::Date64 => encoder.encode_field(&get_date64_value(arr, idx), pg_field)?,
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => {
                encoder.encode_field(&get_time32_second_value(arr, idx), pg_field)?
            }
            TimeUnit::Millisecond => {
                encoder.encode_field(&get_time32_millisecond_value(arr, idx), pg_field)?
            }
            _ => {}
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => {
                encoder.encode_field(&get_time64_microsecond_value(arr, idx), pg_field)?
            }
            TimeUnit::Nanosecond => {
                encoder.encode_field(&get_time64_nanosecond_value(arr, idx), pg_field)?
            }
            _ => {}
        },
        DataType::Timestamp(unit, timezone) => match unit {
            TimeUnit::Second => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<NaiveDateTime>, pg_field);
                }
                let ts_array = arr.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value = ts_array
                        .value_as_datetime_with_tz(idx, tz)
                        .map(|d| d.fixed_offset());

                    encoder.encode_field(&value, pg_field)?;
                } else {
                    let value = ts_array.value_as_datetime(idx);
                    encoder.encode_field(&value, pg_field)?;
                }
            }
            TimeUnit::Millisecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<NaiveDateTime>, pg_field);
                }
                let ts_array = arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value = ts_array
                        .value_as_datetime_with_tz(idx, tz)
                        .map(|d| d.fixed_offset());
                    encoder.encode_field(&value, pg_field)?;
                } else {
                    let value = ts_array.value_as_datetime(idx);
                    encoder.encode_field(&value, pg_field)?;
                }
            }
            TimeUnit::Microsecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<NaiveDateTime>, pg_field);
                }
                let ts_array = arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap();
                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value = ts_array
                        .value_as_datetime_with_tz(idx, tz)
                        .map(|d| d.fixed_offset());
                    encoder.encode_field(&value, pg_field)?;
                } else {
                    let value = ts_array.value_as_datetime(idx);
                    encoder.encode_field(&value, pg_field)?;
                }
            }
            TimeUnit::Nanosecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<NaiveDateTime>, pg_field);
                }
                let ts_array = arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value = ts_array
                        .value_as_datetime_with_tz(idx, tz)
                        .map(|d| d.fixed_offset());
                    encoder.encode_field(&value, pg_field)?;
                } else {
                    let value = ts_array.value_as_datetime(idx);
                    encoder.encode_field(&value, pg_field)?;
                }
            }
        },
        DataType::Interval(interval_unit) => match interval_unit {
            IntervalUnit::YearMonth => {
                let interval_array = arr
                    .as_any()
                    .downcast_ref::<IntervalYearMonthArray>()
                    .unwrap();
                let months = IntervalYearMonthType::to_months(interval_array.value(idx));
                encoder.encode_field(&PgInterval::new(months, 0, 0), pg_field)?;
            }
            IntervalUnit::DayTime => {
                let interval_array = arr.as_any().downcast_ref::<IntervalDayTimeArray>().unwrap();
                let (days, millis) = IntervalDayTimeType::to_parts(interval_array.value(idx));
                encoder
                    .encode_field(&PgInterval::new(0, days, millis as i64 * 1000i64), pg_field)?;
            }
            IntervalUnit::MonthDayNano => {
                let interval_array = arr
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .unwrap();
                let (months, days, nanoseconds) =
                    IntervalMonthDayNanoType::to_parts(interval_array.value(idx));

                encoder.encode_field(
                    &PgInterval::new(months, days, nanoseconds / 1000i64),
                    pg_field,
                )?;
            }
        },
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<PgInterval>, pg_field);
                }
                let duration_array = arr.as_any().downcast_ref::<DurationSecondArray>().unwrap();
                let microseconds = duration_array.value(idx) * 1_000_000i64;
                encoder.encode_field(&PgInterval::new(0, 0, microseconds), pg_field)?;
            }
            TimeUnit::Millisecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<PgInterval>, pg_field);
                }
                let duration_array = arr
                    .as_any()
                    .downcast_ref::<DurationMillisecondArray>()
                    .unwrap();
                let microseconds = duration_array.value(idx) * 1_000i64;
                encoder.encode_field(&PgInterval::new(0, 0, microseconds), pg_field)?;
            }
            TimeUnit::Microsecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<PgInterval>, pg_field);
                }
                let duration_array = arr
                    .as_any()
                    .downcast_ref::<DurationMicrosecondArray>()
                    .unwrap();
                let microseconds = duration_array.value(idx);
                encoder.encode_field(&PgInterval::new(0, 0, microseconds), pg_field)?;
            }
            TimeUnit::Nanosecond => {
                if arr.is_null(idx) {
                    return encoder.encode_field(&None::<PgInterval>, pg_field);
                }
                let duration_array = arr
                    .as_any()
                    .downcast_ref::<DurationNanosecondArray>()
                    .unwrap();
                let microseconds = duration_array.value(idx) / 1_000i64;
                encoder.encode_field(&PgInterval::new(0, 0, microseconds), pg_field)?;
            }
        },
        DataType::List(_) | DataType::FixedSizeList(_, _) | DataType::LargeList(_) => {
            if arr.is_null(idx) {
                return encoder.encode_field(&None::<&[i8]>, pg_field);
            }
            let array = arr.as_any().downcast_ref::<ListArray>().unwrap().value(idx);
            encode_list(encoder, array, pg_field)?
        }
        DataType::Struct(arrow_fields) => encode_struct(encoder, arr, idx, arrow_fields, pg_field)?,
        DataType::Dictionary(_, value_type) => {
            if arr.is_null(idx) {
                return encoder.encode_field(&None::<i8>, pg_field);
            }
            // Get the dictionary values and the mapped row index
            macro_rules! get_dict_values_and_index {
                ($key_type:ty) => {
                    arr.as_any()
                        .downcast_ref::<DictionaryArray<$key_type>>()
                        .map(|dict| (dict.values(), dict.keys().value(idx) as usize))
                };
            }

            // Try to extract values using different key types
            let (values, idx) = get_dict_values_and_index!(Int8Type)
                .or_else(|| get_dict_values_and_index!(Int16Type))
                .or_else(|| get_dict_values_and_index!(Int32Type))
                .or_else(|| get_dict_values_and_index!(Int64Type))
                .or_else(|| get_dict_values_and_index!(UInt8Type))
                .or_else(|| get_dict_values_and_index!(UInt16Type))
                .or_else(|| get_dict_values_and_index!(UInt32Type))
                .or_else(|| get_dict_values_and_index!(UInt64Type))
                .ok_or_else(|| {
                    ToSqlError::from(format!(
                        "Unsupported dictionary key type for value type {value_type}"
                    ))
                })?;

            let inner_arrow_field = Field::new(pg_field.name(), *value_type.clone(), true);

            encode_value(encoder, values, idx, &inner_arrow_field, pg_field)?
        }
        _ => {
            return Err(PgWireError::ApiError(ToSqlError::from(format!(
                "Unsupported Datatype {} and array {:?}",
                arr.data_type(),
                &arr
            ))));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::buffer::NullBuffer;
    use bytes::BytesMut;
    use pgwire::{api::results::FieldFormat, types::format::FormatOptions};
    use postgres_types::Type;

    use super::*;

    #[test]
    fn encodes_dictionary_array() {
        #[derive(Default)]
        struct MockEncoder {
            encoded_value: String,
        }

        impl Encoder for MockEncoder {
            type Item = String;

            fn encode_field<T>(&mut self, value: &T, pg_field: &FieldInfo) -> PgWireResult<()>
            where
                T: ToSql + ToSqlText + Sized,
            {
                let mut bytes = BytesMut::new();
                let _sql_text =
                    value.to_sql_text(pg_field.datatype(), &mut bytes, &FormatOptions::default());
                let string = String::from_utf8(bytes.to_vec());
                self.encoded_value = string.unwrap();
                Ok(())
            }

            fn take_row(&mut self) -> Self::Item {
                std::mem::take(&mut self.encoded_value)
            }
        }

        let val = "~!@&$[]()@@!!";
        let value = StringArray::from_iter_values([val]);
        let keys = Int8Array::from_iter_values([0, 0, 0, 0]);
        let dict_arr: Arc<dyn Array> =
            Arc::new(DictionaryArray::<Int8Type>::try_new(keys, Arc::new(value)).unwrap());

        let mut encoder = MockEncoder::default();

        let arrow_field = Field::new(
            "x",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            true,
        );
        let pg_field = FieldInfo::new("x".to_string(), None, None, Type::TEXT, FieldFormat::Text);
        let result = encode_value(&mut encoder, &dict_arr, 2, &arrow_field, &pg_field);

        assert!(result.is_ok());

        assert!(encoder.encoded_value == val);
    }

    #[test]
    fn encode_struct_null_emits_field() {
        // Regression test: encode_struct must call encoder.encode_field for
        // NULL struct values so a NULL indicator is written to the DataRow.
        // Previously it returned Ok(()) without encoding, corrupting the
        // column count.

        #[derive(Default)]
        struct CountingEncoder {
            call_count: usize,
        }

        impl Encoder for CountingEncoder {
            type Item = ();

            fn encode_field<T>(&mut self, _value: &T, _pg_field: &FieldInfo) -> PgWireResult<()>
            where
                T: ToSql + ToSqlText + Sized,
            {
                self.call_count += 1;
                Ok(())
            }

            fn take_row(&mut self) -> Self::Item {}
        }

        let fields = vec![
            Arc::new(Field::new("a", DataType::Utf8, true)),
            Arc::new(Field::new("b", DataType::Utf8, true)),
        ];
        let a = Arc::new(StringArray::from(vec![Some("hello"), Some("x")])) as Arc<dyn Array>;
        let b = Arc::new(StringArray::from(vec![Some("world"), Some("y")])) as Arc<dyn Array>;

        // Row 0: non-null struct, Row 1: null struct
        let null_buffer = NullBuffer::from(vec![true, false]);
        let struct_arr: Arc<dyn Array> = Arc::new(
            StructArray::try_new(fields.clone().into(), vec![a, b], Some(null_buffer)).unwrap(),
        );

        let arrow_field = Field::new("s", DataType::Struct(fields.into()), true);
        let pg_field = FieldInfo::new("s".to_string(), None, None, Type::TEXT, FieldFormat::Text);

        // Encode the NULL row (index 1).
        let mut encoder = CountingEncoder::default();
        let result = encode_value(&mut encoder, &struct_arr, 1, &arrow_field, &pg_field);
        assert!(result.is_ok());
        assert_eq!(
            encoder.call_count, 1,
            "encode_field must be called exactly once for a NULL struct to emit a NULL indicator"
        );
    }

    #[test]
    fn test_get_time32_second_value() {
        let array = Time32SecondArray::from_iter_values([3723_i32]);
        let array: Arc<dyn Array> = Arc::new(array);
        let value = get_time32_second_value(&array, 0);
        assert_eq!(value, Some(NaiveTime::from_hms_opt(1, 2, 3)).unwrap());
    }

    #[test]
    fn test_get_time32_millisecond_value() {
        let array = Time32MillisecondArray::from_iter_values([3723001_i32]);
        let array: Arc<dyn Array> = Arc::new(array);
        let value = get_time32_millisecond_value(&array, 0);
        assert_eq!(
            value,
            Some(NaiveTime::from_hms_milli_opt(1, 2, 3, 1)).unwrap()
        );
    }

    #[test]
    fn test_get_time64_microsecond_value() {
        let array = Time64MicrosecondArray::from_iter_values([3723001001_i64]);
        let array: Arc<dyn Array> = Arc::new(array);
        let value = get_time64_microsecond_value(&array, 0);
        assert_eq!(
            value,
            Some(NaiveTime::from_hms_micro_opt(1, 2, 3, 1001)).unwrap()
        );
    }

    #[test]
    fn test_get_time64_nanosecond_value() {
        let array = Time64NanosecondArray::from_iter_values([3723001001001_i64]);
        let array: Arc<dyn Array> = Arc::new(array);
        let value = get_time64_nanosecond_value(&array, 0);
        assert_eq!(
            value,
            Some(NaiveTime::from_hms_nano_opt(1, 2, 3, 1001001)).unwrap()
        );
    }
}
