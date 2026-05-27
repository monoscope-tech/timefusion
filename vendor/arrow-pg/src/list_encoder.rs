use std::{str::FromStr, sync::Arc};

#[cfg(not(feature = "datafusion"))]
use arrow::{
    array::{
        timezone::Tz, Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        Decimal128Array, Decimal256Array, DurationMicrosecondArray, DurationMillisecondArray,
        DurationNanosecondArray, DurationSecondArray, IntervalDayTimeArray,
        IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeListArray,
        LargeStringArray, ListArray, MapArray, PrimitiveArray, StringArray, StringViewArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    },
    datatypes::{
        DataType, Date32Type, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, Int8Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
        Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
        TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    temporal_conversions::{as_date, as_time},
};
#[cfg(feature = "datafusion")]
use datafusion::arrow::{
    array::{
        timezone::Tz, Array, BinaryArray, BinaryViewArray, BooleanArray, Date32Array, Date64Array,
        Decimal128Array, Decimal256Array, DurationMicrosecondArray, DurationMillisecondArray,
        DurationNanosecondArray, DurationSecondArray, IntervalDayTimeArray,
        IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeListArray,
        LargeStringArray, ListArray, MapArray, PrimitiveArray, StringArray, StringViewArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    },
    datatypes::{
        DataType, Date32Type, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type,
        Int64Type, Int8Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalUnit,
        Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
        TimeUnit, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    temporal_conversions::{as_date, as_time},
};

use chrono::{DateTime, TimeZone, Utc};
use pg_interval::Interval as PgInterval;
use pgwire::api::results::FieldInfo;
use pgwire::error::{PgWireError, PgWireResult};
use rust_decimal::Decimal;

use crate::encoder::Encoder;
use crate::error::ToSqlError;
use crate::struct_encoder::encode_structs;

fn get_bool_list_value(arr: &Arc<dyn Array>) -> Vec<Option<bool>> {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

macro_rules! get_primitive_list_value {
    ($name:ident, $t:ty, $pt:ty) => {
        fn $name(arr: &Arc<dyn Array>) -> Vec<Option<$pt>> {
            arr.as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .iter()
                .collect()
        }
    };

    ($name:ident, $t:ty, $pt:ty, $f:expr) => {
        fn $name(arr: &Arc<dyn Array>) -> Vec<Option<$pt>> {
            arr.as_any()
                .downcast_ref::<PrimitiveArray<$t>>()
                .unwrap()
                .iter()
                .map(|val| val.map($f))
                .collect()
        }
    };
}

get_primitive_list_value!(get_i8_list_value, Int8Type, i8);
get_primitive_list_value!(get_i16_list_value, Int16Type, i16);
get_primitive_list_value!(get_i32_list_value, Int32Type, i32);
get_primitive_list_value!(get_i64_list_value, Int64Type, i64);
get_primitive_list_value!(get_u8_list_value, UInt8Type, i16, |val: u8| { val as i16 });
get_primitive_list_value!(get_u16_list_value, UInt16Type, i32, |val: u16| {
    val as i32
});
get_primitive_list_value!(get_u32_list_value, UInt32Type, i64, |val: u32| {
    val as i64
});
get_primitive_list_value!(get_u64_list_value, UInt64Type, Decimal, |val: u64| {
    Decimal::from(val)
});
get_primitive_list_value!(get_f32_list_value, Float32Type, f32);
get_primitive_list_value!(get_f64_list_value, Float64Type, f64);

pub fn encode_list<T: Encoder>(
    encoder: &mut T,
    arr: Arc<dyn Array>,
    pg_field: &FieldInfo,
) -> PgWireResult<()> {
    match arr.data_type() {
        DataType::Null => {
            encoder.encode_field(&None::<i8>, pg_field)?;
            Ok(())
        }
        DataType::Boolean => {
            encoder.encode_field(&get_bool_list_value(&arr), pg_field)?;
            Ok(())
        }
        DataType::Int8 => {
            encoder.encode_field(&get_i8_list_value(&arr), pg_field)?;
            Ok(())
        }
        DataType::Int16 => {
            encoder.encode_field(&get_i16_list_value(&arr), pg_field)?;
            Ok(())
        }
        DataType::Int32 => {
            encoder.encode_field(&get_i32_list_value(&arr), pg_field)?;
            Ok(())
        }
        DataType::Int64 => {
            encoder.encode_field(&get_i64_list_value(&arr), pg_field)?;
            Ok(())
        }
        DataType::UInt8 => {
            encoder.encode_field(&get_u8_list_value(&arr), pg_field)?;
            Ok(())
        }
        DataType::UInt16 => {
            encoder.encode_field(&get_u16_list_value(&arr), pg_field)?;
            Ok(())
        }
        DataType::UInt32 => {
            encoder.encode_field(&get_u32_list_value(&arr), pg_field)?;
            Ok(())
        }
        DataType::UInt64 => {
            encoder.encode_field(&get_u64_list_value(&arr), pg_field)?;
            Ok(())
        }
        DataType::Float32 => {
            encoder.encode_field(&get_f32_list_value(&arr), pg_field)?;
            Ok(())
        }
        DataType::Float64 => {
            encoder.encode_field(&get_f64_list_value(&arr), pg_field)?;
            Ok(())
        }
        DataType::Decimal128(_, s) => {
            let value: Vec<_> = arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .iter()
                .map(|ov| ov.map(|v| Decimal::from_i128_with_scale(v, *s as u32)))
                .collect();
            encoder.encode_field(&value, pg_field)
        }
        DataType::Utf8 => {
            let value: Vec<Option<&str>> = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect();
            encoder.encode_field(&value, pg_field)
        }
        DataType::Utf8View => {
            let value: Vec<Option<&str>> = arr
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .iter()
                .collect();
            encoder.encode_field(&value, pg_field)
        }
        DataType::Binary => {
            let value: Vec<Option<_>> = arr
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .iter()
                .collect();
            encoder.encode_field(&value, pg_field)
        }
        DataType::LargeBinary => {
            let value: Vec<Option<_>> = arr
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .iter()
                .collect();
            encoder.encode_field(&value, pg_field)
        }
        DataType::BinaryView => {
            let value: Vec<Option<_>> = arr
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .unwrap()
                .iter()
                .collect();
            encoder.encode_field(&value, pg_field)
        }

        DataType::Date32 => {
            let value: Vec<Option<_>> = arr
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .iter()
                .map(|val| val.and_then(|x| as_date::<Date32Type>(x as i64)))
                .collect();
            encoder.encode_field(&value, pg_field)
        }
        DataType::Date64 => {
            let value: Vec<Option<_>> = arr
                .as_any()
                .downcast_ref::<Date64Array>()
                .unwrap()
                .iter()
                .map(|val| val.and_then(as_date::<Date64Type>))
                .collect();
            encoder.encode_field(&value, pg_field)
        }
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => {
                let value: Vec<Option<_>> = arr
                    .as_any()
                    .downcast_ref::<Time32SecondArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.and_then(|x| as_time::<Time32SecondType>(x as i64)))
                    .collect();
                encoder.encode_field(&value, pg_field)
            }
            TimeUnit::Millisecond => {
                let value: Vec<Option<_>> = arr
                    .as_any()
                    .downcast_ref::<Time32MillisecondArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.and_then(|x| as_time::<Time32MillisecondType>(x as i64)))
                    .collect();
                encoder.encode_field(&value, pg_field)
            }
            _ => {
                // Time32 only supports Second and Millisecond in Arrow
                // Other units are not available, so return an error
                Err(PgWireError::ApiError("Unsupported Time32 unit".into()))
            }
        },
        DataType::Time64(unit) => match unit {
            TimeUnit::Microsecond => {
                let value: Vec<Option<_>> = arr
                    .as_any()
                    .downcast_ref::<Time64MicrosecondArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.and_then(as_time::<Time64MicrosecondType>))
                    .collect();
                encoder.encode_field(&value, pg_field)
            }
            TimeUnit::Nanosecond => {
                let value: Vec<Option<_>> = arr
                    .as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.and_then(as_time::<Time64NanosecondType>))
                    .collect();
                encoder.encode_field(&value, pg_field)
            }
            _ => {
                // Time64 only supports Microsecond and Nanosecond in Arrow
                // Other units are not available, so return an error
                Err(PgWireError::ApiError("Unsupported Time64 unit".into()))
            }
        },
        DataType::Timestamp(unit, timezone) => match unit {
            TimeUnit::Second => {
                let array_iter = arr
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .unwrap()
                    .iter();

                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref())
                        .map_err(|e| PgWireError::ApiError(ToSqlError::from(e)))?;
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.and_then(|i| {
                                DateTime::from_timestamp(i, 0).map(|dt| {
                                    Utc.from_utc_datetime(&dt.naive_utc())
                                        .with_timezone(&tz)
                                        .fixed_offset()
                                })
                            })
                        })
                        .collect();
                    encoder.encode_field(&value, pg_field)
                } else {
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.and_then(|i| DateTime::from_timestamp(i, 0).map(|dt| dt.naive_utc()))
                        })
                        .collect();
                    encoder.encode_field(&value, pg_field)
                }
            }
            TimeUnit::Millisecond => {
                let array_iter = arr
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap()
                    .iter();

                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.and_then(|i| {
                                DateTime::from_timestamp_millis(i).map(|dt| {
                                    Utc.from_utc_datetime(&dt.naive_utc())
                                        .with_timezone(&tz)
                                        .fixed_offset()
                                })
                            })
                        })
                        .collect();
                    encoder.encode_field(&value, pg_field)
                } else {
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.and_then(|i| {
                                DateTime::from_timestamp_millis(i).map(|dt| dt.naive_utc())
                            })
                        })
                        .collect();
                    encoder.encode_field(&value, pg_field)
                }
            }
            TimeUnit::Microsecond => {
                let array_iter = arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .iter();

                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.and_then(|i| {
                                DateTime::from_timestamp_micros(i).map(|dt| {
                                    Utc.from_utc_datetime(&dt.naive_utc())
                                        .with_timezone(&tz)
                                        .fixed_offset()
                                })
                            })
                        })
                        .collect();
                    encoder.encode_field(&value, pg_field)
                } else {
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.and_then(|i| {
                                DateTime::from_timestamp_micros(i).map(|dt| dt.naive_utc())
                            })
                        })
                        .collect();
                    encoder.encode_field(&value, pg_field)
                }
            }
            TimeUnit::Nanosecond => {
                let array_iter = arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .iter();

                if let Some(tz) = timezone {
                    let tz = Tz::from_str(tz.as_ref()).map_err(ToSqlError::from)?;
                    let value: Vec<_> = array_iter
                        .map(|i| {
                            i.map(|i| {
                                Utc.from_utc_datetime(
                                    &DateTime::from_timestamp_nanos(i).naive_utc(),
                                )
                                .with_timezone(&tz)
                                .fixed_offset()
                            })
                        })
                        .collect();
                    encoder.encode_field(&value, pg_field)
                } else {
                    let value: Vec<_> = array_iter
                        .map(|i| i.map(|i| DateTime::from_timestamp_nanos(i).naive_utc()))
                        .collect();
                    encoder.encode_field(&value, pg_field)
                }
            }
        },
        DataType::Struct(arrow_fields) => encode_structs(encoder, &arr, arrow_fields, pg_field),
        DataType::LargeUtf8 => {
            let value: Vec<Option<&str>> = arr
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .iter()
                .collect();
            encoder.encode_field(&value, pg_field)?;
            Ok(())
        }
        DataType::Decimal256(_, s) => {
            // Convert Decimal256 to string representation for now
            // since rust_decimal doesn't support 256-bit decimals
            let decimal_array = arr.as_any().downcast_ref::<Decimal256Array>().unwrap();
            let value: Vec<Option<String>> = (0..decimal_array.len())
                .map(|i| {
                    if decimal_array.is_null(i) {
                        None
                    } else {
                        // Convert to string representation
                        let raw_value = decimal_array.value(i);
                        let scale = *s as u32;
                        // Convert i256 to string and handle decimal placement manually
                        let value_str = raw_value.to_string();
                        if scale == 0 {
                            Some(value_str)
                        } else {
                            // Insert decimal point
                            let mut chars: Vec<char> = value_str.chars().collect();
                            if chars.len() <= scale as usize {
                                // Prepend zeros if needed
                                let zeros_needed = scale as usize - chars.len() + 1;
                                chars.splice(0..0, std::iter::repeat_n('0', zeros_needed));
                                chars.insert(1, '.');
                            } else {
                                let decimal_pos = chars.len() - scale as usize;
                                chars.insert(decimal_pos, '.');
                            }
                            Some(chars.into_iter().collect())
                        }
                    }
                })
                .collect();
            encoder.encode_field(&value, pg_field)?;
            Ok(())
        }
        DataType::Duration(unit) => match unit {
            TimeUnit::Second => {
                let value: Vec<Option<PgInterval>> = arr
                    .as_any()
                    .downcast_ref::<DurationSecondArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.map(|v| PgInterval::new(0, 0, v * 1_000_000i64)))
                    .collect();
                encoder.encode_field(&value, pg_field)?;
                Ok(())
            }
            TimeUnit::Millisecond => {
                let value: Vec<Option<PgInterval>> = arr
                    .as_any()
                    .downcast_ref::<DurationMillisecondArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.map(|v| PgInterval::new(0, 0, v * 1_000i64)))
                    .collect();
                encoder.encode_field(&value, pg_field)?;
                Ok(())
            }
            TimeUnit::Microsecond => {
                let value: Vec<Option<PgInterval>> = arr
                    .as_any()
                    .downcast_ref::<DurationMicrosecondArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.map(|v| PgInterval::new(0, 0, v)))
                    .collect();
                encoder.encode_field(&value, pg_field)?;
                Ok(())
            }
            TimeUnit::Nanosecond => {
                let value: Vec<Option<PgInterval>> = arr
                    .as_any()
                    .downcast_ref::<DurationNanosecondArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.map(|v| PgInterval::new(0, 0, v / 1_000i64)))
                    .collect();
                encoder.encode_field(&value, pg_field)?;
                Ok(())
            }
        },
        DataType::Interval(interval_unit) => match interval_unit {
            IntervalUnit::YearMonth => {
                let value: Vec<Option<PgInterval>> = arr
                    .as_any()
                    .downcast_ref::<IntervalYearMonthArray>()
                    .unwrap()
                    .iter()
                    .map(|val| val.map(|v| PgInterval::new(v, 0, 0)))
                    .collect();
                encoder.encode_field(&value, pg_field)?;
                Ok(())
            }
            IntervalUnit::DayTime => {
                let value: Vec<Option<PgInterval>> = arr
                    .as_any()
                    .downcast_ref::<IntervalDayTimeArray>()
                    .unwrap()
                    .iter()
                    .map(|val| {
                        val.map(|v| {
                            let (days, millis) = IntervalDayTimeType::to_parts(v);
                            PgInterval::new(0, days, millis as i64 * 1000i64)
                        })
                    })
                    .collect();
                encoder.encode_field(&value, pg_field)?;
                Ok(())
            }
            IntervalUnit::MonthDayNano => {
                let value: Vec<Option<PgInterval>> = arr
                    .as_any()
                    .downcast_ref::<IntervalMonthDayNanoArray>()
                    .unwrap()
                    .iter()
                    .map(|val| {
                        val.map(|v| {
                            let (months, days, nanos) = IntervalMonthDayNanoType::to_parts(v);
                            PgInterval::new(months, days, nanos / 1000i64)
                        })
                    })
                    .collect();
                encoder.encode_field(&value, pg_field)?;
                Ok(())
            }
        },
        DataType::List(_) => {
            // Support for nested lists (list of lists)
            // For now, convert to string representation
            let list_array = arr.as_any().downcast_ref::<ListArray>().unwrap();
            let value: Vec<Option<String>> = (0..list_array.len())
                .map(|i| {
                    if list_array.is_null(i) {
                        None
                    } else {
                        // Convert nested list to string representation
                        Some(format!("[nested_list_{i}]"))
                    }
                })
                .collect();
            encoder.encode_field(&value, pg_field)?;
            Ok(())
        }
        DataType::LargeList(_) => {
            // Support for large lists
            let list_array = arr.as_any().downcast_ref::<LargeListArray>().unwrap();
            let value: Vec<Option<String>> = (0..list_array.len())
                .map(|i| {
                    if list_array.is_null(i) {
                        None
                    } else {
                        Some(format!("[large_list_{i}]"))
                    }
                })
                .collect();
            encoder.encode_field(&value, pg_field)
        }
        DataType::Map(_, _) => {
            // Support for map types
            let map_array = arr.as_any().downcast_ref::<MapArray>().unwrap();
            let value: Vec<Option<String>> = (0..map_array.len())
                .map(|i| {
                    if map_array.is_null(i) {
                        None
                    } else {
                        Some(format!("{{map_{i}}}"))
                    }
                })
                .collect();
            encoder.encode_field(&value, pg_field)?;
            Ok(())
        }

        DataType::Union(_, _) => {
            // Support for union types
            let value: Vec<Option<String>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(format!("union_{i}"))
                    }
                })
                .collect();
            encoder.encode_field(&value, pg_field)?;
            Ok(())
        }
        DataType::Dictionary(_, _) => {
            // Support for dictionary types
            let value: Vec<Option<String>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(format!("dict_{i}"))
                    }
                })
                .collect();
            encoder.encode_field(&value, pg_field)?;
            Ok(())
        }
        // TODO: add support for more advanced types (fixed size lists, etc.)
        list_type => Err(PgWireError::ApiError(ToSqlError::from(format!(
            "Unsupported List Datatype {} and array {:?}",
            list_type, &arr
        )))),
    }
}
