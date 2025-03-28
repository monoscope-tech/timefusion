// Preference for stable attributes in the otel spec . With some exceptions
//
macro_rules! define_telemetry_fields {
    ($field_macro:ident) => {
        // Top-level fields
        $field_macro!(timestamp, DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))), false, i64);
        $field_macro!(
            observed_timestamp,
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            false,
            i64
        ); // Logs
        $field_macro!(id, DataType::Utf8, false, String);

        $field_macro!(parent_id, DataType::Utf8, true, Option<String>);
        $field_macro!(name, DataType::Utf8, false, String);
        $field_macro!(kind, DataType::Utf8, true, Option<String>);
        $field_macro!(status_code, DataType::Utf8, true, Option<String>);
        $field_macro!(status_message, DataType::Utf8, true, Option<String>);

        // Logs specific
        $field_macro!(level, DataType::Utf8, true, Option<String>); // same as severity text
        $field_macro!(severity___severity_text, DataType::Utf8, true, Option<String>);
        $field_macro!(severity___severity_number, DataType::Utf8, true, Option<String>);

        $field_macro!(duration, DataType::Duration(TimeUnit::Nanosecond), false, u64); // nanoseconds
        $field_macro!(start_time, DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))), false, i64);
        $field_macro!(
            end_time,
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            true,
            Option<i64>
        );

        // Context
        $field_macro!(context___trace_id, DataType::Utf8, false, String);
        $field_macro!(context___span_id, DataType::Utf8, false, String);
        $field_macro!(context___trace_state, DataType::Utf8, true, Option<String>);
        $field_macro!(context___trace_flags, DataType::Utf8, true, Option<String>);
        $field_macro!(context___is_remote, DataType::Utf8, true, Option<String>);

        // Events
        $field_macro!(events, DataType::Utf8, true, Option<String>); // events json

        // Links
        $field_macro!(links, DataType::Utf8, true, Option<String>); // links json

        // Attributes

        // Server and client
        $field_macro!(attributes___client___address, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___client___port, DataType::UInt32, true, Option<u32>);
        $field_macro!(attributes___server___address, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___server___port, DataType::UInt32, true, Option<u32>);

        // network https://opentelemetry.io/docs/specs/semconv/attributes-registry/network/
        $field_macro!(attributes___network___local__address, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___network___local__port, DataType::Utf8, true, Option<u32>);
        $field_macro!(attributes___network___peer___address, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___network___peer__port, DataType::Utf8, true, Option<u32>);
        $field_macro!(attributes___network___protocol___name, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___network___protocol___version, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___network___transport, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___network___type, DataType::Utf8, true, Option<String>);

        // Source Code Attributes
        $field_macro!(attributes___code___number, DataType::UInt32, true, Option<u32>);
        $field_macro!(attributes___code___file___path, DataType::Utf8, true, Option<u32>);
        $field_macro!(attributes___code___function___name, DataType::Utf8, true, Option<u32>);
        $field_macro!(attributes___code___line___number, DataType::Utf8, true, Option<u32>);
        $field_macro!(attributes___code___stacktrace, DataType::Utf8, true, Option<u32>);

        // Log records. https://opentelemetry.io/docs/specs/semconv/general/logs/
        $field_macro!(attributes___log__record___original, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___log__record___uid, DataType::Utf8, true, Option<String>);

        // Exception https://opentelemetry.io/docs/specs/semconv/exceptions/exceptions-logs/
        $field_macro!(attributes___error___type, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___exception___type, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___exception___message, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___exception___stacktrace, DataType::Utf8, true, Option<String>);

        // URL https://opentelemetry.io/docs/specs/semconv/attributes-registry/url/
        $field_macro!(attributes___url___fragment, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___url___full, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___url___path, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___url___query, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___url___scheme, DataType::Utf8, true, Option<String>);

        // Useragent https://opentelemetry.io/docs/specs/semconv/attributes-registry/user-agent/
        $field_macro!(attributes___user_agent___original, DataType::Utf8, true, Option<String>);

        // HTTP https://opentelemetry.io/docs/specs/semconv/http/http-spans/
        $field_macro!(attributes___http___request___method, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___http___request___method_original, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___http___response___status_code, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___http___request___resend_count, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___http___request___body___size, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___http___response___status_code, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___http___response___status_code, DataType::Utf8, true, Option<String>);

        // Session https://opentelemetry.io/docs/specs/semconv/general/session/
        $field_macro!(attributes___session___id, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___session___previous___id, DataType::Utf8, true, Option<String>);

        // Database https://opentelemetry.io/docs/specs/semconv/database/database-spans/
        $field_macro!(attributes___db___system___name, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___db___collection___name, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___db___namespace, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___db___operation___name, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___db___response___status_code, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___db___operation___batch___size, DataType::UInt32, true, Option<u32>);
        $field_macro!(attributes___db___query___summary, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___db___query___text, DataType::Utf8, true, Option<String>);

        // https://opentelemetry.io/docs/specs/semconv/attributes-registry/user/  in development but
        // adopted.
        $field_macro!(attributes___user___id, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___user___email, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___user___full_name, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___user___name, DataType::Utf8, true, Option<String>);
        $field_macro!(attributes___user___hash, DataType::Utf8, true, Option<String>);

        // Resource Attributes (subset) https://opentelemetry.io/docs/specs/semconv/resource/
        $field_macro!(resource___attributes___service___name, DataType::Utf8, true, Option<String>);
        $field_macro!(resource___attributes___service___version, DataType::Utf8, true, Option<String>);
        $field_macro!(resource___attributes___service___instance___id, DataType::Utf8, true, Option<String>);
        $field_macro!(resource___attributes___service___namespace, DataType::Utf8, true, Option<String>);

        $field_macro!(resource___attributes___telemetry___sdk___language, DataType::Utf8, true, Option<String>);
        $field_macro!(resource___attributes___telemetry___sdk___name, DataType::Utf8, true, Option<String>);
        $field_macro!(resource___attributes___telemetry___sdk___version, DataType::Utf8, true, Option<String>);
        $field_macro!(resource___attributes___telemetry___sdk___name, DataType::Utf8, true, Option<String>);

        $field_macro!(resource___attributes___user_agent___original, DataType::Utf8, true, Option<String>);
    };
}

pub(crate) use define_telemetry_fields;
