# TimeFusion JSON and Date/Time Functions Summary

## Date/Time Functions

### EXTRACT Function (✅ Available - DataFusion Built-in)
The EXTRACT function is fully available and works with the following date parts:

```sql
-- Extract year
SELECT EXTRACT(YEAR FROM timestamp) -- Returns: 2024

-- Extract month
SELECT EXTRACT(MONTH FROM timestamp) -- Returns: 1

-- Extract day
SELECT EXTRACT(DAY FROM timestamp) -- Returns: 15

-- Extract hour
SELECT EXTRACT(HOUR FROM timestamp) -- Returns: 14

-- Extract minute
SELECT EXTRACT(MINUTE FROM timestamp) -- Returns: 30

-- Extract second (integer only, no fractional seconds)
SELECT EXTRACT(SECOND FROM timestamp) -- Returns: 45

-- Extract day of week (Sunday = 0)
SELECT EXTRACT(DOW FROM timestamp)

-- Extract day of year
SELECT EXTRACT(DOY FROM timestamp)

-- Extract quarter
SELECT EXTRACT(QUARTER FROM timestamp)

-- Extract week
SELECT EXTRACT(WEEK FROM timestamp)
```

### date_part Function (✅ Available - DataFusion Built-in)
The `date_part` function is available as an alias for EXTRACT:

```sql
SELECT date_part('year', timestamp) -- Returns: 2024
SELECT date_part('month', timestamp) -- Returns: 1
```

### Custom Date/Time Functions (✅ Available - Implemented in functions.rs)

#### to_char
Formats timestamps according to PostgreSQL-style format patterns:

```sql
SELECT to_char(timestamp, 'YYYY-MM-DD') -- Returns: '2024-01-15'
SELECT to_char(timestamp, 'YYYY-MM-DD HH24:MI:SS') -- Returns: '2024-01-15 14:30:45'
SELECT to_char(timestamp, 'Month DD, YYYY') -- Returns: 'January 15, 2024'
SELECT to_char(timestamp, 'Mon DD, YYYY') -- Returns: 'Jan 15, 2024'
```

#### at_time_zone
Converts timestamps to different timezones (preserves the instant in time):

```sql
SELECT at_time_zone(timestamp, 'America/New_York')
SELECT at_time_zone(timestamp, 'Asia/Tokyo')
```

## JSON Functions

### datafusion-functions-json (⚠️ Registered but NOT working)
The following functions are registered via `datafusion_functions_json::register_all()` but fail with Union datatype errors when used with the current schema:

- `json_get` - Extract any value from JSON path
- `json_get_str` - Extract string value from JSON path
- `json_get_int` - Extract integer value from JSON path
- `json_get_float` - Extract float value from JSON path
- `json_get_bool` - Extract boolean value from JSON path
- `json_length` - Get length of JSON array/object
- `json_contains` - Check if JSON contains a value
- `json_keys` - Get keys of a JSON object

**Error Example:**
```
Postgres error: db error: ERROR: Unsupported Datatype Union([(0, Field { name: "null", data_type: Null, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), (1, Field { name: "bool", data_type: Boolean, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: {} }), ...])
```

### PostgreSQL-style JSON Construction Functions (❌ NOT Available)
The following functions are NOT available:

- `json_build_array` - Build JSON array from values
- `json_build_object` - Build JSON object from key-value pairs
- `to_json` - Convert value to JSON
- `json_object` - Create JSON object
- `json_agg` - Aggregate values into JSON array
- `row_to_json` - Convert row to JSON

### Custom JSON Functions (⚠️ Placeholder only)

#### jsonb_array_elements
This function is registered in `functions.rs` but is not implemented:

```rust
// Note: This is a placeholder implementation
// A full implementation would require table function support in DataFusion
not_impl_err!("jsonb_array_elements is not yet fully implemented - requires table function support")
```

## Working with JSON in TimeFusion

Currently, JSON data can be:
1. **Stored** as strings in VARCHAR columns (e.g., `status_message`)
2. **Retrieved** as plain text
3. **Filtered** using string operations (LIKE, =, etc.)

But JSON path extraction and manipulation functions are not functional due to datatype compatibility issues.

## Recommendations

1. **For Date/Time operations**: Use EXTRACT, date_part, and to_char functions which work well
2. **For JSON operations**: 
   - Store JSON as strings for now
   - Consider parsing JSON in the application layer
   - Or implement custom UDFs that handle the string-to-JSON conversion properly
3. **Future improvements**:
   - Fix the Union datatype issue to enable datafusion-functions-json
   - Implement proper jsonb_array_elements when table functions are supported
   - Consider adding more PostgreSQL-compatible JSON construction functions