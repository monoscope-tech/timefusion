# PostgreSQL-based Configuration for TimeFusion (Optional)

TimeFusion supports two configuration modes:

1. **Default Mode (No Config Database)**: All projects are stored in a single default S3 bucket
2. **Configured Mode (With Config Database)**: Projects can be stored in different S3 buckets/accounts

## Default Mode (No Configuration Database)

When `TIMEFUSION_CONFIG_DATABASE_URL` is NOT set:
- All projects automatically use the default S3 bucket specified in `AWS_S3_BUCKET`
- No registration needed - projects are created on first data insertion
- All projects share the same S3 credentials

```bash
# Required environment variables for default mode
AWS_S3_BUCKET=my-default-bucket
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_REGION=us-east-1
TIMEFUSION_TABLE_PREFIX=timefusion  # Optional, default: "timefusion"
```

In this mode, any project_id sent to TimeFusion will automatically create tables at:
```
s3://my-default-bucket/timefusion/projects/{project_id}/{table_name}/
```

## Configured Mode (With PostgreSQL Database)

When `TIMEFUSION_CONFIG_DATABASE_URL` IS set:
- Projects must be registered in the configuration database
- Each project can have different S3 buckets and credentials
- Enables multi-account/multi-region S3 storage

```bash
# PostgreSQL database URL for configuration
TIMEFUSION_CONFIG_DATABASE_URL=postgresql://user:password@host:port/database

# Optional: Configuration polling interval (default: 30 seconds)
TIMEFUSION_CONFIG_POLL_INTERVAL_SECS=30

# Optional: Default S3 bucket for unregistered projects
AWS_S3_BUCKET=my-default-bucket
AWS_ACCESS_KEY_ID=default-access-key
AWS_SECRET_ACCESS_KEY=default-secret-key
```

### Mixed Mode Behavior

When both `TIMEFUSION_CONFIG_DATABASE_URL` and `AWS_S3_BUCKET` are set:
- Projects registered in the database use their specific S3 settings
- Unregistered projects automatically use the default S3 bucket
- This enables gradual migration and testing

## Database Schema

TimeFusion automatically creates the following table in your PostgreSQL database:

```sql
CREATE TABLE timefusion_projects (
    project_id VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    s3_bucket VARCHAR(255) NOT NULL,
    s3_prefix VARCHAR(500) NOT NULL,
    s3_region VARCHAR(100) NOT NULL,
    s3_access_key_id VARCHAR(500) NOT NULL,
    s3_secret_access_key VARCHAR(500) NOT NULL,
    s3_endpoint VARCHAR(500),  -- Optional, for S3-compatible services
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, table_name)
);
```

## Adding Projects at Runtime

Projects can be added to the configuration database at any time. TimeFusion will automatically detect and load new projects within the polling interval.

### Example: Register a Project with Different S3 Bucket

```sql
-- This project will use a completely different S3 account/bucket
INSERT INTO timefusion_projects (
    project_id,
    table_name,
    s3_bucket,
    s3_prefix,
    s3_region,
    s3_access_key_id,
    s3_secret_access_key,
    s3_endpoint,
    is_active
) VALUES (
    'customer-xyz',
    'otel_logs_and_spans',
    'customer-xyz-bucket',  -- Different bucket
    'timefusion/data/otel_logs_and_spans',
    'eu-west-1',  -- Different region
    'CUSTOMER_ACCESS_KEY',  -- Different credentials
    'CUSTOMER_SECRET_KEY',
    'https://s3.eu-west-1.amazonaws.com',
    true
);
```

### Example: Update Project Configuration

```sql
UPDATE timefusion_projects 
SET 
    s3_bucket = 'new-bucket',
    updated_at = NOW()
WHERE 
    project_id = 'my-project-id' 
    AND table_name = 'otel_logs_and_spans';
```

### Example: Disable a Project

```sql
UPDATE timefusion_projects 
SET 
    is_active = false,
    updated_at = NOW()
WHERE 
    project_id = 'my-project-id';
```

## Multiple Tables per Project

Each project can have multiple tables. Simply insert a new row with the same `project_id` but different `table_name`:

```sql
-- Add a metrics table to existing project
INSERT INTO timefusion_projects (
    project_id,
    table_name,
    s3_bucket,
    s3_prefix,
    s3_region,
    s3_access_key_id,
    s3_secret_access_key
) VALUES (
    'my-project-id',
    'metrics',
    'my-bucket',
    'timefusion/projects/my-project-id/metrics',
    'us-east-1',
    'YOUR_ACCESS_KEY',
    'YOUR_SECRET_KEY'
);
```

## Configuration Watcher

TimeFusion includes an automatic configuration watcher that:
- Polls the PostgreSQL database every 30 seconds (configurable)
- Loads new projects automatically
- Updates existing project configurations
- Removes disabled projects from memory

The watcher runs in the background and doesn't block normal operations.

## Shared Configuration

Multiple TimeFusion instances can share the same configuration database. This enables:
- Horizontal scaling with consistent configuration
- Centralized project management
- Dynamic load distribution
- Multi-tenant architectures

## Security Considerations

1. **Database Credentials**: Store the `TIMEFUSION_CONFIG_DATABASE_URL` securely (e.g., using environment variables or secrets management)
2. **S3 Credentials**: Consider using IAM roles or temporary credentials instead of long-lived access keys
3. **Network Security**: Ensure PostgreSQL connections are encrypted (use SSL/TLS)
4. **Access Control**: Limit PostgreSQL user permissions to only what's needed:

```sql
-- Create a dedicated user for TimeFusion
CREATE USER timefusion_config WITH PASSWORD 'secure_password';

-- Grant only necessary permissions
GRANT CONNECT ON DATABASE your_database TO timefusion_config;
GRANT USAGE ON SCHEMA public TO timefusion_config;
GRANT SELECT, INSERT, UPDATE ON timefusion_projects TO timefusion_config;
```

## Migration from Environment Variables

If you're migrating from environment-based configuration:

1. Set up your PostgreSQL database
2. Insert your existing project configuration:

```sql
INSERT INTO timefusion_projects (
    project_id,
    table_name,
    s3_bucket,
    s3_prefix,
    s3_region,
    s3_access_key_id,
    s3_secret_access_key,
    s3_endpoint
) VALUES (
    'your-project-uuid',  -- Use actual project UUID
    'otel_logs_and_spans',
    'your-existing-bucket',
    'timefusion/projects/your-project-uuid/otel_logs_and_spans',
    'your-region',
    'your-access-key',
    'your-secret-key',
    'your-endpoint'  -- if using MinIO or similar
);
```

3. Update your TimeFusion deployment with `TIMEFUSION_CONFIG_DATABASE_URL`
4. Remove the old environment variables (`AWS_S3_BUCKET`, etc.)