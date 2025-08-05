# Timefusion

A very specialized timeseries database created for events, logs, traces and metrics.

Its designed to allow users plug in their own s3 storage and buckets and have their stored to their accounts.
This way, timefusion is used as a compute and cache engine, not primary data storage.

Timefusion speaks the postgres dialect, so you can insert and read from it using any postgres client or driver.

## Configuration

Timefusion can be configured using the following environment variables:

| Variable               | Description                                      | Default                     |
| ---------------------- | ------------------------------------------------ | --------------------------- |
| `PORT`                 | HTTP server port                                 | `80`                        |
| `PGWIRE_PORT`          | PostgreSQL wire protocol port                    | `5432`                      |
| `AWS_S3_BUCKET`        | AWS S3 bucket name                               | Required                    |
| `AWS_S3_ENDPOINT`      | AWS S3 endpoint URL                              | `https://s3.amazonaws.com`  |
| `AWS_ACCESS_KEY_ID`    | AWS access key                                   | -                           |
| `AWS_SECRET_ACCESS_KEY`| AWS secret key                                   | -                           |
| `AWS_S3_LOCKING_PROVIDER` | Delta Lake locking provider ('dynamodb')      | -                           |
| `DELTA_DYNAMO_TABLE_NAME` | DynamoDB table name for Delta Lake locking    | -                           |
| `TIMEFUSION_TABLE_PREFIX` | Prefix for Delta tables                       | `timefusion`                |
| `BATCH_INTERVAL_MS`    | Interval between batch inserts in milliseconds   | `1000`                      |
| `MAX_BATCH_SIZE`       | Maximum number of rows in a single batch         | `1000`                      |
| `ENABLE_BATCH_QUEUE`   | Whether to use batch queue for inserts           | `false` (direct insertion)  |
| `MAX_PG_CONNECTIONS`   | Maximum number of concurrent PostgreSQL connections | `100`                     |

For local development, you can set `QUEUE_DB_PATH` to a location in your development environment.

### Delta Lake DynamoDB Locking

For multi-writer scenarios where multiple instances of TimeFusion may write to the same Delta tables concurrently, it's recommended to enable DynamoDB locking:

1. Create a DynamoDB table with the following configuration:
   - Table name: Choose any name (e.g., `timefusion-delta-locks`)
   - Partition key: `key` (String type)
   - On-demand billing mode is recommended

2. Set the following environment variables:
   ```
   AWS_S3_LOCKING_PROVIDER=dynamodb
   DELTA_DYNAMO_TABLE_NAME=timefusion-delta-locks
   ```

3. Ensure your AWS credentials have the following DynamoDB permissions:
   - `dynamodb:GetItem`
   - `dynamodb:PutItem`
   - `dynamodb:UpdateItem`
   - `dynamodb:DeleteItem`

This configuration ensures safe concurrent writes to Delta tables by using DynamoDB for distributed locking.

**Note for S3-Compatible Storage (e.g., OVH, MinIO)**: When using S3-compatible stores that don't support conditional PUT operations, DynamoDB locking is strongly recommended to prevent data corruption in multi-writer scenarios. See [DELTA_CONFIG.md](DELTA_CONFIG.md) for detailed configuration options and trade-offs.

## Usage

There currently exists only 1 table. otel_logs_and_spans.
You can access it via psql: eg if running locally:

```
$ psql "postgresql://postgres:postgres@localhost:12345/postgres"

psql (16.8 (Homebrew), server 0.28.0)
WARNING: psql major version 16, server major version 0.28.
         Some psql features might not work.
Type "help" for help.

postgres=> insert into otel_logs_and_spans (name, id, project_id, hashes, timestamp, date) values ('name3', 'id2', 'pid3', ARRAY[], '2025-04-14 02:00:24.898000', '2025-04-14 02:00:24.898000');
INSERT 0 1

postgres=> select name, id, project_id,timestamp from otel_logs_and_spans limit 10;
                            name                             |                  id                  |              project_id              |         timestamp
-------------------------------------------------------------+--------------------------------------+--------------------------------------+----------------------------
 GET api/v1/validations/profundity-interior/(?P<pk>[^/.]+)/$ | 00000000-09ab-47bc-b628-2554626d1261 | 00000000-876e-41fa-be63-52d5bcfc037e | 2025-04-14 20:45:08.713740
 GET api/v1/validations/tire-pressure/(?P<pk>[^/.]+)/$       | 00000000-3d2a-445d-b7bf-3e56125b48d4 | 00000000-876e-41fa-be63-52d5bcfc037e | 2025-04-14 22:01:00.816390
 POST api/v1/validations/warnings-of-wear/$                  | 00000000-4ced-48f4-830d-64d3531eb7f0 | 00000000-876e-41fa-be63-52d5bcfc037e | 2025-04-14 21:18:08.635637

```

```

```
