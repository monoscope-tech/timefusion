#!/bin/bash

# Multi-table per project demonstration script
# This script shows how to register multiple table types for a single project

echo "=== TimeFusion Multi-Table Demo ==="
echo ""

# Base URL for the TimeFusion API
BASE_URL="http://localhost:80"

# Test project configuration
PROJECT_ID="demo_project"
BUCKET="my-data-bucket"
ACCESS_KEY="your-access-key"
SECRET_KEY="your-secret-key"
ENDPOINT="https://s3.amazonaws.com"

echo "1. Registering OTEL logs and spans table for project: $PROJECT_ID"
curl -X POST "$BASE_URL/register_project" \
  -H "Content-Type: application/json" \
  -d "{
    \"project_id\": \"$PROJECT_ID\",
    \"bucket\": \"$BUCKET\",
    \"access_key\": \"$ACCESS_KEY\",
    \"secret_key\": \"$SECRET_KEY\",
    \"endpoint\": \"$ENDPOINT\",
    \"table_name\": \"otel_logs_and_spans\"
  }" | jq .

echo ""
echo "2. Registering metrics table for the same project: $PROJECT_ID"
curl -X POST "$BASE_URL/register_project" \
  -H "Content-Type: application/json" \
  -d "{
    \"project_id\": \"$PROJECT_ID\",
    \"bucket\": \"$BUCKET\",
    \"access_key\": \"$ACCESS_KEY\",
    \"secret_key\": \"$SECRET_KEY\",
    \"endpoint\": \"$ENDPOINT\",
    \"table_name\": \"metrics\"
  }" | jq .

echo ""
echo "3. Registering events table for the same project: $PROJECT_ID"
curl -X POST "$BASE_URL/register_project" \
  -H "Content-Type: application/json" \
  -d "{
    \"project_id\": \"$PROJECT_ID\",
    \"bucket\": \"$BUCKET\",
    \"access_key\": \"$ACCESS_KEY\",
    \"secret_key\": \"$SECRET_KEY\",
    \"endpoint\": \"$ENDPOINT\",
    \"table_name\": \"events\"
  }" | jq .

echo ""
echo "4. Listing all registered tables:"
curl -X GET "$BASE_URL/list_tables" | jq .

echo ""
echo "=== Demo Complete ==="
echo ""
echo "You can now query different tables using PostgreSQL wire protocol:"
echo "  - SELECT * FROM otel_logs_and_spans WHERE project_id = '$PROJECT_ID'"
echo "  - SELECT * FROM metrics WHERE project_id = '$PROJECT_ID'"
echo "  - SELECT * FROM events WHERE project_id = '$PROJECT_ID'"
echo ""
echo "Each table has its own schema and is stored in separate Delta Lake tables:"
echo "  - s3://$BUCKET/timefusion/projects/$PROJECT_ID/otel_logs_and_spans/"
echo "  - s3://$BUCKET/timefusion/projects/$PROJECT_ID/metrics/"
echo "  - s3://$BUCKET/timefusion/projects/$PROJECT_ID/events/"