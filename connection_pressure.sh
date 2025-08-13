#!/bin/bash

# Connection pressure test script that replicates the Rust test behavior
# This creates a connection storm by spawning all connections simultaneously

echo "Starting connection pressure test..."
echo "Clients: 100, Operations per client: 10"
echo "Connection timeout: 0.9s, Query timeout: 0.5s"
echo ""

start_time=$(date +%s)
successful=0
failed=0

# Launch 100 clients simultaneously
for i in {1..100}; do
  (
    # Each client performs 10 operations
    for j in {1..10}; do
      # Try to connect and execute query with timeout
      if timeout 0.9s psql -h localhost -p 12345 -U postgres -At -c "SELECT COUNT(*) FROM otel_logs_and_spans WHERE project_id = 'pressure_test';" postgres 2>/dev/null >/dev/null; then
        ((successful++))
      else
        ((failed++))
        echo "Connection/query failed for client $i op $j"
      fi
    done
  ) &
done

# Wait for all background jobs to complete
wait

end_time=$(date +%s)
duration=$((end_time - start_time))
total_ops=$((100 * 10))

echo ""
echo "=== Connection Pressure Test Results ==="
echo "Duration: ${duration}s"
echo "Total operations attempted: $total_ops"
echo "Note: Success/failure counts may be inaccurate due to subshell limitations"
echo ""
echo "To see real-time failures, check the output above"