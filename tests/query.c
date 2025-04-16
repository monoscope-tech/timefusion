#include <libpq-fe.h>
#include <stdio.h>
#include <stdlib.h>

int main() {
  const char *conninfo =
      "postgresql://postgres:postgres@localhost:12345/postgres";
  PGconn *conn = PQconnectdb(conninfo);

  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "Connection failed: %s", PQerrorMessage(conn));
    PQfinish(conn);
    return 1;
  }

  const char *sql =
      "INSERT INTO otel_logs_and_spans ("
      "project_id, timestamp, observed_timestamp, id, "
      "parent_id, name, kind, "
      "status_code, status_message, level, severity___severity_text, "
      "severity___severity_number, "
      "body, duration, start_time, end_time, "
      "context___trace_id, context___span_id, context___trace_state, "
      "context___trace_flags, "
      "context___is_remote, events, links, "
      "attributes___client___address, attributes___client___port, "
      "attributes___server___address, attributes___server___port, "
      "attributes___network___local__address, "
      "attributes___network___local__port, "
      "attributes___network___peer___address, "
      "attributes___network___peer__port, "
      "attributes___network___protocol___name, "
      "attributes___network___protocol___version, "
      "attributes___network___transport, attributes___network___type, "
      "attributes___code___number, attributes___code___file___path, "
      "attributes___code___function___name, attributes___code___line___number, "
      "attributes___code___stacktrace, attributes___log__record___original, "
      "attributes___log__record___uid, attributes___error___type, "
      "attributes___exception___type, attributes___exception___message, "
      "attributes___exception___stacktrace, attributes___url___fragment, "
      "attributes___url___full, attributes___url___path, "
      "attributes___url___query, attributes___url___scheme, "
      "attributes___user_agent___original, "
      "attributes___http___request___method, "
      "attributes___http___request___method_original, "
      "attributes___http___response___status_code, "
      "attributes___http___request___resend_count, "
      "attributes___http___request___body___size, "
      "attributes___session___id, attributes___session___previous___id, "
      "attributes___db___system___name, attributes___db___collection___name, "
      "attributes___db___namespace, attributes___db___operation___name, "
      "attributes___db___response___status_code, "
      "attributes___db___operation___batch___size, "
      "attributes___db___query___summary, attributes___db___query___text, "
      "attributes___user___id, attributes___user___email, "
      "attributes___user___full_name, attributes___user___name, "
      "attributes___user___hash, resource___service___name, "
      "resource___service___version, resource___service___instance___id, "
      "resource___service___namespace, resource___telemetry___sdk___language, "
      "resource___telemetry___sdk___name, "
      "resource___telemetry___sdk___version, resource___user_agent___original"
      ") VALUES "
      "("
      "'test_project_1', TIMESTAMP '2023-01-02T10:00:00Z', NULL, 'sql_span1', "
      "NULL, 'sql_test_span_1', NULL, "
      "'OK', 'span 1 inserted', 'INFO', NULL, NULL, "
      "NULL, 150000000, TIMESTAMP '2023-01-01T10:00:00Z', NULL, "
      "'trace1', 'span1', NULL, NULL, "
      "NULL, NULL, NULL, "
      "NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL"
      "),"
      "("
      "'test_project_2', TIMESTAMP '2023-01-03T11:00:00Z', NULL, 'sql_span2', "
      "NULL, 'sql_test_span_2', NULL, "
      "'OK', 'span 2 inserted', 'DEBUG', NULL, NULL, "
      "NULL, 200000000, TIMESTAMP '2023-01-02T11:00:00Z', NULL, "
      "'trace2', 'span2', NULL, NULL, "
      "NULL, NULL, NULL, "
      "NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL, NULL, "
      "NULL, NULL, NULL"
      ");";

  PGresult *res = PQexec(conn, sql);

  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    fprintf(stderr, "INSERT failed: %s", PQerrorMessage(conn));
    printf("Command: %s\n", PQcmdStatus(res));
    printf("Rows affected: %s\n", PQcmdTuples(res));

    PQclear(res);
    PQfinish(conn);
    return 1;
  }

  printf("Command: %s\n", PQcmdStatus(res));
  printf("Rows affected: %s\n", PQcmdTuples(res));

  printf("Multi-row INSERT successful.\n");

  PQclear(res);
  PQfinish(conn);
  return 0;
}
