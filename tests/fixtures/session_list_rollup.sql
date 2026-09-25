-- Monoscope Models.Apis.LogQueries.fetchSessions at 8f1aaa2fe.
-- Fixed inputs: project=project, one-day range, 3600-second buckets,
-- last_seen sort, offset=0, limit=50, no optional environment/service/user filters.
-- Routing fixture only; no customer data or production query execution.
WITH filtered AS (
    SELECT COALESCE(NULLIF(attributes___session___id, ''), NULLIF(attributes___user___id, ''), NULLIF(attributes___user___email, '')) AS session_id,
        attributes___user___id AS user_id,
        attributes___user___email AS user_email,
        COALESCE(NULLIF(attributes___user___full_name, ''), NULLIF(attributes___user___name, '')) AS user_name,
        resource___service___name AS service_name,
        context___trace_id AS trace_id,
        attributes___url___path AS url_path,
        attributes___user_agent___original AS user_agent,
        COALESCE(NULLIF(status_message, ''), NULLIF(body::text, '')) AS error_text,
        (lower(level) = 'error' OR severity___severity_number >= 17 OR status_code = 'ERROR') AS is_error,
        timestamp, end_time, level, severity___severity_number, status_code,
        floor(extract(epoch from timestamp) / 3600)::BIGINT AS bi
    FROM otel_logs_and_spans
    WHERE project_id = 'project'
        AND timestamp >= to_timestamp_micros(0) AND timestamp < to_timestamp_micros(86400000000)
        AND COALESCE(NULLIF(attributes___session___id, ''), NULLIF(attributes___user___id, ''), NULLIF(attributes___user___email, '')) IS NOT NULL
), agg AS (
    SELECT session_id,
        MAX(user_id) AS user_id, MAX(user_email) AS user_email, MAX(user_name) AS user_name,
        COUNT(*)::BIGINT AS event_count,
        COUNT(*) FILTER (WHERE is_error)::BIGINT AS error_count,
        MIN(timestamp) AS first_seen,
        MAX(COALESCE(end_time, timestamp)) AS last_seen,
        (EXTRACT(EPOCH FROM (MAX(COALESCE(end_time, timestamp)) - MIN(timestamp))) * 1000000000)::BIGINT AS duration_ns,
        distinct_count(approx_count_distinct(trace_id))::BIGINT AS trace_count,
        (ARRAY_AGG(url_path ORDER BY timestamp) FILTER (WHERE url_path IS NOT NULL AND url_path <> ''))[1] AS landing_url,
        (ARRAY_AGG(user_agent ORDER BY timestamp) FILTER (WHERE user_agent IS NOT NULL AND user_agent <> ''))[1] AS user_agent,
        (ARRAY_AGG(error_text ORDER BY timestamp) FILTER (WHERE is_error AND error_text IS NOT NULL AND error_text <> ''))[1] AS first_error
    FROM filtered GROUP BY session_id
), sess_bkt AS (
    SELECT floor(extract(epoch from first_seen) / 3600)::BIGINT AS bi, (error_count > 0) AS has_err FROM agg
), bkt AS (
    SELECT bi, COUNT(*) FILTER (WHERE NOT has_err)::BIGINT AS c, COUNT(*) FILTER (WHERE has_err)::BIGINT AS e
    FROM sess_bkt GROUP BY bi
), summ AS (
    SELECT COUNT(*)::BIGINT AS total_sessions,
        COUNT(*) FILTER (WHERE error_count > 0)::BIGINT AS errored_sessions,
        distinct_count(approx_count_distinct(COALESCE(user_id, user_email)) FILTER (WHERE COALESCE(user_id, user_email) IS NOT NULL))::BIGINT AS unique_users,
        (SELECT distinct_count(approx_count_distinct(service_name) FILTER (WHERE service_name IS NOT NULL))::BIGINT FROM filtered) AS unique_services,
        COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_ns), 0)::BIGINT AS med_dur,
        COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY duration_ns), 0)::BIGINT AS p95_dur,
        COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY event_count), 0)::BIGINT AS med_evt,
        COALESCE(SUM(event_count), 0)::BIGINT AS total_events,
        COALESCE((SELECT ARRAY_AGG(bi ORDER BY bi) FROM bkt), '{}'::BIGINT[]) AS bis,
        COALESCE((SELECT ARRAY_AGG(c ORDER BY bi) FROM bkt), '{}'::BIGINT[]) AS clean_bkt,
        COALESCE((SELECT ARRAY_AGG(e ORDER BY bi) FROM bkt), '{}'::BIGINT[]) AS err_bkt
    FROM agg
), page AS (
    SELECT * FROM agg ORDER BY last_seen DESC NULLS LAST OFFSET 0 LIMIT 50
), svcs AS (
    SELECT session_id, ARRAY_REMOVE(ARRAY_AGG(DISTINCT service_name), NULL) AS services
    FROM filtered WHERE session_id IN (SELECT session_id FROM page) GROUP BY session_id
)
SELECT p.session_id, p.user_id, p.user_email, p.user_name,
    p.event_count, p.error_count, p.first_seen,
    p.last_seen, p.duration_ns, p.trace_count,
    COALESCE(s.services, '{}'::TEXT[]),
    p.landing_url, p.user_agent, p.first_error,
    sm.total_sessions, sm.errored_sessions, sm.unique_users, sm.unique_services,
    sm.med_dur, sm.p95_dur, sm.med_evt, sm.total_events,
    sm.bis, sm.clean_bkt, sm.err_bkt
FROM page p LEFT JOIN svcs s USING (session_id)
CROSS JOIN summ sm
ORDER BY p.last_seen DESC NULLS LAST
