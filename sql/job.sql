-- === Kafka sources & sinks ===
-- Adjust topic names if needed. Expect JSON payloads.

-- Main events table (from analytical platform ingestion)
CREATE TABLE events (
                        topic STRING,
                        ts_event TIMESTAMP(3),
                        payload STRING,
                        WATERMARK FOR ts_event AS ts_event - INTERVAL '10' SECOND
) WITH (
      'connector' = 'kafka',
      'topic' = 'events',
      'properties.bootstrap.servers' = 'kafka:9092',
      'properties.group.id' = 'flink-events',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json',
      'json.ignore-parse-errors' = 'true'
      );

-- Audit summaries table published by the Audit Agent
CREATE TABLE audit_counts (
                              topic_pattern STRING,
                              window_start TIMESTAMP(3),
                              window_end   TIMESTAMP(3),
                              count        BIGINT,
                              agent_id     STRING,
                              watermark_seen TIMESTAMP(3),
                              payload_crc  STRING,
                              WATERMARK FOR window_end AS window_end
) WITH (
      'connector' = 'kafka',
      'topic' = 'audit_counts',
      'properties.bootstrap.servers' = 'kafka:9092',
      'properties.group.id' = 'flink-audit',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json',
      'json.ignore-parse-errors' = 'true'
      );

-- Sink: mismatches
CREATE TABLE audit_mismatches (
                                  topic_pattern STRING,
                                  window_start TIMESTAMP(3),
                                  window_end   TIMESTAMP(3),
                                  count_agent BIGINT,
                                  count_platform BIGINT,
                                  status STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'audit_mismatches',
      'properties.bootstrap.servers' = 'kafka:9092',
      'format' = 'json'
      );

-- Optional: status/OKs (for dashboards)
CREATE TABLE audit_status (
                              topic_pattern STRING,
                              window_start TIMESTAMP(3),
                              window_end   TIMESTAMP(3),
                              count_agent BIGINT,
                              count_platform BIGINT,
                              status STRING
) WITH (
      'connector' = 'kafka',
      'topic' = 'audit_status',
      'properties.bootstrap.servers' = 'kafka:9092',
      'format' = 'json'
      );

-- === Windowed platform counts (align with agent window size) ===
-- Adjust INTERVAL to your agent's WINDOW_SECONDS (default 5s)
WITH platform_counts AS (
    SELECT
        TUMBLE_START(ts_event, INTERVAL '5' SECOND) AS w_start,
        TUMBLE_END(ts_event,   INTERVAL '5' SECOND) AS w_end,
        COUNT(*) AS count_platform
    FROM events
    GROUP BY TUMBLE(ts_event, INTERVAL '5' SECOND)
)

-- Join platform counts with audit summaries by window
INSERT INTO audit_status
SELECT
    a.topic_pattern,
    a.window_start,
    a.window_end,
    a.count AS count_agent,
    p.count_platform,
    CASE WHEN p.count_platform = a.count THEN 'OK' ELSE 'MISMATCH' END AS status
FROM platform_counts p
         JOIN audit_counts a
              ON p.w_start = a.window_start AND p.w_end = a.window_end;

-- Emit only mismatches to a dedicated topic (for alerting)
INSERT INTO audit_mismatches
SELECT * FROM audit_status WHERE status = 'MISMATCH';


-- === Notes ===
-- 1) Ensure your ingestion writes the agent summaries to Kafka topic `audit_counts`.
--    If your audit agent publishes to MQTT, bridge `_audit/counts/#` to Kafka `audit_counts`.
-- 2) `events` expects JSON with fields: topic, ts_event (RFC3339 or ISO), payload.
--    If your schema differs, adjust the DDL accordingly.
-- 3) To tolerate late data, adjust the watermark on `events` and perhaps use larger tumbling windows.
