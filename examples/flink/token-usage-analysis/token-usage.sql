-- Consume tapes node events from Kafka and aggregate token usage over short windows.
-- Usage lives on response nodes at node.usage and is not part of the content-addressed bucket.

CREATE TABLE tapes_events (
  payload STRING,
  event_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'tapes.e2e.proxy',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'tapes-flink-token-usage-analyzer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

CREATE TABLE token_usage_kafka (
  window_start STRING,
  window_end STRING,
  provider STRING,
  model STRING,
  project STRING,
  response_count BIGINT,
  prompt_tokens BIGINT,
  completion_tokens BIGINT,
  total_tokens BIGINT,
  cache_creation_input_tokens BIGINT,
  cache_read_input_tokens BIGINT,
  total_duration_ns BIGINT,
  prompt_duration_ns BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'tapes.analytics.token_usage',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE token_usage_print (
  window_start STRING,
  window_end STRING,
  provider STRING,
  model STRING,
  project STRING,
  response_count BIGINT,
  prompt_tokens BIGINT,
  completion_tokens BIGINT,
  total_tokens BIGINT,
  cache_creation_input_tokens BIGINT,
  cache_read_input_tokens BIGINT,
  total_duration_ns BIGINT,
  prompt_duration_ns BIGINT
) WITH (
  'connector' = 'print'
);

EXECUTE STATEMENT SET
BEGIN
INSERT INTO token_usage_kafka
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end AS STRING) AS window_end,
  COALESCE(JSON_VALUE(payload, '$.node.bucket.provider'), '') AS provider,
  COALESCE(JSON_VALUE(payload, '$.node.bucket.model'), '') AS model,
  COALESCE(JSON_VALUE(payload, '$.node.project'), '') AS project,
  COUNT(*) AS response_count,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.prompt_tokens') AS BIGINT), 0)) AS prompt_tokens,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.completion_tokens') AS BIGINT), 0)) AS completion_tokens,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.total_tokens') AS BIGINT), 0)) AS total_tokens,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.cache_creation_input_tokens') AS BIGINT), 0)) AS cache_creation_input_tokens,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.cache_read_input_tokens') AS BIGINT), 0)) AS cache_read_input_tokens,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.total_duration_ns') AS BIGINT), 0)) AS total_duration_ns,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.prompt_duration_ns') AS BIGINT), 0)) AS prompt_duration_ns
FROM TABLE(TUMBLE(TABLE tapes_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
WHERE JSON_QUERY(payload, '$.node.usage') IS NOT NULL
GROUP BY window_start, window_end,
  COALESCE(JSON_VALUE(payload, '$.node.bucket.provider'), ''),
  COALESCE(JSON_VALUE(payload, '$.node.bucket.model'), ''),
  COALESCE(JSON_VALUE(payload, '$.node.project'), '');

INSERT INTO token_usage_print
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end AS STRING) AS window_end,
  COALESCE(JSON_VALUE(payload, '$.node.bucket.provider'), '') AS provider,
  COALESCE(JSON_VALUE(payload, '$.node.bucket.model'), '') AS model,
  COALESCE(JSON_VALUE(payload, '$.node.project'), '') AS project,
  COUNT(*) AS response_count,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.prompt_tokens') AS BIGINT), 0)) AS prompt_tokens,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.completion_tokens') AS BIGINT), 0)) AS completion_tokens,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.total_tokens') AS BIGINT), 0)) AS total_tokens,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.cache_creation_input_tokens') AS BIGINT), 0)) AS cache_creation_input_tokens,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.cache_read_input_tokens') AS BIGINT), 0)) AS cache_read_input_tokens,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.total_duration_ns') AS BIGINT), 0)) AS total_duration_ns,
  SUM(COALESCE(CAST(JSON_VALUE(payload, '$.node.usage.prompt_duration_ns') AS BIGINT), 0)) AS prompt_duration_ns
FROM TABLE(TUMBLE(TABLE tapes_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
WHERE JSON_QUERY(payload, '$.node.usage') IS NOT NULL
GROUP BY window_start, window_end,
  COALESCE(JSON_VALUE(payload, '$.node.bucket.provider'), ''),
  COALESCE(JSON_VALUE(payload, '$.node.bucket.model'), ''),
  COALESCE(JSON_VALUE(payload, '$.node.project'), '');
END;
