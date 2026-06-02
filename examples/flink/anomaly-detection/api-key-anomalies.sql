-- Consume tapes node events from Kafka and surface content text that looks like an API key.
-- The tapes event shape is publisher.Event JSON. Text lives in node.bucket.content[].
-- This demo scans that content array JSON for api_... tokens and handles JSON-escaped
-- quotes around the key, e.g. "My API key is \"api_abc123\" !".

CREATE TABLE tapes_events (
  payload STRING,
  event_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'tapes.e2e.proxy',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'tapes-flink-api-key-anomaly-detector',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

CREATE TABLE api_key_anomalies_kafka (
  detected_at STRING,
  root_hash STRING,
  node_hash STRING,
  role STRING,
  provider STRING,
  api_key STRING,
  content_json STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'tapes.anomalies.api_keys',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE api_key_anomalies_print (
  detected_at STRING,
  root_hash STRING,
  node_hash STRING,
  role STRING,
  provider STRING,
  api_key STRING,
  content_json STRING
) WITH (
  'connector' = 'print'
);

EXECUTE STATEMENT SET
BEGIN
INSERT INTO api_key_anomalies_kafka
SELECT
  CAST(CURRENT_TIMESTAMP AS STRING) AS detected_at,
  COALESCE(JSON_VALUE(payload, '$.root_hash'), '') AS root_hash,
  COALESCE(JSON_VALUE(payload, '$.node.hash'), '') AS node_hash,
  COALESCE(JSON_VALUE(payload, '$.node.bucket.role'), '') AS role,
  COALESCE(JSON_VALUE(payload, '$.node.bucket.provider'), '') AS provider,
  REGEXP_EXTRACT(JSON_QUERY(payload, '$.node.bucket.content'), '(api_[A-Za-z0-9][A-Za-z0-9_-]{5,})', 1) AS api_key,
  COALESCE(JSON_QUERY(payload, '$.node.bucket.content'), '') AS content_json
FROM tapes_events
WHERE JSON_QUERY(payload, '$.node.bucket.content') IS NOT NULL
  AND REGEXP_EXTRACT(JSON_QUERY(payload, '$.node.bucket.content'), '(api_[A-Za-z0-9][A-Za-z0-9_-]{5,})', 1) <> '';

INSERT INTO api_key_anomalies_print
SELECT
  CAST(CURRENT_TIMESTAMP AS STRING) AS detected_at,
  COALESCE(JSON_VALUE(payload, '$.root_hash'), '') AS root_hash,
  COALESCE(JSON_VALUE(payload, '$.node.hash'), '') AS node_hash,
  COALESCE(JSON_VALUE(payload, '$.node.bucket.role'), '') AS role,
  COALESCE(JSON_VALUE(payload, '$.node.bucket.provider'), '') AS provider,
  REGEXP_EXTRACT(JSON_QUERY(payload, '$.node.bucket.content'), '(api_[A-Za-z0-9][A-Za-z0-9_-]{5,})', 1) AS api_key,
  COALESCE(JSON_QUERY(payload, '$.node.bucket.content'), '') AS content_json
FROM tapes_events
WHERE JSON_QUERY(payload, '$.node.bucket.content') IS NOT NULL
  AND REGEXP_EXTRACT(JSON_QUERY(payload, '$.node.bucket.content'), '(api_[A-Za-z0-9][A-Za-z0-9_-]{5,})', 1) <> '';
END;
