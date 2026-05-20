-- Consume tapes node events from Kafka and classify user-message sentiment with a tiny
-- regex lexicon. This is intentionally simple for the demo: it acts like an ML inference
-- stream without introducing model-serving infrastructure.

CREATE TABLE tapes_events (
  payload STRING,
  event_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'tapes.e2e.proxy',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'tapes-flink-user-sentiment-analyzer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

CREATE TABLE sentiment_kafka (
  analyzed_at STRING,
  root_hash STRING,
  node_hash STRING,
  provider STRING,
  model STRING,
  sentiment STRING,
  confidence DOUBLE,
  positive_match STRING,
  negative_match STRING,
  content_json STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'tapes.ml.sentiment',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE sentiment_print (
  analyzed_at STRING,
  root_hash STRING,
  node_hash STRING,
  provider STRING,
  model STRING,
  sentiment STRING,
  confidence DOUBLE,
  positive_match STRING,
  negative_match STRING,
  content_json STRING
) WITH (
  'connector' = 'print'
);

EXECUTE STATEMENT SET
BEGIN
INSERT INTO sentiment_kafka
SELECT
  analyzed_at,
  root_hash,
  node_hash,
  provider,
  model,
  sentiment,
  confidence,
  positive_match,
  negative_match,
  content_json
FROM (
  SELECT
    CAST(CURRENT_TIMESTAMP AS STRING) AS analyzed_at,
    root_hash,
    node_hash,
    provider,
    model,
    CASE
      WHEN positive_match <> '' AND negative_match <> '' THEN 'mixed'
      WHEN positive_match <> '' THEN 'positive'
      WHEN negative_match <> '' THEN 'negative'
      ELSE 'neutral'
    END AS sentiment,
    CAST(CASE
      WHEN positive_match <> '' AND negative_match <> '' THEN 0.60
      WHEN positive_match <> '' OR negative_match <> '' THEN 0.85
      ELSE 0.50
    END AS DOUBLE) AS confidence,
    positive_match,
    negative_match,
    content_json
  FROM (
    SELECT
      COALESCE(JSON_VALUE(payload, '$.root_hash'), '') AS root_hash,
      COALESCE(JSON_VALUE(payload, '$.node.hash'), '') AS node_hash,
      COALESCE(JSON_VALUE(payload, '$.node.bucket.provider'), '') AS provider,
      COALESCE(JSON_VALUE(payload, '$.node.bucket.model'), '') AS model,
      REGEXP_EXTRACT(LOWER(JSON_QUERY(payload, '$.node.bucket.content')), '(love|great|excellent|amazing|awesome|happy|thanks|thank you|helpful|perfect)', 1) AS positive_match,
      REGEXP_EXTRACT(LOWER(JSON_QUERY(payload, '$.node.bucket.content')), '(hate|bad|terrible|awful|angry|frustrated|broken|bug|error|fail|failed|worse)', 1) AS negative_match,
      COALESCE(JSON_QUERY(payload, '$.node.bucket.content'), '') AS content_json
    FROM tapes_events
    WHERE JSON_VALUE(payload, '$.node.bucket.role') = 'user'
      AND JSON_QUERY(payload, '$.node.bucket.content') IS NOT NULL
  ) classified
);

INSERT INTO sentiment_print
SELECT
  analyzed_at,
  root_hash,
  node_hash,
  provider,
  model,
  sentiment,
  confidence,
  positive_match,
  negative_match,
  content_json
FROM (
  SELECT
    CAST(CURRENT_TIMESTAMP AS STRING) AS analyzed_at,
    root_hash,
    node_hash,
    provider,
    model,
    CASE
      WHEN positive_match <> '' AND negative_match <> '' THEN 'mixed'
      WHEN positive_match <> '' THEN 'positive'
      WHEN negative_match <> '' THEN 'negative'
      ELSE 'neutral'
    END AS sentiment,
    CAST(CASE
      WHEN positive_match <> '' AND negative_match <> '' THEN 0.60
      WHEN positive_match <> '' OR negative_match <> '' THEN 0.85
      ELSE 0.50
    END AS DOUBLE) AS confidence,
    positive_match,
    negative_match,
    content_json
  FROM (
    SELECT
      COALESCE(JSON_VALUE(payload, '$.root_hash'), '') AS root_hash,
      COALESCE(JSON_VALUE(payload, '$.node.hash'), '') AS node_hash,
      COALESCE(JSON_VALUE(payload, '$.node.bucket.provider'), '') AS provider,
      COALESCE(JSON_VALUE(payload, '$.node.bucket.model'), '') AS model,
      REGEXP_EXTRACT(LOWER(JSON_QUERY(payload, '$.node.bucket.content')), '(love|great|excellent|amazing|awesome|happy|thanks|thank you|helpful|perfect)', 1) AS positive_match,
      REGEXP_EXTRACT(LOWER(JSON_QUERY(payload, '$.node.bucket.content')), '(hate|bad|terrible|awful|angry|frustrated|broken|bug|error|fail|failed|worse)', 1) AS negative_match,
      COALESCE(JSON_QUERY(payload, '$.node.bucket.content'), '') AS content_json
    FROM tapes_events
    WHERE JSON_VALUE(payload, '$.node.bucket.role') = 'user'
      AND JSON_QUERY(payload, '$.node.bucket.content') IS NOT NULL
  ) classified
);
END;
