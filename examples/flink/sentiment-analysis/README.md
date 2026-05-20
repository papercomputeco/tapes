# Flink user message sentiment demo

This example adds a very small sentiment-analysis job for user messages flowing through
tapes. It uses a regex lexicon in Flink SQL, which keeps the demo lightweight while
showing where a streaming ML inference job can plug in.

## Start

From the repository root:

```bash
make up-flink
```

This starts all Flink demo jobs in the compose profile, including the API-key anomaly
and token usage examples. The `flink-sentiment-job` container exits after submitting
this SQL job. The `flink-jobmanager` and `flink-taskmanager` services keep the job
running.

## Trigger sentiment rows

Send messages through the tapes proxy:

```bash
curl -sS http://localhost:8080/api/chat \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "qwen3:0.6b",
    "stream": false,
    "messages": [
      {"role": "user", "content": "Thanks, this is great and really helpful!"}
    ]
  }'
```

Try negative words like `broken`, `terrible`, or `frustrated` to produce negative rows.
Messages with both positive and negative matches are classified as `mixed`; messages with
no lexicon match are classified as `neutral`.

## Output topic

```text
tapes.ml.sentiment
```

## View results

```bash
docker compose exec flink-jobmanager /opt/flink/bin/flink list -m localhost:8081
docker compose logs -f flink-taskmanager

docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tapes.ml.sentiment \
  --from-beginning
```
