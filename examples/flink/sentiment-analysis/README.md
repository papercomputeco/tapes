# Flink user message sentiment inference demo

This example adds a small PyFlink streaming job for user messages flowing through tapes.
It trains a tiny pure-Python Naive Bayes sentiment model from an in-repo dataset and runs
that model as Python UDF inference inside the Flink job.

The implementation is intentionally lightweight: no external model server, no scikit-learn,
and no heavyweight Python dependencies. It is still a real streaming inference path rather
than a SQL regex check.

## Start

From the repository root:

```bash
make up-flink
```

This starts all Flink demo jobs in the compose profile, including the API-key anomaly
and token usage examples. The `flink-sentiment-job` container exits after submitting
this PyFlink job. The `flink-jobmanager` and `flink-taskmanager` services keep the job
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

Try negative phrases like `this is terrible and broken` or neutral prompts like
`please summarize this conversation` to see different predictions.

## Output topic

```text
tapes.ml.sentiment
```

Each output row includes:

- input node metadata: `root_hash`, `node_hash`, `provider`, `model`
- prediction: `sentiment`, `confidence`, `scores_json`
- model metadata: `ml_model`, `ml_model_version`
- raw `content_json` used for inference

## View results

```bash
docker compose exec flink-jobmanager /opt/flink/bin/flink list -m localhost:8081
docker compose logs -f flink-taskmanager

docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tapes.ml.sentiment \
  --from-beginning
```
