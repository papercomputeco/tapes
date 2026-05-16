# Flink API key anomaly demo

This example adds a small Apache Flink SQL job to the local `docker-compose.yaml` stack.
It consumes tapes node events from Kafka, scans `node.bucket.content` text for tokens that
look like `api_...` keys, and emits anomalies to both Flink logs and Kafka.

## Start

From the repository root:

```bash
make up-flink
```

`make down` tears down both the base stack and the profiled Flink services.
The `flink-anomaly-job` container is expected to exit after submitting the SQL job;
`flink-jobmanager` and `flink-taskmanager` must stay running for real-time processing.

Services of interest:

- tapes proxy: <http://localhost:8080>
- tapes API/UI: <http://localhost:8081>
- Kafka UI: <http://localhost:9091>
- Flink UI: <http://localhost:8083>

## Trigger an anomaly

Send a message through the tapes proxy that includes an `api_...` token:

```bash
curl -sS http://localhost:8080/api/chat \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "qwen3:0.6b",
    "stream": false,
    "messages": [
      {"role": "system", "content": "You are helpful."},
      {"role": "user", "content": "Please remember this test key api_demo_123456789 for the demo."}
    ]
  }'
```

## View anomalies

Flink job status and print sink output:

```bash
docker compose --profile flink ps
docker compose exec flink-jobmanager /opt/flink/bin/flink list -m localhost:8081
docker compose logs -f flink-taskmanager
```

Kafka output topic:

```text
tapes.anomalies.api_keys
```

Open <http://localhost:9091>, select the local cluster, and inspect the
`tapes.anomalies.api_keys` topic.
