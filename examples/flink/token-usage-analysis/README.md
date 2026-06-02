# Flink token usage analysis demo

This example adds an Apache Flink SQL job that consumes tapes node events from Kafka,
aggregates response token usage in one-minute windows, and emits the rollups to Kafka
and the Flink print sink.

## Start

From the repository root:

```bash
make up-flink
```

This starts all Flink demo jobs in the compose profile, including the API-key anomaly
and sentiment examples. The `flink-token-usage-job` container exits after submitting
this SQL job. The `flink-jobmanager` and `flink-taskmanager` services keep the job
running.

## Output topic

```text
tapes.analytics.token_usage
```

Each output row contains the window bounds, provider, model, project, response count,
prompt/completion/total tokens, cache tokens, and duration totals.

## View results

```bash
docker compose exec flink-jobmanager /opt/flink/bin/flink list -m localhost:8081
docker compose logs -f flink-taskmanager

docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic tapes.analytics.token_usage \
  --from-beginning
```
