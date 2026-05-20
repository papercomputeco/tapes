# Flink examples

This directory contains Flink examples for anomaly detection, realtime analytics,
and sentiment analysis.

The shared Docker image is used for Flink examples. It installs the Flink
Kafka SQL connector and copies each example's SQL file into `/opt/flink/usrlib/` so the
compose job submitters can run them with `sql-client.sh`.
