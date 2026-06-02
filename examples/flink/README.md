# Flink examples

This directory contains Flink examples for anomaly detection, realtime analytics,
and sentiment analysis.

The shared Docker image is used for all Flink examples. It installs the Flink Kafka SQL
connector, enables Python/PyFlink support, and copies each example's SQL or Python job
into `/opt/flink/usrlib/` so the compose job submitters can run them against the local
Compose-managed Flink cluster.
