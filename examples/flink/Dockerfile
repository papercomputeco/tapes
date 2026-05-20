FROM flink:2.2.1-scala_2.12-java17

# Flink's Kafka SQL connector is versioned independently from the Flink runtime.
# 4.0.1-2.0 is the latest published connector for the Flink 2.x line.
ARG KAFKA_CONNECTOR_VERSION=4.0.1-2.0

ADD --chmod=0644 https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

COPY anomaly-detection/api-key-anomalies.sql /opt/flink/usrlib/api-key-anomalies.sql
COPY token-usage-analysis/token-usage.sql /opt/flink/usrlib/token-usage.sql
COPY sentiment-analysis/sentiment-analysis.sql /opt/flink/usrlib/sentiment-analysis.sql
