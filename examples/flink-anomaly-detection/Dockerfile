FROM flink:2.2.1-scala_2.12-java17

ARG KAFKA_CONNECTOR_VERSION=3.2.0-2.2

ADD --chmod=0644 https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

COPY api-key-anomalies.sql /opt/flink/usrlib/api-key-anomalies.sql
