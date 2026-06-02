FROM flink:2.2.1-scala_2.12-java17

# Flink's Kafka SQL connector is versioned independently from the Flink runtime.
# 4.0.1-2.0 is the latest published connector for the Flink 2.x line.
ARG KAFKA_CONNECTOR_VERSION=4.0.1-2.0

ENV PYTHONPATH=/opt/flink/opt/python/pyflink-lib:/opt/flink/opt/python/py4j-0.10.9.7-src.zip:/opt/flink/opt/python/cloudpickle-2.2.0-src.zip

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-ruamel.yaml python3-typing-extensions \
    && python3 -m pip install --break-system-packages --no-cache-dir apache-beam==2.61.0 avro==1.11.3 \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && cp /opt/flink/opt/flink-python-*.jar /opt/flink/lib/ \
    && mkdir -p /opt/flink/opt/python/pyflink-lib \
    && python3 -m zipfile -e /opt/flink/opt/python/pyflink.zip /opt/flink/opt/python/pyflink-lib \
    && chmod +x /opt/flink/opt/python/pyflink-lib/pyflink/bin/pyflink-udf-runner.sh \
    && rm -rf /root/.cache/pip /var/lib/apt/lists/*

USER flink

ADD --chmod=0644 https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

COPY anomaly-detection/api-key-anomalies.sql /opt/flink/usrlib/api-key-anomalies.sql
COPY token-usage-analysis/token-usage.sql /opt/flink/usrlib/token-usage.sql
COPY sentiment-analysis/job.py /opt/flink/usrlib/sentiment-analysis/job.py
COPY sentiment-analysis/tapes_sentiment /opt/flink/usrlib/sentiment-analysis/tapes_sentiment
