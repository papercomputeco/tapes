"""PyFlink streaming sentiment inference job for tapes user messages."""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, StreamTableEnvironment
from pyflink.table.udf import udf

from tapes_sentiment import MODEL_NAME, MODEL_VERSION, predict_sentiment


@udf(result_type=DataTypes.STRING())
def sentiment_label(content_json: str) -> str:
    return predict_sentiment(content_json).label


@udf(result_type=DataTypes.DOUBLE())
def sentiment_confidence(content_json: str) -> float:
    return float(predict_sentiment(content_json).confidence)


@udf(result_type=DataTypes.STRING())
def sentiment_scores(content_json: str) -> str:
    return predict_sentiment(content_json).scores_json()


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    t_env = StreamTableEnvironment.create(env)
    t_env.get_config().set("pipeline.name", "tapes-user-message-sentiment-inference")

    t_env.create_temporary_system_function("sentiment_label", sentiment_label)
    t_env.create_temporary_system_function("sentiment_confidence", sentiment_confidence)
    t_env.create_temporary_system_function("sentiment_scores", sentiment_scores)

    t_env.execute_sql(
        """
        CREATE TABLE tapes_events (
          payload STRING,
          event_time AS PROCTIME()
        ) WITH (
          'connector' = 'kafka',
          'topic' = 'tapes.e2e.proxy',
          'properties.bootstrap.servers' = 'kafka:9092',
          'properties.group.id' = 'tapes-flink-user-sentiment-inference',
          'scan.startup.mode' = 'earliest-offset',
          'format' = 'raw'
        )
        """
    )

    t_env.execute_sql(
        """
        CREATE TABLE sentiment_kafka (
          analyzed_at STRING,
          root_hash STRING,
          node_hash STRING,
          provider STRING,
          model STRING,
          sentiment STRING,
          confidence DOUBLE,
          ml_model STRING,
          ml_model_version STRING,
          scores_json STRING,
          content_json STRING
        ) WITH (
          'connector' = 'kafka',
          'topic' = 'tapes.ml.sentiment',
          'properties.bootstrap.servers' = 'kafka:9092',
          'format' = 'json'
        )
        """
    )

    t_env.execute_sql(
        """
        CREATE TABLE sentiment_print (
          analyzed_at STRING,
          root_hash STRING,
          node_hash STRING,
          provider STRING,
          model STRING,
          sentiment STRING,
          confidence DOUBLE,
          ml_model STRING,
          ml_model_version STRING,
          scores_json STRING,
          content_json STRING
        ) WITH (
          'connector' = 'print'
        )
        """
    )

    insert_sql = f"""
        SELECT
          CAST(CURRENT_TIMESTAMP AS STRING) AS analyzed_at,
          COALESCE(JSON_VALUE(payload, '$.root_hash'), '') AS root_hash,
          COALESCE(JSON_VALUE(payload, '$.node.hash'), '') AS node_hash,
          COALESCE(JSON_VALUE(payload, '$.node.bucket.provider'), '') AS provider,
          COALESCE(JSON_VALUE(payload, '$.node.bucket.model'), '') AS model,
          sentiment_label(JSON_QUERY(payload, '$.node.bucket.content')) AS sentiment,
          sentiment_confidence(JSON_QUERY(payload, '$.node.bucket.content')) AS confidence,
          '{MODEL_NAME}' AS ml_model,
          '{MODEL_VERSION}' AS ml_model_version,
          sentiment_scores(JSON_QUERY(payload, '$.node.bucket.content')) AS scores_json,
          COALESCE(JSON_QUERY(payload, '$.node.bucket.content'), '') AS content_json
        FROM tapes_events
        WHERE JSON_VALUE(payload, '$.node.bucket.role') = 'user'
          AND JSON_QUERY(payload, '$.node.bucket.content') IS NOT NULL
    """

    statement_set = t_env.create_statement_set()
    statement_set.add_insert_sql(f"INSERT INTO sentiment_kafka {insert_sql}")
    statement_set.add_insert_sql(f"INSERT INTO sentiment_print {insert_sql}")
    statement_set.execute()


if __name__ == "__main__":
    main()
