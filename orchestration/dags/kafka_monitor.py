from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from confluent_kafka import Consumer, TopicPartition
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()


def check_kafka_consumer_lag() -> None:
    topic = os.getenv("KAFKA_FLIGHT_TOPIC", "flight-events")
    consumer_group = os.getenv("KAFKA_MONITOR_CONSUMER_GROUP", "travel-flight-consumer")

    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": consumer_group,
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )

    try:
        metadata = consumer.list_topics(topic=topic, timeout=10)
        if topic not in metadata.topics:
            logging.warning("Kafka topic %s not found while checking lag.", topic)
            return

        total_lag = 0
        partitions = metadata.topics[topic].partitions.keys()

        for partition_id in partitions:
            tp = TopicPartition(topic, partition_id)
            low_offset, high_offset = consumer.get_watermark_offsets(tp, timeout=10)
            committed = consumer.committed([tp], timeout=10)[0].offset
            committed = low_offset if committed < 0 else committed

            partition_lag = max(high_offset - committed, 0)
            total_lag += partition_lag

        if total_lag > 1000:
            logging.warning("Kafka consumer lag is high: %s messages pending in %s", total_lag, topic)
        else:
            logging.info("Kafka consumer lag healthy: %s messages pending in %s", total_lag, topic)
    finally:
        consumer.close()


def check_snowflake_flight_freshness() -> None:
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "TRAVEL_DW"),
        schema="BRONZE",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

    query = """
        SELECT COUNT(*)
        FROM TRAVEL_DW.BRONZE.RAW_FLIGHTS
        WHERE INGESTED_AT >= DATEADD(minute, -30, CURRENT_TIMESTAMP())
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows_last_30_mins = cur.fetchone()[0]

        if rows_last_30_mins == 0:
            logging.warning("No new rows in RAW_FLIGHTS within the last 30 minutes. Streaming pipeline may be down.")
        else:
            logging.info("RAW_FLIGHTS freshness healthy. Rows in last 30 minutes: %s", rows_last_30_mins)
    finally:
        conn.close()


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
}

with DAG(
    dag_id="kafka_monitor",
    description="Monitor Kafka lag and Snowflake streaming freshness",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["monitoring", "kafka", "snowflake"],
) as dag:
    check_lag_task = PythonOperator(
        task_id="check_flight_events_consumer_lag",
        python_callable=check_kafka_consumer_lag,
    )

    check_freshness_task = PythonOperator(
        task_id="check_raw_flights_freshness",
        python_callable=check_snowflake_flight_freshness,
    )

    check_lag_task >> check_freshness_task
