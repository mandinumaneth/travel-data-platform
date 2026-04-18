from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


def create_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "TRAVEL_DW"),
        schema="BRONZE",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def ensure_raw_flights_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS RAW_FLIGHTS (
            ICAO24 STRING,
            CALLSIGN STRING,
            ORIGIN_COUNTRY STRING,
            LONGITUDE FLOAT,
            LATITUDE FLOAT,
            ALTITUDE FLOAT,
            VELOCITY FLOAT,
            HEADING FLOAT,
            ON_GROUND BOOLEAN,
            EVENT_TIMESTAMP TIMESTAMP_NTZ,
            INGESTED_AT TIMESTAMP_NTZ
        )
        """)


def parse_message(raw_value: bytes) -> tuple[Any, ...] | None:
    try:
        payload = json.loads(raw_value.decode("utf-8"))
        return (
            payload.get("icao24"),
            payload.get("callsign"),
            payload.get("origin_country"),
            payload.get("longitude"),
            payload.get("latitude"),
            payload.get("altitude"),
            payload.get("velocity"),
            payload.get("heading"),
            payload.get("on_ground"),
            payload.get("timestamp"),
            datetime.utcnow().isoformat(),
        )
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.warning("Skipping malformed Kafka message: %s", exc)
        return None


def write_batch_to_snowflake(
    cur: snowflake.connector.cursor.SnowflakeCursor,
    rows: list[tuple[Any, ...]],
) -> None:
    cur.executemany(
        """
        INSERT INTO RAW_FLIGHTS (
            ICAO24,
            CALLSIGN,
            ORIGIN_COUNTRY,
            LONGITUDE,
            LATITUDE,
            ALTITUDE,
            VELOCITY,
            HEADING,
            ON_GROUND,
            EVENT_TIMESTAMP,
            INGESTED_AT
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )


def consume_loop() -> None:
    topic = os.getenv("KAFKA_FLIGHT_TOPIC", "flight-events")
    batch_size = int(os.getenv("KAFKA_CONSUMER_BATCH_SIZE", "100"))

    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "group.id": os.getenv(
                "KAFKA_FLIGHT_CONSUMER_GROUP", "travel-flight-consumer"
            ),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([topic])
    logger.info("Subscribed to topic: %s", topic)

    sf_conn = create_snowflake_connection()

    try:
        with sf_conn.cursor() as cur:
            ensure_raw_flights_table(cur)

            while True:
                parsed_rows: list[tuple[Any, ...]] = []

                while len(parsed_rows) < batch_size:
                    message = consumer.poll(timeout=1.0)
                    if message is None:
                        break

                    if message.error():
                        if message.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise RuntimeError(message.error())

                    parsed = parse_message(message.value())
                    if parsed is not None:
                        parsed_rows.append(parsed)

                if not parsed_rows:
                    continue

                write_batch_to_snowflake(cur, parsed_rows)
                sf_conn.commit()
                consumer.commit(asynchronous=False)
                logger.info(
                    "Wrote %s flight events to Snowflake and committed offsets.",
                    len(parsed_rows),
                )

    except KeyboardInterrupt:
        logger.info("Stopping consumer due to keyboard interrupt.")
    finally:
        consumer.close()
        sf_conn.close()
        logger.info("Kafka consumer stopped cleanly.")


def main() -> None:
    consume_loop()


if __name__ == "__main__":
    main()
