from __future__ import annotations

import json
import logging
import os
import random
import signal
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

RUNNING = True


def _shutdown_handler(signum: int, frame: Any) -> None:
    del signum, frame
    global RUNNING
    RUNNING = False
    logger.info("Stopping policy producer...")


def get_postgres_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "travel_db"),
        user=os.getenv("POSTGRES_USER", "travel_user"),
        password=os.getenv("POSTGRES_PASSWORD", "travel_password"),
    )


def fetch_recent_policies(
    conn: psycopg2.extensions.connection,
    limit: int = 200,
) -> list[dict[str, Any]]:
    query = """
        SELECT
            policy_id,
            customer_id,
            policy_number,
            destination_country,
            trip_start_date,
            trip_end_date,
            coverage_type,
            premium_amount,
            status,
            broker_id,
            created_at
        FROM policies
        WHERE created_at >= NOW() - INTERVAL '1 hour'
        ORDER BY created_at DESC
        LIMIT %s
    """

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, (limit,))
        return [dict(row) for row in cur.fetchall()]


def serialize_policy(policy: dict[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = {}
    for key, value in policy.items():
        if isinstance(value, datetime):
            serialized[key] = value.isoformat()
        else:
            serialized[key] = value

    serialized["event_timestamp"] = datetime.now(timezone.utc).isoformat()
    serialized["event_type"] = "policy_purchase"
    return serialized


def main() -> None:
    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    producer = Producer(
        {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}
    )
    topic = os.getenv("KAFKA_POLICY_TOPIC", "policy-events")

    sent_policy_ids: set[int] = set()

    with get_postgres_connection() as pg_conn:
        logger.info("Policy producer started. topic=%s", topic)

        while RUNNING:
            recent_policies = fetch_recent_policies(pg_conn)

            if not recent_policies:
                logger.info(
                    "No policies created in the last hour. Sleeping for 10 seconds."
                )
                time.sleep(10)
                continue

            unsent = [
                p for p in recent_policies if p["policy_id"] not in sent_policy_ids
            ]
            if not unsent:
                sent_policy_ids.clear()
                unsent = recent_policies

            selected_policy = random.choice(unsent)
            payload = serialize_policy(selected_policy)

            producer.produce(
                topic=topic,
                key=str(selected_policy["policy_id"]),
                value=json.dumps(payload),
            )
            producer.flush(timeout=5)
            sent_policy_ids.add(selected_policy["policy_id"])

            logger.info(
                "Published policy event. policy_id=%s policy_number=%s",
                selected_policy["policy_id"],
                selected_policy["policy_number"],
            )
            time.sleep(10)

    producer.flush(timeout=10)
    logger.info("Policy producer stopped.")


if __name__ == "__main__":
    main()
