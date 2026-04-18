from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)

RUNNING = True
LAST_SUCCESS_EPOCH = 0.0


def _shutdown_handler(signum: int, frame: Any) -> None:
    del signum, frame
    global RUNNING
    RUNNING = False
    logger.info("Shutdown signal received. Stopping producer loop...")


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/health":
            self.send_response(404)
            self.end_headers()
            return

        is_healthy = (time.time() - LAST_SUCCESS_EPOCH) < 180 or LAST_SUCCESS_EPOCH == 0
        self.send_response(200 if is_healthy else 503)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        payload = {
            "status": "ok" if is_healthy else "stale",
            "last_success_epoch": LAST_SUCCESS_EPOCH,
        }
        self.wfile.write(json.dumps(payload).encode("utf-8"))

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return


def start_health_server() -> None:
    port = int(os.getenv("PRODUCER_HEALTH_PORT", "8081"))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info("Health endpoint started at /health on port %s", port)


def delivery_report(err: Exception | None, msg: Any) -> None:
    if err is not None:
        logger.error("Delivery failed: %s", err)


def normalize_flight(state_row: list[Any]) -> dict[str, Any]:
    return {
        "icao24": state_row[0],
        "callsign": (state_row[1] or "").strip() if len(state_row) > 1 else None,
        "origin_country": state_row[2] if len(state_row) > 2 else None,
        "longitude": state_row[5] if len(state_row) > 5 else None,
        "latitude": state_row[6] if len(state_row) > 6 else None,
        "altitude": state_row[7] if len(state_row) > 7 else None,
        "velocity": state_row[9] if len(state_row) > 9 else None,
        "heading": state_row[10] if len(state_row) > 10 else None,
        "on_ground": bool(state_row[8]) if len(state_row) > 8 and state_row[8] is not None else False,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def fetch_flights(api_url: str, timeout: int = 30) -> list[dict[str, Any]]:
    response = requests.get(api_url, timeout=timeout)
    response.raise_for_status()
    payload = response.json()

    states = payload.get("states", [])
    return [normalize_flight(state_row) for state_row in states if state_row and state_row[0]]


def main() -> None:
    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    start_health_server()

    topic = os.getenv("KAFKA_FLIGHT_TOPIC", "flight-events")
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    api_url = os.getenv("OPENSKY_API_URL", "https://opensky-network.org/api/states/all")
    poll_seconds = int(os.getenv("OPENSKY_POLL_SECONDS", "30"))
    retry_seconds = int(os.getenv("OPENSKY_RETRY_SECONDS", "60"))

    producer = Producer({"bootstrap.servers": bootstrap_servers})

    logger.info(
        "Starting flight producer. topic=%s bootstrap=%s poll_seconds=%s",
        topic,
        bootstrap_servers,
        poll_seconds,
    )

    global LAST_SUCCESS_EPOCH

    while RUNNING:
        try:
            flights = fetch_flights(api_url)

            for flight in flights:
                producer.produce(
                    topic=topic,
                    key=flight["icao24"],
                    value=json.dumps(flight),
                    on_delivery=delivery_report,
                )
                producer.poll(0)

            producer.flush(timeout=10)
            LAST_SUCCESS_EPOCH = time.time()
            logger.info("Published %s flight events to %s", len(flights), topic)
            time.sleep(poll_seconds)

        except requests.RequestException as exc:
            logger.warning("OpenSky API failed: %s. Retrying in %s seconds.", exc, retry_seconds)
            time.sleep(retry_seconds)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Unexpected producer error: %s", exc)
            time.sleep(retry_seconds)

    producer.flush(timeout=10)
    logger.info("Flight producer stopped cleanly.")


if __name__ == "__main__":
    main()
