from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

REPORT_DIR = Path(__file__).resolve().parent / "recon_reports"
THRESHOLD = 95.0

TABLE_MAP = {
    "customers": "TRAVEL_DW.GOLD.DIM_CUSTOMERS",
    "policies": "TRAVEL_DW.GOLD.FACT_POLICIES",
    "claims": "TRAVEL_DW.GOLD.FACT_CLAIMS",
}


def pg_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "travel_db"),
        user=os.getenv("POSTGRES_USER", "travel_user"),
        password=os.getenv("POSTGRES_PASSWORD", "travel_password"),
    )


def sf_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "TRAVEL_DW"),
        schema="GOLD",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def calculate_match_percentage(source_count: int, destination_count: int) -> float:
    if source_count == 0:
        return 100.0 if destination_count == 0 else 0.0

    difference = abs(source_count - destination_count)
    return max(0.0, (1 - (difference / source_count)) * 100)


def main() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    run_timestamp = datetime.now(timezone.utc).isoformat()
    report: dict[str, object] = {
        "run_timestamp": run_timestamp,
        "tables": {},
    }

    any_failed = False

    with pg_connection() as pg_conn, sf_connection() as sf_conn:
        with pg_conn.cursor() as pg_cur, sf_conn.cursor() as sf_cur:
            for source_table, destination_table in TABLE_MAP.items():
                pg_cur.execute(f"SELECT COUNT(*) FROM {source_table}")
                source_count = int(pg_cur.fetchone()[0])

                sf_cur.execute(f"SELECT COUNT(*) FROM {destination_table}")
                destination_count = int(sf_cur.fetchone()[0])

                difference = destination_count - source_count
                match_percentage = round(calculate_match_percentage(source_count, destination_count), 2)
                passed = match_percentage >= THRESHOLD
                any_failed = any_failed or (not passed)

                report["tables"][source_table] = {
                    "source_count": source_count,
                    "destination_count": destination_count,
                    "difference": difference,
                    "match_percentage": match_percentage,
                    "passed": passed,
                }

    report_file = REPORT_DIR / f"recon_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_file.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("\nReconciliation Summary")
    print("=" * 90)
    print(f"{'TABLE':<15}{'SOURCE':>12}{'DESTINATION':>15}{'DIFF':>12}{'MATCH %':>12}{'PASSED':>12}")
    print("-" * 90)

    for table_name, stats in report["tables"].items():
        print(
            f"{table_name:<15}"
            f"{stats['source_count']:>12}"
            f"{stats['destination_count']:>15}"
            f"{stats['difference']:>12}"
            f"{stats['match_percentage']:>12.2f}"
            f"{str(stats['passed']):>12}"
        )

    print("=" * 90)
    print(f"Report written to: {report_file}")

    if any_failed:
        print(f"One or more tables are below the {THRESHOLD}% match threshold.")
        sys.exit(1)

    print("All tables passed reconciliation threshold.")
    sys.exit(0)


if __name__ == "__main__":
    main()
