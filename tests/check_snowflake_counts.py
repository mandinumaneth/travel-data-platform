from __future__ import annotations

import argparse
import os

import snowflake.connector


def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "TRAVEL_DW"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "BRONZE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="BRONZE")
    args = parser.parse_args()

    table_names = [
        "RAW_CUSTOMERS",
        "RAW_POLICIES",
        "RAW_CLAIMS",
        "RAW_BROKER_COMMISSIONS",
        "RAW_FLIGHTS",
        "BRONZE_FLIGHTS_CLEAN",
        "BRONZE_POLICIES_CLEAN",
        "FLIGHT_DAILY_STATS",
    ]

    placeholders = ", ".join(["%s"] * len(table_names))
    query = f"""
        SELECT TABLE_NAME, ROW_COUNT
        FROM TRAVEL_DW.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME IN ({placeholders})
        ORDER BY TABLE_NAME
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, [args.schema] + table_names)
            rows = cur.fetchall()

    print(f"Snowflake {args.schema} row counts")
    for table_name, row_count in rows:
        print(f"{table_name}: {row_count}")


if __name__ == "__main__":
    main()
