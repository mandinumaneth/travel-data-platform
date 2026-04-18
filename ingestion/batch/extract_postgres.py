from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg2
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

STATE_FILE = Path(__file__).resolve().parent / "state.json"
TABLE_MAPPING = {
    "customers": "RAW_CUSTOMERS",
    "policies": "RAW_POLICIES",
    "claims": "RAW_CLAIMS",
}


def get_postgres_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "travel_db"),
        user=os.getenv("POSTGRES_USER", "travel_user"),
        password=os.getenv("POSTGRES_PASSWORD", "travel_password"),
    )


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE", "TRAVEL_DW"),
        schema="BRONZE",
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def load_state() -> dict[str, Any]:
    if not STATE_FILE.exists():
        return {"last_extracted_at": {}}

    return json.loads(STATE_FILE.read_text(encoding="utf-8"))


def save_state(state: dict[str, Any]) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def extract_table(
    pg_conn: psycopg2.extensions.connection,
    table_name: str,
    last_extracted_at: str | None,
) -> pd.DataFrame:
    if last_extracted_at:
        query = f"SELECT * FROM {table_name} WHERE created_at > %(last_extracted_at)s"
        params = {"last_extracted_at": last_extracted_at}
    else:
        query = f"SELECT * FROM {table_name}"
        params = None

    return pd.read_sql_query(query, pg_conn, params=params)


def load_to_snowflake(
    sf_conn: snowflake.connector.SnowflakeConnection,
    table_name: str,
    dataframe: pd.DataFrame,
) -> int:
    if dataframe.empty:
        return 0

    normalized_df = dataframe.copy()
    normalized_df.columns = [col.upper() for col in normalized_df.columns]

    success, _, num_rows, _ = write_pandas(
        conn=sf_conn,
        df=normalized_df,
        table_name=table_name,
        schema="BRONZE",
        database=os.getenv("SNOWFLAKE_DATABASE", "TRAVEL_DW"),
        auto_create_table=True,
    )

    if not success:
        raise RuntimeError(f"Failed writing DataFrame to Snowflake table {table_name}")

    return int(num_rows)


def main() -> None:
    state = load_state()
    state.setdefault("last_extracted_at", {})

    run_started_at = datetime.now(timezone.utc).isoformat()
    summary: dict[str, dict[str, int]] = {}

    with get_postgres_connection() as pg_conn, get_snowflake_connection() as sf_conn:
        for source_table, target_table in TABLE_MAPPING.items():
            last_extracted_at = state["last_extracted_at"].get(source_table)
            df = extract_table(pg_conn, source_table, last_extracted_at)
            loaded_count = load_to_snowflake(sf_conn, target_table, df)

            summary[source_table] = {
                "extracted": int(df.shape[0]),
                "loaded": loaded_count,
            }

            extracted_count = summary[source_table]["extracted"]
            loaded_rows = summary[source_table]["loaded"]
            print(
                f"Table {source_table}: extracted={extracted_count} "
                f"loaded={loaded_rows}"
            )

        sf_conn.commit()

    for table_name in TABLE_MAPPING:
        state["last_extracted_at"][table_name] = run_started_at

    save_state(state)

    print("Incremental extraction finished.")
    for table_name, result in summary.items():
        print(
            f" - {table_name}: extracted={result['extracted']} loaded={result['loaded']}"
        )


if __name__ == "__main__":
    main()
