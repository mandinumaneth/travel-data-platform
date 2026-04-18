from __future__ import annotations

import glob
import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

CSV_DIR = Path(__file__).resolve().parents[2] / "data" / "csv_samples"


def get_latest_csv() -> Path:
    files = glob.glob(str(CSV_DIR / "broker_commissions_*.csv"))
    if not files:
        raise FileNotFoundError(f"No broker commission CSV found in {CSV_DIR}")

    latest = max(files, key=os.path.getmtime)
    return Path(latest)


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


def ensure_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS RAW_BROKER_COMMISSIONS (
            BROKER_ID NUMBER,
            BROKER_NAME STRING,
            BROKER_COMPANY STRING,
            COUNTRY STRING,
            POLICIES_SOLD_TODAY NUMBER,
            TOTAL_PREMIUM_VALUE NUMBER(12,2),
            COMMISSION_RATE FLOAT,
            COMMISSION_EARNED NUMBER(12,2),
            REPORT_DATE DATE
        )
        """)


def main() -> None:
    latest_csv = get_latest_csv()
    dataframe = pd.read_csv(latest_csv)
    dataframe.columns = [col.upper() for col in dataframe.columns]

    with get_snowflake_connection() as conn:
        with conn.cursor() as cur:
            ensure_table(cur)

        success, _, num_rows, _ = write_pandas(
            conn=conn,
            df=dataframe,
            table_name="RAW_BROKER_COMMISSIONS",
            schema="BRONZE",
            database=os.getenv("SNOWFLAKE_DATABASE", "TRAVEL_DW"),
            auto_create_table=False,
        )

        if not success:
            raise RuntimeError("Failed to load broker commission CSV into Snowflake.")

        conn.commit()

    print(
        f"Loaded {num_rows} rows from {latest_csv.name} into BRONZE.RAW_BROKER_COMMISSIONS"
    )


if __name__ == "__main__":
    main()
