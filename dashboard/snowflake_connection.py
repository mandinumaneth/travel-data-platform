from __future__ import annotations

import os

import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

load_dotenv()


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection | None:
    try:
        return snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE", "TRAVEL_DW"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )
    except Exception as exc:  # noqa: BLE001
        st.error(f"Could not connect to Snowflake: {exc}")
        return None


@st.cache_data(ttl=600, show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    conn = get_snowflake_connection()
    if conn is None:
        return pd.DataFrame()

    try:
        return pd.read_sql(query, conn)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Query failed: {exc}")
        return pd.DataFrame()
    finally:
        conn.close()
