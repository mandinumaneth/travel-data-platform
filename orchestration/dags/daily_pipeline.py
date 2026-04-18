from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")


def check_source_freshness() -> None:
    query = """
        SELECT
            (SELECT COUNT(*) FROM policies WHERE created_at >= NOW() - INTERVAL '24 hours') AS policies_count,
            (SELECT COUNT(*) FROM claims WHERE created_at >= NOW() - INTERVAL '24 hours') AS claims_count
    """

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "travel_db"),
        user=os.getenv("POSTGRES_USER", "travel_user"),
        password=os.getenv("POSTGRES_PASSWORD", "travel_password"),
    )

    try:
        with conn.cursor() as cur:
            cur.execute(query)
            policies_count, claims_count = cur.fetchone()

        if policies_count == 0 and claims_count == 0:
            raise AirflowSkipException(
                "No new policies or claims found in the last 24 hours. Skipping downstream pipeline tasks."
            )

        logging.info(
            "Source freshness check passed. policies_last_24h=%s claims_last_24h=%s",
            policies_count,
            claims_count,
        )
    finally:
        conn.close()


def log_pipeline_status(**context) -> None:
    dag_run = context["dag_run"]
    task_instances = dag_run.get_task_instances()

    failed_tasks = [ti.task_id for ti in task_instances if ti.state == "failed"]
    upstream_failed_tasks = [ti.task_id for ti in task_instances if ti.state == "upstream_failed"]

    if failed_tasks or upstream_failed_tasks:
        logging.warning(
            "Pipeline finished with issues. failed=%s upstream_failed=%s",
            failed_tasks,
            upstream_failed_tasks,
        )
    else:
        logging.info("Pipeline finished successfully. All tasks are green.")


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="travel_data_pipeline",
    description="Daily orchestration DAG for the Travel Intelligence Platform",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["travel", "daily", "medallion"],
) as dag:
    check_source_freshness_task = PythonOperator(
        task_id="check_source_freshness",
        python_callable=check_source_freshness,
    )

    extract_postgres_task = BashOperator(
        task_id="extract_postgres",
        bash_command=f"python {PROJECT_ROOT}/ingestion/batch/extract_postgres.py",
    )

    generate_broker_csv_task = BashOperator(
        task_id="generate_broker_csv",
        bash_command=f"python {PROJECT_ROOT}/ingestion/batch/generate_broker_csv.py",
    )

    load_broker_csv_task = BashOperator(
        task_id="load_broker_csv",
        bash_command=f"python {PROJECT_ROOT}/ingestion/batch/load_broker_csv.py",
    )

    run_spark_cleaning_task = BashOperator(
        task_id="run_spark_cleaning",
        bash_command=f"spark-submit {PROJECT_ROOT}/processing/spark/clean_bronze.py",
    )

    run_dbt_staging_task = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"cd {PROJECT_ROOT}/transformation/dbt_project && dbt run --select staging",
    )

    run_dbt_marts_task = BashOperator(
        task_id="run_dbt_marts",
        bash_command=f"cd {PROJECT_ROOT}/transformation/dbt_project && dbt run --select marts",
    )

    run_dbt_tests_task = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"cd {PROJECT_ROOT}/transformation/dbt_project && dbt test",
    )

    run_reconciliation_task = BashOperator(
        task_id="run_reconciliation",
        bash_command=f"python {PROJECT_ROOT}/tests/reconciliation.py",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    log_pipeline_status_task = PythonOperator(
        task_id="log_pipeline_status",
        python_callable=log_pipeline_status,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        check_source_freshness_task
        >> extract_postgres_task
        >> generate_broker_csv_task
        >> load_broker_csv_task
        >> run_spark_cleaning_task
        >> run_dbt_staging_task
        >> run_dbt_marts_task
        >> run_dbt_tests_task
        >> run_reconciliation_task
        >> log_pipeline_status_task
    )
