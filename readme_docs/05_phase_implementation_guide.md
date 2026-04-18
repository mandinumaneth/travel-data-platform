# Phase-by-Phase Implementation Guide

## Phase 1: Local Infrastructure

Files:

- `infrastructure/docker/docker-compose.yml`

Outcome:

- PostgreSQL, Kafka, Zookeeper, Airflow, Spark, producer/batch app containers, Streamlit service.

## Phase 2: PostgreSQL Setup

Files:

- `data/seed/01_create_tables.sql`
- `data/seed/02_seed_data.py`

Outcome:

- Seeded customer/policy/claim operational dataset.

## Phase 3: Kafka Streaming

Files:

- `ingestion/kafka/flight_producer.py`
- `ingestion/kafka/flight_consumer.py`
- `ingestion/kafka/policy_producer.py`

Outcome:

- Real-time events into Snowflake Bronze.

## Phase 4: Batch Ingestion

Files:

- `ingestion/batch/extract_postgres.py`
- `ingestion/batch/generate_broker_csv.py`
- `ingestion/batch/load_broker_csv.py`

Outcome:

- Incremental batch loading and CSV pipeline.

## Phase 5: Spark Processing

Files:

- `processing/spark/clean_bronze.py`
- `processing/spark/aggregate_flights.py`

Outcome:

- Cleaned bronze tables and daily flight metrics.

## Phase 6: dbt Medallion

Files:

- `transformation/dbt_project/...`

Outcome:

- Silver staging views, Gold marts, schema tests, custom assertions.

## Phase 7: Airflow Orchestration

Files:

- `orchestration/dags/daily_pipeline.py`
- `orchestration/dags/kafka_monitor.py`

Outcome:

- Automated and monitored pipeline scheduling.

## Phase 8: Reconciliation

Files:

- `tests/reconciliation.py`

Outcome:

- Source-vs-target report with pass/fail threshold.

## Phase 9: Containerization

Files:

- `ingestion/kafka/Dockerfile`
- `ingestion/batch/Dockerfile`
- `dashboard/Dockerfile`

Outcome:

- Application components packaged as runnable containers.

## Phase 10: Kubernetes

Files:

- `infrastructure/kubernetes/kafka-producer-deployment.yaml`
- `infrastructure/kubernetes/airflow-deployment.yaml`
- `infrastructure/kubernetes/secrets-template.yaml`

Outcome:

- Local K8s deployment manifests for key services.

## Phase 11: CI/CD

Files:

- `.github/workflows/dbt_ci.yml`
- `.github/workflows/python_lint.yml`
- `.github/workflows/streamlit_test.yml`

Outcome:

- Push-based automated quality gates.

## Phase 12: Streamlit Dashboard

Files:

- `dashboard/app.py`
- `dashboard/snowflake_connection.py`
- `dashboard/.streamlit/config.toml`

Outcome:

- Portfolio-ready analytics interface powered by Snowflake.
