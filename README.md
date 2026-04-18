# Travel Data Platform

An end-to-end production-style data engineering platform for a travel insurance company. This project ingests real-time flight telemetry and batch insurance data, processes it with Spark, models it with dbt in a Bronze/Silver/Gold medallion architecture on Snowflake, orchestrates jobs in Airflow, deploys workloads in Docker and Kubernetes, validates quality with reconciliation tests, and exposes business analytics through Streamlit.

## Documentation Hub

All detailed learning notes, architecture explainers, and runbooks live in [readme_docs/README.md](readme_docs/README.md).

## Architecture Overview

```
                   +---------------------------+
                   | OpenSky Network API       |
                   | (real-time flights)       |
                   +------------+--------------+
                                |
                                v
                       +--------+--------+
                       | Kafka Producer  |
                       +--------+--------+
                                |
                                v
+----------------+     +--------+--------+     +-------------------------+
| PostgreSQL     |---->| Kafka Topics    |---->| Kafka Consumer          |
| customers      |     | flight-events   |     | writes RAW_FLIGHTS      |
| policies       |     | policy-events   |     | to Snowflake BRONZE     |
| claims         |     +--------+--------+     +-------------------------+
+-------+--------+              |
        |                       |
        |               +-------v-------------------+
        +-------------->| Batch Extractors          |
                        | Postgres + CSV to BRONZE  |
                        +------------+--------------+
                                     |
                                     v
                           +---------+----------+
                           | Snowflake BRONZE   |
                           +---------+----------+
                                     |
                                     v
                           +---------+----------+
                           | Spark Cleaning     |
                           | + Aggregations     |
                           +---------+----------+
                                     |
                                     v
                       +-------------+----------------+
                       | dbt SILVER + GOLD models     |
                       | tests + docs                 |
                       +-------------+----------------+
                                     |
                   +-----------------+-------------------+
                   |                                     |
                   v                                     v
         +---------+----------+                 +--------+---------+
         | Streamlit Dashboard|                 | Reconciliation   |
         | portfolio analytics|                 | source-vs-target |
         +--------------------+                 +------------------+

            Airflow orchestrates all batch + transform + test steps
            GitHub Actions validates dbt and Python quality on push
```

## Technology Stack

- Streaming: Kafka + Zookeeper
- Batch Ingestion: Python + psycopg2 + pandas
- Distributed Processing: PySpark 3.5
- Warehouse: Snowflake
- Transformation: dbt (Snowflake adapter)
- Orchestration: Apache Airflow 2.8
- Containerization: Docker + Docker Compose
- Orchestration at scale: Kubernetes (Minikube)
- CI/CD: GitHub Actions
- BI App: Streamlit + Plotly

## Project Structure

```
travel-data-platform/
  ingestion/
    kafka/
    batch/
  processing/
    spark/
  transformation/
    dbt_project/
  orchestration/
    dags/
    plugins/
  infrastructure/
    docker/
    kubernetes/
  dashboard/
  data/
    seed/
    csv_samples/
  tests/
    recon_reports/
  readme_docs/
  .github/workflows/
```

## Quick Start

1. Copy `.env.example` to `.env` and fill in real credentials.
2. Start local stack:
   - `cd infrastructure/docker`
   - `docker-compose up -d`
3. Create PostgreSQL tables and seed data:
   - `docker exec -i <postgres_container> psql -U travel_user -d travel_db < data/seed/01_create_tables.sql`
   - `python data/seed/02_seed_data.py`
4. Run ingestion scripts:
   - `python ingestion/kafka/flight_producer.py`
   - `python ingestion/kafka/flight_consumer.py`
   - `python ingestion/batch/extract_postgres.py`
5. Run Spark jobs:
   - `python processing/spark/clean_bronze.py`
   - `python processing/spark/aggregate_flights.py`
6. Run dbt:
   - `cd transformation/dbt_project`
   - `dbt deps`
   - `dbt debug`
   - `dbt run --select staging`
   - `dbt run --select marts`
   - `dbt test`
7. Start dashboard:
   - `streamlit run dashboard/app.py`

## CI/CD

- `.github/workflows/dbt_ci.yml` runs dbt compile and tests.
- `.github/workflows/python_lint.yml` runs flake8 and black checks.
- `.github/workflows/streamlit_test.yml` validates dashboard syntax.

## Manual Actions Required

See full checklist in [readme_docs/07_manual_actions_checklist.md](readme_docs/07_manual_actions_checklist.md).
