# Runbooks and Operations

## Runbook: Start Local Platform

1. Ensure Docker Desktop is running.
2. Ensure `.env` exists with real values.
3. Start stack:
   - `cd infrastructure/docker`
   - `docker-compose up -d`
4. Confirm service health:
   - `docker-compose ps`
5. Open Airflow: `http://localhost:8080`
6. Open Streamlit: `http://localhost:8501`

## Runbook: Seed PostgreSQL Source

1. Create source tables:
   - run `data/seed/01_create_tables.sql` through postgres container
2. Seed synthetic data:
   - `python data/seed/02_seed_data.py`
3. Validate counts in PostgreSQL.

## Runbook: Validate Streaming Path

1. Start producer:
   - `python ingestion/kafka/flight_producer.py`
2. Start consumer:
   - `python ingestion/kafka/flight_consumer.py`
3. Wait 2 to 3 minutes.
4. Validate `TRAVEL_DW.BRONZE.RAW_FLIGHTS` count in Snowflake.

## Runbook: Validate Batch Path

1. `python ingestion/batch/generate_broker_csv.py`
2. `python ingestion/batch/load_broker_csv.py`
3. `python ingestion/batch/extract_postgres.py`
4. Validate Bronze table row counts in Snowflake.

## Runbook: Execute Transform Layer

1. Spark cleaning:
   - `python processing/spark/clean_bronze.py`
2. Spark aggregation:
   - `python processing/spark/aggregate_flights.py`
3. dbt transformations:
   - `cd transformation/dbt_project`
   - `dbt deps`
   - `dbt run --select staging`
   - `dbt run --select marts`
   - `dbt test`

## Runbook: Reconciliation

1. Execute:
   - `python tests/reconciliation.py`
2. Inspect JSON report under `tests/recon_reports/`.
3. If below 95% threshold:
   - check incremental state file
   - check failed upstream tasks
   - re-run extraction and dbt marts

## Runbook: Common Incidents

### Incident: Kafka producer connection refused

Checks:

- Verify kafka container is healthy.
- Validate bootstrap server in `.env`.
- Check docker logs for Kafka startup completion.

### Incident: dbt cannot connect to Snowflake

Checks:

- Validate account identifier format.
- Validate user/password/role/warehouse.
- Verify warehouse is resumed.

### Incident: Airflow DAG missing in UI

Checks:

- Run DAG file directly to find syntax issues.
- Confirm dags folder is mounted into airflow container.
- Confirm `PYTHONPATH` and paths used in BashOperator.

### Incident: Streamlit page empty

Checks:

- Validate Gold models built.
- Validate schema references in SQL.
- Confirm Snowflake credentials and role permissions.

## Runbook: Controlled Shutdown

1. Stop producer/consumer scripts with Ctrl+C.
2. Stop compose stack:
   - `cd infrastructure/docker`
   - `docker-compose down`
3. For full reset including volumes:
   - `docker-compose down -v`
