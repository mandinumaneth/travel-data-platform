# Tools and Technologies

## Kafka

Role:

- Real-time event bus for flight and simulated policy events.

Used components:

- `flight_producer.py`
- `policy_producer.py`
- `flight_consumer.py`
- Airflow monitor DAG for lag checks

Key learning:

- Producer retry patterns and infinite loops
- Batch consumer commits after successful sink write
- Lag monitoring and health checks

## Airflow

Role:

- Schedules and orchestrates extraction, processing, dbt, and reconciliation.

Used components:

- `daily_pipeline.py`
- `kafka_monitor.py`

Key learning:

- Task dependencies and trigger rules
- PythonOperator vs BashOperator
- Skip behavior with AirflowSkipException

## Spark (PySpark)

Role:

- Cleans and deduplicates large-volume Bronze data.

Used components:

- `clean_bronze.py`
- `aggregate_flights.py`

Key learning:

- Window-based dedupe patterns
- Distributed aggregations
- Snowflake connector configuration

## dbt

Role:

- SQL transformations, testing, and lineage in Snowflake.

Used components:

- staging models (Silver)
- marts models (Gold)
- schema tests and custom test

Key learning:

- source definitions and freshness
- incremental fact models
- dimension/fact design with surrogate keys

## Snowflake

Role:

- Central cloud data warehouse for medallion architecture.

Schemas:

- BRONZE: raw and pre-cleaned
- SILVER: standardized staging views
- GOLD: analytics-ready dimensions and facts

## Docker and Docker Compose

Role:

- Local reproducible platform runtime for data services.

Key learning:

- service dependencies and health checks
- mounting code and DAGs
- adding app services to infra stack

## Kubernetes (Minikube)

Role:

- Local cluster practice for deployment-style operations.

Used components:

- Kafka producer deployment
- Airflow webserver and scheduler deployments
- NodePort service and secret/config templates

## GitHub Actions

Role:

- CI checks for dbt and Python code quality.

Workflows:

- dbt CI
- Python lint
- Streamlit syntax test

## Streamlit

Role:

- Developer portfolio dashboard connected directly to Snowflake.

Pages:

- Revenue Overview
- Claims Analysis
- Broker Performance
- Flight Intelligence
