# Architecture and Data Flow

## High-Level Pattern

The platform uses a hybrid design:

- Real-time flight telemetry via Kafka
- Daily batch ingestion for policy domain data
- Central Snowflake warehouse with medallion layers
- dbt for model governance
- Airflow for orchestration and scheduling

## End-to-End Flow

1. Flight producer calls OpenSky every 30 seconds.
2. Producer publishes events to Kafka topic `flight-events`.
3. Flight consumer reads Kafka batches and writes to `TRAVEL_DW.BRONZE.RAW_FLIGHTS`.
4. PostgreSQL extractor loads customers, policies, claims incrementally into Bronze.
5. Broker CSV generator creates daily finance-like input file.
6. Broker CSV loader lands broker data in Bronze.
7. Spark cleans and deduplicates Bronze data and writes clean Bronze tables.
8. dbt staging models standardize data into Silver views.
9. dbt marts models create Gold dimensions and facts.
10. Reconciliation validates source vs target row-count quality.
11. Streamlit queries Gold/Silver for analytics pages.

## Scheduling Strategy

- Airflow daily pipeline: 06:00 every day
- Kafka monitor DAG: every 15 minutes
- GitHub Actions CI: every push/PR

## Data Contracts

- Raw ingestion tables are append-focused.
- Silver applies standardization and derived columns.
- Gold serves business KPIs with dimensions/facts.
- Tests enforce uniqueness, nullability, relationship integrity, and positive claim amounts.
