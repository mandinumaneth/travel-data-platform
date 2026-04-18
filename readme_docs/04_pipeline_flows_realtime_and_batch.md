# Pipeline Flows: Real-Time and Batch

## Real-Time Flow (Flights)

1. `ingestion/kafka/flight_producer.py`
   - polls OpenSky
   - normalizes state vectors
   - publishes JSON to Kafka
2. `ingestion/kafka/flight_consumer.py`
   - consumes in batches of 100
   - creates RAW_FLIGHTS if needed
   - writes rows to Snowflake BRONZE
   - commits offsets only after successful write
3. `orchestration/dags/kafka_monitor.py`
   - tracks consumer lag
   - checks Snowflake RAW_FLIGHTS freshness

## Simulated Real-Time Flow (Policies)

1. `ingestion/kafka/policy_producer.py`
   - reads recent PostgreSQL policies (last hour)
   - publishes randomized policy-purchase events every 10 seconds

## Batch Flow (PostgreSQL and CSV)

1. `ingestion/batch/extract_postgres.py`
   - loads state.json checkpoint
   - extracts incremental rows by created_at
   - writes RAW_CUSTOMERS, RAW_POLICIES, RAW_CLAIMS
   - updates checkpoint timestamp
2. `ingestion/batch/generate_broker_csv.py`
   - creates daily broker commission file
3. `ingestion/batch/load_broker_csv.py`
   - loads newest broker CSV to RAW_BROKER_COMMISSIONS

## Processing + Modeling Flow

1. `processing/spark/clean_bronze.py`
   - deduplicates and standardizes flight + policy data
2. `processing/spark/aggregate_flights.py`
   - computes daily country aggregates
3. dbt staging models (SILVER)
4. dbt marts models (GOLD)
5. dbt tests + reconciliation

## Orchestrated Daily Flow (06:00)

Implemented in `orchestration/dags/daily_pipeline.py`:

- freshness check
- postgres extract
- broker file generate/load
- spark cleaning
- dbt staging
- dbt marts
- dbt tests
- reconciliation
- pipeline status logging
