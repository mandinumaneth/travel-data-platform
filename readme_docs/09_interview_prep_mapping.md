# Interview Prep Mapping

Use this project to answer interview questions with concrete implementation examples.

## Kafka Questions

Q: How do you ensure at-least-once delivery in Kafka consumer pipelines?
A: Offsets are committed only after successful Snowflake write in `flight_consumer.py`.

Q: How do you handle unstable APIs feeding Kafka producers?
A: Producer has explicit retry backoff for API errors and resilient infinite loop behavior.

## Airflow Questions

Q: How do you design robust DAG dependencies?
A: Daily DAG has explicit ordered dependencies and retries; reconciliation and status logging use ALL_DONE to collect failure context.

Q: How do you skip unnecessary pipeline runs?
A: Freshness pre-check raises AirflowSkipException when no new source rows exist.

## Spark Questions

Q: Why use Spark if dbt exists?
A: Spark handles distributed cleaning/dedup for high-volume datasets before SQL marts.

Q: How do you deduplicate event data?
A: Window function dedupe by `icao24` and 30-second bucket in `clean_bronze.py`.

## dbt Questions

Q: How did you implement medallion architecture?
A: Bronze landing in Snowflake, Silver staging models, Gold dimensions/facts with tests and freshness.

Q: How do you test data quality?
A: schema tests + relationship tests + accepted values + custom positive-amount test + source freshness.

## Snowflake Questions

Q: How do you organize warehouse schemas?
A: Dedicated BRONZE/SILVER/GOLD schemas inside TRAVEL_DW.

Q: How is incremental loading handled?
A: Source extraction uses created_at checkpoints in `state.json`; dbt facts are incremental models.

## DevOps Questions

Q: How do you deploy and validate data pipelines?
A: Docker Compose for local stack, Kubernetes manifests for cluster deployment, GitHub Actions for CI.

Q: What quality gates run on each push?
A: dbt compile/tests, flake8, black check, and Streamlit syntax compilation.

## Analytics Delivery Questions

Q: How do stakeholders consume outputs?
A: Streamlit dashboard with Revenue, Claims, Broker, and Flight Intelligence pages querying Snowflake.
