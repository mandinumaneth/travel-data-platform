# Data Sources and Databases

## Source 1: OpenSky Network API

Type:

- Public live REST API

What it provides:

- Aircraft state vectors and flight telemetry

How used:

- Producer polls endpoint every 30 seconds
- Events are normalized and sent to Kafka

## Source 2: PostgreSQL Operational Store

Type:

- Batch source seeded with synthetic data

Tables:

- `customers`
- `policies`
- `claims`

How used:

- Seed script creates realistic records
- Incremental extractor reads newly created rows only

## Source 3: Broker CSV Files

Type:

- Batch file ingest

What it simulates:

- Daily finance upload for broker commission data

How used:

- Daily CSV generation script
- Loader reads latest file and writes to Snowflake Bronze

## Warehouse: Snowflake TRAVEL_DW

### BRONZE

Purpose:

- Raw landing zone for ingested data.

Typical tables:

- RAW_FLIGHTS
- RAW_CUSTOMERS
- RAW_POLICIES
- RAW_CLAIMS
- RAW_BROKER_COMMISSIONS
- BRONZE_FLIGHTS_CLEAN
- BRONZE_POLICIES_CLEAN
- FLIGHT_DAILY_STATS

### SILVER

Purpose:

- Cleaned and standardized staging views.

Typical models:

- stg_customers
- stg_policies
- stg_claims
- stg_brokers
- stg_flights
- stg_flight_stats

### GOLD

Purpose:

- Business analytics layer with dimensions/facts.

Typical models:

- dim_customers
- dim_destinations
- dim_brokers
- dim_date
- fact_policies
- fact_claims

## Data Quality Boundaries

- dbt source freshness checks on Bronze tables
- dbt tests for uniqueness, not-null, relationships, accepted values
- Custom claim amount positivity assertion
- Cross-system reconciliation threshold (>=95%)
