# dbt Medallion Explainer

## Medallion Design in This Project

### Bronze

Goal:

- Keep source data close to original form.

Examples:

- RAW_CUSTOMERS
- RAW_POLICIES
- RAW_CLAIMS
- RAW_FLIGHTS
- RAW_BROKER_COMMISSIONS

### Silver

Goal:

- Standardize naming, data types, and business-ready attributes.

Examples:

- stg_customers adds `full_name` and `country_iso_code`
- stg_policies adds `trip_duration_days`
- stg_claims adds `is_approved` and `days_to_process`

### Gold

Goal:

- Business consumption and KPI reporting.

Examples:

- dimensions: customers, destinations, brokers, dates
- facts: policies, claims

## Why This Pattern Works

- Bronze protects raw lineage.
- Silver centralizes quality and business rules.
- Gold simplifies BI and analytics.
- dbt lineage graph makes dependencies explicit.

## dbt Commands You Should Know

- `dbt deps`
- `dbt debug`
- `dbt run --select staging`
- `dbt run --select marts`
- `dbt test`
- `dbt docs generate`
- `dbt docs serve`

## Test Strategy Used

- uniqueness and not_null on primary business keys
- accepted values on policy status
- relationship tests claims -> policies
- positive amount checks for claims
- source freshness checks on Bronze
