# Manual Actions Checklist

These steps require your direct execution because they involve credentials, external accounts, local runtime, or UI interactions.

## Accounts and External Services

- Create Snowflake trial account and note account identifier.
- Create Snowflake database `TRAVEL_DW` and schemas `BRONZE`, `SILVER`, `GOLD`.
- Create GitHub repository secrets for CI:
  - SNOWFLAKE_ACCOUNT
  - SNOWFLAKE_USER
  - SNOWFLAKE_PASSWORD
  - SNOWFLAKE_DATABASE
  - SNOWFLAKE_WAREHOUSE
  - SNOWFLAKE_ROLE (if used)

## Local Environment Setup

- Copy `.env.example` to `.env` and fill real credentials.
- Install required tools: Python 3.11, Docker Desktop, dbt CLI, Java (for Spark), Minikube, kubectl.
- Install Python packages:
  - `pip install -r requirements.txt`

## Docker Runtime Actions

- Start stack:
  - `cd infrastructure/docker`
  - `docker-compose up -d`
- Verify services:
  - `docker-compose ps`
- Create Kafka topics if not auto-created.

## Database Seeding

- Run table creation SQL in PostgreSQL.
- Run data seeding script.
- Validate source row counts in PostgreSQL.

## Snowflake Validation

- Confirm Bronze tables receive data from Kafka and batch scripts.
- Confirm Spark outputs exist.
- Run dbt and validate Silver and Gold objects.

## Airflow UI Actions

- Open Airflow UI at localhost:8080.
- Enable DAGs.
- Trigger `travel_data_pipeline` manually once.
- Inspect logs and task outcomes.

## Kubernetes Actions

- Start Minikube.
- Build images into Minikube or push to accessible registry.
- Create `travel-secrets` with real values.
- Apply manifests in `infrastructure/kubernetes`.
- Verify pods and services.

## CI/CD Actions

- Push branch to GitHub.
- Verify all workflow runs are green.
- Fix secrets/config if dbt auth fails.

## Dashboard Actions

- Run `streamlit run dashboard/app.py`.
- Validate all pages return data.
- Optionally run dashboard via Docker service.
