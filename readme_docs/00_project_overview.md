# Project Overview

## What You Built

Travel Data Platform is a production-style data engineering system for a travel insurance business. It combines real-time streaming and daily batch processing, then standardizes and serves data for analytics.

## Business Questions Answered

- Which routes and countries produce high claims?
- How much premium revenue is generated monthly?
- Which brokers are top-performing by commission?
- What are approval and rejection patterns by claim type?
- Where are live flights originating from in near real time?

## Why This Project Is Portfolio-Strong

- Covers complete modern stack expected in data engineering roles.
- Demonstrates both pipeline implementation and operational reliability.
- Includes infrastructure, orchestration, transformation, quality controls, and dashboards.
- Includes CI/CD and container orchestration, not only local scripts.

## Core Layers

- Ingestion: Kafka real-time + batch extractors
- Processing: Spark data cleaning and aggregation
- Transformation: dbt medallion models in Snowflake
- Orchestration: Airflow DAGs
- Observability/Quality: reconciliation and dbt tests
- Delivery: Streamlit analytics app

## Repository Branch Strategy

- `main`: stable releases
- `dev`: active implementation and testing
