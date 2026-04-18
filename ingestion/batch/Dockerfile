FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.10 \
    snowflake-connector-python[pandas]==3.12.2 \
    pandas==2.2.3 \
    faker==30.8.2 \
    python-dotenv==1.0.1

COPY ingestion/batch /app/ingestion/batch
COPY data /app/data

CMD ["python", "/app/ingestion/batch/extract_postgres.py"]
