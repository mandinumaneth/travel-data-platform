FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir \
    confluent-kafka==2.6.1 \
    requests==2.32.3 \
    python-dotenv==1.0.1 \
    psycopg2-binary==2.9.10

COPY ingestion/kafka/flight_producer.py /app/flight_producer.py
COPY ingestion/kafka/policy_producer.py /app/policy_producer.py

CMD ["python", "flight_producer.py"]
