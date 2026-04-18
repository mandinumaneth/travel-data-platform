FROM apache/airflow:2.8.4-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    psycopg2-binary \
    snowflake-connector-python[pandas] \
    faker \
    python-dotenv \
    confluent-kafka==2.6.1 \
    pyspark==3.5.2 \
    dbt-core==1.8.9 \
    dbt-snowflake==1.8.3 \
    && pip uninstall -y apache-airflow-providers-google

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=${JAVA_HOME}/bin:${PATH}
