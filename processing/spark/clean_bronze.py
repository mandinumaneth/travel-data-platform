from __future__ import annotations

import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType
from pyspark.sql.window import Window

load_dotenv()


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages",
            "net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5,net.snowflake:snowflake-jdbc:3.16.1",
        )
        .getOrCreate()
    )


def snowflake_options() -> dict[str, str]:
    account = os.getenv("SNOWFLAKE_ACCOUNT", "")
    return {
        "sfURL": f"{account}.snowflakecomputing.com",
        "sfUser": os.getenv("SNOWFLAKE_USER", ""),
        "sfPassword": os.getenv("SNOWFLAKE_PASSWORD", ""),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE", "TRAVEL_DW"),
        "sfSchema": "BRONZE",
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
        "sfRole": os.getenv("SNOWFLAKE_ROLE", ""),
    }


def read_table(
    spark: SparkSession, options: dict[str, str], table_name: str
) -> DataFrame:
    return (
        spark.read.format("snowflake")
        .options(**options)
        .option("dbtable", table_name)
        .load()
    )


def write_table(df: DataFrame, options: dict[str, str], table_name: str) -> None:
    (
        df.write.format("snowflake")
        .options(**options)
        .option("dbtable", table_name)
        .mode("overwrite")
        .save()
    )


def clean_flights(raw_flights: DataFrame) -> DataFrame:
    flights = raw_flights.withColumn(
        "WINDOW_TS",
        F.to_timestamp(
            F.from_unixtime(
                (F.unix_timestamp("EVENT_TIMESTAMP") / 30).cast("long") * 30
            )
        ),
    )

    window_spec = Window.partitionBy("ICAO24", "WINDOW_TS").orderBy(
        F.col("EVENT_TIMESTAMP").desc()
    )

    deduped = (
        flights.withColumn("ROW_NUM", F.row_number().over(window_spec))
        .filter(F.col("ROW_NUM") == 1)
        .drop("ROW_NUM", "WINDOW_TS")
    )

    cleaned = (
        deduped.filter(F.col("LATITUDE").isNotNull() & F.col("LONGITUDE").isNotNull())
        .withColumn(
            "ON_GROUND",
            F.when(
                F.col("ON_GROUND").isin(True, "true", "TRUE", 1), F.lit(True)
            ).otherwise(F.lit(False)),
        )
        .withColumn("INGESTED_AT", F.current_timestamp())
    )

    return cleaned


def clean_policies(raw_policies: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("POLICY_NUMBER").orderBy(
        F.col("CREATED_AT").desc()
    )

    deduped = (
        raw_policies.withColumn("ROW_NUM", F.row_number().over(window_spec))
        .filter(F.col("ROW_NUM") == 1)
        .drop("ROW_NUM")
    )

    cleaned = deduped
    for field in cleaned.schema.fields:
        if isinstance(field.dataType, StringType):
            cleaned = cleaned.withColumn(field.name, F.trim(F.col(field.name)))

    cleaned = cleaned.withColumn(
        "PREMIUM_AMOUNT", F.col("PREMIUM_AMOUNT").cast(DecimalType(10, 2))
    )

    return cleaned


def main() -> None:
    spark = build_spark_session("clean_bronze")
    options = snowflake_options()

    raw_flights = read_table(spark, options, "TRAVEL_DW.BRONZE.RAW_FLIGHTS")
    raw_policies = read_table(spark, options, "TRAVEL_DW.BRONZE.RAW_POLICIES")

    flights_before = raw_flights.count()
    policies_before = raw_policies.count()

    cleaned_flights = clean_flights(raw_flights)
    cleaned_policies = clean_policies(raw_policies)

    flights_after = cleaned_flights.count()
    policies_after = cleaned_policies.count()

    write_table(cleaned_flights, options, "TRAVEL_DW.BRONZE.BRONZE_FLIGHTS_CLEAN")
    write_table(cleaned_policies, options, "TRAVEL_DW.BRONZE.BRONZE_POLICIES_CLEAN")

    print("Spark cleaning completed.")
    print(
        f"RAW_FLIGHTS before={flights_before} after={flights_after} removed={flights_before - flights_after}"
    )
    print(
        f"RAW_POLICIES before={policies_before} after={policies_after} removed={policies_before - policies_after}"
    )

    spark.stop()


if __name__ == "__main__":
    main()
