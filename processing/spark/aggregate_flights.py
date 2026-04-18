from __future__ import annotations

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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


def main() -> None:
    spark = build_spark_session("aggregate_flights")
    options = snowflake_options()

    flights_df = (
        spark.read.format("snowflake")
        .options(**options)
        .option("dbtable", "TRAVEL_DW.BRONZE.BRONZE_FLIGHTS_CLEAN")
        .load()
    )

    stats_df = (
        flights_df.withColumn("FLIGHT_DAY", F.to_date("EVENT_TIMESTAMP"))
        .groupBy("FLIGHT_DAY", "ORIGIN_COUNTRY")
        .agg(
            F.count("*").alias("FLIGHT_COUNT"),
            F.avg("ALTITUDE").alias("AVG_ALTITUDE"),
            F.avg("VELOCITY").alias("AVG_VELOCITY"),
            (F.avg(F.when(F.col("ON_GROUND") == True, F.lit(1)).otherwise(F.lit(0))) * 100).alias(
                "PCT_ON_GROUND"
            ),
        )
        .orderBy("FLIGHT_DAY", "ORIGIN_COUNTRY")
    )

    (
        stats_df.write.format("snowflake")
        .options(**options)
        .option("dbtable", "TRAVEL_DW.BRONZE.FLIGHT_DAILY_STATS")
        .mode("overwrite")
        .save()
    )

    print(f"Wrote {stats_df.count()} daily country flight aggregate rows to BRONZE.FLIGHT_DAILY_STATS")
    spark.stop()


if __name__ == "__main__":
    main()
