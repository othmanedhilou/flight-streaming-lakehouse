"""Databricks Structured Streaming job: Event Hubs -> bronze -> silver -> gold.

Event Hubs is read through its Kafka endpoint, so no extra connector library is
needed on the cluster. Each layer is its own stream with its own checkpoint,
which means one layer can be restarted or reprocessed without touching the others.

    python jobs/streaming_job.py --trigger 1m
    python jobs/streaming_job.py --available-now      (catch-up run, then stop)
"""
import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from flightstream import transforms as T

CATALOG = os.environ.get("FLIGHTS_CATALOG", "flights")
CHECKPOINTS = os.environ.get("FLIGHTS_CHECKPOINTS", "abfss://lakehouse@stflightslakedev.dfs.core.windows.net/_checkpoints")
EH_NAMESPACE = os.environ.get("EVENTHUB_NAMESPACE", "evhns-flights-dev")
EH_NAME = os.environ.get("EVENTHUB_NAME", "flights")
SECRET_SCOPE = os.environ.get("SECRET_SCOPE", "flights")
AIRPORTS_PATH = os.environ.get("AIRPORTS_PATH", "/Workspace/Shared/flight-streaming-lakehouse/data/airports.csv")


def table(layer, name):
    return f"{CATALOG}.{layer}.{name}"


def kafka_options(connection_string: str) -> dict:
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{connection_string}";'
    )
    return {
        "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
        "subscribe": EH_NAME,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
        "kafka.group.id": "databricks",
        "startingOffsets": "earliest",
        "maxOffsetsPerTrigger": "200000",
        "failOnDataLoss": "false",
    }


def trigger_kwargs(args):
    return {"availableNow": True} if args.available_now else {"processingTime": args.trigger}


def start_bronze(spark, conn, args):
    raw = (
        spark.readStream.format("kafka").options(**kafka_options(conn)).load()
        .select(
            F.col("value").cast("string").alias("value"),
            F.col("timestamp").alias("enqueued_time"),
            "partition",
            "offset",
            F.current_timestamp().alias("_ingested_at"),
        )
    )
    return (
        raw.writeStream.format("delta")
        .option("checkpointLocation", f"{CHECKPOINTS}/bronze_positions")
        .trigger(**trigger_kwargs(args))
        .toTable(table("bronze", "positions_raw"))
    )


def start_silver(spark, args):
    flagged = T.quality_flags(T.parse_events(spark.readStream.table(table("bronze", "positions_raw"))))

    good = T.deduplicate(T.enrich(T.valid(flagged)))
    q1 = (
        good.writeStream.format("delta")
        .option("checkpointLocation", f"{CHECKPOINTS}/silver_positions")
        .trigger(**trigger_kwargs(args))
        .toTable(table("silver", "positions"))
    )
    q2 = (
        T.quarantine(flagged).writeStream.format("delta")
        .option("checkpointLocation", f"{CHECKPOINTS}/silver_quarantine")
        .trigger(**trigger_kwargs(args))
        .toTable(table("silver", "positions_quarantine"))
    )
    return [q1, q2]


def start_gold(spark, args):
    positions = spark.readStream.table(table("silver", "positions"))
    airports = spark.read.option("header", True).option("inferSchema", True).csv(AIRPORTS_PATH)

    q1 = (
        T.airport_traffic(T.assign_airport(positions, airports))
        .writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINTS}/gold_airport_traffic")
        .trigger(**trigger_kwargs(args))
        .toTable(table("gold", "airport_traffic_5min"))
    )
    q2 = (
        T.country_traffic(positions)
        .writeStream.format("delta").outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINTS}/gold_country_traffic")
        .trigger(**trigger_kwargs(args))
        .toTable(table("gold", "country_traffic_5min"))
    )
    return [q1, q2]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--layers", default="bronze,silver,gold")
    p.add_argument("--trigger", default="1 minute")
    p.add_argument("--available-now", action="store_true")
    args = p.parse_args()

    spark = SparkSession.builder.getOrCreate()
    for schema in ("bronze", "silver", "gold"):
        spark.sql(f"create schema if not exists {CATALOG}.{schema}")

    layers = args.layers.split(",")
    queries = []
    if "bronze" in layers:
        from pyspark.dbutils import DBUtils

        conn = DBUtils(spark).secrets.get(SECRET_SCOPE, "eventhub-listen-connection-string")
        queries.append(start_bronze(spark, conn, args))
    if "silver" in layers:
        queries += start_silver(spark, args)
    if "gold" in layers:
        queries += start_gold(spark, args)

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
