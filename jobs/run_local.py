"""Local version of the pipeline, reading the JSON Lines files written by the
producer with `--sink file`.

    python jobs/run_local.py --input data/landing --output data/gold          # batch
    python jobs/run_local.py --input data/landing --output data/gold --stream # micro-batches every 30 s

Batch mode writes Parquet. Stream mode uses the same streaming code path as
Databricks (watermarks, dedup within watermark) and prints each micro-batch;
on Windows it needs Hadoop winutils, so run it from WSL or Linux.
"""
import argparse
import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from flightstream import transforms as T

log = logging.getLogger("flights.local")
ROOT = Path(__file__).resolve().parents[1]


def spark_session():
    s = (
        SparkSession.builder.master("local[*]")
        .appName("flights-local")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    return s


def read_airports(spark):
    return spark.read.option("header", True).option("inferSchema", True).csv(str(ROOT / "data" / "airports.csv"))


def batch(spark, src, out):
    # explicit file list: listing a directory through Hadoop needs winutils on Windows
    files = sorted(str(f) for f in Path(src).glob("*.jsonl"))
    if not files:
        raise SystemExit(f"no .jsonl files in {src}")
    raw = spark.read.text(files)
    flagged = T.quality_flags(T.parse_events(raw)).cache()
    positions = T.deduplicate(T.enrich(T.valid(flagged))).cache()
    rejected = T.quarantine(flagged)

    tables = {
        "positions": positions,
        "quarantine": rejected,
        "airport_traffic_5min": T.airport_traffic(T.assign_airport(positions, read_airports(spark))),
        "country_traffic_5min": T.country_traffic(positions),
    }
    out.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        pdf = df.toPandas()
        pdf.to_parquet(out / f"{name}.parquet", index=False)
        log.info("%-22s %d rows", name, len(pdf))

    top = (
        tables["airport_traffic_5min"].groupBy("airport_icao", "airport_name")
        .agg(F.max("aircraft").alias("peak_aircraft_5min"))
        .orderBy(F.desc("peak_aircraft_5min")).limit(10)
    )
    top.show(truncate=False)


def stream(spark, src):
    raw = spark.readStream.format("text").option("maxFilesPerTrigger", 5).load(src)
    positions = T.deduplicate(T.enrich(T.valid(T.quality_flags(T.parse_events(raw)))), watermark="2 minutes")
    traffic = T.airport_traffic(T.assign_airport(positions, read_airports(spark)), window="1 minute", watermark=None)
    q = (
        traffic.writeStream.outputMode("update").format("console")
        .option("truncate", False).option("numRows", 20)
        .trigger(processingTime="30 seconds").start()
    )
    q.awaitTermination()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="data/landing")
    p.add_argument("--output", default="data/gold")
    p.add_argument("--stream", action="store_true")
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = spark_session()
    src = str(Path(a.input).resolve())
    if a.stream:
        stream(spark, src)
    else:
        batch(spark, src, Path(a.output))
    spark.stop()


if __name__ == "__main__":
    main()
