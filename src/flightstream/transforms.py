"""Transformations shared by the streaming job and the local batch runner.

Every function takes and returns a DataFrame and works on both batch and
streaming DataFrames, except where noted.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .schema import POSITION_SCHEMA

EARTH_RADIUS_KM = 6371.0
AIRPORT_RADIUS_KM = 30.0
AIRPORT_MAX_ALTITUDE_M = 3000.0


def parse_events(raw: DataFrame, value_col: str = "value") -> DataFrame:
    """bronze -> typed rows. Rows that are not valid JSON keep a null payload
    and are sent to quarantine by `quality_flags`."""
    parsed = raw.withColumn("p", F.from_json(F.col(value_col).cast("string"), POSITION_SCHEMA))
    return parsed.select(
        "p.*",
        F.col(value_col).cast("string").alias("raw_value"),
        *[c for c in raw.columns if c in ("enqueued_time", "partition", "offset", "_ingested_at")],
    ).withColumn(
        "event_time",
        F.timestamp_seconds(F.coalesce("time_position", "last_contact", "snapshot_time")),
    )


def quality_flags(df: DataFrame) -> DataFrame:
    reasons = F.array_compact(F.array(
        F.when(F.col("icao24").isNull(), F.lit("unparseable_or_missing_icao24")),
        F.when(~F.col("icao24").rlike("^[0-9a-f]{6}$"), F.lit("bad_icao24")),
        F.when(F.col("latitude").isNull() | F.col("longitude").isNull(), F.lit("no_position")),
        F.when(~F.col("latitude").between(-90, 90) | ~F.col("longitude").between(-180, 180), F.lit("position_out_of_range")),
        F.when(F.col("baro_altitude").isNotNull() & ~F.col("baro_altitude").between(-500, 20000), F.lit("altitude_out_of_range")),
        F.when(F.col("velocity").isNotNull() & ~F.col("velocity").between(0, 400), F.lit("velocity_out_of_range")),
        F.when(F.col("event_time").isNull(), F.lit("no_timestamp")),
    ))
    return df.withColumn("dq_errors", reasons).withColumn("is_valid", F.size("dq_errors") == 0)


def valid(df: DataFrame) -> DataFrame:
    return df.filter("is_valid").drop("is_valid", "dq_errors", "raw_value")


def quarantine(df: DataFrame) -> DataFrame:
    return df.filter(~F.col("is_valid")).select("raw_value", "dq_errors", "event_time", F.current_timestamp().alias("quarantined_at"))


def enrich(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("icao24", F.lower("icao24"))
        .withColumn("altitude_m", F.coalesce("geo_altitude", "baro_altitude"))
        .withColumn("speed_kmh", F.round(F.col("velocity") * 3.6, 1))
        .withColumn(
            "phase",
            F.when(F.col("on_ground"), "ground")
            .when(F.col("vertical_rate") > 1.5, "climbing")
            .when(F.col("vertical_rate") < -1.5, "descending")
            .otherwise("cruise"),
        )
    )


def deduplicate(df: DataFrame, watermark: str = "10 minutes") -> DataFrame:
    """OpenSky returns the last known position, so the same (aircraft, time_position)
    shows up in several consecutive polls. In streaming the state is bounded by the
    watermark; late events beyond it are dropped."""
    keys = ["icao24", "time_position"]
    if df.isStreaming:
        return df.withWatermark("event_time", watermark).dropDuplicatesWithinWatermark(keys)
    return df.dropDuplicates(keys)


def haversine_km(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


def assign_airport(positions: DataFrame, airports: DataFrame) -> DataFrame:
    """Tags positions that are close to an airport and low enough to be landing,
    taking off or on the ground. A cheap lat/lon box filter runs before the
    haversine distance so the cross join stays small."""
    ap = F.broadcast(airports.select(
        F.col("icao").alias("airport_icao"),
        F.col("name").alias("airport_name"),
        F.col("lat").alias("ap_lat"),
        F.col("lon").alias("ap_lon"),
    ))
    box = AIRPORT_RADIUS_KM / 111.0
    near = positions.join(
        ap,
        (F.col("latitude").between(F.col("ap_lat") - box, F.col("ap_lat") + box))
        & (F.col("longitude").between(F.col("ap_lon") - 2 * box, F.col("ap_lon") + 2 * box)),
        "inner",
    )
    return (
        near.withColumn("distance_km", haversine_km(F.col("latitude"), F.col("longitude"), F.col("ap_lat"), F.col("ap_lon")))
        .filter((F.col("distance_km") <= AIRPORT_RADIUS_KM)
                & (F.col("on_ground") | (F.coalesce("altitude_m", F.lit(0.0)) <= AIRPORT_MAX_ALTITUDE_M)))
        .drop("ap_lat", "ap_lon")
    )


def airport_traffic(tagged: DataFrame, window: str = "5 minutes", watermark: str | None = "10 minutes") -> DataFrame:
    # pass watermark=None when the input stream already has one
    if tagged.isStreaming and watermark:
        tagged = tagged.withWatermark("event_time", watermark)
    return (
        tagged.groupBy(F.window("event_time", window).alias("w"), "airport_icao", "airport_name")
        .agg(
            F.approx_count_distinct("icao24").alias("aircraft"),
            F.approx_count_distinct(F.when(F.col("phase") == "descending", F.col("icao24"))).alias("arriving"),
            F.approx_count_distinct(F.when(F.col("phase") == "climbing", F.col("icao24"))).alias("departing"),
            F.approx_count_distinct(F.when(F.col("phase") == "ground", F.col("icao24"))).alias("on_ground"),
        )
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "*")
        .drop("w")
    )


def country_traffic(positions: DataFrame, window: str = "5 minutes", watermark: str | None = "10 minutes") -> DataFrame:
    if positions.isStreaming and watermark:
        positions = positions.withWatermark("event_time", watermark)
    airborne = positions.filter(~F.col("on_ground"))
    return (
        airborne.groupBy(F.window("event_time", window).alias("w"), "origin_country")
        .agg(
            F.approx_count_distinct("icao24").alias("aircraft"),
            F.round(F.avg("speed_kmh"), 1).alias("avg_speed_kmh"),
            F.round(F.avg("altitude_m"), 0).alias("avg_altitude_m"),
        )
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "*")
        .drop("w")
    )
