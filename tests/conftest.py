import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[1]")
        .appName("flights-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def position(**kw):
    base = {
        "icao24": "3c6444", "callsign": "DLH9U", "origin_country": "Germany",
        "time_position": 1764000000, "last_contact": 1764000001,
        "longitude": 2.55, "latitude": 49.01, "baro_altitude": 600.0, "on_ground": False,
        "velocity": 80.0, "true_track": 270.0, "vertical_rate": -4.0, "geo_altitude": 620.0,
        "squawk": "1000", "spi": False, "position_source": 0, "category": 0,
        "snapshot_time": 1764000005,
    }
    base.update(kw)
    return base


@pytest.fixture
def make_raw(spark):
    def _make(rows):
        values = [r if isinstance(r, str) else json.dumps(r) for r in rows]
        return spark.createDataFrame([(v,) for v in values], "value string")
    return _make


@pytest.fixture
def airports(spark):
    return spark.read.option("header", True).option("inferSchema", True).csv(str(ROOT / "data" / "airports.csv"))
