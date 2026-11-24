from pyspark.sql import functions as F

from flightstream import transforms as T

from .conftest import position


def _silver(raw):
    flagged = T.quality_flags(T.parse_events(raw))
    return flagged, T.deduplicate(T.enrich(T.valid(flagged)))


def test_parse_and_event_time(make_raw):
    df = T.parse_events(make_raw([position()]))
    row = df.first()
    assert row.icao24 == "3c6444"
    assert str(row.event_time) == "2025-11-24 16:00:00"


def test_event_time_falls_back_to_last_contact(make_raw):
    row = T.parse_events(make_raw([position(time_position=None)])).first()
    assert str(row.event_time) == "2025-11-24 16:00:01"


def test_quality_rules(make_raw):
    raw = make_raw([
        position(),
        position(icao24="ZZZ"),
        position(latitude=None),
        position(baro_altitude=50000.0),
        position(velocity=900.0),
        "not json at all",
    ])
    flagged, _ = _silver(raw)
    errors = [r.dq_errors for r in flagged.orderBy(F.monotonically_increasing_id()).collect()]
    assert errors[0] == []
    assert errors[1] == ["bad_icao24"]
    assert errors[2] == ["no_position"]
    assert errors[3] == ["altitude_out_of_range"]
    assert errors[4] == ["velocity_out_of_range"]
    assert "unparseable_or_missing_icao24" in errors[5]
    assert T.quarantine(flagged).count() == 5


def test_duplicates_from_consecutive_polls_removed(make_raw):
    raw = make_raw([
        position(snapshot_time=1764000005),
        position(snapshot_time=1764000015),          # same position seen again in next poll
        position(time_position=1764000010, snapshot_time=1764000015),
    ])
    _, silver = _silver(raw)
    assert silver.count() == 2


def test_phase(make_raw):
    raw = make_raw([
        position(icao24="000001", vertical_rate=-4.0),
        position(icao24="000002", vertical_rate=6.0),
        position(icao24="000003", vertical_rate=0.0),
        position(icao24="000004", on_ground=True, vertical_rate=None),
    ])
    _, silver = _silver(raw)
    phases = {r.icao24: r.phase for r in silver.collect()}
    assert phases == {"000001": "descending", "000002": "climbing", "000003": "cruise", "000004": "ground"}


def test_haversine_paris_london(spark):
    df = spark.createDataFrame([(49.0097, 2.5479, 51.4700, -0.4543)], "a double, b double, c double, d double")
    km = df.select(T.haversine_km(F.col("a"), F.col("b"), F.col("c"), F.col("d")).alias("km")).first().km
    assert 340 < km < 350


def test_assign_airport_only_close_and_low(make_raw, airports):
    raw = make_raw([
        position(icao24="00000a", latitude=49.05, longitude=2.60, geo_altitude=800.0),     # near CDG, low
        position(icao24="00000b", latitude=49.05, longitude=2.60, geo_altitude=10000.0),   # above CDG, cruising
        position(icao24="00000c", latitude=46.0, longitude=0.0, geo_altitude=900.0),       # countryside
        position(icao24="00000d", latitude=33.37, longitude=-7.58, on_ground=True),        # on ground at CMN
    ])
    _, silver = _silver(raw)
    tagged = {r.icao24: r.airport_icao for r in T.assign_airport(silver, airports).collect()}
    assert tagged == {"00000a": "LFPG", "00000d": "GMMN"}


def test_airport_traffic_window(make_raw, airports):
    t0 = 1764000000
    raw = make_raw([
        position(icao24="00000a", time_position=t0, vertical_rate=-5.0),
        position(icao24="00000b", time_position=t0 + 60, vertical_rate=5.0),
        position(icao24="00000c", time_position=t0 + 120, on_ground=True),
        position(icao24="00000a", time_position=t0 + 400, vertical_rate=-5.0),   # next 5 min window
    ])
    _, silver = _silver(raw)
    rows = T.airport_traffic(T.assign_airport(silver, airports)).orderBy("window_start").collect()
    assert len(rows) == 2
    first = rows[0]
    assert first.airport_icao == "LFPG"
    assert (first.aircraft, first.arriving, first.departing, first.on_ground) == (3, 1, 1, 1)
    assert rows[1].aircraft == 1


def test_country_traffic_excludes_ground(make_raw):
    raw = make_raw([
        position(icao24="00000a"),
        position(icao24="00000b", origin_country="France"),
        position(icao24="00000c", on_ground=True),
    ])
    _, silver = _silver(raw)
    rows = {r.origin_country: r.aircraft for r in T.country_traffic(silver).collect()}
    assert rows == {"Germany": 1, "France": 1}
