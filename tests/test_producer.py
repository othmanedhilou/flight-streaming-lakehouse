import json

from flightstream.opensky import parse_states
from flightstream.sinks import FileSink

PAYLOAD = {
    "time": 1764000005,
    "states": [
        ["3c6444", "DLH9U   ", "Germany", 1764000000, 1764000001, 2.55, 49.01, 600.0, False,
         80.0, 270.0, -4.0, None, 620.0, "1000", False, 0, 0],
        ["02a1b2", "        ", "Morocco", None, 1764000003, -7.58, 33.37, None, True,
         0.0, 0.0, None, None, None, None, False, 0],
    ],
}


def test_parse_states():
    rows = parse_states(PAYLOAD)
    assert len(rows) == 2
    assert rows[0]["callsign"] == "DLH9U"
    assert rows[0]["snapshot_time"] == 1764000005
    assert "sensors" not in rows[0]
    assert rows[1]["callsign"] is None
    assert rows[1].get("category") is None


def test_parse_empty():
    assert parse_states({"time": 1, "states": None}) == []


def test_file_sink(tmp_path):
    sink = FileSink(str(tmp_path))
    assert sink.send(parse_states(PAYLOAD)) == 2
    files = list(tmp_path.glob("*.jsonl"))
    assert len(files) == 1
    lines = files[0].read_text(encoding="utf-8").splitlines()
    assert json.loads(lines[0])["icao24"] == "3c6444"
    assert not list(tmp_path.glob(".*.tmp"))
