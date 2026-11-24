"""Polls OpenSky and pushes every aircraft position to a sink.

    python -m flightstream.producer --sink file --out data/landing --interval 60
    python -m flightstream.producer --sink eventhub --interval 30

Event Hubs settings come from EVENTHUB_CONNECTION_STRING and EVENTHUB_NAME.
OpenSky credentials (optional, higher quota) from OPENSKY_CLIENT_ID / OPENSKY_CLIENT_SECRET.
"""
from __future__ import annotations

import argparse
import logging
import os
import signal
import time

import requests

from .opensky import EUROPE_MAGHREB, BoundingBox, OpenSkyClient, RateLimited
from .sinks import EventHubSink, FileSink

log = logging.getLogger("producer")


def build_sink(args):
    if args.sink == "file":
        return FileSink(args.out)
    return EventHubSink(os.environ["EVENTHUB_CONNECTION_STRING"], os.environ.get("EVENTHUB_NAME", "flights"))


def run(client: OpenSkyClient, sink, bbox: BoundingBox | None, interval: int, max_polls: int | None = None):
    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    polls = 0
    while not stop["flag"]:
        started = time.monotonic()
        try:
            records = client.states(bbox)
            n = sink.send(records)
            log.info("poll %d: %d positions sent", polls + 1, n)
        except RateLimited as e:
            log.warning("%s", e)
            time.sleep(e.retry_after)
            continue
        except requests.RequestException as e:
            log.error("request failed: %s", e)
        polls += 1
        if max_polls and polls >= max_polls:
            break
        time.sleep(max(0.0, interval - (time.monotonic() - started)))
    sink.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sink", choices=["file", "eventhub"], default="file")
    p.add_argument("--out", default="data/landing")
    p.add_argument("--interval", type=int, default=60)
    p.add_argument("--max-polls", type=int)
    p.add_argument("--world", action="store_true", help="no bounding box (costs more API credits)")
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    client = OpenSkyClient(os.environ.get("OPENSKY_CLIENT_ID"), os.environ.get("OPENSKY_CLIENT_SECRET"))
    run(client, build_sink(args), None if args.world else EUROPE_MAGHREB, args.interval, args.max_polls)


if __name__ == "__main__":
    main()
