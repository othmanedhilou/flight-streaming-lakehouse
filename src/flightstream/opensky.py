"""Small client for the OpenSky Network REST API (/states/all)."""
from __future__ import annotations

import time
from dataclasses import dataclass

import requests

API_URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"

# order of the fields in each state vector, see https://openskynetwork.github.io/opensky-api/rest.html
STATE_FIELDS = [
    "icao24", "callsign", "origin_country", "time_position", "last_contact",
    "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
    "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
    "spi", "position_source", "category",
]


@dataclass(frozen=True)
class BoundingBox:
    lamin: float
    lomin: float
    lamax: float
    lomax: float

    def params(self) -> dict:
        return {"lamin": self.lamin, "lomin": self.lomin, "lamax": self.lamax, "lomax": self.lomax}


# Europe + North Africa, covers every airport in data/airports.csv except the long-haul ones
EUROPE_MAGHREB = BoundingBox(27.0, -18.0, 61.0, 32.0)


def parse_states(payload: dict) -> list[dict]:
    snapshot = payload.get("time")
    records = []
    for raw in payload.get("states") or []:
        row = dict(zip(STATE_FIELDS, raw, strict=False))  # older responses have 17 fields, no category
        row.pop("sensors", None)
        if row.get("callsign"):
            row["callsign"] = row["callsign"].strip() or None
        row["snapshot_time"] = snapshot
        records.append(row)
    return records


class OpenSkyClient:
    def __init__(self, client_id: str | None = None, client_secret: str | None = None, timeout: int = 20):
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout
        self.session = requests.Session()
        self._token: str | None = None
        self._token_expiry = 0.0

    def _auth_header(self) -> dict:
        if not (self.client_id and self.client_secret):
            return {}
        if self._token is None or time.time() > self._token_expiry - 60:
            r = self.session.post(
                TOKEN_URL,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=self.timeout,
            )
            r.raise_for_status()
            body = r.json()
            self._token = body["access_token"]
            self._token_expiry = time.time() + body.get("expires_in", 1800)
        return {"Authorization": f"Bearer {self._token}"}

    def states(self, bbox: BoundingBox | None = None) -> list[dict]:
        params = bbox.params() if bbox else {}
        r = self.session.get(API_URL, params=params, headers=self._auth_header(), timeout=self.timeout)
        if r.status_code == 429:
            retry = int(r.headers.get("X-Rate-Limit-Retry-After-Seconds", "60"))
            raise RateLimited(retry)
        r.raise_for_status()
        return parse_states(r.json())


class RateLimited(Exception):
    def __init__(self, retry_after: int):
        super().__init__(f"rate limited, retry in {retry_after}s")
        self.retry_after = retry_after
