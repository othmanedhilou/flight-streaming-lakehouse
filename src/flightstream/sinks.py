from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path


class FileSink:
    """Writes one JSON Lines file per poll. Used for local runs and tests."""

    def __init__(self, directory: str):
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)

    def send(self, records: list[dict]) -> int:
        if not records:
            return 0
        name = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f") + ".jsonl"
        tmp = self.dir / f".{name}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.writelines(json.dumps(r, separators=(",", ":")) + "\n" for r in records)
        # rename so a streaming reader never picks up a half-written file
        os.replace(tmp, self.dir / name)
        return len(records)

    def close(self):
        pass


class EventHubSink:
    """Sends records to Azure Event Hubs, partitioned by aircraft so that all
    positions of one aircraft stay ordered."""

    def __init__(self, connection_string: str, eventhub_name: str, max_batch_events: int = 500):
        from azure.eventhub import EventHubProducerClient

        self.client = EventHubProducerClient.from_connection_string(
            connection_string, eventhub_name=eventhub_name
        )
        self.max_batch_events = max_batch_events

    def send(self, records: list[dict]) -> int:
        from azure.eventhub import EventData

        by_key: dict[str, list[dict]] = {}
        for r in records:
            by_key.setdefault(r["icao24"][-1], []).append(r)

        sent = 0
        for key, rows in by_key.items():
            batch = self.client.create_batch(partition_key=key)
            for r in rows:
                event = EventData(json.dumps(r, separators=(",", ":")))
                try:
                    batch.add(event)
                except ValueError:
                    self.client.send_batch(batch)
                    sent += len(batch)
                    batch = self.client.create_batch(partition_key=key)
                    batch.add(event)
            if len(batch):
                self.client.send_batch(batch)
                sent += len(batch)
        return sent

    def close(self):
        self.client.close()
