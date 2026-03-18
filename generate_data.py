"""
Generates test event datasets of varying sizes.
Run this to produce the test fixtures.
"""

import json
import random
import uuid
from datetime import datetime, timedelta

SOURCES = ["web-app", "mobile-api", "batch-ingest", "partner-feed", "internal-cron"]
EVENT_TYPES = ["transaction", "security", "system", "user_action", "audit"]
SEVERITIES = ["critical", "error", "warning", "info", "debug"]

# The time window the processor defaults to
WINDOW_START = datetime(2024, 1, 1, 0, 0, 0)
WINDOW_END = datetime(2024, 12, 31, 23, 59, 59)


def random_timestamp(include_boundary: bool = False) -> str:
    """Generate a random timestamp in 2024. Occasionally generate boundary values."""
    if include_boundary and random.random() < 0.05:
        # 5% chance of hitting exactly the window start boundary
        return WINDOW_START.isoformat()

    delta = WINDOW_END - WINDOW_START
    random_seconds = random.randint(0, int(delta.total_seconds()))
    ts = WINDOW_START + timedelta(seconds=random_seconds)
    return ts.isoformat()


def make_event(include_boundary: bool = False) -> dict:
    return {
        "id": str(uuid.uuid4()),
        "source": random.choice(SOURCES),
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": random_timestamp(include_boundary),
        "payload": {
            "severity": random.choice(SEVERITIES),
            "message": f"Event {uuid.uuid4().hex[:8]}",
            "value": round(random.uniform(0, 1000), 2),
        },
    }


def make_duplicate(event: dict) -> dict:
    """Create a duplicate with a different ID but same source/type/payload."""
    dup = event.copy()
    dup["id"] = str(uuid.uuid4())  # new ID, same fingerprint
    # Slightly different timestamp (as if received again later)
    original_ts = datetime.fromisoformat(event["timestamp"])
    dup["timestamp"] = (original_ts + timedelta(seconds=random.randint(1, 300))).isoformat()
    return dup


def generate_dataset(size: int, duplicate_ratio: float = 0.15) -> list:
    """
    Generate a dataset of `size` events.
    `duplicate_ratio` of them will be duplicates of earlier events.
    """
    events = []
    originals = []

    for _ in range(size):
        if originals and random.random() < duplicate_ratio:
            # Create a duplicate of a random earlier event
            original = random.choice(originals)
            events.append(make_duplicate(original))
        else:
            event = make_event(include_boundary=True)
            events.append(event)
            originals.append(event)

    # Shuffle so duplicates aren't clustered
    random.shuffle(events)
    return events


def main():
    random.seed(42)  # Reproducible datasets

    datasets = {
        "data/events_small.json": 50,
        "data/events_medium.json": 500,
        "data/events_large.json": 5000,
    }

    for filepath, size in datasets.items():
        data = generate_dataset(size)
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Generated {filepath}: {size} events")

    # Also generate a targeted dataset that exposes the off-by-one bug
    boundary_events = []
    for i in range(20):
        event = make_event()
        # Force some events to exactly the window start
        if i < 5:
            event["timestamp"] = WINDOW_START.isoformat()
        boundary_events.append(event)

    with open("data/events_boundary_test.json", "w") as f:
        json.dump(boundary_events, f, indent=2)
    print("Generated data/events_boundary_test.json: 20 events (5 at exact boundary)")


if __name__ == "__main__":
    main()
