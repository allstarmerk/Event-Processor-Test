"""
Event Processor Service
-----------------------
Ingests events from a JSON file, deduplicates them, applies business rules
(time-window filtering + priority scoring), and writes processed output.

Supports concurrent processing for performance.
"""

import json
import hashlib
import threading
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed


@dataclass
class Event:
    id: str
    source: str
    event_type: str
    timestamp: str
    payload: Dict
    priority: int = 0

    def fingerprint(self) -> str:
        """Generate a dedup fingerprint based on source + type + payload."""
        raw = f"{self.source}:{self.event_type}:{json.dumps(self.payload, sort_keys=True)}"
        return hashlib.md5(raw.encode()).hexdigest()


class DeduplicationCache:
    """
    Tracks seen event fingerprints to filter duplicates.
    Thread-safe via locking.

    ttl_seconds: if set, fingerprints older than this are evicted so memory
    does not grow unboundedly across long-running or high-volume workloads.
    """

    def __init__(self, ttl_seconds: Optional[float] = None):
        self._seen: Dict[str, float] = {}  # fingerprint -> timestamp_added
        self._lock = threading.Lock()
        self.ttl_seconds = ttl_seconds

    def _evict_expired(self) -> None:
        """Remove entries older than TTL. Caller must hold self._lock."""
        if self.ttl_seconds is None:
            return
        cutoff = time.time() - self.ttl_seconds
        expired = [fp for fp, ts in self._seen.items() if ts < cutoff]
        for fp in expired:
            del self._seen[fp]

    def check_and_mark(self, fingerprint: str) -> bool:
        """
        Atomically check whether fingerprint is a duplicate and, if not,
        mark it as seen.  Returns True if it was already seen (duplicate).

        This is the correct method to use from concurrent code — it avoids
        the TOCTOU race that existed when is_duplicate() and mark_seen()
        were called as two separate operations without a shared lock.
        """
        with self._lock:
            self._evict_expired()
            if fingerprint in self._seen:
                return True
            self._seen[fingerprint] = time.time()
            return False

    def is_duplicate(self, fingerprint: str) -> bool:
        """Read-only duplicate check (non-atomic — prefer check_and_mark)."""
        with self._lock:
            return fingerprint in self._seen

    def mark_seen(self, fingerprint: str) -> None:
        """Record that we've processed this fingerprint."""
        with self._lock:
            self._seen[fingerprint] = time.time()

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._seen)


class TimeWindowFilter:
    """
    Filters events to only include those within a specified time window.
    Window is defined as [start, end] inclusive on both sides.
    """

    def __init__(self, window_start: datetime, window_end: datetime):
        self.window_start = window_start
        self.window_end = window_end

    def filter_events(self, events: List[Event]) -> List[Event]:
        """Return events whose timestamp falls within the window (inclusive)."""
        filtered = []
        for i in range(len(events)):
            event = events[i]
            ts = datetime.fromisoformat(event.timestamp)

            if ts >= self.window_start and ts <= self.window_end:
                filtered.append(event)

        return filtered


class PriorityScorer:
    """
    Assigns a priority score to events based on business rules.
    Higher score = higher priority.
    """

    PRIORITY_WEIGHTS = {
        "critical": 100,
        "error": 75,
        "warning": 50,
        "info": 25,
        "debug": 10,
    }

    TYPE_MULTIPLIERS = {
        "transaction": 2.0,
        "security": 3.0,
        "system": 1.5,
        "user_action": 1.0,
        "audit": 1.2,
    }

    def score(self, event: Event) -> int:
        severity = event.payload.get("severity", "info")
        base = self.PRIORITY_WEIGHTS.get(severity, 25)
        multiplier = self.TYPE_MULTIPLIERS.get(event.event_type, 1.0)
        return int(base * multiplier)


class EventProcessor:
    """
    Main processor. Reads events, deduplicates, filters by time window,
    scores priority, and writes output.
    """

    def __init__(
        self,
        window_start: datetime,
        window_end: datetime,
        max_workers: int = 4,
        dedup_ttl_seconds: Optional[float] = None,
    ):
        self.dedup_cache = DeduplicationCache(ttl_seconds=dedup_ttl_seconds)
        self.time_filter = TimeWindowFilter(window_start, window_end)
        self.scorer = PriorityScorer()
        self.max_workers = max_workers
        self._results: List[Dict] = []
        self._results_lock = threading.Lock()

    def load_events(self, filepath: str) -> List[Event]:
        """Load events from a JSON file."""
        with open(filepath, "r") as f:
            raw = json.load(f)

        events = []
        for item in raw:
            events.append(Event(
                id=item["id"],
                source=item["source"],
                event_type=item["event_type"],
                timestamp=item["timestamp"],
                payload=item.get("payload", {}),
            ))
        return events

    def _process_single(self, event: Event) -> Optional[Dict]:
        """Process one event: dedup check, score, return if valid."""
        fp = event.fingerprint()

        if self.dedup_cache.check_and_mark(fp):
            return None

        event.priority = self.scorer.score(event)
        return asdict(event)

    def _process_batch(self, events: List[Event]) -> None:
        """Process a batch of events concurrently."""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._process_single, event): event
                for event in events
            }
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    with self._results_lock:
                        self._results.append(result)

    def run(self, input_path: str, output_path: str) -> Dict:
        """
        Full pipeline: load -> filter time window -> dedup + score -> write.
        Returns processing stats.
        """
        # Load
        all_events = self.load_events(input_path)
        total_loaded = len(all_events)

        # Time window filter
        windowed = self.time_filter.filter_events(all_events)
        after_window_filter = len(windowed)

        # Process (dedup + scoring) in concurrent batches
        self._results = []
        self._process_batch(windowed)

        # Sort by priority descending
        self._results.sort(key=lambda x: x["priority"], reverse=True)

        # Write output
        with open(output_path, "w") as f:
            json.dump(self._results, f, indent=2)

        return {
            "total_loaded": total_loaded,
            "after_time_filter": after_window_filter,
            "after_dedup": len(self._results),
            "duplicates_removed": after_window_filter - len(self._results),
            "cache_size": self.dedup_cache.size,
        }


def main():
    import sys

    if len(sys.argv) < 3:
        print("Usage: python event_processor.py <input.json> <output.json>")
        print("Optional: --start 2024-01-01T00:00:00 --end 2024-12-31T23:59:59")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # Default window: all of 2024
    start = datetime(2024, 1, 1, 0, 0, 0)
    end = datetime(2024, 12, 31, 23, 59, 59)

    # Parse optional flags
    args = sys.argv[3:]
    for i in range(0, len(args), 2):
        if args[i] == "--start":
            start = datetime.fromisoformat(args[i + 1])
        elif args[i] == "--end":
            end = datetime.fromisoformat(args[i + 1])

    processor = EventProcessor(window_start=start, window_end=end)
    stats = processor.run(input_path, output_path)

    print("Processing complete:")
    for k, v in stats.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
