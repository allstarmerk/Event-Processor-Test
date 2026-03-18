"""
ABANDONED BRANCH: Exactly-Once Delivery Guarantee
Author: Jamie (no longer on team)
Status: Incomplete — works for small datasets but unclear if it scales.

This approach uses a write-ahead log (WAL) + checkpoint mechanism to ensure
each event is processed exactly once, even across restarts. Jamie left before
finishing the recovery logic.

The approach is clever but has significant tradeoffs:
  - Adds I/O overhead (WAL writes on every event)
  - Checkpoint files accumulate if cleanup isn't implemented
  - Recovery from partial failures is incomplete
  - No tests were written
"""

import json
import os
import time
from typing import Dict, List, Optional


class WriteAheadLog:
    """
    Append-only log that records each event before processing.
    On recovery, replay the log and skip already-checkpointed events.
    """

    def __init__(self, log_dir: str = ".wal"):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self.log_path = os.path.join(log_dir, f"wal_{int(time.time())}.jsonl")
        self._handle = open(self.log_path, "a")

    def append(self, event_id: str, fingerprint: str) -> None:
        entry = {
            "event_id": event_id,
            "fingerprint": fingerprint,
            "status": "pending",
            "ts": time.time(),
        }
        self._handle.write(json.dumps(entry) + "\n")
        self._handle.flush()

    def mark_complete(self, event_id: str) -> None:
        entry = {
            "event_id": event_id,
            "status": "complete",
            "ts": time.time(),
        }
        self._handle.write(json.dumps(entry) + "\n")
        self._handle.flush()

    def close(self):
        self._handle.close()


class CheckpointManager:
    """
    Periodically writes a checkpoint file containing the set of
    fully-processed event fingerprints. On restart, load the latest
    checkpoint to know what's already done.
    """

    def __init__(self, checkpoint_dir: str = ".checkpoints"):
        self.checkpoint_dir = checkpoint_dir
        os.makedirs(checkpoint_dir, exist_ok=True)
        self._processed: set = set()
        self._load_latest()

    def _load_latest(self) -> None:
        files = sorted(
            [f for f in os.listdir(self.checkpoint_dir) if f.endswith(".json")],
            reverse=True,
        )
        if files:
            with open(os.path.join(self.checkpoint_dir, files[0])) as f:
                self._processed = set(json.load(f))

    def is_processed(self, fingerprint: str) -> bool:
        return fingerprint in self._processed

    def record(self, fingerprint: str) -> None:
        self._processed.add(fingerprint)

    def save_checkpoint(self) -> None:
        path = os.path.join(
            self.checkpoint_dir,
            f"checkpoint_{int(time.time())}.json",
        )
        with open(path, "w") as f:
            json.dump(list(self._processed), f)

    @property
    def count(self) -> int:
        return len(self._processed)


class ExactlyOnceProcessor:
    """
    Wraps the event processing logic with WAL + checkpoint for
    exactly-once semantics.

    INCOMPLETE:
    - Recovery logic (replaying WAL after crash) is not implemented
    - No cleanup of old WAL/checkpoint files
    - Not tested under concurrent access
    - Unclear how this interacts with the existing ThreadPoolExecutor batching
    """

    def __init__(self):
        self.wal = WriteAheadLog()
        self.checkpoint = CheckpointManager()

    def should_process(self, event_id: str, fingerprint: str) -> bool:
        if self.checkpoint.is_processed(fingerprint):
            return False
        self.wal.append(event_id, fingerprint)
        return True

    def complete(self, event_id: str, fingerprint: str) -> None:
        self.wal.mark_complete(event_id)
        self.checkpoint.record(fingerprint)

    def finalize(self) -> None:
        self.checkpoint.save_checkpoint()
        self.wal.close()

    # TODO: implement recover() to replay WAL entries that are
    # "pending" but not "complete" and not in the checkpoint.
    # Jamie's notes say "just replay from last checkpoint"
    # but that doesn't handle the case where checkpoint save
    # itself failed mid-write.
