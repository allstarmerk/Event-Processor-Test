"""
Test suite for the Event Processor.

Covers:
  - Original passing tests (kept as-is)
  - Regression tests for each of the three bugs that were fixed
  - Additional robustness tests for concurrent dedup, boundary conditions,
    TTL eviction, priority scoring edge cases, and pipeline integration
"""

import json
import os
import tempfile
import threading
import time
import pytest
from datetime import datetime
from event_processor import (
    Event,
    DeduplicationCache,
    TimeWindowFilter,
    PriorityScorer,
    EventProcessor,
)


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def sample_events():
    """A small set of events for basic testing."""
    return [
        Event(
            id="evt-001",
            source="web-app",
            event_type="transaction",
            timestamp="2024-06-15T10:30:00",
            payload={"severity": "critical", "message": "Payment failed", "value": 500.0},
        ),
        Event(
            id="evt-002",
            source="mobile-api",
            event_type="user_action",
            timestamp="2024-06-15T11:00:00",
            payload={"severity": "info", "message": "User login", "value": 0},
        ),
        Event(
            id="evt-003",
            source="web-app",
            event_type="transaction",
            timestamp="2024-06-15T10:30:00",
            payload={"severity": "critical", "message": "Payment failed", "value": 500.0},
        ),  # duplicate of evt-001 (same source + type + payload)
        Event(
            id="evt-004",
            source="batch-ingest",
            event_type="security",
            timestamp="2024-03-01T08:00:00",
            payload={"severity": "error", "message": "Brute force detected", "value": 42},
        ),
    ]


@pytest.fixture
def time_window():
    return (
        datetime(2024, 1, 1, 0, 0, 0),
        datetime(2024, 12, 31, 23, 59, 59),
    )


# ── DeduplicationCache tests ─────────────────────────────────────

class TestDeduplicationCache:
    # --- original tests (kept) ---

    def test_new_fingerprint_is_not_duplicate(self):
        cache = DeduplicationCache()
        assert cache.is_duplicate("abc123") is False

    def test_seen_fingerprint_is_duplicate(self):
        cache = DeduplicationCache()
        cache.mark_seen("abc123")
        assert cache.is_duplicate("abc123") is True

    def test_different_fingerprints_are_independent(self):
        cache = DeduplicationCache()
        cache.mark_seen("abc123")
        assert cache.is_duplicate("xyz789") is False

    def test_cache_size_tracks_entries(self):
        cache = DeduplicationCache()
        cache.mark_seen("a")
        cache.mark_seen("b")
        cache.mark_seen("c")
        assert cache.size == 3

    # --- regression: Bug 1 — TOCTOU race condition ---

    def test_check_and_mark_returns_false_for_new_fingerprint(self):
        """check_and_mark on a new fingerprint should return False (not duplicate)
        and record it in one atomic operation."""
        cache = DeduplicationCache()
        assert cache.check_and_mark("fp-new") is False
        assert cache.is_duplicate("fp-new") is True  # now recorded

    def test_check_and_mark_returns_true_for_seen_fingerprint(self):
        """check_and_mark on an already-seen fingerprint should return True."""
        cache = DeduplicationCache()
        cache.check_and_mark("fp-seen")
        assert cache.check_and_mark("fp-seen") is True

    def test_check_and_mark_is_atomic_under_concurrency(self):
        """
        Regression test for the TOCTOU race condition (Bug 1).

        Spawn many threads that all call check_and_mark for the same fingerprint
        simultaneously.  Exactly ONE thread should get False (first writer wins);
        all others must get True (duplicate).  Before the fix, multiple threads
        could slip through the non-atomic is_duplicate + mark_seen pair and all
        return False, letting duplicates pass.
        """
        cache = DeduplicationCache()
        fingerprint = "shared-fp"
        results = []
        barrier = threading.Barrier(50)

        def worker():
            barrier.wait()  # all threads start at the same instant
            results.append(cache.check_and_mark(fingerprint))

        threads = [threading.Thread(target=worker) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly one thread should have seen it as new (False = not duplicate)
        assert results.count(False) == 1
        assert results.count(True) == 49

    # --- regression: Bug 3 — unbounded memory / TTL eviction ---

    def test_ttl_eviction_removes_expired_entries(self):
        """
        Regression test for the memory-leak bug (Bug 3).

        With a very short TTL, entries added before the TTL expires should be
        evicted on the next check_and_mark call.  Before the fix, _seen grew
        forever with no eviction path at all.
        """
        cache = DeduplicationCache(ttl_seconds=0.05)  # 50 ms TTL
        cache.check_and_mark("old-fp")
        assert cache.size == 1

        time.sleep(0.1)  # let it expire

        # Trigger eviction via a new check_and_mark
        cache.check_and_mark("trigger-eviction")
        assert cache.is_duplicate("old-fp") is False  # evicted
        assert cache.size == 1  # only the new entry remains

    def test_no_ttl_retains_all_entries(self):
        """Without a TTL the cache keeps every entry (existing behaviour)."""
        cache = DeduplicationCache()
        for i in range(100):
            cache.check_and_mark(f"fp-{i}")
        assert cache.size == 100

    def test_same_fingerprint_after_ttl_is_not_duplicate(self):
        """An evicted fingerprint should be processable again after TTL."""
        cache = DeduplicationCache(ttl_seconds=0.05)
        cache.check_and_mark("fp")
        time.sleep(0.1)
        # After TTL, same fingerprint should appear as new
        assert cache.check_and_mark("fp") is False


# ── TimeWindowFilter tests ────────────────────────────────────────

class TestTimeWindowFilter:
    # --- original tests (kept) ---

    def test_filters_events_within_window(self):
        tw = TimeWindowFilter(
            datetime(2024, 6, 1), datetime(2024, 6, 30)
        )
        events = [
            Event("1", "a", "b", "2024-06-15T12:00:00", {}),
            Event("2", "a", "b", "2024-07-01T12:00:00", {}),  # outside
            Event("3", "a", "b", "2024-05-31T12:00:00", {}),  # outside
        ]
        result = tw.filter_events(events)
        assert len(result) == 1
        assert result[0].id == "1"

    def test_end_boundary_is_inclusive(self):
        tw = TimeWindowFilter(
            datetime(2024, 6, 1), datetime(2024, 6, 30)
        )
        events = [
            Event("1", "a", "b", "2024-06-30T00:00:00", {}),
        ]
        result = tw.filter_events(events)
        assert len(result) == 1

    def test_empty_input(self):
        tw = TimeWindowFilter(datetime(2024, 1, 1), datetime(2024, 12, 31))
        assert tw.filter_events([]) == []

    # --- regression: Bug 2 — start-boundary off-by-one ---

    def test_start_boundary_is_inclusive(self):
        """
        Regression test for the off-by-one bug (Bug 2).

        An event whose timestamp equals window_start exactly must be included.
        Before the fix the condition was `ts > window_start` (exclusive), which
        silently dropped every event at the boundary.
        """
        window_start = datetime(2024, 1, 1, 0, 0, 0)
        tw = TimeWindowFilter(window_start, datetime(2024, 12, 31, 23, 59, 59))
        events = [
            Event("boundary", "src", "system", "2024-01-01T00:00:00", {}),
        ]
        result = tw.filter_events(events)
        assert len(result) == 1
        assert result[0].id == "boundary"

    def test_event_just_before_start_is_excluded(self):
        """An event 1 second before window_start must be excluded."""
        tw = TimeWindowFilter(
            datetime(2024, 1, 1, 0, 0, 0),
            datetime(2024, 12, 31, 23, 59, 59),
        )
        events = [
            Event("before", "src", "system", "2023-12-31T23:59:59", {}),
        ]
        result = tw.filter_events(events)
        assert result == []

    def test_both_boundaries_inclusive(self):
        """Both start and end boundaries are inclusive — neither edge is dropped."""
        start = datetime(2024, 6, 1, 0, 0, 0)
        end = datetime(2024, 6, 30, 23, 59, 59)
        tw = TimeWindowFilter(start, end)
        events = [
            Event("start", "src", "system", "2024-06-01T00:00:00", {}),
            Event("mid",   "src", "system", "2024-06-15T12:00:00", {}),
            Event("end",   "src", "system", "2024-06-30T23:59:59", {}),
            Event("out",   "src", "system", "2024-07-01T00:00:00", {}),
        ]
        result = tw.filter_events(events)
        ids = [e.id for e in result]
        assert ids == ["start", "mid", "end"]


# ── PriorityScorer tests ─────────────────────────────────────────

class TestPriorityScorer:
    # --- original tests (kept) ---

    def test_critical_transaction_gets_highest_score(self):
        scorer = PriorityScorer()
        event = Event("1", "a", "transaction", "2024-01-01T00:00:00",
                       {"severity": "critical"})
        score = scorer.score(event)
        assert score == 200  # 100 * 2.0

    def test_security_error_scores_high(self):
        scorer = PriorityScorer()
        event = Event("1", "a", "security", "2024-01-01T00:00:00",
                       {"severity": "error"})
        score = scorer.score(event)
        assert score == 225  # 75 * 3.0

    def test_unknown_severity_defaults(self):
        scorer = PriorityScorer()
        event = Event("1", "a", "system", "2024-01-01T00:00:00",
                       {"severity": "unknown_level"})
        score = scorer.score(event)
        assert score == 37  # 25 * 1.5 = 37.5 -> int = 37

    def test_unknown_type_uses_1x_multiplier(self):
        scorer = PriorityScorer()
        event = Event("1", "a", "unknown_type", "2024-01-01T00:00:00",
                       {"severity": "warning"})
        score = scorer.score(event)
        assert score == 50  # 50 * 1.0

    # --- additional: scoring edge cases ---

    def test_missing_severity_defaults_to_info(self):
        """Events with no 'severity' in payload should score as 'info'."""
        scorer = PriorityScorer()
        event = Event("1", "a", "transaction", "2024-01-01T00:00:00", {})
        score = scorer.score(event)
        assert score == 50  # 25 (info) * 2.0 (transaction)

    def test_all_known_severities_produce_positive_scores(self):
        """Every documented severity level must yield a positive integer score."""
        scorer = PriorityScorer()
        for severity in ("critical", "error", "warning", "info", "debug"):
            event = Event("1", "a", "system", "2024-01-01T00:00:00",
                          {"severity": severity})
            assert scorer.score(event) > 0

    def test_score_truncates_float_not_rounds(self):
        """int() truncates toward zero — confirm 37.5 becomes 37, not 38."""
        scorer = PriorityScorer()
        event = Event("1", "a", "system", "2024-01-01T00:00:00",
                       {"severity": "unknown"})
        assert scorer.score(event) == 37  # 25 * 1.5 = 37.5 → int(37.5) = 37


# ── Integration tests ──────────────────────────────────────────────

class TestEventProcessorIntegration:
    # --- original tests (kept, single-worker variant preserved) ---

    def test_full_pipeline_small_dataset(self, sample_events, time_window):
        """Run the full pipeline on a small in-memory dataset."""
        raw = []
        for e in sample_events:
            raw.append({
                "id": e.id,
                "source": e.source,
                "event_type": e.event_type,
                "timestamp": e.timestamp,
                "payload": e.payload,
            })

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(raw, f)
            input_path = f.name

        output_path = input_path.replace(".json", "_output.json")

        try:
            processor = EventProcessor(
                window_start=time_window[0],
                window_end=time_window[1],
                max_workers=1,
            )
            stats = processor.run(input_path, output_path)

            assert stats["total_loaded"] == 4
            assert stats["after_dedup"] == 3  # evt-003 is a dup of evt-001
            assert stats["duplicates_removed"] == 1

            with open(output_path) as f:
                output = json.load(f)
            assert len(output) == 3
            priorities = [e["priority"] for e in output]
            assert priorities == sorted(priorities, reverse=True)
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_pipeline_removes_out_of_window_events(self):
        """Events outside the time window should be excluded."""
        raw = [
            {"id": "1", "source": "a", "event_type": "system",
             "timestamp": "2024-06-15T12:00:00",
             "payload": {"severity": "info", "message": "ok"}},
            {"id": "2", "source": "b", "event_type": "system",
             "timestamp": "2023-06-15T12:00:00",
             "payload": {"severity": "info", "message": "old"}},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(raw, f)
            input_path = f.name

        output_path = input_path.replace(".json", "_output.json")

        try:
            processor = EventProcessor(
                window_start=datetime(2024, 1, 1),
                window_end=datetime(2024, 12, 31),
            )
            stats = processor.run(input_path, output_path)

            assert stats["total_loaded"] == 2
            assert stats["after_time_filter"] == 1
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    # --- regression: Bug 1 — concurrent dedup must not pass duplicates ---

    def test_concurrent_dedup_no_duplicates_pass_through(self):
        """
        Regression test for the TOCTOU race condition (Bug 1) at the pipeline
        level, using real concurrency (max_workers=8).

        Build a dataset where every event is an identical duplicate of event-0
        (same source / type / payload).  After processing, exactly one event
        should survive.  Before the fix, multiple threads could simultaneously
        pass the non-atomic is_duplicate check and two or more copies would
        appear in the output.
        """
        base_payload = {"severity": "critical", "message": "dup-test", "value": 1}
        raw = [
            {
                "id": f"evt-{i}",
                "source": "web-app",
                "event_type": "transaction",
                "timestamp": "2024-06-15T12:00:00",
                "payload": base_payload,
            }
            for i in range(100)
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(raw, f)
            input_path = f.name

        output_path = input_path.replace(".json", "_output.json")

        try:
            processor = EventProcessor(
                window_start=datetime(2024, 1, 1),
                window_end=datetime(2024, 12, 31),
                max_workers=8,
            )
            stats = processor.run(input_path, output_path)

            assert stats["after_dedup"] == 1, (
                f"Expected 1 unique event after dedup, got {stats['after_dedup']}. "
                "Race condition may still be present."
            )
            assert stats["duplicates_removed"] == 99
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    # --- regression: Bug 2 — boundary-start events must not be dropped ---

    def test_pipeline_includes_events_at_window_start(self):
        """
        Regression test for the off-by-one boundary bug (Bug 2).

        Events timestamped at exactly window_start must appear in the output.
        Before the fix they were silently dropped by the > (exclusive) comparison.
        """
        window_start = datetime(2024, 1, 1, 0, 0, 0)
        raw = [
            {"id": "boundary-1", "source": "a", "event_type": "system",
             "timestamp": "2024-01-01T00:00:00",
             "payload": {"severity": "info", "message": "at-start"}},
            {"id": "interior-1", "source": "b", "event_type": "system",
             "timestamp": "2024-06-15T12:00:00",
             "payload": {"severity": "info", "message": "interior"}},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(raw, f)
            input_path = f.name

        output_path = input_path.replace(".json", "_output.json")

        try:
            processor = EventProcessor(
                window_start=window_start,
                window_end=datetime(2024, 12, 31, 23, 59, 59),
            )
            stats = processor.run(input_path, output_path)

            assert stats["after_time_filter"] == 2, (
                "Event at window_start should be included (off-by-one fix)."
            )
            with open(output_path) as f:
                output = json.load(f)
            ids = {e["id"] for e in output}
            assert "boundary-1" in ids
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    # --- additional: output is sorted by priority descending ---

    def test_output_sorted_by_priority_descending(self):
        """Output events must always be ordered highest-priority first."""
        raw = [
            {"id": "low",  "source": "a", "event_type": "user_action",
             "timestamp": "2024-06-01T00:00:00", "payload": {"severity": "debug"}},
            {"id": "high", "source": "b", "event_type": "security",
             "timestamp": "2024-06-01T00:00:00", "payload": {"severity": "critical"}},
            {"id": "mid",  "source": "c", "event_type": "system",
             "timestamp": "2024-06-01T00:00:00", "payload": {"severity": "warning"}},
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(raw, f)
            input_path = f.name

        output_path = input_path.replace(".json", "_output.json")

        try:
            processor = EventProcessor(
                window_start=datetime(2024, 1, 1),
                window_end=datetime(2024, 12, 31),
            )
            processor.run(input_path, output_path)

            with open(output_path) as f:
                output = json.load(f)

            priorities = [e["priority"] for e in output]
            assert priorities == sorted(priorities, reverse=True)
            assert output[0]["id"] == "high"
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)

    # --- additional: empty input produces empty output ---

    def test_empty_input_file(self):
        """An empty events file should produce an empty output with zero stats."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([], f)
            input_path = f.name

        output_path = input_path.replace(".json", "_output.json")

        try:
            processor = EventProcessor(
                window_start=datetime(2024, 1, 1),
                window_end=datetime(2024, 12, 31),
            )
            stats = processor.run(input_path, output_path)

            assert stats["total_loaded"] == 0
            assert stats["after_dedup"] == 0

            with open(output_path) as f:
                assert json.load(f) == []
        finally:
            os.unlink(input_path)
            if os.path.exists(output_path):
                os.unlink(output_path)
