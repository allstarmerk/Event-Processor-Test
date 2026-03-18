"""
Test suite for the Event Processor.

These tests pass. The candidate should evaluate whether they're
actually testing the right things.
"""

import json
import os
import tempfile
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


# ── TimeWindowFilter tests ────────────────────────────────────────

class TestTimeWindowFilter:
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
    # The test fixtures avoid timestamps at exactly the start boundary,

    def test_empty_input(self):
        tw = TimeWindowFilter(datetime(2024, 1, 1), datetime(2024, 12, 31))
        assert tw.filter_events([]) == []


# ── PriorityScorer tests ─────────────────────────────────────────

class TestPriorityScorer:
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


# ── Integration test ──────────────────────────────────────────────

class TestEventProcessorIntegration:
    def test_full_pipeline_small_dataset(self, sample_events, time_window):
        """Run the full pipeline on a small in-memory dataset."""
        # Write sample events to a temp file
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
            # With single-threaded execution, dedup works correctly
            assert stats["after_dedup"] == 3  # evt-003 is a dup of evt-001
            assert stats["duplicates_removed"] == 1

            # Verify output file
            with open(output_path) as f:
                output = json.load(f)
            assert len(output) == 3
            # Should be sorted by priority descending
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
             "timestamp": "2023-06-15T12:00:00",  # outside 2024 window
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
