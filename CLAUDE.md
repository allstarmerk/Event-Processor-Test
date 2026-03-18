# Event Processor — Claude Code Guide

## Project Overview

Take-home coding challenge: evaluate, debug, and improve a small event-processing service.

## File Structure

```
event_processor.py          # Main logic — dedup, time-filter, priority scoring, concurrent processing
generate_data.py            # Generates test JSON datasets (run once to populate data/)
tests/
  test_event_processor.py   # Existing test suite (passes but has coverage gaps)
  conftest.py               # Empty
abandoned_branch/
  exactly_once.py           # Incomplete WAL+checkpoint exactly-once prototype by "Jamie"
data/                       # Created by running generate_data.py
  events_small.json         # 50 events
  events_medium.json        # 500 events
  events_large.json         # 5,000 events
  events_boundary_test.json # 20 events, 5 at exact window_start boundary
```

## Running the Project

```bash
pip install pytest
python generate_data.py                              # generate test data
python event_processor.py data/events_small.json output.json
pytest tests/ -v
```

## Known Bugs (Found During Init)

### Bug 1 — Race condition in `DeduplicationCache.is_duplicate` (event_processor.py:45)
`is_duplicate()` reads `_seen` without holding the lock. Two threads can simultaneously
see a fingerprint as "not seen", both pass the check, and both call `mark_seen` — resulting
in duplicate events passing through. The comment at line 46 acknowledges this. The test
papers over it by using `max_workers=1`. Fix: make the check-and-set atomic under `_lock`.

### Bug 2 — Off-by-one in `TimeWindowFilter` (event_processor.py:90)
Condition is `ts > self.window_start` (exclusive), but the docstring says the window is
`[start, end]` inclusive on both sides. Events with timestamp exactly equal to `window_start`
are silently dropped. The `events_boundary_test.json` file (5 events at exactly WINDOW_START)
was generated to expose this. Fix: change `>` to `>=`.

### Bug 3 — Unbounded memory in `DeduplicationCache` (event_processor.py:58)
`_seen` dict grows forever with no eviction, TTL, or max-size limit. Only affects large/long-running workloads. Comment at line 58 acknowledges this.

## Task Requirements

- [ ] Find and document bugs (with debugging process)
- [ ] Fix each bug
- [ ] Add a regression test for each bug
- [ ] Write `DECISIONS.md` covering: bugs found, tools used, what you'd do differently
