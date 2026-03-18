# DECISIONS.md — Event Processor Bug Hunt

## Tools Used

- **Claude Code (claude-sonnet-4-6)** — primary analysis, code reading, bug identification, fix writing, test authoring
- **Python 3.14 / pytest 9.0** — running the test suite to verify fixes
- **Git** — version history review

---

## Bugs Found and Fixed

### Bug 1 — Race Condition in `DeduplicationCache` (TOCTOU)

**File:** `event_processor.py`, `DeduplicationCache.is_duplicate` / `mark_seen`

**What was wrong:**

`_process_single` called `is_duplicate()` and `mark_seen()` as two separate operations:

```python
if self.dedup_cache.is_duplicate(fp):
    return None
self.dedup_cache.mark_seen(fp)
```

`is_duplicate()` read `_seen` without holding any lock.  Under concurrent execution with
`ThreadPoolExecutor`, two threads processing the same fingerprint could both call
`is_duplicate()`, both find it absent, and both proceed — resulting in the same logical
event appearing twice in the output.  This is a classic TOCTOU (time-of-check /
time-of-use) race.  The original code even had a comment acknowledging this on line 46,
and the integration test papered over it by forcing `max_workers=1`.

**How we found it:**

The comment on lines 46–48 of the original source named the race explicitly.  The
integration test's `max_workers=1` comment ("With single-threaded execution, dedup works
correctly") confirmed the bug was known but untested under real concurrency.

**Fix:**

Added `check_and_mark(fingerprint)` — a single method that performs the check and the
write inside one `with self._lock:` block, making the operation atomic.  Updated
`_process_single` to call this instead:

```python
if self.dedup_cache.check_and_mark(fp):
    return None
```

`is_duplicate()` and `mark_seen()` are kept for backward compatibility but are now also
properly locked (they were not before).

**Tests added:**

- `test_check_and_mark_returns_false_for_new_fingerprint` — verifies the happy path:
  new fingerprint returns False and is recorded.
- `test_check_and_mark_returns_true_for_seen_fingerprint` — verifies the duplicate path.
- `test_check_and_mark_is_atomic_under_concurrency` — 50 threads all call
  `check_and_mark` for the same fingerprint at the same instant using a `threading.Barrier`.
  Asserts exactly 1 thread gets `False` (first writer wins) and 49 get `True`.  This
  test would have failed reliably against the old code.
- `test_concurrent_dedup_no_duplicates_pass_through` — integration-level regression:
  100 events all sharing the same fingerprint are processed with `max_workers=8`.
  Exactly 1 must survive dedup.  This is the test the original suite should have had.

---

### Bug 2 — Off-by-One at `TimeWindowFilter` Start Boundary

**File:** `event_processor.py`, `TimeWindowFilter.filter_events` line 90

**What was wrong:**

```python
if ts > self.window_start and ts <= self.window_end:   # BEFORE fix
```

The class docstring declares the window as `[start, end]` — inclusive on both sides.
The implementation used `>` (strict greater-than) for `window_start`, making it exclusive.
Any event timestamped at exactly `window_start` was silently dropped.

The data generator (`generate_data.py`) deliberately creates 5 events at exactly
`WINDOW_START` in `events_boundary_test.json` and includes a 5% chance of hitting the
boundary in the general datasets — making this a real production concern.  The original
test suite had a comment acknowledging the gap: *"The test fixtures avoid timestamps at
exactly the start boundary"*.

**How we found it:**

Code comment at line 87–90 described the problem verbatim.  The `generate_data.py`
`events_boundary_test.json` generation (lines 94–105) was clearly written to expose this
exact bug.

**Fix:**

Changed `>` to `>=`:

```python
if ts >= self.window_start and ts <= self.window_end:   # AFTER fix
```

**Tests added:**

- `test_start_boundary_is_inclusive` — an event at exactly `window_start` must be
  returned.  This is the test the comment said was intentionally missing.
- `test_event_just_before_start_is_excluded` — confirms we did not accidentally make
  the boundary too permissive.
- `test_both_boundaries_inclusive` — exercises start, interior, and end in one fixture
  to document the full contract.
- `test_pipeline_includes_events_at_window_start` — integration-level regression:
  a boundary event plus an interior event; both must appear in the output after the fix.

---

### Bug 3 — Unbounded Memory Growth in `DeduplicationCache`

**File:** `event_processor.py`, `DeduplicationCache._seen`

**What was wrong:**

The `_seen` dict accumulated every fingerprint forever.  No eviction, no TTL, no
max-size cap.  For a short-lived batch job processing a fixed file this is harmless, but
for a long-running service or very large datasets memory usage climbs without bound.  The
original code had a comment (lines 58–62) explicitly calling this out.

**How we found it:**

The comment in the original source named the problem and even specified the symptom
("only visible if you profile or run a very large dataset (10k+ events)").

**Fix:**

Added an optional `ttl_seconds` parameter to `DeduplicationCache`.  When set, the cache
evicts entries older than the TTL on every `check_and_mark` call.  The eviction runs
inside the existing lock so it remains thread-safe.

```python
cache = DeduplicationCache(ttl_seconds=3600)   # evict fingerprints older than 1 h
```

`EventProcessor` exposes this through a new `dedup_ttl_seconds` parameter so callers
can tune it without touching the cache directly.  Default is `None` (no eviction —
preserves original behaviour for batch workloads that want exact once-per-run dedup).

**Tradeoff:** a TTL means a re-submitted event that arrives after the TTL expires will
be processed again.  For a batch job with a fixed input file this should never happen;
for a streaming service the TTL should be set longer than the maximum expected duplicate
re-delivery window.

**Tests added:**

- `test_ttl_eviction_removes_expired_entries` — adds an entry, sleeps past the TTL,
  triggers a new `check_and_mark`, and confirms the old entry was evicted.
- `test_no_ttl_retains_all_entries` — without a TTL, all 100 entries remain (original
  batch-job semantics preserved).
- `test_same_fingerprint_after_ttl_is_not_duplicate` — after eviction, the same
  fingerprint is no longer treated as a duplicate.

---

## Additional Tests (Not Bug Regressions)

These were added to catch common mistakes and document expected behaviour:

| Test | Class | Purpose |
|------|-------|---------|
| `test_missing_severity_defaults_to_info` | `TestPriorityScorer` | Payload with no `severity` key should score as `info` (weight 25), not crash. |
| `test_all_known_severities_produce_positive_scores` | `TestPriorityScorer` | Every documented severity must yield a positive score — prevents accidentally mapping a level to zero or negative. |
| `test_score_truncates_float_not_rounds` | `TestPriorityScorer` | `int()` truncates toward zero; 37.5 → 37.  Documents the rounding contract so future changes do not accidentally switch to `round()`. |
| `test_output_sorted_by_priority_descending` | `TestEventProcessorIntegration` | Confirms the output file is always ordered highest-priority first, including a name-check on `output[0]`. |
| `test_empty_input_file` | `TestEventProcessorIntegration` | Empty input produces empty output and zero stats — guards against index-out-of-range regressions. |

---

## What I Would Do With More Time

1. **Add exception handling in `_process_batch`.**  Currently, if a single event has a
   malformed timestamp or a non-serialisable payload, `future.result()` re-raises and
   the entire batch fails.  A `try/except` with a dead-letter list would let bad events
   be flagged without killing valid ones.

2. **Validate event fields at load time.**  `load_events` uses `item["id"]` with no
   guard — a `KeyError` on a malformed file produces an unhelpful traceback.  Adding a
   validation step (or using a schema library like Pydantic) would give callers a clear
   error message.

3. **Complete or remove the `exactly_once.py` prototype.**  Jamie's WAL + checkpoint
   approach is interesting but the missing `recover()` implementation means it does not
   actually provide the exactly-once guarantee it promises.  I would either finish it
   (implement WAL replay on startup, clean up stale checkpoint files, add concurrency
   tests) or remove it to avoid giving future readers false confidence.

4. **Benchmark the TTL eviction strategy.**  Evicting inside `check_and_mark` means
   every single call may iterate the full `_seen` dict.  For very high throughput a
   background eviction thread or a size-bounded LRU structure (e.g. `cachetools.TTLCache`)
   would be more efficient.

5. **Property-based testing.**  Tools like `hypothesis` would let us generate arbitrary
   event payloads and timestamps to find edge cases in fingerprinting and time-window
   logic that hand-written fixtures might miss.
