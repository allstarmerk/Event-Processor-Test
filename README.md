# Event Processor — Take-Home Challenge

## Overview

You're inheriting a small event-processing service. It reads events from a JSON file, deduplicates them, applies time-window filtering and priority scoring, and writes processed output. It works — or at least, the tests pass.

Your job is to 
  - evaluate this codebase
  - find and fix issues
  - make a design decisions

Use any tools, LLMs IDEs you are comfortable with, just let us knwo what you used. 

## Time Expectation

We expect this to take about an hour. If you're spending significantly more, you're likely overengineering. We value your time.

## Getting Started

```bash
# Generate test datasets
python generate_data.py

# Run the processor
python event_processor.py data/events_small.json output.json

# Run tests
pip install pytest
pytest tests/ -v
```

## What We'd Like You To Do

### 1. Bug Hunt

The tests pass, but the system has real bugs that affect correctness in production conditions. Find them.

For each bug you discover:
- Describe what's wrong
- Explain how you found it (your debugging process matters to us)
- Try to fix it
- Add a test that would have caught it

### 2. Write DECISIONS.md

Create a `DECISIONS.md` file that covers:
- Which bugs you found and how
- What tools you used
- What you'd do differently with more time


## What's in the Repo

```
event_processor.py      # Main processing logic
generate_data.py        # Generates test datasets of varying sizes
tests/                  # Existing test suite
  test_event_processor.py
abandoned_branch/       # Jamie's incomplete exactly-once work
  exactly_once.py
data/                   # Generated after running generate_data.py
  events_small.json     # 50 events
  events_medium.json    # 500 events
  events_large.json     # 5,000 events
  events_boundary_test.json  # 20 events with boundary timestamps
```

## Questions?

If something is ambiguous, make an assumption, state it in DECISIONS.md, and move on. That's part of the exercise.

Good luck!
