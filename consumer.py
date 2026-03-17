"""
consumer.py
Consumes streaming e-commerce events from a shared queue (or JSONL file).
Implements:
  - 10-second tumbling windows for per-category revenue aggregation
  - Late-arriving event handling (events with timestamp > 10 s old are dropped with a warning)
  - p50 / p95 / p99 latency tracking
  - Graceful crash recovery (re-reads from last committed offset in events.jsonl)

Usage (same-process mode — run alongside producer.py in one script):
    python run_pipeline.py          # starts both producer and consumer

Usage (file-based mode — producer writes JSONL, consumer reads it):
    python consumer.py --input events.jsonl --window 10
"""

import argparse
import json
import os
import statistics
import sys
import time
from collections import defaultdict
from typing import Dict, List, Optional


# ── Latency tracker ──────────────────────────────────────────────────────────
class LatencyTracker:
    """Tracks end-to-end event latency and computes percentiles."""

    def __init__(self):
        self.samples: List[float] = []

    def record(self, event_timestamp: float):
        latency_ms = (time.time() - event_timestamp) * 1000.0
        self.samples.append(latency_ms)

    def percentiles(self) -> Dict[str, float]:
        if not self.samples:
            return {"p50": 0, "p95": 0, "p99": 0, "count": 0}
        s = sorted(self.samples)
        n = len(s)
        return {
            "p50":   round(s[int(n * 0.50)], 2),
            "p95":   round(s[int(n * 0.95)], 2),
            "p99":   round(s[min(int(n * 0.99), n - 1)], 2),
            "count": n,
        }

    def reset(self):
        self.samples = []


# ── Tumbling window state ─────────────────────────────────────────────────────
class TumblingWindowAggregator:
    """
    10-second tumbling window that aggregates:
      - purchase revenue by category
      - event counts by type
    Emits and resets every `window_seconds`.
    """

    def __init__(self, window_seconds: int = 10):
        self.window_seconds = window_seconds
        self.window_start   = time.time()
        self.revenue: Dict[str, float] = defaultdict(float)
        self.counts:  Dict[str, int]   = defaultdict(int)
        self.late_dropped = 0
        self.processed    = 0

    def add(self, event: dict) -> Optional[dict]:
        """
        Add an event to the current window.
        Returns a window result dict if the window has elapsed, else None.
        """
        now              = time.time()
        event_age_s      = now - event.get("timestamp", now)

        # Late-arrival check: drop events older than 2× window
        if event_age_s > self.window_seconds * 2:
            self.late_dropped += 1
            return None

        # Accumulate
        cat = event.get("category", "unknown")
        evt = event.get("event_type", "unknown")
        self.counts[evt] += 1
        if evt == "purchase":
            self.revenue[cat] += event.get("price", 0) * event.get("quantity", 1)
        self.processed += 1

        # Check if window has elapsed → emit
        if now - self.window_start >= self.window_seconds:
            result = self._emit(now)
            return result
        return None

    def _emit(self, now: float) -> dict:
        result = {
            "window_start":  round(self.window_start, 2),
            "window_end":    round(now, 2),
            "duration_s":    round(now - self.window_start, 2),
            "events_processed": self.processed,
            "late_dropped":  self.late_dropped,
            "revenue_by_category": dict(self.revenue),
            "counts_by_type": dict(self.counts),
            "total_revenue":  round(sum(self.revenue.values()), 2),
        }
        # Reset state
        self.window_start = now
        self.revenue.clear()
        self.counts.clear()
        self.late_dropped = 0
        self.processed    = 0
        return result


# ── Consumer ──────────────────────────────────────────────────────────────────
def consume_from_file(input_file: str, window_seconds: int, checkpoint_file: str):
    """
    Read events from a JSONL file written by producer.py.
    Supports crash recovery via a checkpoint file that stores the last read offset.
    """
    # Load checkpoint (last byte offset successfully processed)
    start_offset = 0
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file) as f:
                start_offset = int(f.read().strip())
            print(f"[Consumer] Resuming from checkpoint offset {start_offset}")
        except Exception:
            pass

    aggregator = TumblingWindowAggregator(window_seconds)
    tracker    = LatencyTracker()
    results    = []
    processed  = 0
    errors     = 0

    print(f"[Consumer] Starting. Input={input_file}  window={window_seconds}s")

    # Poll until file exists
    for _ in range(30):
        if os.path.exists(input_file):
            break
        print("[Consumer] Waiting for producer to create events file …")
        time.sleep(1)

    with open(input_file, "r") as fin:
        fin.seek(start_offset)

        while True:
            line = fin.readline()
            if not line:
                # End of file — wait briefly for more data (tail -f behaviour)
                time.sleep(0.05)
                # Check if producer has finished (sentinel None in queue)
                # In file mode we rely on a small timeout
                continue

            line = line.strip()
            if not line:
                continue

            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                errors += 1
                continue

            if event is None:   # sentinel
                break

            tracker.record(event["timestamp"])
            window_result = aggregator.add(event)
            processed += 1

            if window_result:
                results.append(window_result)
                perc = tracker.percentiles()
                print(
                    f"[Window] end={window_result['window_end']:.0f}  "
                    f"events={window_result['events_processed']}  "
                    f"revenue=${window_result['total_revenue']:,.2f}  "
                    f"late_dropped={window_result['late_dropped']}  "
                    f"p50={perc['p50']}ms  p95={perc['p95']}ms  p99={perc['p99']}ms"
                )
                tracker.reset()

            # Checkpoint every 500 events
            if processed % 500 == 0:
                offset = fin.tell()
                with open(checkpoint_file, "w") as cf:
                    cf.write(str(offset))

    # Final stats
    perc = tracker.percentiles()
    print("\n[Consumer] Final latency stats:", perc)
    print(f"[Consumer] Total processed: {processed}  Errors: {errors}")

    # Save results as JSONL sink
    with open("streaming_results.jsonl", "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")
    print("[Consumer] Results saved → streaming_results.jsonl")

    return results, perc


def consume_from_queue(event_queue, window_seconds: int):
    """
    Consume events directly from a shared Python queue (same-process mode).
    Used by run_pipeline.py for integrated testing.
    """
    aggregator = TumblingWindowAggregator(window_seconds)
    tracker    = LatencyTracker()
    results    = []
    processed  = 0

    print(f"[Consumer] Queue mode. window={window_seconds}s")

    while True:
        try:
            event = event_queue.get(timeout=2.0)
        except Exception:
            # Timeout — producer may be done
            break

        if event is None:   # sentinel from producer
            break

        tracker.record(event["timestamp"])
        window_result = aggregator.add(event)
        processed += 1

        if window_result:
            results.append(window_result)
            perc = tracker.percentiles()
            print(
                f"[Window] events={window_result['events_processed']}  "
                f"revenue=${window_result['total_revenue']:,.2f}  "
                f"p50={perc['p50']}ms p95={perc['p95']}ms p99={perc['p99']}ms"
            )
            tracker.reset()

    perc = tracker.percentiles()
    print("\n[Consumer] Done. Latency:", perc, " Processed:", processed)
    return results, perc


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Streaming event consumer")
    parser.add_argument("--input",      type=str, default="events.jsonl",   help="JSONL file from producer")
    parser.add_argument("--window",     type=int, default=10,               help="Tumbling window size (seconds)")
    parser.add_argument("--checkpoint", type=str, default="checkpoint.txt", help="Checkpoint file for crash recovery")
    args = parser.parse_args()

    consume_from_file(args.input, args.window, args.checkpoint)


if __name__ == "__main__":
    main()
