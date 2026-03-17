"""
run_pipeline.py
Runs the producer and consumer together in a single process for easy testing.
Records latency at multiple load levels for the streaming report.

Usage:
    python run_pipeline.py
"""

import json
import os
import threading
import time

from producer import EVENT_QUEUE, produce
from consumer import consume_from_queue

LOAD_LEVELS = [
    {"rate": 100,   "duration": 30, "burst": False, "label": "Low (100 msg/s)"},
    {"rate": 1_000, "duration": 30, "burst": False, "label": "Medium (1K msg/s)"},
    {"rate": 5_000, "duration": 20, "burst": True,  "label": "High (5K msg/s) + burst"},
]

WINDOW_SECONDS = 10
RESULTS_FILE   = "load_test_results.json"


def run_load_level(rate, duration, burst, label):
    print(f"\n{'='*60}")
    print(f"Load level: {label}")
    print(f"{'='*60}")

    # Clear queue
    while not EVENT_QUEUE.empty():
        try:
            EVENT_QUEUE.get_nowait()
        except Exception:
            break

    results_holder = {}

    def consumer_thread():
        results, perc = consume_from_queue(EVENT_QUEUE, WINDOW_SECONDS)
        results_holder["windows"] = results
        results_holder["latency"] = perc

    ct = threading.Thread(target=consumer_thread, daemon=True)
    ct.start()

    time.sleep(0.2)   # let consumer start
    produce(rate=rate, duration=duration, burst=burst, output_file=f"events_{rate}.jsonl")

    ct.join(timeout=duration + 10)

    return {
        "label":   label,
        "rate":    rate,
        "burst":   burst,
        "latency": results_holder.get("latency", {}),
        "windows": len(results_holder.get("windows", [])),
    }


def main():
    all_results = []
    for cfg in LOAD_LEVELS:
        r = run_load_level(**cfg)
        all_results.append(r)
        print(f"\n  → {cfg['label']}: latency={r['latency']}")

    with open(RESULTS_FILE, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nLoad test results saved → {RESULTS_FILE}")

    # Print summary table
    print("\n" + "="*70)
    print(f"{'Load Level':<28} {'p50 (ms)':>10} {'p95 (ms)':>10} {'p99 (ms)':>10} {'Count':>8}")
    print("-"*70)
    for r in all_results:
        lat = r["latency"]
        print(f"{r['label']:<28} {lat.get('p50','-'):>10} {lat.get('p95','-'):>10} {lat.get('p99','-'):>10} {lat.get('count','-'):>8}")
    print("="*70)


if __name__ == "__main__":
    main()
