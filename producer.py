"""
producer.py
Generates streaming e-commerce events and puts them onto a shared queue.
Simulates steady-state and burst traffic patterns.

Usage:
    # Run producer alongside consumer.py (open two terminals):
    python producer.py --rate 200 --duration 60
    python producer.py --rate 1000 --duration 30 --burst
"""

import argparse
import json
import queue
import random
import threading
import time
import sys

# ── Shared queue (imported by consumer.py) ────────────────────────────────────
# Use a module-level queue so producer and consumer can share it in the same
# process, or write/read a named FIFO file when run separately.
EVENT_QUEUE: queue.Queue = queue.Queue(maxsize=50_000)

CATEGORIES  = ["electronics", "clothing", "books", "home", "sports", "beauty"]
EVENT_TYPES = ["view", "add_to_cart", "purchase", "wishlist"]
WEIGHTS     = [0.55, 0.20, 0.15, 0.10]


def make_event(seq_id: int) -> dict:
    """Generate a single realistic e-commerce event."""
    return {
        "event_id":   seq_id,
        "user_id":    random.randint(1, 100_000),
        "product_id": random.randint(1, 50_000),
        "category":   random.choice(CATEGORIES),
        "event_type": random.choices(EVENT_TYPES, weights=WEIGHTS, k=1)[0],
        "price":      round(random.uniform(1.0, 999.99), 2),
        "quantity":   random.randint(1, 10),
        "timestamp":  time.time(),
    }


def produce(rate: int, duration: int, burst: bool, output_file: str):
    """
    Produce events at `rate` msg/s for `duration` seconds.
    If burst=True, doubles the rate for 5-second bursts every 20 seconds.
    Writes JSON lines to output_file AND puts events on EVENT_QUEUE.
    """
    rng = random.Random(42)   # seeded for reproducibility
    interval      = 1.0 / rate
    t_start       = time.time()
    seq_id        = 0
    total_sent    = 0

    print(f"[Producer] Starting  rate={rate} msg/s  duration={duration}s  burst={burst}")

    with open(output_file, "w") as fout:
        while time.time() - t_start < duration:
            now     = time.time()
            elapsed = now - t_start

            # Burst mode: double rate every 20 s for 5 s
            in_burst = burst and (int(elapsed) % 20 < 5)
            effective_rate = rate * 2 if in_burst else rate
            sleep_time = 1.0 / effective_rate

            event = make_event(seq_id)
            seq_id += 1

            line = json.dumps(event)
            fout.write(line + "\n")
            fout.flush()

            try:
                EVENT_QUEUE.put_nowait(event)
            except queue.Full:
                pass   # drop oldest if queue full (back-pressure signal)

            total_sent += 1
            time.sleep(sleep_time)

            if total_sent % 1000 == 0:
                q_depth = EVENT_QUEUE.qsize()
                print(f"[Producer] {total_sent:6d} events sent  "
                      f"queue_depth={q_depth}  burst={'YES' if in_burst else 'no '}")

    print(f"[Producer] Done. Total events produced: {total_sent}")
    # Signal consumer to stop
    EVENT_QUEUE.put(None)


def main():
    parser = argparse.ArgumentParser(description="Streaming event producer")
    parser.add_argument("--rate",     type=int, default=200,           help="Events per second")
    parser.add_argument("--duration", type=int, default=60,            help="Run duration in seconds")
    parser.add_argument("--burst",    action="store_true",             help="Enable burst traffic simulation")
    parser.add_argument("--output",   type=str, default="events.jsonl", help="Output JSONL file")
    args = parser.parse_args()

    produce(args.rate, args.duration, args.burst, args.output)


if __name__ == "__main__":
    main()
