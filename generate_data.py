"""
generate_data.py
Generates synthetic e-commerce user event data for distributed feature engineering.

Usage:
    python generate_data.py --rows 10000000 --output data/ --seed 42
    python generate_data.py --rows 1000 --output test_data/ --seed 42  # small test
"""

import argparse
import os
import numpy as np
import pandas as pd
import time

# ── Constants ────────────────────────────────────────────────────────────────
CATEGORIES    = ["electronics", "clothing", "books", "home", "sports", "beauty"]
EVENT_TYPES   = ["view", "add_to_cart", "purchase", "wishlist", "remove_from_cart"]
EVENT_WEIGHTS = [0.55, 0.20, 0.12, 0.08, 0.05]   # realistic distribution
NUM_USERS     = 100_000
NUM_PRODUCTS  = 50_000
CHUNK_SIZE    = 500_000   # rows written per chunk (keeps memory low)


def generate_chunk(n_rows: int, seed: int, chunk_id: int) -> pd.DataFrame:
    """Generate one chunk of synthetic e-commerce events."""
    rng = np.random.default_rng(seed + chunk_id)   # deterministic per chunk

    user_ids    = rng.integers(1, NUM_USERS + 1,   size=n_rows)
    product_ids = rng.integers(1, NUM_PRODUCTS + 1, size=n_rows)
    categories  = rng.choice(CATEGORIES,  size=n_rows)
    event_types = rng.choice(EVENT_TYPES, size=n_rows, p=EVENT_WEIGHTS)
    prices      = np.round(rng.uniform(1.0, 999.99, size=n_rows), 2)
    quantities  = rng.integers(1, 11, size=n_rows)
    session_ids = rng.integers(1, 1_000_001, size=n_rows)
    ratings     = rng.choice([np.nan, 1, 2, 3, 4, 5],
                              size=n_rows,
                              p=[0.7, 0.03, 0.05, 0.08, 0.07, 0.07])

    # Timestamps spread across 90 days
    base_ts = 1_700_000_000
    timestamps = base_ts + rng.integers(0, 90 * 24 * 3600, size=n_rows)

    return pd.DataFrame({
        "event_id":   np.arange(chunk_id * CHUNK_SIZE, chunk_id * CHUNK_SIZE + n_rows),
        "user_id":    user_ids,
        "product_id": product_ids,
        "category":   categories,
        "event_type": event_types,
        "price":      prices,
        "quantity":   quantities,
        "session_id": session_ids,
        "rating":     ratings,
        "timestamp":  timestamps,
    })


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data")
    parser.add_argument("--rows",   type=int, default=10_000_000, help="Total rows to generate")
    parser.add_argument("--output", type=str, default="data/",    help="Output directory")
    parser.add_argument("--seed",   type=int, default=42,         help="Random seed for reproducibility")
    parser.add_argument("--format", type=str, default="parquet",  choices=["parquet", "csv"],
                        help="Output file format")
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    print(f"Generating {args.rows:,} rows → {args.output}  (seed={args.seed})")
    t0 = time.time()

    n_chunks = max(1, args.rows // CHUNK_SIZE)
    rows_generated = 0

    for chunk_id in range(n_chunks):
        remaining = args.rows - rows_generated
        n_rows    = min(CHUNK_SIZE, remaining)
        if n_rows <= 0:
            break

        df = generate_chunk(n_rows, args.seed, chunk_id)
        rows_generated += n_rows

        out_path = os.path.join(args.output, f"part_{chunk_id:04d}.{args.format}")
        if args.format == "parquet":
            df.to_parquet(out_path, index=False)
        else:
            df.to_csv(out_path, index=False)

        print(f"  chunk {chunk_id+1}/{n_chunks}  ({rows_generated:,} rows so far)  → {out_path}")

    elapsed = time.time() - t0
    print(f"\nDone. {rows_generated:,} rows written in {elapsed:.1f}s")
    print(f"Output: {os.path.abspath(args.output)}")


if __name__ == "__main__":
    main()
