# IDS568 Milestone 4 — Distributed & Streaming Pipeline

## Overview
This project implements a distributed feature engineering pipeline using **PySpark**
and an optional streaming pipeline using a **mock Python queue** (no Kafka required).
The domain is synthetic e-commerce user events (views, cart adds, purchases, etc.).

---

## Repository Structure

```
.
├── generate_data.py        # Synthetic data generator (10M+ rows)
├── pipeline.py             # Distributed PySpark feature engineering
├── producer.py             # Streaming event producer
├── consumer.py             # Streaming event consumer (windowed aggregation)
├── run_pipeline.py         # Integrated load-test runner (producer + consumer)
├── README.md               # This file
├── REPORT.md               # Performance analysis and architecture evaluation
├── STREAMING_REPORT.md     # Streaming analysis and metrics
└── requirements.txt        # Python dependencies
```

---

## Prerequisites

- Python 3.9+
- Java 8 or 11 (required by PySpark — check with `java -version`)
- Install dependencies:

```bash
pip install -r requirements.txt
```

---

## Part 1 — Distributed Feature Engineering

### Step 1 — Generate synthetic data (small test, ~1000 rows)
```bash
python generate_data.py --rows 1000 --output test_data/ --seed 42
```

### Step 2 — Run pipeline on small data to verify correctness
```bash
python pipeline.py --input test_data/ --output test_output/ --mode local --workers 1
```

### Step 3 — Generate full dataset (10M rows)
```bash
python generate_data.py --rows 10000000 --output data/ --seed 42
```
Expected: ~20 parquet files, ~1.2 GB total. Runtime: ~3–5 minutes.

### Step 4 — Run local baseline (single core)
```bash
python pipeline.py --input data/ --output output_local/ --mode local --workers 1
```
Metrics are saved to `output_local/metrics_local.txt`.

### Step 5 — Run distributed (4 workers)
```bash
python pipeline.py --input data/ --output output_dist/ --mode distributed --workers 4
```
Metrics are saved to `output_dist/metrics_distributed.txt`.

### Reproducibility check
```bash
# Run twice with same seed → outputs must be identical
python generate_data.py --rows 100 --seed 42 --output run1/
python generate_data.py --rows 100 --seed 42 --output run2/
diff -r run1/ run2/ && echo "REPRODUCIBLE" || echo "NOT REPRODUCIBLE"
```

---

## Part 2 — Streaming Pipeline (Bonus)

### Option A — Integrated load test (recommended, single terminal)
```bash
python run_pipeline.py
```
This runs three load levels (100, 1K, 5K msg/s) and prints a latency summary table.
Results are saved to `load_test_results.json`.

### Option B — Separate terminals
**Terminal 1 (producer):**
```bash
python producer.py --rate 500 --duration 60 --output events.jsonl
```

**Terminal 2 (consumer):**
```bash
python consumer.py --input events.jsonl --window 10
```

### Burst traffic simulation
```bash
python producer.py --rate 1000 --duration 60 --burst
```
The `--burst` flag doubles the rate for 5-second bursts every 20 seconds,
revealing backpressure behaviour.

---

## Outputs

| File/Directory | Contents |
|---|---|
| `data/` | Raw synthetic parquet data (10M rows) |
| `output_local/user_features/` | Per-user features (local mode) |
| `output_local/product_features/` | Per-product features |
| `output_local/category_features/` | Per-category features |
| `output_dist/` | Same as above, distributed mode |
| `output_local/metrics_local.txt` | Timing + shuffle metrics |
| `output_dist/metrics_distributed.txt` | Timing + shuffle metrics |
| `load_test_results.json` | Streaming latency results |
| `streaming_results.jsonl` | Window aggregation outputs |

---

## Reproducing Results

All random number generation uses `--seed 42` by default.
Two runs with the same seed will produce byte-identical parquet files.
PySpark shuffle operations use deterministic partitioning (`repartition("user_id")`).
