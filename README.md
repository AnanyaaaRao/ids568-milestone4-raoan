# IDS568 Milestone 4 — Distributed & Streaming Pipeline

## Overview

Distributed feature engineering pipeline (PySpark) + streaming pipeline (mock
Python queue). Processes 10M synthetic e-commerce events to produce user, product,
and category ML features.

---

## Prerequisites

### 1. Java 11 (required by PySpark)

Download from <https://adoptium.net/temurin/releases/?version=11>

**Windows — set environment variables:**
```
JAVA_HOME = C:\Program Files\Eclipse Adoptium\jdk-11.x.x.x-hotspot
Path      += %JAVA_HOME%\bin
```

**Mac/Linux:**
```bash
brew install openjdk@11          # macOS
sudo apt install openjdk-11-jdk  # Ubuntu/Debian
```

Verify: `java -version`

### 2. WinUtils (Windows only)

Download `winutils.exe` for Hadoop 3.3.5:
<https://github.com/cdarlint/winutils/tree/master/hadoop-3.3.5/bin>

```
Create folder:  C:\hadoop\bin
Place:          winutils.exe inside it
HADOOP_HOME  =  C:\hadoop
Path        +=  %HADOOP_HOME%\bin
```

### 3. Python dependencies

```bash
pip install -r requirements.txt
```

---

## Repository Structure

```
ids568-milestone4-raoan/
├── pipeline.py          # PySpark distributed feature engineering
├── generate_data.py     # Synthetic data generator (10M+ rows, seeded)
├── producer.py          # Streaming event producer (configurable rate + burst)
├── consumer.py          # Streaming consumer (tumbling windows, checkpointing)
├── run_pipeline.py      # Integrated load-test runner (3 load levels)
├── README.md            # This file
├── REPORT.md            # Performance analysis and architecture evaluation
├── STREAMING_REPORT.md  # Streaming analysis and metrics
└── requirements.txt     # Python dependencies
```

---

## Part 1 — Distributed Feature Engineering

### Step 1 — Correctness check on small data

```bash
python generate_data.py --rows 1000 --output test_data/ --seed 42
python pipeline.py --input test_data/ --output test_output/ --mode local --workers 1
```

Expected: three parquet subdirectories (`user_features/`, `product_features/`,
`category_features/`) and two JSON files (`metrics_local.json`,
`output_hashes.json`) in `test_output/`.

### Step 2 — Generate full dataset (10M rows, ~1.2 GB)

```bash
python generate_data.py --rows 10000000 --output data/ --seed 42
```

Expected runtime: ~3–5 minutes. Output: ~20 parquet part files.

### Step 3 — Run local baseline (single thread)

```bash
python pipeline.py --input data/ --output output_local/ --mode local --workers 1
```

Metrics saved to `output_local/metrics_local.json`.

### Step 4 — Run distributed (4 threads)

```bash
python pipeline.py --input data/ --output output_dist/ --mode distributed --workers 4
```

Metrics saved to `output_dist/metrics_distributed.json`.

### Step 5 — Compare performance

```bash
python -c "
import json
local = json.load(open('output_local/metrics_local.json'))
dist  = json.load(open('output_dist/metrics_distributed.json'))
print(f'Local runtime:  {local[\"runtime_seconds\"]}s')
print(f'Dist runtime:   {dist[\"runtime_seconds\"]}s')
print(f'Speedup:        {local[\"runtime_seconds\"]/dist[\"runtime_seconds\"]:.2f}x')
print(f'Shuffle read (dist):  {dist[\"shuffle_read_mb\"]} MB')
print(f'Shuffle write (dist): {dist[\"shuffle_write_mb\"]} MB')
"
```

---

## Reproducibility Verification

Run twice with the same seed — outputs must be identical:

```bash
# Generate identical input data twice
python generate_data.py --rows 100 --seed 42 --output run1_data/
python generate_data.py --rows 100 --seed 42 --output run2_data/

# Run pipeline on each
python pipeline.py --input run1_data/ --output run1_out/ --mode distributed --workers 4
python pipeline.py --input run2_data/ --output run2_out/ --mode distributed --workers 4

# Verify hashes match
python -c "
import json
h1 = json.load(open('run1_out/output_hashes.json'))['hashes']
h2 = json.load(open('run2_out/output_hashes.json'))['hashes']
print('REPRODUCIBLE' if h1 == h2 else 'MISMATCH - check seed')
"
```

---

## Part 2 — Streaming Pipeline (Bonus)

### Option A — Integrated load test (single terminal, recommended)

```bash
python run_pipeline.py
```

Runs three load levels (100 / 1K / 5K msg/s) automatically and prints a
p50/p95/p99 latency summary table. Results saved to `load_test_results.json`.

### Option B — Separate terminals

**Terminal 1 — Consumer:**
```bash
python consumer.py --input events.jsonl --window 10
```

**Terminal 2 — Producer:**
```bash
# Steady state
python producer.py --rate 500 --duration 60 --output events.jsonl

# With burst traffic (doubles rate every 20 seconds)
python producer.py --rate 1000 --duration 60 --output events.jsonl --burst
```

### Consumer crash recovery

```bash
# Consumer writes a checkpoint every 500 events.
# Kill it (Ctrl-C), then restart — it resumes from last checkpoint.
python consumer.py --input events.jsonl --window 10
```

---

## Configuration Reference

| Argument | Default | Description |
|---|---|---|
| `--rows` | 10,000,000 | Rows to generate |
| `--seed` | 42 | Random seed (controls reproducibility) |
| `--mode` | `local` | `local` = 1 thread; `distributed` = N threads |
| `--workers` | 4 | Parallel threads in distributed mode |
| `--rate` | 500 | Events per second (producer) |
| `--duration` | 60 | Producer run duration (seconds) |
| `--window` | 10 | Tumbling window size (seconds) |
| `--burst` | off | Enable burst simulation |

---

## Expected Runtimes (4-core laptop, 16 GB RAM)

| Step | Approximate Time |
|---|---|
| Data generation (10M rows) | 3–5 min |
| Local pipeline (`local[1]`) | 8–15 min |
| Distributed pipeline (`local[4]`) | 4–8 min |
| Streaming load test (all levels) | ~5 min |

---

## Troubleshooting

**`winutils.exe` not found** → Re-check `HADOOP_HOME` and restart terminal.

**`OutOfMemoryError`** → Reduce `--rows` or add `-Xmx` to `JAVA_TOOL_OPTIONS`.

**`java.io.IOException: Permission denied` on Windows** → Run terminal as Administrator.

**Spark UI** → Open `http://localhost:4040` while pipeline is running to inspect
stage DAG, task timeline, and shuffle metrics.
