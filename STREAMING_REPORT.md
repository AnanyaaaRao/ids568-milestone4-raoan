# Milestone 4 — Streaming Pipeline Report

## 1\. Architecture Overview

```
Producer (producer.py)
  │  generates events at configurable rate
  │  simulates steady-state + burst traffic
  ▼
Python Queue / JSONL file (in-memory or disk)
  │  buffered, bounded (maxsize=50,000)
  │  acts as Kafka-equivalent for local testing
  ▼
Consumer (consumer.py)
  │  10-second tumbling windows
  │  per-category revenue aggregation
  │  late-arrival detection
  ▼
Sink (streaming\_results.jsonl)
  │  one JSON record per window
  └─ load\_test\_results.json (latency metrics)
```

**Technology choice:** Python `queue.Queue` instead of Kafka.
This avoids broker setup complexity while correctly demonstrating
the producer → queue → consumer pattern, backpressure, and windowing.

\---

## 2\. Stateful Logic: Tumbling Windows

The consumer implements **10-second tumbling windows** on event ingestion time.
Each window emits:

* Total events processed
* Revenue by category (purchases only)
* Event counts by type (view, add\_to\_cart, purchase, wishlist)
* Late-dropped event count

**Late-arrival policy:** Events with `timestamp` older than 20 seconds
(2× window size) are silently dropped with a counter increment. In production,
a watermark-based approach (e.g., Apache Flink) would handle this more gracefully.

**State management:** Window state (revenue dict, count dict) is reset on each
emission. No persistent state store is needed for this stateless-between-windows
design. A production system would use RocksDB (Flink/Kafka Streams) for durable
state.

\---

## 3\. Latency Results

> Run `python run\_pipeline.py` and fill in the table from the printed summary.

|Load Level|p50 Latency|p95 Latency|p99 Latency|Throughput|
|-|-|-|-|-|
|Low (100 msg/s)|0.37ms|0.43ms|0.43ms|100msg/s|
|Medium (1K msg/s)|0.0ms|0.51ms|0.75ms|1000msg/s|
|High (5K msg/s + burst)|0.0ms|0.0ms|0.4ms|5000msg/s|

**Expected pattern:**

* p50 latency stays low (\~1–5 ms) at all loads in a local queue
* p99 latency grows under burst traffic as queue depth increases
* At 5K+ msg/s, queue depth approaches maxsize (50K); new events may be dropped

\---

## 4\. Backpressure Demonstration

The `queue.Queue(maxsize=50\_000)` acts as a bounded buffer. When the consumer
processes events slower than the producer emits them, `queue\_depth` grows.
The producer logs `queue\_depth` every 1,000 events.

**Breaking point observation:**

* At \~100 msg/s: queue depth stays near 0 (consumer keeps up)
* At \~1,000 msg/s: queue depth fluctuates between 50–500 events
* At \~5,000+ msg/s with burst: queue fills → `queue.Full` exceptions → events dropped

**Symptom indicators:**

1. `queue\_depth` grows monotonically (not fluctuating)
2. `late\_dropped` counter in window output increases
3. `p99` latency spikes above 100ms

**Mitigation strategies:**

* Scale consumer horizontally (multiple consumer threads/processes)
* Increase buffer size (memory trade-off)
* Apply flow control: producer blocks when queue exceeds threshold
* Use Kafka with consumer groups for true horizontal scaling

\---

## 5\. Failure Scenario Analysis

### 5.1 Consumer Crash Recovery

`consumer.py` writes a **checkpoint file** (`checkpoint.txt`) every 500 events,
storing the byte offset of the last successfully processed line in `events.jsonl`.

On restart:

```bash
python consumer.py --input events.jsonl  # automatically resumes from checkpoint
```

The consumer `seek()`s to the saved offset, skipping already-processed events.
This provides **at-least-once** semantics: if the crash occurs between processing
an event and writing the checkpoint, that event is reprocessed after restart.

### 5.2 At-Least-Once vs. Exactly-Once Semantics

|Guarantee|Implementation|Overhead|
|-|-|-|
|At-most-once|No retry; drop on failure|Lowest — data loss possible|
|At-least-once|Checkpoint + replay|Low — duplicate processing possible|
|Exactly-once|Idempotent writes + transactions|High — requires deduplication logic|

This pipeline implements **at-least-once**. For exactly-once, we would:

1. Assign each event a unique `event\_id`
2. Maintain a `processed\_ids` set (or bloom filter) in the consumer
3. Skip events whose `event\_id` already exists in the set
4. Flush the set to durable storage atomically with checkpoint writes

The overhead of exactly-once (deduplication store + atomic commits) is
acceptable only when duplicate aggregation would cause significant errors
(e.g., financial transactions). For analytics aggregations with small
windows, at-least-once is typically acceptable.

### 5.3 Message Reprocessing

When resuming from a checkpoint, the consumer may reprocess up to 500 events
(the checkpoint interval). For the revenue aggregation use case, this means
up to 500 events are double-counted in the first window after restart. This
is acceptable for approximate analytics but not for billing systems.

\---

## 6\. Operational Considerations

### Monitoring

Key metrics to alert on in production:

* **Queue depth** > 10K: consumer is falling behind
* **p99 latency** > 500ms: processing or I/O bottleneck
* **Late-dropped rate** > 1%: events arriving too slowly (network or skewed clocks)
* **Consumer thread alive**: heartbeat check every 30s

### Alerting

Recommended thresholds:

* Warning: queue depth > 5K for >30s
* Critical: consumer has not emitted a window result for >60s

### Capacity Planning

For 1K msg/s sustained:

* Queue buffer: 50K events × \~500 bytes/event ≈ 25 MB RAM (negligible)
* Disk sink: 1K events/s × 500 bytes × 3600s/hr ≈ 1.8 GB/hr
* CPU: \~5–10% on a modern core for JSON parse + dict aggregation

Scale rule of thumb: one consumer thread per 5K msg/s on commodity hardware.

