import os
import matplotlib.pyplot as plt

os.makedirs("charts", exist_ok=True)

local_runtime = 107.62
distributed_runtime = 38.40

worker_counts = [1, 2, 4, 8]
worker_runtimes = [95.0, 61.0, 38.4, 34.8]

partition_counts = [4, 16, 32, 64]
partition_runtimes = [57.2, 43.8, 38.4, 41.1]

stream_loads = ["100 msg/s", "1000 msg/s", "5000 msg/s"]
p50 = [12, 21, 36]
p95 = [25, 48, 92]
p99 = [39, 71, 151]

plt.figure()
plt.bar(["Local","Distributed"], [local_runtime, distributed_runtime])
plt.ylabel("Runtime (seconds)")
plt.title("Local vs Distributed Runtime")
plt.savefig("charts/runtime_comparison.png")
plt.close()

plt.figure()
plt.plot(worker_counts, worker_runtimes, marker="o")
plt.xlabel("Workers")
plt.ylabel("Runtime (seconds)")
plt.title("Runtime vs Workers")
plt.savefig("charts/runtime_vs_workers.png")
plt.close()

plt.figure()
plt.plot(partition_counts, partition_runtimes, marker="o")
plt.xlabel("Partitions")
plt.ylabel("Runtime (seconds)")
plt.title("Runtime vs Partitions")
plt.savefig("charts/runtime_vs_partitions.png")
plt.close()

plt.figure()
plt.plot(stream_loads, p50, marker="o", label="p50")
plt.plot(stream_loads, p95, marker="o", label="p95")
plt.plot(stream_loads, p99, marker="o", label="p99")
plt.xlabel("Load Level")
plt.ylabel("Latency (ms)")
plt.title("Streaming Latency")
plt.legend()
plt.savefig("charts/streaming_latency.png")
plt.close()

print("Charts generated!")