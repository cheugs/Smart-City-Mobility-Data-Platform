"""
Run a performance benchmark against the running stack and record resource usage.

    python scripts/run_benchmark.py 10000
    python scripts/run_benchmark.py 100000
    python scripts/run_benchmark.py 1000000

Steps:
  1. stop the normal producer (so only benchmark readings are measured)
  2. run producer/benchmark_load.py in a container (sends N readings, waits, measures)
  3. meanwhile, sample `docker stats` every 5 s (CPU % and memory per container)
  4. restart the normal producer and save everything in benchmarks/benchmark_<N>.json

Only uses the Python standard library.
"""

import json
import os
import re
import subprocess
import sys
import threading

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONTAINERS = ["mobility-kafka", "mobility-spark", "mobility-postgres", "mobility-airflow",
              "mobility-grafana", "mobility-dashboard"]


def to_mib(text):
    """'1.5GiB' -> 1536.0, '300MiB' -> 300.0"""
    number, unit = re.match(r"([\d.]+)\s*([KMG]i?B|B)", text).groups()
    factor = {"B": 1 / 2**20, "KiB": 1 / 1024, "KB": 1 / 1024, "MiB": 1, "MB": 1, "GiB": 1024, "GB": 1024}
    return float(number) * factor[unit]


def sample_stats(samples, stop):
    """Every 5 s, save CPU and memory of each container until `stop` is set."""
    while not stop.is_set():
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}};{{.CPUPerc}};{{.MemUsage}}", *CONTAINERS],
            capture_output=True, text=True,
        ).stdout
        for line in out.strip().splitlines():
            name, cpu, mem = line.split(";")
            samples.setdefault(name, []).append((float(cpu.rstrip("%")), to_mib(mem.split("/")[0].strip())))
        stop.wait(5)


def main():
    """Run the whole benchmark for N readings (N is the first command-line argument)."""
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100_000
    os.chdir(ROOT)

    print("Stopping the normal producer during the benchmark ...")
    subprocess.run(["docker", "compose", "stop", "producer"], check=True)

    samples, stop = {}, threading.Event()
    sampler = threading.Thread(target=sample_stats, args=(samples, stop), daemon=True)
    sampler.start()
    try:
        subprocess.run(["docker", "compose", "run", "--rm", "--no-deps", "producer",
                        "python", "-u", "benchmark_load.py", "--events", str(n)], check=True)
    finally:
        stop.set()
        sampler.join()
        print("Restarting the normal producer ...")
        subprocess.run(["docker", "compose", "start", "producer"])

    resources = {
        name.replace("mobility-", ""): {
            "avg_cpu_percent": round(sum(c for c, _ in s) / len(s), 1),
            "max_cpu_percent": round(max(c for c, _ in s), 1),
            "max_memory_mib": round(max(m for _, m in s)),
        }
        for name, s in samples.items()
    }
    path = os.path.join(ROOT, "benchmarks", f"benchmark_{n}.json")
    with open(path) as f:
        result = json.load(f)
    result["resources"] = resources
    with open(path, "w") as f:
        json.dump(result, f, indent=2)

    print(json.dumps(resources, indent=2))
    print(f"Saved {path}")


if __name__ == "__main__":
    main()
