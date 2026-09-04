#!/usr/bin/env python3
"""Side-by-side RESP benchmark: PolyCache vs Redis.

Speaks the Redis protocol (RESP2) to both servers and drives them with
`redis-benchmark` so the numbers are directly comparable. Each scenario pins
a PolyCache eviction policy and the closest Redis `allkeys-*` policy, runs the
same workload against both, and reports throughput (req/s) plus live hit-rate.

Scenarios (strict memory limit, keyspace >> cache so eviction actually fires):
    lru   vs Redis allkeys-lru
    sieve vs Redis allkeys-lfu
    lfu   vs Redis allkeys-lfu
    sieve vs Redis allkeys-lru

Usage:
    tools/bench_redis.py                    # run the full matrix
    tools/bench_redis.py --mem-limit-mb 16  # tighter cache (more pressure)

Requires: redis-server, redis-cli, redis-benchmark (e.g. `brew install redis`).
"""

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.request

# (polycache_policy, redis_maxmemory_policy)
SCENARIOS = [
    ("lru", "allkeys-lru"),
    ("sieve", "allkeys-lfu"),
    ("lfu", "allkeys-lfu"),
    ("sieve", "allkeys-lru"),
]

COMMANDS = ["set", "get", "ping_mbulk"]  # RESP only; PING_INLINE is inline text


def find_binary(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        sys.exit("error: '%s' not found on PATH (install redis, e.g. "
                 "`brew install redis`)" % name)
    return path


def wait_for_tcp(port: int, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def polycache_ready(admin_port: int, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(
                    "http://127.0.0.1:%d/metrics" % admin_port,
                    timeout=0.5) as r:
                json.loads(r.read().decode())
            return True
        except Exception:
            time.sleep(0.1)
    return False


def resp_command(port: int, *parts: str, timeout: float = 2.0) -> str:
    """One RESP2 round trip: encode a *<n>\r\n array of bulk strings, read the
    reply line, and return it (e.g. "+OK", "-ERR ...")."""
    frame = b"*%d\r\n" % len(parts)
    for p in parts:
        raw = p.encode()
        frame += b"$%d\r\n" % len(raw) + raw + b"\r\n"
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as s:
        s.sendall(frame)
        s.settimeout(timeout)
        return s.recv(256).decode(errors="replace").strip()


def start_redis(port: int, mem_mb: int, policy: str, io_threads: int = None):
    redis_server = find_binary("redis-server")
    cmd = [redis_server, "--port", str(port), "--maxmemory", "%dmb" % mem_mb,
           "--maxmemory-policy", policy, "--save", "", "--appendonly", "no"]
    if io_threads is not None:
        cmd += ["--io-threads", str(io_threads)]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not wait_for_tcp(port):
        sys.exit("redis-server failed to come up on port %d" % port)
    return proc


def start_polycache(port: int, admin_port: int, mem_mb: int, policy: str,
                   aof: str, no_aof: bool = False):
    cmd = ["./polycache", "--port", str(port), "--admin-port", str(admin_port),
           "--memory-limit", str(mem_mb)]
    if no_aof:
        cmd.append("--no-aof")
    else:
        cmd += ["--aof-file", aof]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if not polycache_ready(admin_port):
        sys.exit("polycache failed to come up on port %d" % port)
    if policy != "lru":  # lru is the default; explicit switch for the rest
        reply = resp_command(port, "SWITCH_POLICY", policy)
        if reply != "+OK":
            print("  warning: SWITCH_POLICY %s -> %s" % (policy, reply))
    return proc


def redis_benchmark(port: int, cmd: str, requests: int, keyspace: int,
                    data_size: int) -> float:
    rb = find_binary("redis-benchmark")
    summary = subprocess.run(
        [rb, "-h", "127.0.0.1", "-p", str(port), "-t", cmd,
         "-n", str(requests), "-r", str(keyspace), "-d", str(data_size)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True).stdout
    reqs = []
    for line in summary.splitlines():
        idx = line.find("requests per second")
        if idx != -1:
            token = line[:idx].strip().split()[-1]
            try:
                reqs.append(float(token.replace(",", "")))
            except ValueError:
                pass
    return reqs[0] if reqs else 0.0


def polycache_hit_rate(admin_port: int) -> float:
    with urllib.request.urlopen(
            "http://127.0.0.1:%d/metrics" % admin_port, timeout=2) as r:
        m = json.loads(r.read().decode())
    return float(m.get("hit_rate", 0.0))


def redis_hit_rate(port: int) -> float:
    cli = find_binary("redis-cli")
    info = subprocess.run([cli, "-p", str(port), "INFO", "stats"],
                          stdout=subprocess.PIPE, text=True).stdout
    hits = misses = 0
    for line in info.splitlines():
        if line.startswith("keyspace_hits:"):
            hits = int(line.split(":", 1)[1])
        elif line.startswith("keyspace_misses:"):
            misses = int(line.split(":", 1)[1])
    total = hits + misses
    return (hits / total) if total else 0.0


def run_scenario(policy: str, redis_policy: str, args) -> dict:
    pc_port, pc_admin = args.base_port, args.base_port + 1
    r_port = args.base_port + 2
    aof = "/tmp/bench_redis_%s_%d.aof" % (policy, pc_port)

    procs = []
    results = {"policy": policy, "redis_policy": redis_policy}
    try:
        # Default: --no-aof disables PolyCache's synchronous per-write fsync
        # so the benchmark measures protocol/CPU throughput. With --with-aof
        # on, writes are intentionally fsync-bound (durability-by-design) and
        # far slower -- that is the honest tradeoff to show.
        procs.append(start_polycache(pc_port, pc_admin, args.mem_limit_mb,
                                     policy, aof, no_aof=not args.with_aof))
        procs.append(start_redis(r_port, args.mem_limit_mb, redis_policy,
                                 args.io_threads))

        pc_rps, r_rps = {}, {}
        for cmd in COMMANDS:
            pc_rps[cmd] = redis_benchmark(pc_port, cmd, args.requests,
                                          args.keyspace, args.data_size)
            r_rps[cmd] = redis_benchmark(r_port, cmd, args.requests,
                                         args.keyspace, args.data_size)
        results["pc_rps"], results["r_rps"] = pc_rps, r_rps
        results["pc_hit"] = polycache_hit_rate(pc_admin)
        results["r_hit"] = redis_hit_rate(r_port)
    finally:
        for p in procs:
            p.terminate()
            try:
                p.wait(3)
            except subprocess.TimeoutExpired:
                p.kill()
        try:
            os.remove(aof)
        except OSError:
            pass
    return results


def print_results(rows: list, args) -> None:
    header = ("%-10s | %-12s | %9s | %9s | %9s | %7s"
              % ("side", "policy", "SET/s", "GET/s", "PING/s", "hit%"))
    print(header)
    print("-" * len(header))
    for row in rows:
        pc, rc = row["pc_rps"], row["r_rps"]
        print("%-10s | %-12s | %9.0f | %9.0f | %9.0f | %6.1f%%"
              % ("PolyCache", row["policy"], pc["set"], pc["get"], pc["ping_mbulk"],
                 row["pc_hit"] * 100))
        print("%-10s | %-12s | %9.0f | %9.0f | %9.0f | %6.1f%%"
              % ("Redis", row["redis_policy"], rc["set"], rc["get"], rc["ping_mbulk"],
                 row["r_hit"] * 100))
        print("-" * len(header))
    print("Strict %dMB limit, keyspace -r %d >> capacity => eviction fires. "
          % (args.mem_limit_mb, args.keyspace))
    if args.with_aof:
        print("PolyCache AOF ON: every SET/DEL is synchronously fsynced "
              "(durability-by-design) -- write throughput is disk-bound.")
    else:
        print("PolyCache --no-aof (no per-write fsync) for fair throughput.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mem-limit-mb", type=int, default=64,
                    help="strict memory limit for BOTH servers (default 64)")
    ap.add_argument("--keyspace", type=int, default=1000000,
                    help="redis-benchmark -r (random keys); >> capacity so "
                         "eviction fires (default 1000000)")
    ap.add_argument("--requests", type=int, default=100000,
                    help="redis-benchmark -n per command (default 100000)")
    ap.add_argument("--data-size", type=int, default=256,
                    help="redis-benchmark -d value size in bytes (default 256, "
                         "keeps capacity small enough to observe eviction)")
    ap.add_argument("--base-port", type=int, default=7100,
                    help="starting port; allocates +0/+1 PolyCache, +2 Redis")
    ap.add_argument("--with-aof", action="store_true",
                    help="keep PolyCache AOF ON (synchronous per-write "
                         "fsync); default is --no-aof for fair throughput")
    ap.add_argument("--io-threads", type=int, default=None,
                    help="Redis I/O threads count (default: single-threaded compatible; "
                         "e.g. 2, 4)")
    args = ap.parse_args()

    rows = []
    for policy, redis_policy in SCENARIOS:
        print("== scenario: PolyCache %s vs Redis %s ==" % (policy, redis_policy),
              flush=True)
        rows.append(run_scenario(policy, redis_policy, args))
    print()
    print_results(rows, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
