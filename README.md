# PolyCache
**C++17** • **Epoll** • **SIEVE / LRU / LFU** • **AOF Persistence** • **Dockerised**

A high-performance in-memory cache server written in C++17 with hot-swappable eviction policies and a rigorous statistical benchmarking harness.

## Architecture & Tech Stack

| Component | Technology |
| :--- | :--- |
| **Network I/O** | Single-threaded `epoll` event loop (macOS `poll` compat), non-blocking I/O |
| **Eviction Policies** | Hand-rolled SIEVE (NSDI 2024), LRU, LFU (behind a unified `EvictionPolicy` interface) |
| **Persistence** | Synchronous AOF (Append-Only File) with JSON-line formatting and crash replay |
| **Dependencies** | `nlohmann/json` (vendored single header) |
| **Testing** | Plain C++17 assertions (`CHECK` macros), zero external test frameworks |

## Core Features

* **Runtime Policy Switching:** A single `SWITCH_POLICY` TCP command swaps between SIEVE, LRU, and LFU live. The rebuild preserves the eviction frontier (metadata sort, not a flush), ensuring zero data loss and no downtime.
* **Modern Eviction (SIEVE):** Implements the 2024 SIEVE algorithm, which uses a single hand pointer to clear visited bits, offering $O(1)$ amortized operations and superior scan-resistance compared to LRU.
* **Durability by Design:** Prioritizes data safety over write throughput via synchronous `fsync`-per-write AOF logging.
* **Admin & Metrics:** A background worker thread runs an HTTP server exposing live cache metrics (`/metrics`) and health checks (`/health`).

## Quick Start

```bash
# Build the image and run the server (ports 6379 TCP, 8080 admin)
docker build -t polycache .
docker run --rm -p 6379:6379 -p 8080:8080 polycache
```

*Note: Graceful shutdown is triggered by `SIGINT` or `SIGTERM`.*

## Protocol & API Reference

PolyCache speaks **RESP2, the Redis wire protocol** (default port `6379`).
Commands are `*<count>\r\n` arrays of `$<len>\r\n<data>\r\n` bulk strings —
length-prefixed and binary-safe, with pipelining supported. The server is a
drop-in target for `redis-benchmark` and `redis-cli`.

### TCP Commands

| Command | RESP2 form | Response |
| :--- | :--- | :--- |
| `SET key value [ttl]` | `SET key value [EX secs\|PX ms]` | `+OK` |
| `GET key` | `GET key` | `$<len>\r\n<value>` (hit) or `$-1` (miss) |
| `DEL key` | `DEL key` | `:1` (removed) or `:0` (absent) |
| `PING [msg]` | `PING [msg]` | `+PONG` (or bulk echo of `msg`) |
| `SELECT db` | `SELECT db` | `+OK` (single-database, ignored) |
| `SWITCH_POLICY lru\|lfu\|sieve` | `SWITCH_POLICY sieve` | `+OK` (Atomic swap under storage lock) |
| `METRICS` | `METRICS` | bulk string of cache stats JSON |

```bash
# RESP2 via redis-cli (the protocol is Redis's own)
redis-cli -p 6379 SET user:42 alice
redis-cli -p 6379 GET user:42

# Or raw RESP2 frames over netcat (binary-safe, length-prefixed):
printf '*3\r\n$3\r\nSET\r\n$7\r\nuser:42\r\n$5\r\nalice\r\n' | nc 127.0.0.1 6379
```

### Redis-benchmark comparison

Because the wire format is RESP2, PolyCache can be benchmarked head-to-head with
Redis using the official `redis-benchmark` tool:

```bash
# Run the 4-scenario matrix (PolyCache vs Redis, strict memory limit):
python3 tools/bench_redis.py --mem-limit-mb 64 --keyspace 1000000

# Or a single ad-hoc run (RESP-based tests only):
redis-benchmark -h 127.0.0.1 -p 6379 -t set,get,ping_mbulk -n 100000 -r 1000000
```

> Note: PolyCache speaks RESP2 only, so `redis-benchmark`'s `PING_INLINE`
> test — which sends a raw `PING\r\n` line, not RESP — is unsupported (use
> `ping_mbulk`, the RESP form). Every real client speaks RESP, so `redis-cli`
> and libraries work untouched.

`tools/bench_redis.py` pins each PolyCache policy against the closest Redis
`allkeys-*` policy (`lru`↔`allkeys-lru`, `sieve`/`lfu`↔`allkeys-lfu`), forces a
keyspace far larger than the cache so eviction actually fires, and reports
per-command throughput (req/s) plus live hit-rate for both sides.

> Note: the benchmark runs PolyCache with `--no-aof`, which disables the
> synchronous per-write fsync so throughput reflects protocol/CPU cost. With
> AOF on (the default, durability-by-design), `SET` throughput is intentionally
> fsync-bound. `redis-benchmark` v8 has no standalone `DEL` test, so the matrix
> covers `set`, `get`, `ping`.

### Admin HTTP API

```bash
curl -s http://127.0.0.1:8080/metrics
# {"evictions":12,"hit_rate":0.85,"hits":850,"memory_bytes":1048576,"policy":"sieve"}

curl -s http://127.0.0.1:8080/health
# {"status":"ok"}
```

## Performance & Benchmarking

Included is a Python benchmarking harness (`benchmark.py`) that starts a fresh server per mode, preloads the cache to capacity, and replays identical phased workloads to measure policy divergence under adversarial conditions.

### Workload Design
The workload forces the eviction frontier to walk the preload order via 50% cold-write churn. A "burst pool" at the tail of the preload is read once and left to idle; LRU drops these keys under churn, while LFU and SIEVE retain them due to frequency and visited-bit protections.

### Benchmark Results (90K requests, 1MB cache, 5 seeds)
SIEVE and LFU tie and consistently outperform LRU in scan-heavy phases.

| Mode | Overall Hit Rate (mean ± std) | P1 (Zipf) | P2 (Scan) | P3 (Mixed) |
| :--- | :--- | :--- | :--- | :--- |
| **static_sieve** | **0.3041 ± 0.0502** | 0.2890 | 0.2293 | **0.3868** |
| **static_lfu** | **0.3041 ± 0.0502** | 0.2890 | 0.2293 | **0.3868** |
| static_lru | 0.1630 ± 0.0508 | 0.2885 | 0.0846 | 0.1200 |

## Project Structure

* `/src`: C++17 server core (network I/O, storage engine, eviction policies, AOF persistence, RESP parser).
* `/tests`: Plain C++17 unit tests for protocol parsing, RESP framing, storage mechanics, and SIEVE semantics.
* `benchmark.py`: Python 3 statistical benchmarking harness with multi-seed aggregation.
* `tools/bench_redis.py`: RESP head-to-head harness driving `redis-benchmark` against PolyCache and Redis.
* `Makefile`: Build system for the server and test binaries.
* `Dockerfile`: Slim reproducible C++ build image.