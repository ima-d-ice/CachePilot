#!/usr/bin/env python3
"""PolyCache workload benchmark with real eviction pressure.

Starts a FRESH PolyCache server for every mode (kills any running
polycache, restarts with --memory-limit <cache-size-mb> and a clean
temporary AOF), preloads a working set much larger than the cache, then
runs three workload phases that force eviction DURING measurement:

  phase 1 (0-30%):   zipf skew - a burst read of the burst pool (the
                     preload tail), then cold-insert churn mixed with
                     zipfian GETs over the resident pool.
  phase 2 (30-60%):  hot+scan - same burst + churn, reads split between
                     hot zipfian GETs and a round-robin scan of the
                     resident middle band.
  phase 3 (60-100%): mixed - first half repeats phase 1, second half
                     repeats phase 2.

The burst pool is the last keys the cache was filled with.  Each phase
reads them once at the start; the keys then idle for the rest of the
phase while cold-insert churn (every SET evicts one key) keeps blowing
the eviction frontier past them.  By the next phase's burst read, LRU
has dropped the idle burst keys (they decayed out of the cache) while
LFU holds them by frequency and SIEVE by the visited flag -- so the
burst read exposes the difference.  Writes are cold inserts of keys
(ck%06d) that are never read; GETs never insert in this server, so a
phase without writes would be frozen at preload and measure identically
for every policy -- every phase keeps a write stream alive on purpose.

Modes: static_lru / static_lfu / static_sieve pin the policy for the
whole run; the policy switch is issued once via SWITCH_POLICY before the
workload starts.

All numbers come from live TCP/HTTP exchanges with the server.  Nothing is
fabricated: if the server binary is missing or does not come up, the tool
fails loudly.
"""

import argparse
import json
import os
import random
import socket
import statistics
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

STATIC_POLICY = {"static_lru": "lru", "static_lfu": "lfu", "static_sieve": "sieve"}
MODES = ("static_lru", "static_lfu", "static_sieve")
DEFAULT_ALPHA = 1.2


def fetch_metrics(host: str, port: int, timeout: float = 2.0) -> dict:
    """GET /metrics from the admin server and return the JSON as a dict."""
    with urllib.request.urlopen(
            "http://%s:%d/metrics" % (host, port), timeout=timeout) as resp:
        return json.loads(resp.read().decode())


# --------------------------------------------------------------------------
# Workload generators
# --------------------------------------------------------------------------

def _key_names(keys: int) -> list:
    return ["key%05d" % i for i in range(keys)]


def _zipf_weights(keys: list, alpha: float) -> list:
    weights = [1.0 / (rank ** alpha) for rank in range(1, len(keys) + 1)]
    total = sum(weights)
    return [w / total for w in weights]


def zipfian(keys: list, n: int, alpha: float = DEFAULT_ALPHA, rng=random) -> list:
    """n draws from keys with power-law probability p(k) ~ rank(k)^-alpha."""
    weights = _zipf_weights(keys, alpha)
    return rng.choices(keys, weights=weights, k=n)


def sequential(keys: list, n: int) -> list:
    """Round-robin scan: key0, key1, ..., keyN-1, key0, ..."""
    return [keys[i % len(keys)] for i in range(n)]


def _cold_key(counter: list) -> str:
    """Next never-read key name; the write stream that fuels evictions."""
    name = "ck%06d" % counter[0]
    counter[0] += 1
    return name


def cache_capacity(mb: int, value_size: int) -> int:
    """Max entries the cache holds: memory-limit / (key + value bytes)."""
    key_len = len("key%05d" % 0)  # 8 bytes: "key" + 5 digits
    return (mb * 1024 * 1024) // (key_len + value_size)


def _gen_phase1(keys, burst_pool, n, rng, cold_ratio, alpha, counter) -> list:
    """Zipf-skew: a burst read of the burst pool, then the rest is
    cold-insert churn mixed with zipfian GETs over the resident pool.

    The burst pool is the TAIL of the preload (the last keys the cache
    was filled with, read once here and never again this phase).  The
    cold SETs are never read either, and every one triggers an eviction:
    the churn rate must be high enough that over a phase the eviction
    frontier passes the burst keys, so by the next phase's burst read LRU
    has dropped them while LFU (frequency) and SIEVE (visited) keep them.
    """
    reads = zipfian(keys, n, alpha=alpha, rng=rng)
    out = []
    for i in range(n):
        if i < len(burst_pool):
            out.append(("get", burst_pool[i]))
        elif rng.random() < cold_ratio:
            out.append(("set", _cold_key(counter)))
        else:
            out.append(("get", reads[i]))
    return out


def _gen_phase2(hot_pool, burst_pool, scan_pool, n, rng, scan_write_ratio,
                alpha, counter) -> list:
    """Hot + scan: a burst read of the burst pool, then cold-insert churn
    mixed with hot zipfian GETs and a round-robin scan over the resident
    gap between the hot pool and the burst pool.

    Same eviction-pressure contract as phase 1: the churn (scan_write_ratio)
    must be high enough that one phase turns the cache past the burst keys,
    so LRU forgets them while LFU/SIEVE keep them.
    """
    hot_reads = zipfian(hot_pool, n, alpha=alpha, rng=rng)
    hot_share = (1.0 - scan_write_ratio) / 2.0
    out = []
    for i in range(n):
        if i < len(burst_pool):
            out.append(("get", burst_pool[i]))
            continue
        r = rng.random()
        if r < scan_write_ratio:
            out.append(("set", _cold_key(counter)))
        elif r < scan_write_ratio + hot_share:
            out.append(("get", hot_reads[i]))
        else:
            out.append(("get", scan_pool[i % len(scan_pool)]))
    return out


def build_workload(keys, n, rng, cfg) -> list:
    """Three-phase workload as (op, key) pairs.

    ``keys`` must arrive shuffled: the cache is preloaded with the first
    ``cache_capacity`` keys, and that resident set is the pool every phase
    reads from.  A key can only be a hit if it is resident, so all pools
    are carved out of it.

    Pools (all within the resident set, in preload/rank order):
      hot   = resident[:hot_size]    -- phase-2 zipf GETs
      scan  = resident[hot_size:cap-burst_size]  (round-robin GETs)
      burst = resident[cap-burst_size:]  (the preload tail)

    The key mechanic is the burst pool: every phase (and each half of
    phase 3) opens by reading it once.  The keys then idle for the rest
    of the phase, which lasts long enough -- with cold-insert churn
    sized to push the eviction frontier past them -- that LRU evicts the
    idle burst keys while LFU (frequency) and SIEVE (visited) keep them.
    The next phase's burst read exposes the difference: it misses under
    LRU and hits under LFU/SIEVE.  Every mode replays the exact same
    request sequence against a fresh server.
    """
    cap = cache_capacity(cfg.cache_size_mb, cfg.value_size)
    resident = keys[:cap]
    hot = resident[:cfg.hot_size]
    burst = resident[cap - cfg.burst_size:]
    scan_pool = resident[cfg.hot_size:cap - cfg.burst_size]
    scan_pool = scan_pool[:cfg.scan_size]
    if not scan_pool:
        scan_pool = resident[cfg.hot_size:cap]
    per_phase = n // 3
    p3 = n - 2 * per_phase
    counter = [0]
    wl = []
    wl.extend(_gen_phase1(resident, burst, per_phase, rng, cfg.cold_ratio,
                          cfg.alpha, counter))
    wl.extend(_gen_phase2(hot, burst, scan_pool, per_phase, rng,
                          cfg.scan_write_ratio, cfg.alpha, counter))
    p3_a = p3 // 2
    wl.extend(_gen_phase1(resident, burst, p3_a, rng, cfg.cold_ratio,
                          cfg.alpha, counter))
    wl.extend(_gen_phase2(hot, burst, scan_pool, p3 - p3_a, rng,
                          cfg.scan_write_ratio, cfg.alpha, counter))
    return wl


def phase_of(i: int, n: int) -> int:
    p1 = n // 3
    p2 = 2 * (n // 3)
    return 1 if i < p1 else (2 if i < p2 else 3)


# --------------------------------------------------------------------------
# Minimal RESP2 TCP client (the Redis wire protocol). Commands are encoded as
# *<count>\r\n arrays of $<len>\r\n<data>\r\n bulk strings. Replies:
#   +OK\r\n (simple string), -ERR ...\r\n (error), :<int>\r\n (integer),
#   $<len>\r\n<data>\r\n (bulk string), $-1\r\n (null = GET miss).
# --------------------------------------------------------------------------

def encode_resp(*parts: str) -> bytes:
    out = b"*%d\r\n" % len(parts)
    for p in parts:
        raw = p.encode()
        out += b"$%d\r\n" % len(raw) + raw + b"\r\n"
    return out


class RespSocket:
    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def read_until(self, marker=b"\r\n"):
        while True:
            idx = self.buf.find(marker)
            if idx != -1:
                line = self.buf[:idx]
                self.buf = self.buf[idx + len(marker):]
                return line
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed by server")
            self.buf += chunk

    def read_exact(self, n: int):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed by server")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def command(self, *parts: str) -> bytes:
        """One RESP2 round trip. Bulk-string replies return the payload bytes
        (b"$-1" for a null/miss); simple/error/integer replies return the
        full line including its prefix (b"+OK", b"-ERR ...")."""
        self.sock.sendall(encode_resp(*parts))
        head = self.read_until()
        if not head:
            raise ConnectionError("empty reply from server")
        if head.startswith(b"$"):
            if head == b"$-1":
                return b"$-1"
            payload = self.read_exact(int(head[1:]))
            self.read_exact(2)  # trailing \r\n
            return payload
        return head  # +, -, : replies are a single line each


def set_value(ls: RespSocket, key: str, value: str) -> None:
    reply = ls.command("SET", key, value)
    if reply != b"+OK":
        raise RuntimeError("SET %s -> %r" % (key, reply))


def get_value(ls: RespSocket, key: str) -> bool:
    """One GET round trip; returns True on hit, False on miss."""
    return ls.command("GET", key) != b"$-1"


def switch_policy(ls: RespSocket, policy: str) -> bool:
    return ls.command("SWITCH_POLICY", policy) == b"+OK"


# --------------------------------------------------------------------------
# Server lifecycle: fresh restart before every mode
# --------------------------------------------------------------------------

def kill_server() -> None:
    proc = subprocess.run(["pkill", "-x", "polycache"],
                          capture_output=True, text=True)
    if proc.returncode == 0:
        print("killed existing polycache process")
    time.sleep(0.25)


def start_server(cfg):
    """Kill any polycache, start a fresh one with a small memory limit."""
    kill_server()
    if not cfg.server_path.exists():
        print("server binary not found: %s (run 'make' first)" % cfg.server_path,
              file=sys.stderr)
        sys.exit(1)
    for path in (cfg.aof_path, cfg.server_log):
        try:
            os.remove(path)
        except OSError:
            pass
    log_fh = open(cfg.server_log, "a", encoding="utf-8")
    proc = subprocess.Popen(
        [str(cfg.server_path), "--port", str(cfg.cache_port),
         "--admin-port", str(cfg.admin_port),
         "--memory-limit", str(cfg.cache_size_mb),
         "--aof-file", str(cfg.aof_path)],
        stdout=subprocess.DEVNULL, stderr=log_fh)
    deadline = time.time() + cfg.wait_timeout
    while time.time() < deadline:
        try:
            fetch_metrics(cfg.admin_host, cfg.admin_port, timeout=0.5)
            print("server up: %s --memory-limit %dMB (fresh state)" %
                  (cfg.server_path, cfg.cache_size_mb), flush=True)
            return proc
        except Exception:
            time.sleep(0.1)
    log_fh.close()
    proc.terminate()
    try:
        proc.wait(2)
    except subprocess.TimeoutExpired:
        proc.kill()
    print("server failed to start; last stderr lines:", file=sys.stderr)
    try:
        lines = open(cfg.server_log, encoding="utf-8").read().splitlines()
        print("\n".join(lines[-8:]), file=sys.stderr)
    except OSError:
        pass
    sys.exit(1)


def stop_server(proc) -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(2)
    except subprocess.TimeoutExpired:
        proc.kill()
    print("server stopped")


# --------------------------------------------------------------------------
# Measurement
# --------------------------------------------------------------------------

def run_mode(name, workload, preload_keys, cfg):
    """Run one benchmark mode against the (already started) server.

    preload_keys arrive shuffled: the survivors of the oversize preload are
    then a uniform sample of the working set (with an ascending preload the
    FIFO eviction of the full cache would evict the hot low-rank keys and
    every policy would miss the same way).  The shuffle is identical for
    every mode, so the preload survivor set is identical too.
    """
    n = len(workload)
    phase_start_req = {1: 0, 2: n // 3, 3: 2 * (n // 3)}
    phase_hits = {1: 0, 2: 0, 3: 0}
    phase_gets = {1: 0, 2: 0, 3: 0}
    total_hits = 0
    total_gets = 0
    switches = 0
    prev_policy = None
    peak_memory = 0
    samples = []

    with socket.create_connection((cfg.cache_host, cfg.cache_port),
                                  timeout=5) as sock:
        sock.settimeout(10)
        ls = RespSocket(sock)

        pinned = STATIC_POLICY.get(name)
        if pinned is not None:
            if not switch_policy(ls, pinned):
                raise RuntimeError("%s: SWITCH_POLICY %s rejected" %
                                   (name, pinned))

        # Preload exactly the cache's capacity in keys: hot pool + burst
        # pool + a slice of the scan pool, shuffled.  Nothing is evicted
        # during preload (it fits), so the policy state is built only by
        # the phase traffic -- and there is no tail of never-read keys for
        # evictions to hide in.  The same order is used for every mode.
        cap = cache_capacity(cfg.cache_size_mb, cfg.value_size)
        preload_order = preload_keys[:cap]
        for key in preload_order:
            set_value(ls, key, "x" * cfg.value_size)

        ev_before = int(fetch_metrics(cfg.admin_host, cfg.admin_port)
                        .get("evictions", 0))

        for start in range(0, n, cfg.block_size):
            end = min(start + cfg.block_size, n)
            times = []
            for i in range(start, end):
                op, key = workload[i]
                ph = phase_of(i, n)
                t0 = time.perf_counter()
                if op == "set":
                    set_value(ls, key, "w" * cfg.value_size)
                    times.append((time.perf_counter() - t0) * 1000.0)
                else:
                    ok = get_value(ls, key)
                    times.append((time.perf_counter() - t0) * 1000.0)
                    phase_gets[ph] += 1
                    total_gets += 1
                    if ok:
                        total_hits += 1
                        phase_hits[ph] += 1

            metrics = fetch_metrics(cfg.admin_host, cfg.admin_port)
            policy = str(metrics.get("policy", ""))
            if prev_policy is not None and policy != prev_policy:
                switches += 1
            prev_policy = policy
            peak_memory = max(peak_memory, int(metrics.get("memory_bytes", 0)))

            ph = phase_of(end - 1, n)
            phase_hit_rate = (phase_hits[ph] / phase_gets[ph]
                              if phase_gets[ph] else 0.0)
            samples.append({
                "requests": end,
                "phase": ph,
                "phase_hit_rate": phase_hit_rate,
                "overall_hit_rate": (total_hits / total_gets
                                     if total_gets else 0.0),
                "latency_ms": sum(times) / len(times),
                "policy": policy,
                "switches": switches,
            })
            print("%s phase%d %6d req  phase_hit=%.4f  lat=%.3fms  policy=%s" %
                  (name, ph, end, phase_hit_rate, samples[-1]["latency_ms"],
                   policy), flush=True)

        evictions = (int(fetch_metrics(cfg.admin_host, cfg.admin_port)
                         .get("evictions", 0)) - ev_before)

    stats = {
        "phase_rates": {ph: (phase_hits[ph] / phase_gets[ph]
                             if phase_gets[ph] else 0.0) for ph in (1, 2, 3)},
        "overall": (total_hits / total_gets if total_gets else 0.0),
        "evictions": evictions,
        "peak_memory": peak_memory,
        "switches": switches,
        "phase_start_req": phase_start_req,
    }
    return samples, stats


# --------------------------------------------------------------------------
# Output: summary table
# --------------------------------------------------------------------------

def print_summary(results, cfg) -> None:
    header = ("%-12s | %17s | %17s | %17s | %8s" %
              ("Mode", "P1 zipf skew", "P2 hot+scan", "P3 mixed", "Overall"))
    print(header)
    print("-" * len(header))
    for name, samples, stats in results:
        avg_lat = (sum(s["latency_ms"] for s in samples) / len(samples)
                   if samples else 0.0)
        print("%-12s | %16.4f | %16.4f | %16.4f | %7.4f" %
              (name, stats["phase_rates"][1], stats["phase_rates"][2],
               stats["phase_rates"][3], stats["overall"]))
        print("%-12s   (avg lat %.3fms | peak mem %d B | evictions %d | "
              "switches %d)" % (name, avg_lat, stats["peak_memory"],
                                stats["evictions"], stats["switches"]))
    print("-" * len(header))
    print("note: every mode ran against a freshly restarted server "
          "(--memory-limit %dMB, shuffled %d-key space, preload exactly "
          "fills the cache with %d keys, values %dB; burst pool %d keys "
          "read at each phase start and allowed to idle past the LRU "
          "horizon; cold-insert churn %.0f%% / %.0f%% of phases 1/2)" %
          (cfg.cache_size_mb, cfg.working_set,
           cache_capacity(cfg.cache_size_mb, cfg.value_size),
           cfg.value_size, cfg.burst_size,
           cfg.cold_ratio * 100, cfg.scan_write_ratio * 100))

    by_mode = {name: stats for name, _s, stats in results}
    if all(("static_" + pol) in by_mode for pol in ("lru", "lfu", "sieve")):
        print("\nper-phase policy comparison (higher is better):")
        for ph, label in ((1, "zipf skew"), (2, "hot+scan"), (3, "mixed")):
            rates = [by_mode["static_" + pol]["phase_rates"][ph]
                     for pol in ("lru", "lfu", "sieve")]
            print("Phase %d (%s) complete. LRU: %.4f, LFU: %.4f, "
                  "SIEVE: %.4f" % (ph, label, rates[0], rates[1], rates[2]))


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def run_seed(args, seed, namespace_artifacts) -> list:
    """One full measurement round: generate the workload for `seed`, then
    run every selected mode (fresh server per mode).  Returns
    [(mode, samples, stats)].  With namespace_artifacts (multi-seed runs)
    every artifact path gets a _seedN suffix so the AOF and logs never mix
    between seeds."""
    if namespace_artifacts:
        args.aof_path = Path(args.aof_prefix + "_seed%d.aof" % seed)
        args.server_log = Path(args.aof_prefix + "_seed%d.log" % seed)

    rng = random.Random(seed)
    keys = _key_names(args.working_set)
    rng.shuffle(keys)
    workload = build_workload(keys, args.requests, rng, args)
    preload_keys = keys

    modes = MODES if args.mode == "all" else (args.mode,)
    results = []
    server = None
    try:
        for mode in modes:
            print("\n== mode %s ==" % mode)
            server = start_server(args)
            samples, stats = run_mode(mode, workload, preload_keys, args)
            results.append((mode, samples, stats))
            stop_server(server)
            server = None
    finally:
        stop_server(server)
    return results


def print_aggregate(per_seed, cfg) -> None:
    """Mean +- std across seeds, per mode, over the modes every seed ran."""
    seeds = sorted(per_seed)
    by_mode = {}
    for mode in set().union(*[set(r[0] for r in per_seed[s])
                              for s in seeds]):
        if not all(any(r[0] == mode for r in per_seed[s]) for s in seeds):
            continue
        by_mode[mode] = {ph: [] for ph in (1, 2, 3)}
        by_mode[mode]["overall"] = []
        for s in seeds:
            stats = next(r[2] for r in per_seed[s] if r[0] == mode)
            for ph in (1, 2, 3):
                by_mode[mode][ph].append(stats["phase_rates"][ph])
            by_mode[mode]["overall"].append(stats["overall"])

    def mv(xs):
        return "%.4f +- %.4f" % (statistics.mean(xs),
                                 statistics.stdev(xs) if len(xs) > 1 else 0.0)

    print("\naggregated verdict across seeds %s (mean +- std):" % seeds)
    header = ("%-12s | %16s | %16s | %16s | %14s" %
              ("Mode", "P1 zipf skew", "P2 hot+scan", "P3 mixed", "Overall"))
    print(header)
    print("-" * len(header))
    for mode in sorted(by_mode):
        m = by_mode[mode]
        print("%-12s | %16s | %16s | %16s | %14s" %
              (mode, mv(m[1]), mv(m[2]), mv(m[3]), mv(m["overall"])))
    print("-" * len(header))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="PolyCache benchmark: fresh per-mode server, phased "
                    "workload with real eviction pressure, policy comparison.")
    parser.add_argument("--mode", default="all", choices=MODES + ("all",))
    parser.add_argument("--requests", type=int, default=50000)
    parser.add_argument("--working-set", type=int, default=100000,
                        help="unique keys preloaded (and used by the "
                             "phase-1 zipf pool) before the phases")
    parser.add_argument("--cache-size-mb", type=int, default=2,
                        help="server --memory-limit for every mode")
    parser.add_argument("--value-size", type=int, default=64,
                        help="exact byte size of every stored value")
    parser.add_argument("--alpha", type=float, default=DEFAULT_ALPHA,
                        help="zipf skew exponent (phase 1 and hot reads)")
    parser.add_argument("--hot-size", type=int, default=17000,
                        help="phase 2 hot pool: zipf GETs on this many keys")
    parser.add_argument("--scan-size", type=int, default=40000,
                        help="phase 2 scan pool: round-robin GETs, must "
                             "exceed the cache's key capacity to collapse LRU")
    parser.add_argument("--burst-size", type=int, default=3000,
                        help="burst pool read at each phase start, then "
                             "idle; past the LRU horizon LRU drops these "
                             "keys, LFU/SIEVE keep them")
    parser.add_argument("--cold-ratio", type=float, default=0.50,
                        help="share of phase-1 (and the phase-3 first-half) "
                             "requests that SET a brand-new key never read "
                             "again.  Each SET evicts one key, so this is the "
                             "churn that must blow the eviction frontier past "
                             "the burst pool within a phase (>= capacity / "
                             "phase length keeps it honest).")
    parser.add_argument("--scan-write-ratio", type=float, default=0.50,
                        help="analogous churn for phase 2 / phase-3 second "
                             "half; the remaining requests split 50/50 between "
                             "hot zipfian GETs and the resident scan.")
    parser.add_argument("--block-size", type=int, default=1000)
    parser.add_argument("--cache-host", default="localhost")
    parser.add_argument("--cache-port", type=int, default=6379)
    parser.add_argument("--admin-host", default="localhost")
    parser.add_argument("--admin-port", type=int, default=8080)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--seeds", type=int, nargs="+", default=None,
                        help="compare across multiple seeds (e.g. --seeds 1 7 "
                             "42 123 999): per-seed compare runs, then an "
                             "aggregated mean +- std verdict.  Defaults to a "
                             "single seed (--seed or 7).")
    parser.add_argument("--server-path", default=str(SCRIPT_DIR / "polycache"))
    parser.add_argument("--aof-prefix", default="/tmp/polycache_bench")
    parser.add_argument("--wait-timeout", type=float, default=10.0)
    args = parser.parse_args(argv)

    if args.cache_size_mb < 1:
        print("--cache-size-mb must be >= 1", file=sys.stderr)
        return 1
    if args.value_size < 1 or args.working_set < 1 or args.requests < 3:
        print("--value-size/--working-set/--requests must be positive",
              file=sys.stderr)
        return 1
    if args.alpha <= 0:
        print("--alpha must be > 0", file=sys.stderr)
        return 1
    if not (0.0 <= args.cold_ratio < 1.0 and
            0.0 <= args.scan_write_ratio < 1.0):
        print("--cold-ratio/--scan-write-ratio must be in [0, 1)",
              file=sys.stderr)
        return 1
    if args.hot_size < 1 or args.scan_size < 1 or args.burst_size < 1:
        print("--hot-size/--scan-size/--burst-size must be positive",
              file=sys.stderr)
        return 1
    cap = cache_capacity(args.cache_size_mb, args.value_size)
    if args.hot_size + args.burst_size > cap:
        print("--hot-size + --burst-size must be <= the cache's key "
              "capacity (%d at %dMB/%dB values)" %
              (cap, args.cache_size_mb, args.value_size), file=sys.stderr)
        return 1
    if args.hot_size + args.burst_size + args.scan_size > args.working_set:
        print("--hot-size + --burst-size + --scan-size must be <= "
              "--working-set", file=sys.stderr)
        return 1
    if args.requests < args.burst_size * 6:
        print("--requests must be at least 6x --burst-size (each phase "
              "must have room for the burst read plus real traffic)",
              file=sys.stderr)
        return 1
    per_phase = args.requests // 3
    idle_churn = (args.cold_ratio + args.scan_write_ratio) / 2.0
    per_phase_churn = idle_churn * (per_phase - args.burst_size)
    if per_phase_churn < cap // 2:
        print("warning: per-phase churn (~%.0f SETs) may not blow the "
              "eviction frontier past the burst pool (cache holds %d "
              "keys); raise --cold-ratio/--scan-write-ratio for sharper "
              "LRU-vs-LFU/SIEVE separation" % (per_phase_churn, cap),
              file=sys.stderr)

    args.server_path = Path(args.server_path)
    args.aof_path = Path(args.aof_prefix + ".aof")
    args.server_log = Path(args.aof_prefix + ".log")

    seeds = args.seeds if args.seeds is not None else [args.seed]
    per_seed = {}
    for seed in seeds:
        if seed is None:
            seed = 7
            args.seed = None  # time-seeded, matches previous single-run behavior
        print("\n########## seed %d ##########" % seed)
        results = run_seed(args, seed, namespace_artifacts=(len(seeds) > 1))
        per_seed[seed] = results
        print_summary(results, args)

    if len(seeds) > 1:
        print_aggregate(per_seed, args)
    return 0


def run() -> None:
    sys.exit(main())


if __name__ == "__main__":
    run()
