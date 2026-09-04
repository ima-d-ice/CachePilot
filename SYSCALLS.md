# PolyCache -- Linux Syscalls Reference

> **Purpose:** One file to study for interviews. Covers everything the project actually uses, plus core syscalls you should know, plus PolyCache-specific patterns you can draw in 30 seconds.

---

## 1. PolyCache Syscalls (Used in Production Code)

| Syscall | Signature | Purpose | Where Used |
|---------|-----------|---------|------------|
| `epoll_create1` | `int epoll_create1(int flags)` | Create epoll instance (fd) | `server.cpp:72`, `admin.cpp:89` |
| `epoll_ctl` | `int epoll_ctl(int epfd, int op, int fd, struct epoll_event *ev)` | Register/modify/delete fd interest | `server.cpp:96,99,180,298,303` (`ADD`/`MOD`/`DEL`) |
| `epoll_wait` | `int epoll_wait(int epfd, struct epoll_event *events, int maxevents, int timeout)` | Block until fds ready; returns count | `server.cpp:104`, `admin.cpp:121` |
| `socket` | `int socket(int domain, int type, int protocol)` | Create socket (`AF_INET`/`AF_UNIX`, `SOCK_STREAM`) | `server.cpp:44,80` |
| `socketpair` | `int socketpair(int domain, int type, int protocol, int sv[2])` | Wake pipe: 2 connected sockets | `server.cpp:80`, `admin.cpp:97` |
| `fcntl` | `int fcntl(int fd, int cmd, ...)` | `F_GETFL`/`F_SETFL` -> `O_NONBLOCK` | All fds: listen, client, wake pipe |
| `setsockopt` | `int setsockopt(int sockopt, int level, int optname, const void *optval, socklen_t optlen)` | `SO_REUSEADDR` (port reuse on restart) | `server.cpp:53`, `admin.cpp:69` |
| `bind` | `int bind(int sockfd, const struct sockaddr *addr, socklen_t addrlen)` | Bind to IP:port | `server.cpp:59`, `admin.cpp:75` |
| `listen` | `int listen(int sockfd, int backlog)` | Mark as passive listener (`SOMAXCONN`) | `server.cpp:65`, `admin.cpp:82` |
| `accept` | `int accept(int sockfd, struct sockaddr *addr, socklen_t *addrlen)` | Accept new connection | `server.cpp:166`, `admin.cpp:172` |
| `recv` | `ssize_t recv(int sockfd, void *buf, size_t len, int flags)` | Read (non-blocking -> `EAGAIN`) | `server.cpp:190,116` |
| `send` | `ssize_t send(int sockfd, const void *buf, size_t len, int flags)` | Write (`MSG_NOSIGNAL` prevents SIGPIPE) | `server.cpp:268` |
| `close` | `int close(int fd)` | Close fd + `epoll_ctl(DEL)` first | `server.cpp:303-304` |
| `epoll_ctl(..., EPOLL_CTL_DEL, ...)` | Remove fd from epoll before `close` (fd-reuse safety) | `server.cpp:303` |
| `write` | `ssize_t write(int fd, const void *buf, size_t count)` | Wake pipe (1 byte to `wake_pipe_[1]`) | `server.cpp:160`, `admin.cpp:166` |

---

## 2. Epoll Deep Dive

### Epoll States

| Flag | Meaning | Default? |
|------|---------|----------|
| `EPOLLIN` | fd is readable (data in kernel recv queue) | Yes (always arm) |
| `EPOLLOUT` | fd is writable (send buffer has space) | Only when we re-arm after `EAGAIN` |
| `EPOLLET` | **Edge-triggered** -- fire once when state changes | No (default = LT) |
| `EPOLLHUP` | Peer closed socket (half-close or full close) | Check after `EPOLLIN` |
| `EPOLLERR` | Socket error (RST, etc.) | Check after `EPOLLIN` |

### LT vs ET (Level-Triggered vs Edge-Triggered)

| | Level-Triggered (LT) -- **used in PolyCache** | Edge-Triggered (ET) |
|---|---|---|
| **When it fires** | Every `epoll_wait` while condition is true | Only when state *changes* |
| **Drain requirement** | Drain until `EAGAIN` to make it stop | Drain *completely*; partial read keeps firing |
| **Safety** | Harder to miss events | Easier to miss if you don't drain fully |
| **Complexity** | Simpler code | Needs careful loop |

**PolyCache uses LT** -- simpler: `recv` loop until `EAGAIN`, then `epoll_wait` next iteration.

### Wake Pipe Pattern

1. `socketpair(AF_UNIX, SOCK_STREAM, 0, wake_pipe_)` -- creates `wake_pipe_[0]` (read) + `wake_pipe_[1]` (write)
2. Both fds set `O_NONBLOCK`
3. Both added to epoll via `epoll_ctl(ADD)` with `EPOLLIN`
4. `epoll_wait(-1)` blocks
5. `stop()` -> `write(wake_pipe_[1], &byte, 1)` -- makes read end readable -> `epoll_wait` returns
6. Loop drains byte: `while (recv(wake_pipe_[0], discard, sizeof(discard), 0) > 0) {}`
7. `running_ == false` -> exit main loop cleanly

### Backpressure with `EPOLLOUT`

```text
client slow -> send() returns EAGAIN -> 
queue reply in outbox_[fd] -> 
epoll_ctl(MOD, EPOLLIN|EPOLLOUT) -> 
kernel wakes when send buffer has space -> 
try_send(fd) -> send data -> 
epoll_ctl(MOD, EPOLLIN only) -> back to read mode
```

### `close()` + `epoll_ctl(DEL)` Order

```text
BUG: close(fd) first, THEN epoll_ctl(DEL) later
-> if fd number gets reused for a NEW connection,
   the epoll loop will watch the WRONG fd.

FIX: epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr) FIRST
     -> then close(fd)
```

---

## 3. Socket Lifecycle (from Bind to Close)

### Server Setup

```text
socket(AF_INET, SOCK_STREAM, 0)
│
fcntl(fd, F_SETFL, O_NONBLOCK)           ← non-blocking
setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt)  ← port reuse
bind(fd, &addr, len)                     ← IP:port
listen(fd, SOMAXCONN)                    ← passive listen
```

### Accept Loop

```text
epoll_wait returns client fd
│
fcntl(fd, F_SETFL, O_NONBLOCK)           ← non-blocking
epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev)  ← watch for EPOLLIN
│
handle_readable(fd):
  │
  recv(fd, buf, len, 0)                  ← read data (EAGAIN -> break)
  │
  process_frames(fd):                     ← protocol::try_parse
    │
    │ -> execute_command -> storage operations
    │
  │ -> queue_response(fd, reply)           ← add to outbox_[fd]
  │
  │ -> try_send(fd):                       ← send if possible
    │       send(fd, data, len, MSG_NOSIGNAL)
    │       if EAGAIN: leave data in outbox,
    │                epoll_ctl(MOD, EPOLLIN|EPOLLOUT)
    │       if sent: epoll_ctl(MOD, EPOLLIN only)
    ▼
│
│
```

### Close Connection

```text
epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr)  ← REMOVE first
close(fd)                                    ← THEN close
```

---

## 4. Threading & Synchronization

| Operation | Syscall/Primitive | Purpose |
|-----------|-------------------|---------|
| `std::thread` | `pthread_create` underneath | Server thread (epoll loop), Admin thread, TTL sweeper |
| `std::mutex` / `lock_guard` | `pthread_mutex_lock/unlock` | Protect `data_` in Storage, `expiries_` in TTL |
| `std::condition_variable` | `pthread_cond_wait/signal` | TTL sweeper 100ms sleep |
| `pthread_mutexattr_settype(PTHREAD_MUTEX_RECURSIVE_NP)` | Not used -- simple mutex sufficient |

### Lock Ordering (no deadlock)

```
Thread A (TTL sweeper):
  1. lock_guard lk(ttl.lock_)     -> scan expiries_
  2. unlock (lk destroyed)
  3. for key : dead { on_expire_(key); }
  4. (on_expire_ -> Storage::expire_key takes Storage.lock_ only)
```

### Why Two Locks?

| Map | Owner | Mutex | When Locked |
|-----|-------|-------|-------------|
| `data_` + `KeyMeta` + `policy_` | Storage | `Storage.lock_` | Every `set`/`get`/`del`/`switch`/`metrics`/`expire_key` |
| `expiries_` | TTLManager | `TTL.lock_` | `set_ttl`/`erase`/`is_expired`/`expired_keys` |

**Never both at once** -- callback runs after TTL lock released.

---

## 5. macOS Compatibility Layer (`src/epoll_compat.h`)

| Syscall | PolyCache Implementation |
|---------|-------------------------|
| `epoll_create1` | -> `poll()` + internal `mutex + unordered_map<int, Instance>` |
| `epoll_ctl` | -> `pollfd` array management |
| `epoll_wait` | -> `poll()` with timeout |
| Multiple epoll instances | Global `instances_mutex()` protects `instances_[epfd]` map |

**Why:** macOS doesn't have `epoll` natively on the production path. This shim translates epoll -> poll while supporting the same API.

---

## 6. Syscalls NOT Used (But Worth Knowing)

| Syscall | When You'd Use It | Why Not In PolyCache |
|---------|-------------------|----------------------|
| `io_uring` | High-throughput, batched async (Linux 5.1+) | Simple 1-thread design suffices |
| `sendfile` / `splice` | Zero-copy file->socket (nginx) | AOF replay uses `read`/`write`; sufficient throughput |
| `timerfd_create` + `settime` | Timers as fds (epoll-able) | TTL thread is simpler for this project |
| `signalfd` | Signals as fd -> epoll-able | Signals handled via `sigaction` + `pthread_sigmask` |
| `eventfd` | Lightweight thread notification | `condition_variable` + `mutex` used instead |
| `copy_file_range` | In-kernel file copy | AOF writes are small per-SSET; `write` is fine |
| `mmap` | Memory-mapped files, shared memory | Not needed -- data in `unordered_map` + `vector` |

---

## 6. Common Interview Questions & Answers

### Q: Why epoll over select/poll?

> A: `epoll` is O(ready) not O(total). Kernel maintains a ready list; `epoll_wait` returns only active fds. `select`/`poll` scan all fds every call, hit 1024 fd limit (select) or are O(n) (poll). Epoll registers once with `epoll_ctl`; no need to rebuild fd sets.

### Q: LT vs ET?

> A: **LT** (Level-Triggered, our choice): fd is readable -> `epoll_wait` keeps returning it until you drain it completely (`recv` until `EAGAIN`). **ET** (Edge-Triggered): fires only when state *changes* (e.g., data arrives). Must drain all data in one shot or you'll miss it. PolyCache uses LT for simplicity.

### Q: How do you handle slow clients?

> A: Per-client `64KB` read buffer cap (`kMaxBufferSize`). `send()` returns `EAGAIN` -> queue reply in `outbox_[fd]` -> `epoll_ctl(MOD, EPOLLIN|EPOLLOUT)` -> kernel wakes when send buffer has space -> `try_send(fd)` drains it -> re-arm `EPOLLIN` only.

### Q: What's the wake pipe?

> A: `socketpair(AF_UNIX, SOCK_STREAM, 0, wake_pipe[2])` -- two connected Unix sockets. The write end (`wake_pipe_[1]`) is held by the thread that calls `stop()`. Writing 1 byte makes the read end (`wake_pipe_[0]`) readable -> `epoll_wait` returns -> loop drains the byte -> checks `running_ == false` -> exits cleanly.

### Q: How do you avoid deadlock with TTL + Storage locks?

> A: Two independent locks: `Storage.lock_` guards `data_`, `TTL.lock_` guards `expiries_`. The callback `on_expire_(key)` runs *after* the TTL lock is released (in `sweep_loop`), so only the Storage lock is held during `expire_key`. Two locks, two separate maps, zero deadlock.

### Q: What syscalls are you using in the project?

> A: `epoll_create1`, `epoll_ctl` (ADD/MOD/DEL), `epoll_wait`, `socket`, `bind`, `listen`, `accept`, `fcntl(O_NONBLOCK)`, `setsockopt(SO_REUSEADDR)`, `recv`, `send(MSG_NOSIGNAL)`, `close`, `epoll_ctl(DEL)`, `socketpair` (wake pipe), `write` (wake pipe), `pthread_create`/`mutex`/`condvar` (threads), `close` (cleanup).

### Q: What would you improve for production scale?

> A: `io_uring` for batched async I/O (reduces syscalls per op), `sendfile`/`splice` for zero-copy file serving, `timerfd_create` + epoll to replace the TTL sweeper thread, `signalfd` to handle signals as fd, `eventfd` for thread notification.

---

## 7. Error Codes Cheat Sheet

| Code | When It Happens | Handling |
|------|-----------------|----------|
| `EAGAIN` / `EWOULDBLOCK` | Non-blocking fd not ready (most common) | Re-arm epoll, retry later |
| `EINTR` | Interrupted by signal (e.g., SIGINT) | Retry syscall (or check `running_`) |
| `EPIPE` / `ECONNRESET` | Peer closed socket / RST | Close fd, cleanup buffers |
| `EMFILE` | Per-process fd limit hit (`ulimit -n`) | Raise limit, connection pooling |
| `ENFILE` | System-wide fd limit hit | Raise system limit |
| `EADDRINUSE` | Port already in use on `bind` | `SO_REUSEADDR`, wait/retry |
| `EACCES` | Permission denied on `bind`/`listen` | Check user/run as root |
| `EINVAL` | Invalid argument (e.g., bad `epoll_ctl` op) | Debug code path |

---

## 7. Quick-Reference Card (One Page)

```
epoll_create1 -> epoll_ctl(ADD) -> epoll_wait(-1)
      │
      ▼
recv() EAGAIN? ── no ──► process_frames() ──► execute_command -> storage
      │                                         │
      └─ yes ──► break ──► next epoll_wait iteration
               │
               ▼
recv bytes accumulate in buffers_[fd]
      │
      ▼
EPOLLOUT armed? ── no ──► continue loop
      │
      yes ──► try_send() ──► send() ──► EAGAIN? ──► queue in outbox_[fd] ──►
                           │                                              │
                           └─ yes ──► epoll_ctl(MOD, EPOLLIN|EPOLLOUT) ──────┘
                           │                                              │
                           └─ no ──► epoll_ctl(MOD, EPOLLIN only) ──────────────┘
                                           │
                                           ▼
                           close: epoll_ctl(DEL) first, THEN close(fd)
```

---

## 8. Build & Test

```sh
make              # compiles with g++ -std=c++17 -O2 -Wall -Wextra -pthread
make test         # runs 28 test groups (sieve+protocol+storage+resp)
make clean        # removes *.o, *.d, polycache, test binaries
```

---

*Generated from codebase analysis (plan mode -> build mode).*