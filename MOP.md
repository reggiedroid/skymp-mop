# SkyMP: MOP — Multiplayer Optimization Project

Server-side performance work for [SkyMP](https://github.com/skyrim-multiplayer/skymp).
We went through the hot path with a bucket and a mop: measured what was slow,
fixed what was broken, and threw out our own bad numbers along the way.

**Everything here is opt-in.** With `parallelism.enabled` absent or false, the
original code path runs byte for byte.

| | |
| --- | --- |
| Crowded-area tick | **1.39×** (400 players, measured) |
| Legacy JSON ingest | **3.4–6.3×** (closes `TODO(#2257)`) |
| Hang bugs fixed | 1 tick-stopping race |
| Tests | 480,346 assertions, 310 cases, 12/12 ctest |
| Risk if unused | 0 — off by default |

---

## The problem

Movement is the highest-volume packet, and its cost is quadratic: every update
is relayed to everyone who can see the sender, so *N* players in one square cost
on the order of *N²* relay decisions per tick — all on the single Node thread
that drives `ScampServer::Tick`.

## The unusual part of this pitch

An earlier draft of this work claimed 4×/8×/15× speedups from a relay-edge cost
model. We built a benchmark, discovered those numbers were **wrong**, and
retracted them. Everything below is what a stopwatch says, and the benchmark
ships with the branch so you never have to take our word for the next number
either:

```bash
./unit/unit "[ParallelBench]"
```

---

## What's in the bucket

### 1. Interest management — always on

Players within half a chunk get every update. Beyond that the rate steps down by
distance, capped so a distant player still receives ~15 updates a second at
60Hz. This attacks the N² term directly, and unlike the offload it helps at
**every** population.

400 players spread across one chunk:

| configuration | relays / tick | µs / tick |
| --- | --- | --- |
| base SkyMP (inline) | 160,000 | 1233 |
| + offload | 160,000 | 1048 |
| + offload + interest management | **119,866** | **890** |

**1.39× against base**, with a send target that does nothing. With real network
sends each avoided relay also skips serialization and queueing, so the gap
should widen.

Each pair's phase is derived from a hash of the pair, so traffic spreads across
the window instead of bursting. The suite asserts every pair still transmits
exactly once per window: reduced rate, never silence.

### 2. Parallel area offload — opt-in

Relay decisions move to worker threads, partitioned into areas that provably
cannot influence one another, then sharded so one crowded hub can still use
every core.

| players | 25 | 50 | 100 | 150 | 250 | 400 |
| --- | --- | --- | --- | --- | --- | --- |
| speedup | 0.25× | 0.43× | 0.66× | 0.81× | 0.88× | **1.10×** |

**Break-even sits near 300–400 players**, which is why `minActorsToOffload`
defaults to 300 and not to something optimistic.

The parallel phase itself is not the limit — at 400 players it retires 966µs of
task work in 96µs of wall clock, about tenfold. The ceiling is the join:
emitting 160,000 relays serially costs 787µs of an 1118µs tick *even with a
no-op send target*. No amount of parallelism fixes that; sending fewer relays
does.

### 3. JSON deserializer — unconditional

`MessageSerializer::Deserialize` walked the whole deserializer table for JSON
messages, and each candidate allocated a `std::string` *and constructed a fresh
`simdjson::dom::parser`* before re-parsing the entire document, until one
matched. It now reads the message type once and dispatches directly, reuses a
`thread_local` parser, and only materializes the message text when trace logging
is enabled.

Measured 3.4–6.3× cheaper on that path. Production clients send binary, so this
only helps legacy ones — but it closes the `TODO(#2257)` that lived there.

### 4. The benchmark — infrastructure

`unit/ParallelBenchmark.cpp`, hidden behind Catch2's `[.]` tag so ctest ignores
it. Times inline against offloaded over identical work, on the binary wire
format real clients use, counting full ingest **plus** `PartOne::Tick` — the
inline path relays during ingest while the offloaded path defers to the join, so
timing only one half would flatter whichever was picked.

---

## The bug that justifies the branch on its own

`ThreadPool::Run` returned as soon as the task counter hit zero — but the worker
that ran the final task decrements that counter and only *then* loops back to
check the task cursor. If `Run` returned in that window and the next tick started
a batch, the cursor reset handed the still-draining worker index 0 of the
*previous* task vector.

The failure mode is not a duplicated packet: the straggler decrements a counter
it was never part of, underflowing it, so the barrier never releases — **the
server tick hangs permanently.**

The natural window is a few instructions wide, so nothing failed on an idle
machine. To establish it was real rather than theoretical, we reverted only the
fix and inserted a 300µs delay where a straggler would sit:

```
Alternating batch sizes stay consistent
  pre-fix  + widened window   HUNG    3 of 3 runs
  with fix + widened window   passed  3 of 3 runs
```

This race is in code MOP introduces — it is **not** a latent bug in base SkyMP
today. It is the class of bug that any threading work invites, and it is why the
branch ships the delay-injection experiment documented rather than a passing
test and a shrug.

---

## Why adopting it is close to free

- **Off by default.** No behavioural change, no new threads, no config required.
- **Deterministic when on.** Clusters ordered by lowest chunk, members by
  submission index, shards contiguous. Same sequence of world writes on 2 cores
  or 32, whole cluster or split sixteen ways. Thread count is a performance knob,
  not a behaviour knob.
- **Provably safe partitioning.** Derived from SkyMP's own `Grid.h` stencil:
  reach 1 for visibility plus reach 1 for movement equals reach 2, so a
  separation of 3 chunks contains every possible recipient. Property-tested over
  400 randomized populations, 23,810 actors.
- **Tested against the real server.** `PartOne_MovementParallelTest` drives the
  real `PartOne`, packet parser and send target with the offload enabled,
  asserting identical messages reach identical users.

---

## What we are not claiming

A pitch that only lists upsides is one you should distrust.

- **The offload is a regression below ~300 players.** At typical population it is
  slower. That is why it defaults off and why the threshold is 300. On a normal
  server the useful half of this work is interest management.
- **No real-client load test yet.** The benchmark drives the server directly with
  a no-op send target, so it measures server-side relay cost, not the network
  stack. A live test with hundreds of real clients would confirm or move these
  figures, and it has not been run.
- **One machine, one topology.** All figures come from a single 32-core host with
  every player in one chunk. Re-run the benchmark on target hardware before
  tuning.

## Two behavioural differences when enabled

- **Movement relays batch to the end of the tick** instead of being emitted
  mid-ingest. Movement is unreliable and superseded by the next update, so it is
  not observable in play — but it is visible in packet captures, and tests must
  tick before asserting on `Messages()`.
- **A world/cell form id from an unloaded plugin** is rejected with a teleport
  correction instead of throwing out of the packet handler. We think the new
  behaviour is safer, but it is a change.

Interest management also sends distant players fewer updates by design. Set
`interestManagement: false` to disable.

---

## Try it

```bash
./unit/unit "[ParallelBench]"      # the numbers above, on your hardware
ctest -C Release                   # 12/12
./unit/unit "[ParallelOffload]"    # dispatcher, parity, sharding, interest mgmt
```

Then in `server-settings.json`:

```json5
"parallelism": {
  "enabled": true,
  "workerThreads": 0,          // auto: cores minus two
  "interestManagement": true,  // the setting that matters most
  "metricsLogIntervalTicks": 5000
}
```

The server emits a line you can steer by:

```
MpParallel: tick=5000 actors=214 clusters=37 biggest=151 units=58 (pooled 54)
            relays=9871 throttled=0 parallel=2104us join=610us speedup=6.31x
```

- `biggest` vs `actors` — how concentrated the population is
- `units` vs `clusters` — whether sharding engaged
- `speedup` near 1 — the offload is not earning its keep at that population
- `join` creeping toward `parallel` — the serial tail is the limit now

Full option list and tuning guidance:
[`docs/docs_parallel_area_offload.md`](docs/docs_parallel_area_offload.md).
Proposal writeup: [`PULL_REQUEST.md`](PULL_REQUEST.md).
