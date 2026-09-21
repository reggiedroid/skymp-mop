# parallel_bench

A benchmark for the area offload that builds without vcpkg.

```bash
./build.sh && ./parallel_bench 400        # 400 timed ticks per point
DEPS_PREFIX=$HOME/deps ./build.sh         # headers somewhere other than /usr
```

## What this is, and what it is not

`unit/ParallelBenchmark.cpp` is the authority. It drives the real `PartOne`,
the real packet parser and the real send target, and it times full ingest plus
`Tick` — what an operator actually pays. Run it, on the target hardware,
before changing a default:

```bash
./unit/unit "[ParallelBench]"
```

It also needs slikenet, mongo-cxx-driver and the rest of the vcpkg tree. On a
host where that build is unavailable, every question about a setting has had
to be answered with a cost model instead of a measurement, and
`misc/perf_model.py` records how that went.

This file is the smaller thing that can always be built: `parallel/*.cpp`,
`FormDesc.cpp`, a C++17 compiler, and fmt/spdlog/nlohmann-json headers. It
drives `OffloadDispatcher` through `IOffloadSink` and times `ExecuteTick`
alone.

So:

- **Absolute numbers here are not comparable** to the was/now table in
  `ParallelConfig.h`. There is no ingest and no deserialization, and both the
  inline and the offloaded path pay those, so every ratio here is further from
  1.0 than the same ratio measured end to end.
- **Ratios between configurations are the point.**
- **One machine is one machine.** Nothing here may become a shipped default on
  its own.

## Results, 2026-09-21

AMD Ryzen 9 PRO 8945HS, 8 physical cores / 16 threads, Linux 6.x, g++ 13.3,
`-O2`. 400 timed ticks per point after 40 warm-up ticks, median of
`ExecuteTick`. Every player in one chunk, all moving every tick, interest
management off. Run-to-run spread across three full runs was 3–8% with the
shape unchanged.

### Worker count, shard budget left to auto-size

`vs inline` is against a true inline baseline, taken by putting
`minActorsToOffload` out of reach. Note that `workerThreads = 0` is *auto*,
not inline — it resolves to physical cores minus one, 7 here.

| workers | 100 players | 200 players | 400 players |
| --- | --- | --- | --- |
| inline baseline | 55.1µs | 221.9µs | 886.5µs |
| 0 (auto → 7) | 0.83× | 3.20× | 4.34× |
| 2 | **1.36×** | 1.72× | 2.54× |
| 4 | 0.98× | 2.25× | 2.74× |
| 8 | 0.82× | 3.08× | 4.60× |
| 12 | 0.89× | **3.59×** | 5.62× |
| 16 | 0.94× | 2.56× | **5.61×** |
| 24 | 0.95× | 1.17× | 2.77× |
| 32 | 1.00× | 1.02× | 2.46× |

Three things this says:

1. **The fall-off past the machine is real and it is not gentle.** At 200
   players a 32-worker pool gives back everything the offload bought
   (1.02×). At 400 it gives back more than half. `153559c2` raised
   `kMaxAutoWorkerThreads` from 8 to 32 on the grounds that "scaling
   continues smoothly up to the core limit"; on this machine it does not.
   Restoring the cap was right.
2. **The cap of 8 is conservative here but on the safe side of the peak.** The
   peak is 12–16 workers at both 200 and 400 players, and auto-sizing lands on
   7. That leaves something on the table on a 16-thread host — and the
   benchmark has the machine to itself, which a server does not, which is
   exactly why auto-sizing reserves a core for the Node thread.
3. **At 100 players the offload does not pay for itself in this phase**
   (0.83× at auto). That is not in tension with the end-to-end table's 0.99×
   at 100 players: ingest is common to both paths, so including it pulls the
   ratio toward 1.0. It does say the break-even is not comfortably below 100.

### Pool size against unit count, 400 players, units pinned

µs/tick. The sweep above cannot separate the two variables, because the
`slots × 2` ceiling grows the unit count with the pool. This pins the unit
count so only the pool size varies.

| pool size | 4 units | 8 units | 16 units | 32 units |
| --- | --- | --- | --- | --- |
| 4 | 311.8 | 311.5 | 298.9 | 264.2 |
| 8 | 415.0 | 259.5 | 200.9 | 194.0 |
| 12 | 378.8 | 383.3 | 188.9 | 177.1 |
| 16 | 432.4 | 379.8 | 190.8 | **160.3** |
| 24 | 398.2 | 356.4 | 225.3 | 338.4 |
| 32 | 378.7 | 361.7 | 341.8 | 374.2 |

`docs_parallel_area_offload.md` reads down these columns and concludes that
with the unit count fixed, going from 8 workers to 24 costs about 1%. **That
holds only while the pool fits the machine.** Here it costs 12% at 16 units
and 74% at 32, and the penalty arrives exactly where the pool crosses the 16
logical CPUs this host has. Both variables matter; the unit count is the
larger one up to that crossing, and past it the pool size dominates.

### Why the oversubscribed pool costs what it does — a hypothesis that failed

If the penalty were surplus workers spinning before they park, shortening the
spin would take it back. 400 players, 32 units pinned, µs/tick:

| pool size | 250µs (default) | 50µs | 10µs | 0µs (park immediately) |
| --- | --- | --- | --- | --- |
| 8 | **191.3** | 225.1 | 284.1 | 301.9 |
| 16 | **151.7** | 256.7 | 293.1 | 292.2 |
| 24 | **320.1** | 388.6 | 388.9 | 433.6 |
| 32 | **354.3** | 468.7 | 515.2 | 513.0 |

It does not. Shortening the spin is worse at every pool size including the
oversubscribed ones, so surplus spinning is not the mechanism. What the
metrics show instead is that aggregate task time stays flat across pool sizes
(≈1000–1100µs at 400 players, from 8 workers to 32) while the wall clock of
the fork/join phase nearly triples. The work is the same and takes the same
CPU time; the phase spends the difference waiting. That is consistent with the
barrier waiting on a claimant the scheduler has not run yet, which is what
more threads than CPUs produces — but this benchmark does not observe
scheduling directly, so that is the reading, not a measurement.

The table does measure one thing outright: **`kDefaultSpinMicros = 250` is
well chosen.** Dropping it costs 18–93% everywhere, including where the pool
fits.

### minShardMicros, 400 players, 8 workers

| minShardMicros | 10 | 20 (default) | 30 | 40 | 60 | 95 | 150 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| µs/tick | 193.8 | 192.2 | 192.7 | 195.8 | 198.7 | 240.8 | 293.8 |
| work units | 18 | 18 | 18 | 18 | 17 | 10 | 6 |

10 through 60 are within run-to-run noise of each other; 95 costs 25% and 150
costs 53%, because the budget starves the unit count.

`153559c2` added a CPUID probe that rewrote `minShardMicros` from 20 to 60 on
Intel parts, citing a model's "55–95µs is optimal for Ice Lake". On this
machine — AMD, so the probe would not have fired — 60 is free and 95 is not.
That is weak evidence about the value and none at all about the vendor test,
which was removed for a reason that does not need a benchmark: its trigger was
`minShardMicros == 20`, the documented default, so it could not tell an
operator who had measured 20 from someone who had never touched it.

## Reproducing

The dependencies are header-only and need no root:

```bash
python3 -m venv venv && ./venv/bin/pip install cmake ninja
# then build fmt, nlohmann/json and spdlog (SPDLOG_FMT_EXTERNAL=ON)
# into a prefix, and point DEPS_PREFIX at it.
```

`parallel_bench <ticks>` takes the tick count per point; 400 is the default
and about four minutes for the whole sweep on this host.
