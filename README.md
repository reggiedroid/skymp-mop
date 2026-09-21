

# SkyMP: MOP (Multiplayer Optimization Project)

Skyrim itself is effectively single-threaded, and so was the SkyMP server: one
Node thread drives `ScampServer::Tick`, which pumps every packet and runs every
handler in sequence. On a modern host that leaves most of the CPU idle while
one core saturates, and the symptom players notice is that crowded places —
a city square, a market, a raid — get choppy for everybody in them, even though
the machine is mostly asleep.

This framework spreads that per-area work across the cores the server host
actually has. It is **off by default**; a server that does not opt in behaves
exactly as it did before.

A fork of [SkyMP](https://github.com/skyrim-multiplayer/skymp) carrying
server-side optimizations, intended to be proposed upstream rather than to
diverge from it.

| | |
| --- | --- |
| Parallel offload | **2.17×** — 400 players, measured |
| Interest management | **1.39×** — 400 players, 160k relays down to 120k |
| Legacy JSON ingest | **3.4–6.3×** — closes upstream `TODO(#2257)` |
| Hang bugs fixed | 2 tick-stopping races |
| Tests | 75 parallel cases · 97,589 assertions |
| Risk if unused | 0 — off by default |

Jump to: [what it costs, measured](#what-it-actually-costs-measured) ·
[interest management](#interest-management--the-part-that-actually-pays) ·
[configuration](#configuration) · [build & terms](#building)

## What actually gets parallelised

Movement is the dominant packet by volume, and its cost is not linear in player
count. Every update from one player is relayed to every other player who can
see them, so a crowd of *N* players in one area produces on the order of *N²*
relay decisions per tick. That quadratic term is what makes dense areas hurt.

Per tick, the server now:

1. **Ingests** movement packets on the main thread as before, but instead of
   relaying immediately it flattens each update — the actor's transform, the
   animation flags and the raw packet bytes — into a plain-data snapshot, and
   records every active player's position once. That second list is O(N) in
   the player count, not O(N²) in the relay edges.
2. **Partitions** the actors into *area clusters* that provably cannot
   influence one another this tick, then splits each cluster into **work
   units** small enough to spread across the pool.
3. **Processes each unit on a worker thread**: validating the movement,
   spatially filtering that snapshot down to the recipients who can actually
   see the sender, and building the outbound send list.
4. **Joins** on the main thread, applying world state and handing the packets
   to the network in a fixed, reproducible order.

Steps 1 and 4 stay serial because they touch the world, the Papyrus VM, the
RakNet peer, and V8 — none of which are thread-safe. Step 3 touches nothing but
immutable snapshot data and its own output buffer, which is the invariant the
whole design rests on.

The second, and often larger, win is that step 3 can decide to **send less**.
See [Adaptive throttling](#adaptive-throttling).

## Why the clusters are safe

Visibility in SkyMP is the 3×3 chunk stencil in `Grid.h`: an actor in chunk *C*
relays to actors in chunks within Chebyshev distance 1 of *C*. Movement
validation caps a single update at just under one chunk, so within one tick an
actor's relay set can reach chunks up to distance 2 away.

Clusters are therefore built as connected components under the relation *same
worldOrCell, and Chebyshev chunk distance ≤ S*. Any `S ≥ 3` guarantees that
every actor a cluster member can relay to is also in that cluster, which is why
the setting is clamped up to 3. The default of 4 buys a chunk of margin.

Actors in different worldspaces or interior cells never share a grid, so they
always separate. Instanced content parallelises perfectly.

## Why clusters are not the unit of work

The obvious design is one task per cluster. It does not work, and it is worth
being explicit about why, because the failure is invisible until you measure
a realistic population.

Players are not spread evenly. A populated server has one main hub plus a long
tail of quieter places, and because relay cost within an area grows with the
square of the people in it, the hub dominates. Modelling a plausible
distribution (a main hub, two secondary towns, two interiors, and scattered
travellers) puts **about 70% of all relay work in a single cluster**, at every
population from 40 to 300 players. One task per cluster therefore caps the
whole feature at roughly **1.4×** — and, worse, adding cores past two buys
literally nothing.

That is precisely backwards: the framework would do the least where the server
hurts the most.

So the scheduling unit is a *shard*: a contiguous slice of one cluster's
members. Each sender's work depends only on the immutable snapshot, so any
split of a cluster's members is safe; the cluster boundary is what makes
throttling and recipient sets coherent, not what makes the work independent.

### What it actually costs, measured

An earlier version of this document quoted modelled speedups of 4×/8×/15×
from a relay-edge cost model. **Those numbers were wrong.**
`unit/ParallelBenchmark.cpp` now exists so that nothing here has to be taken
on faith:

```bash
./unit/unit "[ParallelBench]"
```

Every player in one chunk, everyone sending movement every tick, timing the
full ingest **plus** `PartOne::Tick` so both paths are compared over the same
unit of work. 16-core/32-thread Ryzen 9950X3D, binary wire format:

| players | 25 | 50 | 100 | 150 | 250 | 400 |
| --- | --- | --- | --- | --- | --- | --- |
| speedup | 0.85× | 0.88× | 0.99× | **1.29×** | **1.87×** | **2.17×** |

**Break-even is around 100 players.** Below that the offload is a regression:
barrier and snapshot costs are paid every tick while the parallel phase is
still small. That is why `minActorsToOffload` defaults to 100 rather than to
something optimistic.

Break-even used to sit near 300–400 (0.25×/0.43×/0.66×/0.81×/0.88×/1.10× for
the same populations). What moved it was not the parallel phase, which was
always small, but the three serial costs around it: the per-tick barrier, a
join that made one virtual call per relay edge, and shards sized by actor
count rather than by work.

The ceiling now is the join: at 400 players it emits 160,000 relays serially
and costs 275µs of a 551µs tick, *even
with a send target that does nothing*. Parallelising decisions cannot fix
that. Sending fewer relays can, which is what interest management is for.

Clusters still matter. They are what makes each shard's recipient set and
pressure level well defined, and they let two distant crowds be costed
separately. They are just no longer the thing handed to a core.

## Determinism

Clusters come back ordered by their lowest chunk coordinate, members are
ordered by submission index, and shards are contiguous slices of that order.
The join walks the work units by index, so the sequence of world writes is the
same on 2 cores or 32, and the same whether a cluster was processed whole or
split ten ways. Thread count and shard count are performance knobs, not
behaviour knobs.

## Interest management — the part that actually pays

Relay volume is the quadratic term, and emitting those sends is serial no
matter how many cores decided them. So the highest-value lever is sending
less — and unlike the offload, it helps at every population rather than only
above 100.

Recipients closer than `interestFullRateUnits` (2048 by default, about half a
chunk) always receive every update, so anything a player is realistically
fighting, trading with or watching stays at full fidelity. Beyond that the
rate steps down: half, then a third, then a quarter, capped by
`maxInterestSkipTicks`. At a 60Hz tick a distant player still gets roughly 15
updates a second.

This is on by default and does not wait for the server to be in trouble: a
player sixty metres away does not need sixty position updates a second even
on an idle server.

Measured on the same benchmark, 400 players spread across one chunk:

| configuration | relays/tick | µs/tick |
| --- | --- | --- |
| inline (baseline) | 160,000 | 1233 |
| offload only | 160,000 | 1048 |
| offload + interest management | 119,866 | **890** |

**1.39× against the inline baseline** — and that is with a send target that
does nothing. With real RakNet sends, each avoided relay also skips
serialization and queueing, so the gap widens.

The phase of each reduced pair is derived from a hash of the pair, so traffic
spreads evenly across the window instead of bursting. `[ParallelOffload]`
asserts that every pair still transmits exactly once per window: reduced
rate, never silence.

## Adaptive throttling

When a cluster's smoothed cost exceeds its share of the tick budget, distant
relays within that cluster get spaced out over several ticks instead of firing
every tick. Players close to each other — the ones actually fighting or talking
— are never throttled at any pressure level.

The phase of each throttled pair is derived from a hash of the pair, so a
throttled area spreads its relays evenly across the skip window rather than
sending everything on one tick and nothing in between.

This is graceful degradation: a density spike costs distant players some update
frequency instead of costing everyone a stalled tick.

## Configuration

Add a `parallelism` object to `server-settings.json`:

```json5
{
  // ...
  "parallelism": {
    "enabled": true,
    "workerThreads": 0,
    "minActorsToOffload": 100,
    "adaptiveParallelism": true,
    "minOffloadWorkMicros": 100,
    "relayFromWorkers": false,
    "minClusterActors": 4,
    "minShardActors": 4,
    "minShardMicros": 20,
    "maxShardsPerCluster": 0,
    "clusterSeparationChunks": 4,
    "interestManagement": true,
    "interestFullRateUnits": 2048,
    "maxInterestSkipTicks": 4,
    "adaptiveThrottling": true,
    "targetTickBudgetMicros": 2000,
    "throttleDistanceUnits": 4096,
    "maxThrottleSkipTicks": 3,
    "metricsLogIntervalTicks": 0
  }
  // ...
}
```

| Key | Default | Meaning |
| --- | --- | --- |
| `enabled` | `false` | Master switch. Off means the original code path, byte for byte. |
| `workerThreads` | `0` | `0` auto-detects: *physical* cores (not SMT siblings) minus one for the Node main thread, capped at 8. The cap bounds the auto-sized work-unit count via the `slots × 2` ceiling, which is the quantity measurement showed to matter. An explicit value is bounded only by 32. |
| `minActorsToOffload` | `100` | Below this the fork/join barrier costs more than it saves — measured, see the table above. Break-even used to sit near 300 and moved to 100 once the barrier, the join and shard sizing were fixed. |
| `interestManagement` | `true` | Distance-based update-rate reduction, always on. The highest-value setting here. |
| `interestFullRateUnits` | `2048` | Recipients closer than this always get every update. |
| `maxInterestSkipTicks` | `4` | Ceiling on how far apart interest management may space an update. |
| `minClusterActors` | `4` | Clusters smaller than this are swept up on the main thread instead of getting their own task. |
| `minShardActors` | `4` | Fewest players a shard of a crowded cluster may carry. Lower splits a crowd more finely; too low and per-task overhead starts to show. |
| `maxShardsPerCluster` | `0` | `0` auto-sizes to twice the slot count. Raise only if profiling shows one area still bottlenecking. |
| `minShardMicros` | `20` | Smallest estimated work, in µs, that justifies its own shard. A quiet tick collapses to one unit and skips the barrier. Raising it starves the unit count: 95 cost 25% and 150 cost 53% in `misc/parallel_bench`. |
| `workerSpinMicros` | `250` | How long a worker spins on the cursor before parking. Measured well chosen — dropping it to 0 cost 18–93% at every pool size tried. |
| `relayFromWorkers` | `false` | Whether workers hand relays straight to the send target, or leave them for the join thread. **`true` requires a network stack that tolerates `Send` from several threads at once**, which nothing in this repository establishes for slikenet, so the default is the safe one. Costs roughly 1.3× at 100 players, 1.4–1.7× at 200 and ~1.5× at 400 of `ExecuteTick` — and the offload still beats inline by about 3.8× at 400 with it off. Identical relays either way; see *Live testing* below. |
| `clusterSeparationChunks` | `4` | Chunk distance separating clusters. Clamped up to 3. Raise it if you want more margin, at the cost of merging nearby crowds. |
| `maxWorkUnitsPerTick` | `0` | `0` is unlimited. Units past the limit run on the calling thread. |
| `adaptiveThrottling` | `true` | Enables the degradation described above. Only ever activates under measured overload. |
| `targetTickBudgetMicros` | `2000` | Wall-clock target for the parallel phase. Overshooting raises pressure. Twice what a whole tick has to give: the server loop is `tick(); sleep(1)`, so the previous `8000` meant no area was judged under pressure until the server was already catastrophically late. |
| `throttleDistanceUnits` | `4096` | Base radius of the near band, which is never throttled. Halves per pressure level, so at the highest it is 512 units — still inside a fight. It never shrinks below an eighth of this value. |
| `maxThrottleSkipTicks` | `3` | Hard ceiling on how far apart a throttled relay may be spaced. |
| `metricsLogIntervalTicks` | `0` | `0` disables. Otherwise logs a summary line every N ticks. |

Deciding whether to offload at all. The dispatcher does not compare a statistic
against a threshold: it periodically runs both paths and keeps whichever
measured cheaper per mover. Declining is not the same as running the tick
without the pool — it hands the tick back to `ActionListener`'s original
relay-then-validate path, which is the real floor.

| Key | Default | Meaning |
| --- | --- | --- |
| `adaptiveParallelism` | `true` | The paired A/B trial. Off makes the decision fall back to `minOffloadSpeedup`. |
| `minOffloadWorkMicros` | `100` | Estimated work below which a tick is declined outright, without trialling. Cheap early-out for populations too small to be worth measuring. `0` never declines on this ground, which is how to get interest management unconditionally. |
| `minOffloadSpeedup` | `2.5` | Achieved-speedup floor used only before any trial has reached a verdict. |
| `adaptiveProbeIntervalTicks` | `240` | Ticks the gate may stay shut before taking one tick on to refresh its evidence. Everything the decision rests on is measured only on accepted ticks, so without this the gate cannot notice a crowd forming under it. `0` disables the probe. |
| `abTrialIntervalTicks` | `1800` | Ticks between trials. |
| `abTrialBlockTicks` | `4` | Ticks per arm per block. |
| `abTrialBlocks` | `12` | Blocks per trial, split between the two arms. With the defaults a trial costs 48 ticks in 1800, half of them on whichever path turns out to be worse. |

### Suggested starting point

On a 8-core host running a busy server:

```json5
"parallelism": {
  "enabled": true,
  "workerThreads": 0,
  "interestManagement": true,
  "metricsLogIntervalTicks": 5000
}
```

Watch the log line, then tune:

```
MpParallel: tick=5000 actors=214 clusters=37 biggest=151 chunks=96 units=58
            (pooled 54) relays=9871 throttled=0 parallel=2104us join=610us
            speedup=6.31x declinedTotal=412 verdict=accept relays_from=join
            staleTotal=0
```

A tick the gate declined prints a shorter line instead, and only when players
were actually trying to move:

```
MpParallel: tick=5000 declined attempts=180 declinedTotal=4998
            verdict=decline ticksSinceAccept=237
```

Seeing that line is the point of it. A server that declines every tick used to
log nothing at all, which looks exactly like the feature being switched off.

- `speedup` is summed task time over wall-clock parallel time. Near 1 means the
  offload is buying nothing — usually because `minActorsToOffload` is never
  reached.
- `biggest` against `actors` tells you how concentrated your population is. When
  `biggest` is most of `actors`, sharding is doing the work, and `units` should
  be comfortably larger than `clusters`. If it is not, lower `minShardActors`.
- `chunks` against `clusters` says how the population is laid out: `chunks` is
  how many 4096-unit squares are occupied, `clusters` how many independent
  groups those squares fall into. Hundreds of chunks in a handful of clusters
  is a map with cities on it; one of each is a crowd.
- `staleTotal` counts submissions whose actor had gone by the time the join
  ran — a disconnect or a cell change between ingest and the join. A few
  during churn are normal. Steadily climbing on a stable population is a bug
  worth reporting.
- `join` growing toward `parallel` means the serial tail is becoming the limit;
  more cores will not help past that point. With the default
  `relayFromWorkers: false` the join *also* carries every relay, so a large
  `join` is expected and is not by itself evidence of a serial bottleneck.
  Compare the two settings before concluding anything from it.

### Live testing

The whole subsystem is off unless `enabled` is set, so a live test is opt-in
and the kill switch is the same setting. `Reconfigure` picks up a changed
worker count between ticks, and setting `enabled: false` returns the server to
the original code path byte for byte.

Start here, and change one thing at a time:

```json5
"parallelism": {
  "enabled": true,
  "workerThreads": 0,
  "relayFromWorkers": false,
  "metricsLogIntervalTicks": 5000
}
```

1. **Leave `relayFromWorkers` at `false` for the first run.** With it off, no
   worker thread touches the send target at all: relays are emitted from the
   join, one thread, in work-unit order. That also makes a failure
   reproducible, which a concurrent path does not.
2. **Watch `verdict` and `declinedTotal` in the metrics line.** The offload
   decides whether to engage by measuring both paths, and `verdict=decline`
   with a climbing `declinedTotal` means it chose not to. If that happens on a
   population you expected it to take on, that is the thing to report, not a
   setting to override. `minOffloadWorkMicros: 0` forces it to engage and is
   the way to get a comparison, not a way to run a server.
3. **Only then consider `relayFromWorkers: true`**, and only if you have
   established that your build's send path tolerates concurrent `Send`. On
   this tree that means slikenet's `RakPeer::Send`, which is a vcpkg
   dependency and was not inspected here. Everything else on that path was:
   the connected-user bitmap is built before the tick and only read during it,
   `PartOne::GetSendTarget` is a const accessor, and `IdManager::find` is a
   bounds-checked vector read whose mutators run in a different phase of the
   main thread.

What it is worth: about 1.5× of `ExecuteTick` at 400 players. What it costs if
the assumption is wrong: memory corruption in a running server. That is the
trade, stated so it can be made deliberately.

### What to expect from a few hundred players spread across a map

The tables at the top of this file are a crowd: every player inside one chunk,
everybody visible to everybody. A live server is usually something else —
cities and markets with real crowds in them, most of the map thinly occupied,
and parties walking between the two. That population is measured separately,
by the `province` and `roaming` shapes in `misc/parallel_bench`, and it
behaves differently in three ways worth knowing before the first test.

**The gate will decline, most of the time, and that is the right answer.**
Relay volume is what the offload parallelises, and it is quadratic in how many
players can see *each other*, not in how many are logged in. 100 players
spread over a province generate about 700 relay edges in a whole tick; 400
players in one chunk generate 160,000. There is nothing in the first case for
a fork and a join to repay, so `verdict=decline` on a quiet map is the gate
working, not failing. It engages when a city fills, and the probe means it
notices within four seconds.

**Cluster count is not a density signal.** Clusters merge at four chunks'
separation, so players strung along a road closer together than that join into
one cluster however far apart the two ends are. On the `province` shape 800
players across 337 occupied chunks came out as 14 clusters with 669 of them in
the largest. That is not a bug — the separation is what makes a cluster safe
to process on its own — but it does mean the parallelism comes from *sharding*
the big cluster rather than from having many. Watch `units` against
`clusters`, not `clusters` alone.

**Below a few hundred movers the offloaded path is slower, and above it
faster.** Both arms priced with the decline grounds forced off, three runs,
median `ExecuteTick`, on an 8-core Ryzen 9 PRO 8945HS:

| shape | 100 | 200 | 400 | 800 |
| --- | --- | --- | --- | --- |
| province | 0.36× | 0.59× | **1.36×** | **2.29×** |
| roaming | 0.33× | 0.51× | **1.11×** | **1.87×** |

Above 1.0 the offload is cheaper. These are `ExecuteTick` only — no ingest, no
`PartOne` — so an end-to-end ratio will sit closer to 1.0 than these do. What
they establish is the shape of the curve, and that the break-even on a map
population is in the low hundreds rather than nowhere.

**Expect `throttled` to be non-zero sooner than the budget suggests.** An area
is judged under pressure against `targetTickBudgetMicros` divided by the
cluster count, so on a population that merges into a few large clusters each
one's share is a fraction of the tick, and the cluster holding a city will
exceed its share while the tick as a whole is comfortable. What that costs is
bounded and deliberate -- relays beyond the near band are spaced out, never
dropped, and the near band is never smaller than 512 units -- so a non-zero
`throttled` on a map population is expected rather than a sign of trouble.
`adaptiveThrottling: false` switches the mechanism off if you would rather the
first test measured without it.

### When to stop the test

Stop and set `enabled: false` — which returns the server to the original code
path byte for byte — if any of these appear. Each is a symptom of something
this branch could be wrong about, and none of them is a tuning problem:

- Players report seeing each other teleport, stutter, or vanish at the edge of
  a chunk, or stop seeing each other at all across a boundary. The offloaded
  path recomputes the relay neighbourhood from positions instead of walking
  the subscription lists; a mismatch would show up exactly there. (The two are
  asserted identical on scattered maps, parties crossing boundaries, several
  cities at once, and both relay modes, by `[ParallelWorld]` — but those are
  the code's own terms, not a live client's.)
- `staleTotal` in the metrics line climbing steadily while the population is
  stable.
- Any crash inside the send path, particularly with `relayFromWorkers: true`.
  That setting is the one unverified concurrency assumption in this branch;
  turn it off first and see whether the crash goes away, then report it either
  way.
- Tick time worse than it was with the subsystem off, on a population where
  the metrics line says `verdict=accept`. That is the gate reaching the wrong
  conclusion, and the numbers in the line are what makes it diagnosable.

What to capture when reporting anything: the whole metrics line, the
`parallelism` block from `server-settings.json`, and roughly how the
population was distributed — one crowd, several cities, or spread out. The
third of those is what decides which of the measurements above applies.

## One deliberate behavioural difference

The offloaded validator compares world/cell form ids numerically rather than
building a `FormDesc`. The two are equivalent for well-formed input, but they
differ for a client that sends a form id belonging to a plugin the server has
not loaded: the inline path throws out of the packet handler, while the
offloaded path simply rejects the update and sends a teleport correction. The
new behaviour is the safer one.

Movement relays are also batched to the end of the tick rather than emitted
mid-ingest, so within a single tick a movement relay may now reach a client
after a non-movement message that was processed later. Movement is unreliable
and superseded by the next update, so this is not observable in play, but it is
a real ordering change and worth knowing when debugging packet captures.

It does matter for tests: with the offload on, nothing goes out until
`PartOne::Tick` runs the join, so a test that asserts on `Messages()`
immediately after feeding a packet will see an empty list. The parallel
movement tests tick before asserting for exactly this reason.

## Where the code lives

```
skymp5-server/cpp/server_guest_lib/parallel/
  ParallelConfig.{h,cpp}     settings parsing, clamping, defaults
  ThreadPool.{h,cpp}         fork/join pool; the caller is a worker too
  AreaKey.h                  chunk identity, matching GetGridPos exactly;
                             also the minimum safe cluster separation
  AreaCluster.h              one independently processable group
  AreaPartitioner.{h,cpp}    union-find over occupied chunks
  TickSnapshot.h             the plain-data view workers are allowed to read
  RelayPlan.h                what a worker produces
  InterestManager.{h,cpp}    the pure logic: validate, cull, throttle.
                             ProcessRange is the whole parallel phase
  LoadBalancer.{h,cpp}       cost tracking, longest-first scheduling
  ParallelMetrics.h          counters
  OffloadDispatcher.{h,cpp}  sharding, orchestration, deterministic join

skymp5-server/cpp/server_guest_lib/PartOneOffloadSink.{h,cpp}
  the main-thread half of the join
```

Integration points are deliberately small: `ActionListener::OnUpdateMovement`
chooses a path, `PartOne::Tick` runs the join, and `ScampServer` reads the
config.

## Tests

```bash
./unit/unit "[ParallelPool]"
```

```bash
./unit/unit "[ParallelPartition]"
```

```bash
./unit/unit "[ParallelOffload]"
```

```bash
./unit/unit "[ParallelConfig],[ParallelBalancer]"
```

```bash
./unit/unit "[ParallelWorld]"    # cities, scatter, parties crossing chunks
./unit/unit "[ParallelTrial]"    # the gate's caller contract
./unit/unit "[ParallelSurface]"  # the API the live server compiles against
```

End-to-end parity against the real server lives in
`unit/PartOne_MovementParallelTest.cpp`. It runs the same scenarios as
`PartOne_MovementTest.cpp` with the offload enabled, driving the real
`PartOne`, packet parser and send target, and asserts the same messages reach
the same users. It also pins the sharding behaviour: twelve players standing
together form one cluster, are split into several work units, and still
produce all 144 relays with none throttled.

```bash
./unit/unit "[PartOne]"
```

The partitioner suite asserts the safety property directly: for every pair of
actors placed in different clusters, they must be further apart than the
separation distance.

`[ParallelWorld]` is the one that covers a live population rather than a
crowd: a few hundred players across four cities, open country and interiors,
parties walking at just under a chunk per tick, and players sitting exactly on
a chunk seam. Every case compares the relays the offloaded path produces
against a second implementation of the server's own rule — same grid,
Chebyshev chunk distance at most one — and requires them identical, at one,
three and eight workers, with relays emitted from the workers and from the
join.

`[ParallelTrial]` covers the contract a caller has to keep for the gate to
work at all, including the two ways of breaking it that fail silently: a
caller that never reports ingest time makes the trial decline for ever, and a
caller that never asks before submitting is never trialled.

`[ParallelSurface]` exists because `PartOne.cpp`, `ActionListener.cpp` and
`PartOneOffloadSink.cpp` cannot be compiled without the vcpkg tree. It makes
the same calls those three make, so a field or signature they depend on cannot
quietly disappear on a host that can only build the parallel suite.

Two suites are worth knowing about specifically:

- `[ParallelPool]` includes a regression test for a barrier bug where `Run`
  could return while the worker that ran the final task was still spinning on
  the task cursor. The next tick reset that cursor, so the straggler could
  re-run a task from the previous batch and decrement a counter it did not
  belong to. The observed failure mode is worse than a duplicated packet:
  `tasksRemaining` underflows and the barrier never releases, hanging the
  server tick. The pool now also waits for every drainer to leave, not just
  for the task count to reach zero.

  `Alternating batch sizes stay consistent` is the test that actually catches
  it — a long batch followed by a one-task batch is what leaves a worker
  draining across the boundary. Verified by reverting the fix and inserting a
  300us delay at the end of the drain loop: that build hangs on this test in
  3 of 3 runs, while the fixed build passes in 3 of 3. The natural window is
  only a few instructions wide, so without the delay neither build fails —
  this test is a guard against regression under load, not a reliable detector
  on an idle machine.
- `[ParallelOffload]` asserts that sharding is behaviour-neutral: the same
  population processed as one unit and as sixteen produces byte-identical
  relay sequences, in the same order.

---

## Building

Prerequisites and build instructions are unchanged from upstream — see
[CONTRIBUTING.md](CONTRIBUTING.md).

```bash
cmake --build .                  # from inside your build directory
ctest -C Release                 # 12/12
./unit/unit "[ParallelBench]"    # the measurements above, on your hardware
./unit/unit "[ParallelOffload]"  # dispatcher, parity, sharding, interest mgmt
```

## Further reading

| | |
| --- | --- |
| [MOP.md](MOP.md) | The pitch: what it does, the numbers, and what it does not claim |
| [docs/docs_parallel_area_offload.md](docs/docs_parallel_area_offload.md) | Design notes and full config reference |
| [PULL_REQUEST.md](PULL_REQUEST.md) | Proposal writeup for upstream |

## About the parent project

SkyMP is an open-source multiplayer mod for Skyrim, built on top of
[SkyrimPlatform](docs/docs_skyrim_platform.md) — a tool for writing Skyrim mods
in TypeScript and Chromium.

[![Discord Chat](https://img.shields.io/discord/699653182946803722?label=Discord&logo=Discord)](https://discord.gg/k39uQ9Yudt)
[![Players](https://skymp-badges.vercel.app/badges/players_online.svg)](https://discord.gg/k39uQ9Yudt)
[![Servers](https://skymp-badges.vercel.app/badges/servers_online.svg)](https://discord.gg/k39uQ9Yudt)

All credit for SkyMP itself belongs to the
[upstream project and its contributors](https://github.com/skyrim-multiplayer/skymp/graphs/contributors).

### Terms of Use

See [TERMS.md](TERMS.md). **TL;DR: disclose the source code of your forks.**

Third-party code licenses are in [THIRD_PARTY_LICENSES](THIRD_PARTY_LICENSES).
Contributing upstream requires accepting their Contributor Assignment
Agreement — see [CLA.md](CLA.md).
