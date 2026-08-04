# Multi-Core Area Offload

Skyrim itself is effectively single-threaded, and so was the SkyMP server: one
Node thread drives `ScampServer::Tick`, which pumps every packet and runs every
handler in sequence. On a modern host that leaves most of the CPU idle while
one core saturates, and the symptom players notice is that crowded places —
a city square, a market, a raid — get choppy for everybody in them, even though
the machine is mostly asleep.

This framework spreads that per-area work across the cores the server host
actually has. It is **off by default**; a server that does not opt in behaves
exactly as it did before.

## What actually gets parallelised

Movement is the dominant packet by volume, and its cost is not linear in player
count. Every update from one player is relayed to every other player who can
see them, so a crowd of *N* players in one area produces on the order of *N²*
relay decisions per tick. That quadratic term is what makes dense areas hurt.

Per tick, the server now:

1. **Ingests** movement packets on the main thread as before, but instead of
   relaying immediately it flattens each update — the actor's transform, the
   animation flags, the raw packet bytes, and the list of players who can
   currently see them — into a plain-data snapshot.
2. **Partitions** the actors into *area clusters* that provably cannot
   influence one another this tick, then splits each cluster into **work
   units** small enough to spread across the pool.
3. **Processes each unit on a worker thread**: validating the movement,
   deciding which recipients should receive the update this tick, and building
   the outbound send list.
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
Sharding the same modelled populations lifts the ceiling to near-linear:

| players | one task per cluster | sharded, 4 cores | sharded, 8 cores | sharded, 16 cores |
| --- | --- | --- | --- | --- |
| 80 | 1.45× | 3.87× | 7.64× | 7.86× |
| 160 | 1.47× | 3.93× | 7.84× | 15.21× |
| 300 | 1.43× | 4.00× | 7.96× | 14.71× |

(Modelled on relay-edge counts, not measured on live hardware — treat the
shape as the claim, not the third decimal. Real gains are bounded by memory
bandwidth and by the serial ingest and join phases.)

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
    "minActorsToOffload": 24,
    "minClusterActors": 4,
    "minShardActors": 4,
    "maxShardsPerCluster": 0,
    "clusterSeparationChunks": 4,
    "adaptiveThrottling": true,
    "targetTickBudgetMicros": 8000,
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
| `workerThreads` | `0` | `0` auto-detects: cores minus two, reserved for the Node main thread and the async save thread. Capped at 32. |
| `minActorsToOffload` | `24` | Below this many pending updates the fork/join barrier costs more than it saves, so the tick runs inline. |
| `minClusterActors` | `4` | Clusters smaller than this are swept up on the main thread instead of getting their own task. |
| `minShardActors` | `4` | Fewest players a shard of a crowded cluster may carry. Lower splits a crowd more finely; too low and per-task overhead starts to show. |
| `maxShardsPerCluster` | `0` | `0` auto-sizes to twice the slot count. Raise only if profiling shows one area still bottlenecking. |
| `clusterSeparationChunks` | `4` | Chunk distance separating clusters. Clamped up to 3. Raise it if you want more margin, at the cost of merging nearby crowds. |
| `maxWorkUnitsPerTick` | `0` | `0` is unlimited. Units past the limit run on the calling thread. |
| `repartitionIntervalTicks` | `30` | Reserved for incremental repartitioning. |
| `adaptiveThrottling` | `true` | Enables the degradation described above. Only ever activates under measured overload. |
| `targetTickBudgetMicros` | `8000` | Wall-clock target for the parallel phase. Overshooting raises pressure. |
| `throttleDistanceUnits` | `4096` | Relays closer than this are never throttled. One exterior cell. |
| `maxThrottleSkipTicks` | `3` | Hard ceiling on how far apart a throttled relay may be spaced. |
| `metricsLogIntervalTicks` | `0` | `0` disables. Otherwise logs a summary line every N ticks. |

### Suggested starting point

On a 8-core host running a busy server:

```json5
"parallelism": {
  "enabled": true,
  "workerThreads": 0,
  "minActorsToOffload": 24,
  "adaptiveThrottling": true,
  "metricsLogIntervalTicks": 5000
}
```

Watch the log line, then tune:

```
MpParallel: tick=5000 actors=214 clusters=37 biggest=151 units=58 (pooled 54)
            relays=9871 throttled=0 parallel=2104us join=610us speedup=6.31x
```

- `speedup` is summed task time over wall-clock parallel time. Near 1 means the
  offload is buying nothing — usually because `minActorsToOffload` is never
  reached.
- `biggest` against `actors` tells you how concentrated your population is. When
  `biggest` is most of `actors`, sharding is doing the work, and `units` should
  be comfortably larger than `clusters`. If it is not, lower `minShardActors`.
- `join` growing toward `parallel` means the serial tail is becoming the limit;
  more cores will not help past that point.

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
