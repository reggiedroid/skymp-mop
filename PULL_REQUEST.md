# perf(skymp5-server): parallel area offload + interest management

Two changes to how movement relays are handled at scale, both **opt-in**. With
`parallelism.enabled` absent or false the original code path runs unchanged.

Every performance number below is measured by `unit/ParallelBenchmark.cpp`,
included in this branch. Run it yourself:

```bash
./unit/unit "[ParallelBench]"
```

## The problem

Movement is the highest-volume packet, and its cost is not linear in player
count: each update is relayed to everyone who can see the sender, so *N*
players in one area cost on the order of *N²* relay decisions per tick. All of
it runs on the single Node thread that drives `ScampServer::Tick`.

## What was measured

Every player in one chunk, everyone sending movement every tick, over the
binary wire format a real client uses. Timing covers full packet ingest **plus**
`PartOne::Tick`, so the inline and offloaded paths are compared over the same
unit of work — the inline path relays during ingest, the offloaded path defers
relays to the join.

| players | 25 | 50 | 100 | 150 | 250 | 400 |
| --- | --- | --- | --- | --- | --- | --- |
| offload speedup | 0.25× | 0.43× | 0.66× | 0.81× | 0.88× | **1.10×** |

**Break-even is around 300–400 players.** Below that the offload is a
regression, because the barrier and snapshot costs are paid every tick while
the parallel phase is still small. `minActorsToOffload` therefore defaults to
**300**.

The parallel phase itself scales fine — at 400 players it retires 966µs of task
work in 96µs of wall clock, roughly 10×. The ceiling is elsewhere: the join
emits 160,000 relays serially and costs 787µs of an 1118µs tick, *even with a
send target that does nothing*. Parallelising decisions cannot fix that.

## Interest management

Since relay volume is the quadratic term and emitting the sends is serial
regardless, the higher-value lever is sending less — and it helps at every
population, not only above 300.

Recipients closer than `interestFullRateUnits` (2048, about half a chunk)
always receive every update, so anything a player is realistically fighting,
trading with or watching stays at full fidelity. Beyond that the rate steps
down to a half, a third, a quarter, capped by `maxInterestSkipTicks`. At a 60Hz
tick a distant player still gets roughly 15 updates a second.

400 players spread across one chunk:

| configuration | relays/tick | µs/tick |
| --- | --- | --- |
| inline (baseline) | 160,000 | 1233 |
| offload only | 160,000 | 1048 |
| offload + interest management | 119,866 | **890** |

**1.39× against the inline baseline**, with a send target that does nothing.
With real RakNet sends each avoided relay also skips serialization and
queueing, so the gap widens.

It stacks with `adaptiveThrottling` by taking the stronger factor rather than
multiplying, so an overloaded server never starves a distant player past
whichever cap is more conservative. Each pair's phase is derived from a hash of
the pair, so traffic spreads across the window instead of bursting — and the
suite asserts every pair still transmits exactly once per window: reduced rate,
never silence.

## How the offload works

Per tick:

1. **Ingest** (main thread) — packets parsed as before, each update flattened
   into a plain-data snapshot instead of relayed immediately.
2. **Partition** (main thread) — actors grouped into area clusters that
   provably cannot influence one another this tick, then sliced into work units.
3. **Process** (workers) — validate, spatially filter recipients, build the
   outbound send list.
4. **Join** (main thread) — apply world state, hand packets to the network in a
   fixed order.

Steps 1 and 4 stay serial because they touch `MpActor`, `WorldState`, the
Papyrus VM, RakNet and V8. Workers read only the immutable snapshot and write
only their own output buffer, so the parallel phase needs no locks.

Recipients are derived from an O(N) per-tick snapshot of active players that
workers filter spatially, rather than enumerating O(N²) edges on the main
thread. That distinction matters: an earlier revision materialized every relay
edge during ingest and spent more time building the list than the parallel
phase saved.

### Why clusters are safe

`Grid.h` uses a 3×3 chunk stencil (reach 1) and movement validation caps a
single update just under one chunk (reach 1), so an actor's influence spans at
most 2 chunks per tick. Clusters are connected components under *same
`worldOrCell`, Chebyshev chunk distance ≤ S*, with `S` clamped up to 3 so every
possible recipient lands in the sender's own cluster. Different worldspaces and
interiors never share a grid, so instanced content separates perfectly.

Verified by property test over 400 randomized populations (23,810 actors):
actors in different clusters are always beyond relay range, actors within range
always share a cluster, and the result is independent of packet arrival order.

### Why shards, not clusters

One task per cluster does not work. On a realistic population one hub holds
most of a tick's relay work, so a cluster-sized task hands that block to a
single core. The scheduling unit is therefore a *shard*: a contiguous slice of
one cluster's members. Each sender's work depends only on the snapshot, so any
split is safe.

### Determinism

Clusters are ordered by lowest chunk coordinate, members by submission index,
shards are contiguous slices of that order, and the join walks work units by
index. Same inputs produce the same sequence of world writes on 2 cores or 32,
and whether a cluster ran whole or split sixteen ways.

## Also included

**JSON deserialization fix.** `MessageSerializer::Deserialize` walked the whole
deserializer table for JSON messages, and each candidate allocated a
`std::string` *and constructed a fresh `simdjson::dom::parser`* before
re-parsing the entire document, until one matched. It now reads the message type
once and dispatches directly, reuses a `thread_local` parser, and only
materializes the message text when trace logging is enabled. Measured **3.4–6.3×**
cheaper on the JSON path. This addresses the `TODO(#2257)` that lived there.

Production clients send binary — a live client produced no JSON packets at all —
so this only helps legacy clients, but it is a hot path either way.

## Configuration

```json5
{
  // ...
  "parallelism": {
    "enabled": true,
    "workerThreads": 0,          // 0 auto-detects: cores minus two
    "interestManagement": true,  // the setting that matters most
    "metricsLogIntervalTicks": 5000
  }
}
```

Full option list and tuning guidance in
[`docs/docs_parallel_area_offload.md`](docs/docs_parallel_area_offload.md).

## Behavioural differences when enabled

- **Movement relays batch to the end of the tick** rather than being emitted
  mid-ingest. Movement is unreliable and superseded by the next update, so this
  is not observable in play, but it is visible in packet captures, and tests
  must tick before asserting on `Messages()`.
- **A world/cell form id from an unloaded plugin** is rejected with a teleport
  correction instead of throwing out of the packet handler.
- **Distant players receive fewer updates** when interest management is on.
  That is the point; set `interestManagement: false` to disable.

## Testing

```
ctest: 100% tests passed, 0 failed out of 12
unit:  All tests passed (480,346 assertions in 310 test cases)
```

| Tag | Covers |
| --- | --- |
| `[ParallelPool]` | fork/join barrier, exception safety, back-to-back batches |
| `[ParallelPartition]` | the safety property, connectivity, order-independence |
| `[ParallelOffload]` | dispatcher, validation parity, sharding, interest management |
| `[ParallelConfig]` / `[ParallelBalancer]` | settings clamping, cost model |
| `[.][ParallelBench]` | the measurements above; excluded from ctest |

`unit/PartOne_MovementParallelTest.cpp` drives the real `PartOne`, packet parser
and send target with the offload enabled, asserting the same messages reach the
same users as the inline path.

### A race worth calling out

`ThreadPool::Run` originally returned as soon as `tasksRemaining` hit zero. The
worker that ran the final task decrements that counter and only *then* loops
back to the task cursor — so if `Run` returned in that window and the next tick
started a batch, the cursor reset would hand the still-draining worker index 0
of the *previous* task vector. The failure mode is not a duplicated packet: the
straggler decrements a counter it was never part of, underflowing it, and the
barrier never releases — the server tick hangs.

`Run` now also waits for every drainer to leave. Verified by reverting only the
fix and inserting a 300µs delay where a straggler would sit: that build hangs on
`Alternating batch sizes stay consistent` in 3 of 3 runs, the fixed build passes
3 of 3. The natural window is a few instructions wide, so on an idle machine
that test guards against regression rather than reliably detecting the race.

## Honest limitations

- **Not yet load-tested with real clients.** The benchmark drives `PartOne`
  directly with a no-op send target, so it measures server-side relay cost, not
  the network stack. Real RakNet sends would change the absolute numbers —
  probably in interest management's favour, since it avoids that work entirely.
- **The offload is a regression below ~300 players.** That is why it defaults
  off and why `minActorsToOffload` is 300. On a typical server the useful half
  of this PR is interest management.
- Numbers come from one 32-core host. Re-run the benchmark on target hardware
  before tuning.
