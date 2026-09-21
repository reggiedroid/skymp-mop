// A benchmark for the area offload that does not need the vcpkg build.
//
// This is not a replacement for unit/ParallelBenchmark.cpp. That one is the
// authority: it drives the real PartOne, the real packet parser and the real
// send target, and it times full ingest plus Tick, which is what an operator
// actually pays. This one drives OffloadDispatcher directly through
// IOffloadSink and measures only ExecuteTick.
//
// It exists because the authoritative benchmark cannot be built without
// slikenet, mongo-cxx-driver and the rest of the vcpkg tree, and on any host
// where that build is unavailable the answer to "what does this setting cost"
// has been a cost model rather than a measurement. This file needs a C++17
// compiler, parallel/*.cpp, FormDesc.cpp and four header-only libraries.
//
// What that scope means when reading the output:
//
//   * Absolute numbers are NOT comparable to the was/now table in
//     ParallelConfig.h. There is no ingest and no deserialization here, and
//     both paths pay those, so every ratio below is further from 1.0 than the
//     same ratio measured end to end.
//   * Ratios between configurations are the point, and those are the
//     quantities the sweeps below are for.
//   * One machine is one machine. Nothing here may become a shipped default
//     on its own; see misc/perf_model.py for the rule this follows.

#include "parallel/CoreCount.h"
#include "parallel/OffloadDispatcher.h"
#include "parallel/ParallelConfig.h"
#include "parallel/ParallelMetrics.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

using namespace MpParallel;

namespace {

// Counts what the join hands over and nothing else. The real sink memcpys
// into a socket buffer; charging that here would measure the network stack,
// which both the inline and the offloaded path pay identically.
// SendRelayBatch is called from the worker that owns the shard, so the two
// counters it touches are atomic. They are folded once per batch rather than
// once per relay: a relaxed fetch_add per work unit is lost in the noise,
// while one per relay would put every worker on the same cache line and
// charge the offloaded path for contention the real sink does not have.
// ApplyMovement and SendCorrection are still join-thread only, but are
// atomic too so that no counter here depends on which thread ran it.
class CountingSink : public IOffloadSink
{
public:
  void ApplyMovement(const ActorSnapshot&) override
  {
    applied.fetch_add(1, std::memory_order_relaxed);
  }
  void SendCorrection(const ActorSnapshot&) override
  {
    corrected.fetch_add(1, std::memory_order_relaxed);
  }
  void SendRelayBatch(const OutboundSend* sends, size_t count, const uint8_t*,
                      size_t) override
  {
    uint64_t batchBytes = 0;
    for (size_t i = 0; i < count; ++i) {
      batchBytes += sends[i].byteLength;
    }
    relays.fetch_add(count, std::memory_order_relaxed);
    bytes.fetch_add(batchBytes, std::memory_order_relaxed);
  }
  std::atomic<uint64_t> applied{ 0 };
  std::atomic<uint64_t> corrected{ 0 };
  std::atomic<uint64_t> relays{ 0 };
  std::atomic<uint64_t> bytes{ 0 };
};

struct Result
{
  double medianMicros = 0.0;
  double p90Micros = 0.0;
  double parallelMicros = 0.0;
  double joinMicros = 0.0;
  double aggregateMicros = 0.0;
  double speedup = 0.0;
  size_t workUnits = 0;
  uint64_t relays = 0;
};

// Every player in one chunk, every player moving every tick: the N^2 relay
// case, which is the shape the framework exists for.
Result Run(size_t players, size_t workers, size_t maxShards,
           uint32_t minShardMicros, int ticks, int warmup,
           bool forceInline = false, uint32_t spinMicros = UINT32_MAX)
{
  ParallelConfig config;
  config.enabled = true;
  config.workerThreads = workers;

  // The inline baseline is taken by putting the threshold out of reach, not
  // by asking for zero workers: `workerThreads == 0` means *auto*, which
  // Normalize turns into physical cores minus one. Confusing the two makes
  // the auto configuration look like the thing it should be measured against.
  config.minActorsToOffload = forceInline ? 1000000 : 1;

  config.minClusterActors = 1;
  // Both rate-reduction mechanisms off, so this measures the raw relay
  // fan-out rather than how much of it was skipped.
  config.adaptiveThrottling = false;
  config.interestManagement = false;
  // A control loop keyed on wall clock would make every number below a
  // function of the numbers before it.
  config.adaptiveParallelism = false;

  if (maxShards > 0) {
    config.maxShardsPerCluster = maxShards;
  }
  if (minShardMicros > 0) {
    config.minShardMicros = minShardMicros;
  }
  if (spinMicros != UINT32_MAX) {
    config.workerSpinMicros = spinMicros;
  }
  config.Normalize();

  OffloadDispatcher dispatcher(config);

  // Sized like the binary UpdateMovementMessage the real benchmark sends.
  const std::vector<uint8_t> packet(64, 0xab);

  std::vector<double> samples;
  samples.reserve(static_cast<size_t>(ticks));
  CountingSink sink;
  Result out;

  for (int tick = 0; tick < warmup + ticks; ++tick) {
    std::vector<RelayTarget> targets;
    targets.reserve(players);
    for (size_t i = 0; i < players; ++i) {
      // All inside one 4096-unit chunk.
      const float x = static_cast<float>(i % 64) * 60.f;
      const float y = static_cast<float>(i / 64) * 60.f;

      RelayTarget target;
      target.userId = static_cast<Networking::UserId>(i);
      target.listenerFormId = 0xff000001 + static_cast<uint32_t>(i);
      target.worldOrCell = 0x3c;
      target.chunkX = static_cast<int16_t>(x / 4096.f);
      target.chunkY = static_cast<int16_t>(y / 4096.f);
      target.pos[0] = x;
      target.pos[1] = y;
      targets.push_back(target);

      MovementSubmission submission;
      submission.formId = 0xff000001 + static_cast<uint32_t>(i);
      submission.idx = static_cast<uint32_t>(i);
      submission.ownerUserId = static_cast<Networking::UserId>(i);
      submission.currentPos[0] = x;
      submission.currentPos[1] = y;
      submission.currentWorldOrCell = 0x3c;
      // A short step, so validation accepts it and the actor stays put.
      submission.proposedPos[0] = x + 1.f;
      submission.proposedPos[1] = y + 1.f;
      submission.proposedWorldOrCell = 0x3c;
      submission.isStanding = true;
      submission.packetData = packet.data();
      submission.packetLength = packet.size();
      dispatcher.SubmitMovement(submission);
    }
    dispatcher.SetPotentialTargets(std::move(targets));

    const auto begin = std::chrono::steady_clock::now();
    dispatcher.ExecuteTick(sink);
    const auto end = std::chrono::steady_clock::now();

    // The first ticks pay for a cold microsPerActorEma, which decides the
    // shard budget, so the shape of the tick is still settling.
    if (tick >= warmup) {
      samples.push_back(
        std::chrono::duration<double, std::micro>(end - begin).count());
      const ParallelMetrics& metrics = dispatcher.GetMetrics();
      out.parallelMicros += static_cast<double>(metrics.lastParallelMicros);
      out.joinMicros += static_cast<double>(metrics.lastJoinMicros);
      out.aggregateMicros +=
        static_cast<double>(metrics.lastAggregateTaskMicros);
      out.workUnits = metrics.lastWorkUnitCount;
      out.relays = metrics.lastRelayEdgesEmitted;
    }
  }

  std::sort(samples.begin(), samples.end());
  out.medianMicros = samples[samples.size() / 2];
  out.p90Micros = samples[static_cast<size_t>(samples.size() * 0.9)];
  const double count = static_cast<double>(samples.size());
  out.parallelMicros /= count;
  out.joinMicros /= count;
  out.aggregateMicros /= count;
  out.speedup =
    out.parallelMicros > 0.0 ? out.aggregateMicros / out.parallelMicros : 0.0;
  return out;
}

void Header(const char* title)
{
  printf("\n%s\n", title);
  for (const char* p = title; *p; ++p) {
    putchar('-');
  }
  putchar('\n');
}

}

int main(int argc, char** argv)
{
  const int ticks = argc > 1 ? atoi(argv[1]) : 400;
  const int warmup = 40;

  printf("machine: hardware_concurrency=%u usable=%zu physical=%zu\n",
         std::thread::hardware_concurrency(), GetUsableCpuCount(),
         GetPhysicalCoreCount());
  printf("ticks per point: %d (plus %d warmup), median of ExecuteTick\n",
         ticks, warmup);
  printf("scenario: every player in one chunk, all moving, interest "
         "management off\n");
  printf("NOTE: ExecuteTick only. No ingest, no PartOne. See the file "
         "header.\n");

  for (size_t players : { size_t(100), size_t(200), size_t(400) }) {
    char title[128];
    snprintf(title, sizeof(title),
             "Worker-count sweep at %zu players (N^2 relay)", players);
    Header(title);
    printf("%8s %11s %10s %10s %9s %10s %8s %7s\n", "workers", "median us",
           "p90 us", "parallel", "join", "aggregate", "speedup", "units");

    const Result inlineRun =
      Run(players, 0, 0, 0, ticks, warmup, /*forceInline=*/true);
    printf("%8s %11.1f %10.1f %10.1f %9.1f %10.1f %7.2fx %7zu   (baseline)\n",
           "inline", inlineRun.medianMicros, inlineRun.p90Micros,
           inlineRun.parallelMicros, inlineRun.joinMicros,
           inlineRun.aggregateMicros, inlineRun.speedup, inlineRun.workUnits);

    for (size_t workers : { size_t(0), size_t(1), size_t(2), size_t(3),
                            size_t(4), size_t(6), size_t(7), size_t(8),
                            size_t(10), size_t(12), size_t(16), size_t(24),
                            size_t(32) }) {
      const Result r = Run(players, workers, 0, 0, ticks, warmup);
      printf("%8zu %11.1f %10.1f %10.1f %9.1f %10.1f %7.2fx %7zu", workers,
             r.medianMicros, r.p90Micros, r.parallelMicros, r.joinMicros,
             r.aggregateMicros, r.speedup, r.workUnits);
      printf("   %5.2fx vs inline%s\n", inlineRun.medianMicros / r.medianMicros,
             workers == 0 ? "   <- auto" : "");
    }
  }

  // The sweep above cannot separate pool size from unit count, because the
  // `slots * 2` ceiling grows the second with the first. This does.
  Header("Pool size vs unit count at 400 players, us/tick (units pinned)");
  const size_t pinnedUnits[] = { 4, 8, 16, 32 };
  printf("%10s", "pool size");
  for (size_t units : pinnedUnits) {
    printf("%9zu units", units);
  }
  putchar('\n');
  for (size_t pool : { size_t(4), size_t(8), size_t(12), size_t(16),
                       size_t(24), size_t(32) }) {
    printf("%10zu", pool);
    for (size_t units : pinnedUnits) {
      const Result r = Run(400, pool, units, 0, ticks, warmup);
      printf("%9.1f (%2zu)", r.medianMicros, r.workUnits);
    }
    putchar('\n');
  }

  // Whether an oversubscribed pool costs what it costs because surplus
  // workers spin before parking. If so, shortening the spin takes it back.
  Header("Spin budget vs pool size at 400 players, 32 units pinned");
  printf("%10s %12s %12s %12s %12s\n", "pool size", "250us (def)", "50us",
         "10us", "0us (park)");
  for (size_t pool : { size_t(8), size_t(16), size_t(24), size_t(32) }) {
    printf("%10zu", pool);
    for (uint32_t spin : { 250u, 50u, 10u, 0u }) {
      const Result r = Run(400, pool, 32, 0, ticks, warmup, false, spin);
      printf("%12.1f", r.medianMicros);
    }
    putchar('\n');
  }

  Header("minShardMicros at 400 players, 8 workers (20 is the default)");
  printf("%10s %11s %10s %8s %7s\n", "minShardUs", "median us", "p90 us",
         "speedup", "units");
  for (uint32_t micros : { 10u, 20u, 30u, 40u, 60u, 95u, 150u }) {
    const Result r = Run(400, 8, 0, micros, ticks, warmup);
    printf("%10u %11.1f %10.1f %7.2fx %7zu\n", micros, r.medianMicros,
           r.p90Micros, r.speedup, r.workUnits);
  }

  return 0;
}
