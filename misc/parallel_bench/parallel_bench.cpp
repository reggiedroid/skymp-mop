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

#include "parallel/AreaPartitioner.h"
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

  // What the population looked like to the partitioner. Meaningless for the
  // packed shape, where they are always 1, 1 and everybody; the point of the
  // map shapes is that they are not.
  size_t clusters = 0;
  size_t chunks = 0;
  size_t largestCluster = 0;

  // No timed tick took any work on. The dispatcher declined, which under the
  // A/B-trial gate means the tick's work belongs to ActionListener and never
  // reaches ExecuteTick at all -- so every number above is the cost of doing
  // nothing, and a ratio taken against one is meaningless. This harness has
  // no inline path of its own, so a declined point is not a measurement and
  // must not be printed as one.
  bool declined = false;
  uint64_t acceptedSubmissions = 0;
};

// The shapes a population can have.
//
// Packed is the one the framework exists for and the one every sweep above
// this comment uses: one chunk, everybody visible to everybody, the N^2 relay
// term at full strength. It is also the shape a live server is in least
// often, which is why the other two exist.
enum class Shape
{
  // One chunk, all moving.
  Packed,
  // Cities and country: half the population concentrated in four hubs of
  // uneven size, the rest spread over the map in ones and twos, some indoors.
  // Nobody moves far, so the partition is stable tick to tick.
  Province,
  // The same province, plus parties of eight walking at a little under a
  // chunk per tick on headings of their own, so cluster membership churns and
  // the partition has to be rebuilt in earnest every tick.
  Roaming
};

const char* ShapeName(Shape shape)
{
  switch (shape) {
    case Shape::Packed:
      return "packed";
    case Shape::Province:
      return "province";
    case Shape::Roaming:
      return "roaming";
  }
  return "?";
}

struct BenchPlayer
{
  float pos[3] = { 0.f, 0.f, 0.f };
  float vel[2] = { 0.f, 0.f };
  uint32_t worldOrCell = 0x3c;
};

// Deterministic so that two runs of the benchmark compare like with like.
// xorshift64*.
class Rng
{
public:
  explicit Rng(uint64_t seed)
    : state(seed)
  {
  }
  uint64_t Next()
  {
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    return state * 0x2545f4914f6cdd1dULL;
  }
  float Float(float lo, float hi)
  {
    const double unit =
      static_cast<double>(Next() >> 11) / static_cast<double>(1ULL << 53);
    return static_cast<float>(lo + unit * (hi - lo));
  }

private:
  uint64_t state;
};

struct BenchHub
{
  uint32_t worldOrCell;
  float x;
  float y;
  float radius;
  int weight; // out of 100, among hub dwellers
};

// One busy capital and three smaller towns, tens of chunks apart. Uneven on
// purpose: equal hubs would hide whether the biggest is being sharded.
const BenchHub kBenchHubs[] = {
  { 0x3c, 10000.f, 10000.f, 1800.f, 45 },
  { 0x3c, -70000.f, 35000.f, 3000.f, 25 },
  { 0x3c, 50000.f, -60000.f, 2400.f, 20 },
  { 0x1f4, -20000.f, -20000.f, 2000.f, 10 }
};

std::vector<BenchPlayer> MakePopulation(Shape shape, size_t players)
{
  std::vector<BenchPlayer> population;
  population.reserve(players);

  if (shape == Shape::Packed) {
    for (size_t i = 0; i < players; ++i) {
      BenchPlayer player;
      player.pos[0] = static_cast<float>(i % 64) * 60.f;
      player.pos[1] = static_cast<float>(i / 64) * 60.f;
      population.push_back(player);
    }
    return population;
  }

  Rng rng(0x5eed0f0fULL);
  const size_t hubCount = sizeof(kBenchHubs) / sizeof(kBenchHubs[0]);

  // In Roaming, an eighth of the population is out on the roads in parties of
  // eight; the rest is the same province.
  const size_t partyPlayers =
    shape == Shape::Roaming ? (players / 8) / 8 * 8 : 0;

  for (size_t i = 0; i < players - partyPlayers; ++i) {
    BenchPlayer player;
    const uint64_t roll = rng.Next() % 100;
    if (roll < 50) {
      // Pick a hub by weight.
      int pick = static_cast<int>(rng.Next() % 100);
      size_t hub = 0;
      for (size_t h = 0; h < hubCount; ++h) {
        pick -= kBenchHubs[h].weight;
        if (pick < 0) {
          hub = h;
          break;
        }
      }
      player.worldOrCell = kBenchHubs[hub].worldOrCell;
      player.pos[0] =
        kBenchHubs[hub].x + rng.Float(-kBenchHubs[hub].radius,
                                      kBenchHubs[hub].radius);
      player.pos[1] =
        kBenchHubs[hub].y + rng.Float(-kBenchHubs[hub].radius,
                                      kBenchHubs[hub].radius);
    } else if (roll < 60) {
      // Indoors: its own grid, two or three players each.
      player.worldOrCell = 0x10000 + static_cast<uint32_t>(i % 12);
      player.pos[0] = rng.Float(-2000.f, 2000.f);
      player.pos[1] = rng.Float(-2000.f, 2000.f);
    } else {
      player.worldOrCell = 0x3c;
      player.pos[0] = rng.Float(-90000.f, 90000.f);
      player.pos[1] = rng.Float(-90000.f, 90000.f);
    }
    player.pos[2] = rng.Float(-500.f, 500.f);
    // Everyone twitches a little, which is what a standing player's client
    // still sends. Far too small to change chunk.
    player.vel[0] = rng.Float(-6.f, 6.f);
    player.vel[1] = rng.Float(-6.f, 6.f);
    population.push_back(player);
  }

  for (size_t party = 0; party < partyPlayers / 8; ++party) {
    const float originX = rng.Float(-60000.f, 60000.f);
    const float originY = rng.Float(-60000.f, 60000.f);
    const float velX = rng.Float(-3000.f, 3000.f);
    const float velY = rng.Float(-3000.f, 3000.f);
    for (size_t m = 0; m < 8; ++m) {
      BenchPlayer player;
      player.worldOrCell = 0x3c;
      player.pos[0] = originX + rng.Float(-300.f, 300.f);
      player.pos[1] = originY + rng.Float(-300.f, 300.f);
      player.vel[0] = velX;
      player.vel[1] = velY;
      population.push_back(player);
    }
  }

  return population;
}

// Every player in one chunk, every player moving every tick: the N^2 relay
// case, which is the shape the framework exists for. `shape` widens that to
// the populations a live server actually has; see Shape above.
Result Run(size_t players, size_t workers, size_t maxShards,
           uint32_t minShardMicros, int ticks, int warmup,
           bool forceInline = false, uint32_t spinMicros = UINT32_MAX,
           bool forceAccept = false, bool relayFromWorkers = false,
           Shape shape = Shape::Packed)
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
  // Always off. A control loop keyed on wall clock would make
  // every number below a function of the numbers before it, and this harness
  // cannot judge the trial anyway -- see the note on the last section, which
  // explains why a caller that does not report ingest time makes the trial
  // decline unconditionally.
  config.adaptiveParallelism = false;

  // Defaults to the shipped value, false, so every sweep above measures what
  // an operator actually gets. The last section compares the two.
  config.relayFromWorkers = relayFromWorkers;

  // Both decline grounds switched off, which is what the documented
  // `minOffloadWorkMicros: 0` is for. Used to price the offloaded path on a
  // population the gate would normally refuse, so that what the gate decided
  // can be checked against what the two paths actually cost.
  if (forceAccept) {
    config.minOffloadWorkMicros = 0;
    config.minOffloadSpeedup = 0.f;
  }

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

  std::vector<BenchPlayer> population = MakePopulation(shape, players);

  for (int tick = 0; tick < warmup + ticks; ++tick) {
    std::vector<RelayTarget> targets;
    targets.reserve(players);
    for (size_t i = 0; i < players; ++i) {
      const BenchPlayer& player = population[i];

      RelayTarget target;
      target.userId = static_cast<Networking::UserId>(i);
      target.listenerFormId = 0xff000001 + static_cast<uint32_t>(i);
      target.worldOrCell = player.worldOrCell;
      target.chunkX = ToChunkCoord(player.pos[0]);
      target.chunkY = ToChunkCoord(player.pos[1]);
      target.pos[0] = player.pos[0];
      target.pos[1] = player.pos[1];
      target.pos[2] = player.pos[2];
      targets.push_back(target);

      MovementSubmission submission;
      submission.formId = 0xff000001 + static_cast<uint32_t>(i);
      submission.idx = static_cast<uint32_t>(i);
      submission.ownerUserId = static_cast<Networking::UserId>(i);
      submission.currentPos[0] = player.pos[0];
      submission.currentPos[1] = player.pos[1];
      submission.currentPos[2] = player.pos[2];
      submission.currentWorldOrCell = player.worldOrCell;
      // A step the validator accepts: under a chunk, same grid.
      submission.proposedPos[0] = player.pos[0] +
        (shape == Shape::Packed ? 1.f : player.vel[0]);
      submission.proposedPos[1] = player.pos[1] +
        (shape == Shape::Packed ? 1.f : player.vel[1]);
      submission.proposedPos[2] = player.pos[2];
      submission.proposedWorldOrCell = player.worldOrCell;
      submission.isStanding = true;
      submission.packetData = packet.data();
      submission.packetLength = packet.size();
      // The return value is the gate's answer. False means the caller is
      // expected to handle this update itself, so counting these is the only
      // way to tell a fast tick from a tick that never happened.
      const bool takenOn = dispatcher.SubmitMovement(submission);
      if (takenOn && tick >= warmup) {
        ++out.acceptedSubmissions;
      }
    }
    dispatcher.SetPotentialTargets(std::move(targets));

    // The travellers actually travel. Everyone else twitches in place, which
    // is enough to keep every player submitting without moving the partition.
    if (shape != Shape::Packed) {
      for (BenchPlayer& player : population) {
        player.pos[0] += player.vel[0];
        player.pos[1] += player.vel[1];
        // Turn round at the edge of the map rather than walking off it.
        if (player.pos[0] > 95000.f || player.pos[0] < -95000.f) {
          player.vel[0] = -player.vel[0];
        }
        if (player.pos[1] > 95000.f || player.pos[1] < -95000.f) {
          player.vel[1] = -player.vel[1];
        }
      }
    }

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
      out.clusters = metrics.lastClusterCount;
      out.chunks = metrics.lastChunkCount;
      out.largestCluster = metrics.lastLargestClusterSize;
    }
  }

  out.declined = out.acceptedSubmissions == 0;

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

// One median cell. A declined point prints as such rather than as 0.0, which
// is what it would otherwise read as: ExecuteTick returning immediately.
void MedianCell(const Result& r, int width)
{
  if (r.declined) {
    printf("%*s", width, "declined");
  } else {
    printf("%*.1f", width, r.medianMicros);
  }
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

    // Raising minActorsToOffload out of reach used to produce an inline run,
    // because the dispatcher did the work on the calling thread. Under the
    // A/B-trial gate it produces a *declined* run instead, and the work moves
    // to ActionListener, which this harness does not have. When that happens
    // there is no baseline here and no ratio can be formed against one.
    const Result inlineRun =
      Run(players, 0, 0, 0, ticks, warmup, /*forceInline=*/true);
    if (inlineRun.declined) {
      printf("%8s %11s %10s %10s %9s %10s %8s %7s   (declined: no inline "
             "baseline on this build)\n",
             "inline", "-", "-", "-", "-", "-", "-", "-");
    } else {
      printf("%8s %11.1f %10.1f %10.1f %9.1f %10.1f %7.2fx %7zu   (baseline)\n",
             "inline", inlineRun.medianMicros, inlineRun.p90Micros,
             inlineRun.parallelMicros, inlineRun.joinMicros,
             inlineRun.aggregateMicros, inlineRun.speedup, inlineRun.workUnits);
    }

    for (size_t workers : { size_t(0), size_t(1), size_t(2), size_t(3),
                            size_t(4), size_t(6), size_t(7), size_t(8),
                            size_t(10), size_t(12), size_t(16), size_t(24),
                            size_t(32) }) {
      const Result r = Run(players, workers, 0, 0, ticks, warmup);
      const char* autoMark = workers == 0 ? "   <- auto" : "";
      if (r.declined) {
        printf("%8zu %11s %10s %10s %9s %10s %8s %7s   %s%s\n", workers, "-",
               "-", "-", "-", "-", "-", "-", "gate declined every tick",
               autoMark);
        continue;
      }
      printf("%8zu %11.1f %10.1f %10.1f %9.1f %10.1f %7.2fx %7zu", workers,
             r.medianMicros, r.p90Micros, r.parallelMicros, r.joinMicros,
             r.aggregateMicros, r.speedup, r.workUnits);
      if (inlineRun.declined) {
        printf("   %s%s\n", "(no baseline)", autoMark);
      } else {
        printf("   %5.2fx vs inline%s\n",
               inlineRun.medianMicros / r.medianMicros, autoMark);
      }
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
      MedianCell(r, 9);
      printf(" (%2zu)", r.workUnits);
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
      MedianCell(r, 12);
    }
    putchar('\n');
  }

  Header("minShardMicros at 400 players, 8 workers (20 is the default)");
  printf("%10s %11s %10s %8s %7s\n", "minShardUs", "median us", "p90 us",
         "speedup", "units");
  for (uint32_t micros : { 10u, 20u, 30u, 40u, 60u, 95u, 150u }) {
    const Result r = Run(400, 8, 0, micros, ticks, warmup);
    if (r.declined) {
      printf("%10u %11s %10s %8s %7s\n", micros, "declined", "-", "-", "-");
      continue;
    }
    printf("%10u %11.1f %10.1f %7.2fx %7zu\n", micros, r.medianMicros,
           r.p90Micros, r.speedup, r.workUnits);
  }

  // What the two paths cost on the same population, with both decline
  // grounds switched off so there is a number even where the gate would
  // refuse. This is the comparison the gate is trying to make.
  //
  // It deliberately stops short of scoring the gate's own decision, and the
  // reason is a trap worth knowing about. The paired trial prices a tick as
  // `ingestNanosThisTick + lastParallelMicros + lastJoinMicros`. On a
  // declined tick the last two are zero, so the whole cost is whatever the
  // caller reported through AddIngestNanos -- and a caller that does not
  // implement WillAcceptThisTick / IsMeasuringThisTick / AddIngestNanos
  // reports nothing. The declined arm then costs zero per mover, no accepted
  // arm can beat it, and the trial declines forever.
  //
  // This harness is such a caller: it drives the dispatcher directly and has
  // no inline path to charge for. Engagement measured here would therefore be
  // a property of the harness, not of the decision procedure, so it is not
  // reported. ActionListener does implement the contract; unit/ParallelBench-
  // mark.cpp drives ActionListener, and that is where the gate can be judged.
  Header("Both paths priced with the gate forced (see note on the trial)");
  printf("%9s %12s %12s %10s %12s\n", "players", "inline us", "offload us",
         "cheaper", "ratio");
  for (size_t players : { size_t(100), size_t(200), size_t(400) }) {
    // Both paths priced with the decline grounds off. For the inline arm that
    // means the dispatcher accepts the tick and then runs it serially,
    // because minActorsToOffload is out of reach -- which is the serial cost
    // of the same work, and the only inline number this harness can produce.
    const Result inlineRun = Run(players, 0, 0, 0, ticks, warmup,
                                 /*forceInline=*/true, UINT32_MAX,
                                 /*forceAccept=*/true);
    // Priced with both decline grounds off, so there is a number even where
    // the gate would refuse. That refusal is the thing under test; measuring
    // only what the gate already agreed to would assume the answer.
    const Result offload = Run(players, 0, 0, 0, ticks, warmup, false,
                               UINT32_MAX, /*forceAccept=*/true);
    const char* cheaper = "?";
    double ratio = 0.0;
    if (!inlineRun.declined && !offload.declined && offload.medianMicros > 0.0) {
      ratio = inlineRun.medianMicros / offload.medianMicros;

      // A deadband, because this harness overstates the gap: it excludes the
      // ingest and deserialization both paths pay, so a ratio here is always
      // further from 1.0 than the same ratio measured end to end. Calling a
      // 1.1x difference a win either way would be reading precision this
      // benchmark does not have.
      cheaper = ratio >= 1.25 ? "offload" : (ratio <= 0.8 ? "inline" : "near-tie");
    }

    printf("%9zu ", players);
    if (inlineRun.declined) {
      printf("%12s ", "declined");
    } else {
      printf("%12.1f ", inlineRun.medianMicros);
    }
    if (offload.declined) {
      printf("%12s ", "declined");
    } else {
      printf("%12.1f ", offload.medianMicros);
    }
    if (ratio > 0.0) {
      printf("%10s %11.2fx\n", cheaper, ratio);
    } else {
      printf("%10s %12s\n", cheaper, "-");
    }
  }

  // What the safe default costs.
  //
  // relayFromWorkers defaults to false because nothing in this tree
  // establishes that slikenet's RakPeer::Send may be called from several
  // threads at once, and being wrong about that corrupts a live server. The
  // price of that caution belongs in a table rather than in a claim, so that
  // an operator who has confirmed their network stack can see exactly what
  // flipping it back is worth.
  //
  // Both columns do identical work and emit identical relays -- that much is
  // pinned by `Deferring relays to the join changes nothing but the thread`
  // in the unit suite. The only difference is which thread calls the sink.
  Header("Relay dispatch: from workers vs deferred to the join");
  printf("%9s %14s %14s %10s\n", "players", "join (default)", "workers",
         "cost");
  for (size_t players : { size_t(100), size_t(200), size_t(400) }) {
    const Result join = Run(players, 0, 0, 0, ticks, warmup, false, UINT32_MAX,
                            /*forceAccept=*/true, /*relayFromWorkers=*/false);
    const Result workers = Run(players, 0, 0, 0, ticks, warmup, false,
                               UINT32_MAX, /*forceAccept=*/true,
                               /*relayFromWorkers=*/true);

    printf("%9zu ", players);
    if (join.declined) {
      printf("%14s ", "declined");
    } else {
      printf("%14.1f ", join.medianMicros);
    }
    if (workers.declined) {
      printf("%14s ", "declined");
    } else {
      printf("%14.1f ", workers.medianMicros);
    }
    if (!join.declined && !workers.declined && workers.medianMicros > 0.0) {
      printf("%9.2fx\n", join.medianMicros / workers.medianMicros);
    } else {
      printf("%10s\n", "-");
    }
  }

  // The population a live test actually has: several cities at once, the rest
  // of the map thinly occupied, and -- in the roaming shape -- parties walking
  // between the two fast enough to change chunk every tick.
  //
  // Both arms are priced with the decline grounds forced off, for the reason
  // the previous section gives: what the gate would decide is the question,
  // so measuring only the populations it already agreed to would assume the
  // answer.
  Header("A live-test population, both paths priced with the gate forced");
  printf("%10s %8s %11s %11s %8s %9s %8s %9s %10s\n", "shape", "players",
         "inline us", "offload us", "ratio", "clusters", "chunks", "biggest",
         "relays");
  for (const Shape shape : { Shape::Province, Shape::Roaming }) {
    for (size_t players : { size_t(100), size_t(200), size_t(400),
                            size_t(800) }) {
      const Result inlineRun =
        Run(players, 0, 0, 0, ticks, warmup, /*forceInline=*/true, UINT32_MAX,
            /*forceAccept=*/true, /*relayFromWorkers=*/false, shape);
      const Result offload =
        Run(players, 0, 0, 0, ticks, warmup, false, UINT32_MAX,
            /*forceAccept=*/true, /*relayFromWorkers=*/false, shape);

      printf("%10s %8zu ", ShapeName(shape), players);
      if (inlineRun.declined) {
        printf("%11s ", "declined");
      } else {
        printf("%11.1f ", inlineRun.medianMicros);
      }
      if (offload.declined) {
        printf("%11s ", "declined");
      } else {
        printf("%11.1f ", offload.medianMicros);
      }
      if (!inlineRun.declined && !offload.declined &&
          offload.medianMicros > 0.0) {
        printf("%7.2fx ", inlineRun.medianMicros / offload.medianMicros);
      } else {
        printf("%8s ", "-");
      }
      printf("%9zu %8zu %9zu %10llu\n", offload.clusters, offload.chunks,
             offload.largestCluster,
             static_cast<unsigned long long>(offload.relays));
    }
  }

  // What it costs to work out where everybody is.
  //
  // Partitioning is the one phase whose cost is driven by how *spread out* a
  // population is rather than by how dense it is: it walks the occupied
  // chunks and probes the window around each one. A crowd occupies a single
  // chunk and the pass is free; a few hundred travellers occupy a few hundred
  // chunks and it is not. That is the opposite of the workload the rest of
  // this file measures, so it gets its own timing rather than being inferred
  // from a tick.
  Header("Partitioning alone, by occupied chunk count (separation 4)");
  printf("%9s %9s %12s %12s %10s\n", "actors", "chunks", "us/partition",
         "ns/chunk", "clusters");
  {
    AreaPartitioner partitioner;
    std::vector<AreaCluster> clusters;
    for (const size_t actorCount : { size_t(100), size_t(200), size_t(400),
                                     size_t(800), size_t(1600) }) {
      for (const bool spread : { false, true }) {
        std::vector<ActorSnapshot> actors(actorCount);
        Rng rng(0x1234abcdULL);
        for (size_t i = 0; i < actorCount; ++i) {
          // Either everyone in one chunk, or one chunk each: the two ends of
          // the range a real map sits between.
          const float x = spread ? rng.Float(-120000.f, 120000.f)
                                 : static_cast<float>(i % 64) * 60.f;
          const float y = spread ? rng.Float(-120000.f, 120000.f)
                                 : static_cast<float>(i / 64) * 60.f;
          actors[i].formId = 0xff000001 + static_cast<uint32_t>(i);
          actors[i].currentPos[0] = x;
          actors[i].currentPos[1] = y;
          actors[i].worldOrCell = 0x3c;
          actors[i].area = AreaKey{ 0x3c, ToChunkCoord(x), ToChunkCoord(y) };
        }

        std::vector<double> samples;
        const int partitionRuns = 200;
        for (int r = 0; r < partitionRuns; ++r) {
          const auto begin = std::chrono::steady_clock::now();
          partitioner.Partition(actors, 4, clusters);
          const auto end = std::chrono::steady_clock::now();
          samples.push_back(
            std::chrono::duration<double, std::micro>(end - begin).count());
        }
        std::sort(samples.begin(), samples.end());
        const double median = samples[samples.size() / 2];
        const size_t chunkCount = partitioner.GetLastChunkCount();
        printf("%9zu %9zu %12.2f %12.1f %10zu\n", actorCount, chunkCount,
               median, median * 1000.0 / static_cast<double>(chunkCount),
               clusters.size());
      }
    }
  }

  return 0;
}
