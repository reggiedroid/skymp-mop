#include "parallel/AreaPartitioner.h"
#include "parallel/InterestManager.h"
#include "parallel/OffloadDispatcher.h"
#include <algorithm>
#include <array>
#include <catch2/catch_all.hpp>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <mutex>
#include <set>
#include <vector>

// The population a live server actually has.
//
// Every other case in this suite puts its players in one chunk, or in a
// handful of chunks chosen by hand. That is the shape the offload exists for,
// and it is not the shape a server spends most of its time in: a few hundred
// players scattered over a province, in ones and twos and in small parties
// that walk from one chunk to the next while the tick is running.
//
// Two things are only testable on that shape.
//
// The first is the relay set. The offloaded path does not consult the
// subscription lists the inline path relays to; it recomputes the
// neighbourhood from the snapshot, as the 3x3 chunk stencil around the
// sender's server-authoritative chunk. The two agree by construction only if
// the stencil arithmetic matches the grid's, at every boundary, in both signs,
// and across worldspaces and interiors. One chunk of players cannot tell,
// because everyone is everyone's neighbour there and any stencil wide enough
// is right.
//
// The second is what partitioning does with a map rather than a crowd. A
// scattered population produces hundreds of occupied chunks instead of one,
// and clusters merge by proximity, so the interesting questions -- is a
// cluster self-contained, does a party that crosses a boundary stay whole,
// does the outcome depend on how many workers happened to be free -- only
// have content when the players are spread out.
//
// The reference below is deliberately a second implementation rather than a
// call into the first: same worldspace, Chebyshev chunk distance at most one,
// which is Grid.h's neighbourhood and what SendToNeighbours walks.

using namespace MpParallel;

namespace {

// One player, tracked across ticks.
struct Mover
{
  uint32_t formId = 0;
  Networking::UserId userId = Networking::InvalidUserId;
  uint32_t worldOrCell = 0;
  float pos[3] = { 0.f, 0.f, 0.f };
  float vel[2] = { 0.f, 0.f };
};

// A relay the sink was handed: who received it, and whose packet it was.
using Edge = std::pair<Networking::UserId, uint32_t>;

// Deterministic, so a failure is reproducible without a seed to carry
// around. Any cheap generator would do; this one is xorshift64*.
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

  // [lo, hi)
  float Float(float lo, float hi)
  {
    const double unit =
      static_cast<double>(Next() >> 11) / static_cast<double>(1ULL << 53);
    return static_cast<float>(lo + unit * (hi - lo));
  }

  size_t Index(size_t count) { return static_cast<size_t>(Next() % count); }

private:
  uint64_t state;
};

// Records relays as (recipient, sender) pairs. The sender is recovered from
// the packet, which carries its index in its first four bytes, so the test
// checks that the right bytes reached the right player rather than just
// counting.
class WorldSink : public IOffloadSink
{
public:
  void ApplyMovement(const ActorSnapshot& actor) override
  {
    applied.push_back(actor.formId);
    appliedPos[actor.formId] = { actor.proposedPos[0], actor.proposedPos[1],
                                 actor.proposedPos[2] };
  }

  void SendCorrection(const ActorSnapshot& actor) override
  {
    corrected.push_back(actor.formId);
  }

  void SendRelayBatch(const OutboundSend* sends, size_t count,
                      const uint8_t* packetBytes,
                      size_t packetBytesLength) override
  {
    // Guarded: with relayFromWorkers on, this is called from every worker.
    std::lock_guard<std::mutex> lock(mtx);
    for (size_t i = 0; i < count; ++i) {
      const OutboundSend& send = sends[i];
      // Recorded rather than asserted. This runs on a worker thread when
      // relayFromWorkers is set, and Catch2's assertion macros are not for
      // calling off the main thread; the flag is checked after the join.
      if (send.byteLength < 4 ||
          static_cast<size_t>(send.byteOffset) + send.byteLength >
            packetBytesLength) {
        malformed = true;
        continue;
      }
      const uint8_t* payload = packetBytes + send.byteOffset;
      const uint32_t senderIndex = static_cast<uint32_t>(payload[0]) |
        (static_cast<uint32_t>(payload[1]) << 8) |
        (static_cast<uint32_t>(payload[2]) << 16) |
        (static_cast<uint32_t>(payload[3]) << 24);
      edges.emplace_back(send.userId, senderIndex);
    }
  }

  void Clear()
  {
    edges.clear();
    applied.clear();
    corrected.clear();
    malformed = false;
  }

  // Set if the dispatcher ever handed over a send whose byte range does not
  // fit the packet buffer. Checked on the main thread after each tick.
  bool malformed = false;

  std::vector<Edge> edges;
  std::vector<uint32_t> applied;
  std::vector<uint32_t> corrected;
  std::map<uint32_t, std::array<float, 3>> appliedPos;
  std::mutex mtx;
};

ParallelConfig WorldConfig(size_t workerThreads)
{
  ParallelConfig config;
  config.enabled = true;
  config.workerThreads = workerThreads;
  // These cases are about which relays are produced, not about when the
  // dispatcher thinks producing them is worth it, so every gate is opened and
  // both rate-reduction mechanisms are off. Each of those has its own cases
  // elsewhere in the suite.
  config.minActorsToOffload = 1;
  config.minClusterActors = 1;
  config.minShardActors = 1;
  config.minOffloadWorkMicros = 0;
  config.minOffloadSpeedup = 0.f;
  config.adaptiveParallelism = false;
  config.adaptiveThrottling = false;
  config.interestManagement = false;
  config.Normalize();
  return config;
}

// What the inline path would relay: for each mover, every connected player in
// the 3x3 chunk stencil around it, in the same worldspace or interior cell.
std::vector<Edge> ExpectedEdges(const std::vector<Mover>& senders,
                                const std::vector<Mover>& population)
{
  std::vector<Edge> expected;
  for (size_t s = 0; s < senders.size(); ++s) {
    const Mover& sender = senders[s];
    const int32_t sx = ToChunkCoord(sender.pos[0]);
    const int32_t sy = ToChunkCoord(sender.pos[1]);
    for (const Mover& target : population) {
      if (target.worldOrCell != sender.worldOrCell) {
        continue;
      }
      const int32_t tx = ToChunkCoord(target.pos[0]);
      const int32_t ty = ToChunkCoord(target.pos[1]);
      if (std::abs(tx - sx) <= 1 && std::abs(ty - sy) <= 1) {
        expected.emplace_back(target.userId, static_cast<uint32_t>(s));
      }
    }
  }
  std::sort(expected.begin(), expected.end());
  return expected;
}

std::vector<Edge> Sorted(std::vector<Edge> edges)
{
  std::sort(edges.begin(), edges.end());
  return edges;
}

// Four bytes of sender index, then filler sized like a movement packet.
std::vector<std::vector<uint8_t>> BuildPackets(size_t count)
{
  std::vector<std::vector<uint8_t>> packets;
  packets.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    std::vector<uint8_t> packet(64, 0xab);
    packet[0] = static_cast<uint8_t>(i & 0xff);
    packet[1] = static_cast<uint8_t>((i >> 8) & 0xff);
    packet[2] = static_cast<uint8_t>((i >> 16) & 0xff);
    packet[3] = static_cast<uint8_t>((i >> 24) & 0xff);
    packets.push_back(std::move(packet));
  }
  return packets;
}

MovementSubmission SubmissionFor(const Mover& mover,
                                 const std::vector<uint8_t>& packet,
                                 float stepX, float stepY)
{
  MovementSubmission submission;
  submission.formId = mover.formId;
  submission.idx = mover.formId & 0xffff;
  submission.ownerUserId = mover.userId;
  submission.currentPos[0] = mover.pos[0];
  submission.currentPos[1] = mover.pos[1];
  submission.currentPos[2] = mover.pos[2];
  submission.currentWorldOrCell = mover.worldOrCell;
  submission.proposedPos[0] = mover.pos[0] + stepX;
  submission.proposedPos[1] = mover.pos[1] + stepY;
  submission.proposedPos[2] = mover.pos[2];
  submission.proposedWorldOrCell = mover.worldOrCell;
  submission.isStanding = true;
  submission.packetData = packet.data();
  submission.packetLength = packet.size();
  return submission;
}

void PublishTargets(OffloadDispatcher& dispatcher,
                    const std::vector<Mover>& population)
{
  std::vector<RelayTarget>& targets = dispatcher.BeginPotentialTargets();
  for (const Mover& mover : population) {
    RelayTarget target;
    target.userId = mover.userId;
    target.listenerFormId = mover.formId;
    target.worldOrCell = mover.worldOrCell;
    target.pos[0] = mover.pos[0];
    target.pos[1] = mover.pos[1];
    target.pos[2] = mover.pos[2];
    target.chunkX = ToChunkCoord(mover.pos[0]);
    target.chunkY = ToChunkCoord(mover.pos[1]);
    targets.push_back(target);
  }
  dispatcher.CommitPotentialTargets();
}

// Where the cities are. Far enough apart that no two of them can ever land
// in the same cluster, which is what makes "one cluster per city" a property
// worth asserting rather than an accident of the seed.
struct Hub
{
  uint32_t worldOrCell;
  float x;
  float y;
  float radius;
};

const Hub kHubs[] = {
  { 0x3c, 10000.f, 10000.f, 1800.f },   // a market square: one chunk
  { 0x3c, -70000.f, 35000.f, 3000.f },  // a city, spilling over a boundary
  { 0x3c, 50000.f, -60000.f, 2400.f },
  { 0x1f4, -20000.f, -20000.f, 2000.f } // and one in the other worldspace
};

// A province with cities in it.
//
// Half the population is concentrated in the four hubs above -- the shape the
// offload exists for, except that there are several of them at once, which is
// what a real server looks like at peak and what makes the difference between
// parallelising across clusters and sharding within one. The rest are spread
// over the map in ones and twos, some indoors, some sitting exactly on a
// chunk seam.
//
// Coordinates straddle zero deliberately. ToChunkCoord truncates towards
// zero, so chunk 0 is twice as wide as every other chunk and the boundaries
// at +/-4096 are the two the stencil arithmetic is most likely to get wrong.
std::vector<Mover> MakeProvince(size_t playerCount, uint64_t seed)
{
  Rng rng(seed);
  std::vector<Mover> population;
  population.reserve(playerCount);

  const uint32_t tamriel = 0x3c;
  const uint32_t otherWorld = 0x1f4;
  const uint32_t interiorBase = 0x10000;
  const size_t hubCount = sizeof(kHubs) / sizeof(kHubs[0]);

  for (size_t i = 0; i < playerCount; ++i) {
    Mover mover;
    mover.formId = 0xff000001 + static_cast<uint32_t>(i);
    mover.userId = static_cast<Networking::UserId>(i);

    const uint64_t roll = rng.Next() % 100;
    if (roll < 50) {
      // In a city or a market. Hubs are weighted unevenly on purpose: one
      // busy capital and three smaller towns is a harder scheduling problem
      // than four equal ones, and it is the usual one.
      const size_t pick = (roll < 22) ? 0 : (1 + rng.Index(hubCount - 1));
      const Hub& hub = kHubs[pick];
      mover.worldOrCell = hub.worldOrCell;
      mover.pos[0] = hub.x + rng.Float(-hub.radius, hub.radius);
      mover.pos[1] = hub.y + rng.Float(-hub.radius, hub.radius);
    } else if (roll < 60) {
      // Indoors. Each interior is its own grid, shared by two or three
      // players.
      mover.worldOrCell = interiorBase + static_cast<uint32_t>(i % 5);
      mover.pos[0] = rng.Float(-2000.f, 2000.f);
      mover.pos[1] = rng.Float(-2000.f, 2000.f);
    } else if (roll < 68) {
      mover.worldOrCell = otherWorld;
      mover.pos[0] = rng.Float(-60000.f, 60000.f);
      mover.pos[1] = rng.Float(-60000.f, 60000.f);
    } else {
      // Out in the province, including right on the chunk seams.
      mover.worldOrCell = tamriel;
      mover.pos[0] = rng.Float(-90000.f, 90000.f);
      mover.pos[1] = rng.Float(-90000.f, 90000.f);
      if (roll % 7 == 0) {
        // Snap onto a boundary, one unit either side of it.
        const float boundary =
          4096.f * static_cast<float>(static_cast<int>(rng.Next() % 21) - 10);
        mover.pos[0] = boundary + (rng.Next() % 2 ? 1.f : -1.f);
      }
    }
    mover.pos[2] = rng.Float(-500.f, 500.f);
    population.push_back(mover);
  }
  return population;
}

}

TEST_CASE("A scattered population relays exactly what the grid would",
          "[ParallelWorld]")
{
  const size_t playerCount = 400;
  std::vector<Mover> population = MakeProvince(playerCount, 0x5eed1234ULL);
  const auto packets = BuildPackets(playerCount);

  OffloadDispatcher dispatcher(WorldConfig(4));
  WorldSink sink;

  PublishTargets(dispatcher, population);
  for (size_t i = 0; i < population.size(); ++i) {
    REQUIRE(dispatcher.WillAcceptThisTick());
    REQUIRE(dispatcher.SubmitMovement(
      SubmissionFor(population[i], packets[i], 8.f, -6.f)));
  }
  dispatcher.ExecuteTick(sink);

  REQUIRE_FALSE(sink.malformed);
  REQUIRE(Sorted(sink.edges) == ExpectedEdges(population, population));

  // Every update was a short step, so every one of them was applied and
  // nobody was corrected.
  REQUIRE(sink.applied.size() == playerCount);
  REQUIRE(sink.corrected.empty());

  // The point of the shape: this is a map, not a crowd.
  const ParallelMetrics& metrics = dispatcher.GetMetrics();
  REQUIRE(metrics.lastChunkCount > 50);
  REQUIRE(metrics.lastClusterCount > 1);
  REQUIRE(metrics.lastLargestClusterSize < playerCount);
}

TEST_CASE("Several cities at once are separate clusters, each sharded",
          "[ParallelWorld]")
{
  // Peak hour: four crowds on the map at the same time, sized unevenly, with
  // nobody in between. This is the case a single crowded chunk cannot stand
  // in for. Parallelising across clusters and sharding inside one are
  // different mechanisms, and only a population with several hubs exercises
  // both at once -- one hub would hide whether clusters are scheduled
  // sensibly, and an even split would hide whether the biggest one is still
  // broken up.
  const size_t hubCount = sizeof(kHubs) / sizeof(kHubs[0]);
  const size_t sizes[hubCount] = { 160, 90, 60, 40 };

  Rng rng(0xc17135ULL);
  std::vector<Mover> population;
  std::vector<size_t> hubOfPlayer;
  for (size_t h = 0; h < hubCount; ++h) {
    for (size_t i = 0; i < sizes[h]; ++i) {
      Mover mover;
      mover.formId = 0xff000001 + static_cast<uint32_t>(population.size());
      mover.userId = static_cast<Networking::UserId>(population.size());
      mover.worldOrCell = kHubs[h].worldOrCell;
      mover.pos[0] = kHubs[h].x + rng.Float(-kHubs[h].radius, kHubs[h].radius);
      mover.pos[1] = kHubs[h].y + rng.Float(-kHubs[h].radius, kHubs[h].radius);
      population.push_back(mover);
      hubOfPlayer.push_back(h);
    }
  }

  const auto packets = BuildPackets(population.size());
  OffloadDispatcher dispatcher(WorldConfig(8));
  WorldSink sink;

  PublishTargets(dispatcher, population);
  for (size_t i = 0; i < population.size(); ++i) {
    REQUIRE(dispatcher.WillAcceptThisTick());
    REQUIRE(dispatcher.SubmitMovement(
      SubmissionFor(population[i], packets[i], 7.f, -7.f)));
  }
  dispatcher.ExecuteTick(sink);

  REQUIRE_FALSE(sink.malformed);
  REQUIRE(Sorted(sink.edges) == ExpectedEdges(population, population));

  const ParallelMetrics& metrics = dispatcher.GetMetrics();
  // One cluster per hub: the hubs are tens of chunks apart, far outside the
  // separation, and there is nobody between them to chain them together.
  REQUIRE(metrics.lastClusterCount == hubCount);
  REQUIRE(metrics.lastLargestClusterSize == sizes[0]);
  // And the biggest one is not left as a single task: most of the tick's
  // relay work is in it, so a unit count equal to the cluster count would
  // mean three quarters of the pool idle while the capital is processed.
  REQUIRE(metrics.lastWorkUnitCount > hubCount);

  // Every cluster holds exactly one hub's players.
  const std::vector<AreaCluster>& clusters = dispatcher.GetLastClusters();
  REQUIRE(clusters.size() == hubCount);
  size_t placed = 0;
  for (const AreaCluster& cluster : clusters) {
    REQUIRE_FALSE(cluster.actorIndices.empty());
    const size_t hub = hubOfPlayer[cluster.actorIndices.front()];
    for (const uint32_t index : cluster.actorIndices) {
      REQUIRE(hubOfPlayer[index] == hub);
      ++placed;
    }
    REQUIRE(cluster.Size() == sizes[hub]);
  }
  REQUIRE(placed == population.size());
}

TEST_CASE("A city is still one cluster when a party walks into it",
          "[ParallelWorld]")
{
  // The two shapes meeting: a market square that is busy on its own, and a
  // party arriving from outside. Once the party is inside the separation the
  // partitioner must fold it into the city's cluster rather than leaving a
  // second cluster whose members can see the first's -- that is what makes a
  // cluster safe to process on its own.
  const Hub& city = kHubs[0];
  Rng rng(0x9a11eeULL);

  std::vector<Mover> population;
  for (size_t i = 0; i < 80; ++i) {
    Mover mover;
    mover.formId = 0xff000001 + static_cast<uint32_t>(i);
    mover.userId = static_cast<Networking::UserId>(i);
    mover.worldOrCell = city.worldOrCell;
    mover.pos[0] = city.x + rng.Float(-city.radius, city.radius);
    mover.pos[1] = city.y + rng.Float(-city.radius, city.radius);
    population.push_back(mover);
  }

  // A party of ten, ten chunks out -- well outside the separation, so they
  // start as a cluster of their own -- walking in at a little under a chunk a
  // tick.
  const size_t partyBegin = population.size();
  for (size_t i = 0; i < 10; ++i) {
    Mover mover;
    mover.formId = 0xff000001 + static_cast<uint32_t>(population.size());
    mover.userId = static_cast<Networking::UserId>(population.size());
    mover.worldOrCell = city.worldOrCell;
    mover.pos[0] = city.x - 10.f * 4096.f + rng.Float(-200.f, 200.f);
    mover.pos[1] = city.y + rng.Float(-200.f, 200.f);
    mover.vel[0] = 3500.f;
    population.push_back(mover);
  }

  const auto packets = BuildPackets(population.size());
  OffloadDispatcher dispatcher(WorldConfig(4));
  WorldSink sink;

  bool sawTwoClusters = false;
  bool sawPartyInsideCityCluster = false;

  for (int tick = 0; tick < 16; ++tick) {
    sink.Clear();
    PublishTargets(dispatcher, population);
    for (size_t i = 0; i < population.size(); ++i) {
      REQUIRE(dispatcher.WillAcceptThisTick());
      REQUIRE(dispatcher.SubmitMovement(SubmissionFor(
        population[i], packets[i], population[i].vel[0], 0.f)));
    }
    const std::vector<Edge> expected = ExpectedEdges(population, population);
    dispatcher.ExecuteTick(sink);
    REQUIRE_FALSE(sink.malformed);
    REQUIRE(Sorted(sink.edges) == expected);

    const size_t clusterCount = dispatcher.GetMetrics().lastClusterCount;
    if (clusterCount > 1) {
      sawTwoClusters = true;
    } else {
      // Merged. Every party member is in the city's cluster, and the relay
      // parity above says the merge did not change a single edge.
      REQUIRE(dispatcher.GetLastClusters().size() == 1);
      REQUIRE(dispatcher.GetLastClusters().front().Size() ==
              population.size());
      sawPartyInsideCityCluster = true;
    }

    for (Mover& mover : population) {
      const auto& applied = sink.appliedPos[mover.formId];
      mover.pos[0] = applied[0];
      mover.pos[1] = applied[1];
    }
  }

  REQUIRE(sawTwoClusters);
  REQUIRE(sawPartyInsideCityCluster);
  REQUIRE(partyBegin == 80);
}

TEST_CASE("Parties walking across chunk boundaries keep their neighbours",
          "[ParallelWorld]")
{
  // Groups of eight, each walking a heading of its own at a little under a
  // chunk per tick -- the fastest the validator accepts -- so every group
  // crosses several boundaries over the run, and some cross into each other.
  const size_t groupCount = 24;
  const size_t groupSize = 8;
  const size_t playerCount = groupCount * groupSize;
  const int ticks = 20;

  Rng rng(0xa11ce99ULL);
  std::vector<Mover> population;
  population.reserve(playerCount);
  for (size_t g = 0; g < groupCount; ++g) {
    const float originX = rng.Float(-50000.f, 50000.f);
    const float originY = rng.Float(-50000.f, 50000.f);
    const float velX = rng.Float(-3000.f, 3000.f);
    const float velY = rng.Float(-3000.f, 3000.f);
    for (size_t m = 0; m < groupSize; ++m) {
      Mover mover;
      mover.formId =
        0xff000001 + static_cast<uint32_t>(g * groupSize + m);
      mover.userId = static_cast<Networking::UserId>(g * groupSize + m);
      mover.worldOrCell = 0x3c;
      mover.pos[0] = originX + rng.Float(-300.f, 300.f);
      mover.pos[1] = originY + rng.Float(-300.f, 300.f);
      mover.vel[0] = velX;
      mover.vel[1] = velY;
      population.push_back(mover);
    }
  }

  const auto packets = BuildPackets(playerCount);
  OffloadDispatcher dispatcher(WorldConfig(4));
  WorldSink sink;

  size_t boundaryCrossings = 0;

  for (int tick = 0; tick < ticks; ++tick) {
    sink.Clear();
    PublishTargets(dispatcher, population);

    for (size_t i = 0; i < population.size(); ++i) {
      REQUIRE(dispatcher.WillAcceptThisTick());
      REQUIRE(dispatcher.SubmitMovement(SubmissionFor(
        population[i], packets[i], population[i].vel[0],
        population[i].vel[1])));
    }

    // Computed from the positions the tick started with, which is what both
    // paths relay against: the relay happens before the move is applied.
    const std::vector<Edge> expected = ExpectedEdges(population, population);
    dispatcher.ExecuteTick(sink);
    REQUIRE_FALSE(sink.malformed);
    REQUIRE(Sorted(sink.edges) == expected);
    REQUIRE(sink.applied.size() == playerCount);
    REQUIRE(sink.corrected.empty());

    // Walk everyone forward to where the join just put them.
    for (Mover& mover : population) {
      const int32_t before = ToChunkCoord(mover.pos[0]);
      const auto& applied = sink.appliedPos[mover.formId];
      mover.pos[0] = applied[0];
      mover.pos[1] = applied[1];
      mover.pos[2] = applied[2];
      if (ToChunkCoord(mover.pos[0]) != before) {
        ++boundaryCrossings;
      }
    }
  }

  // The scenario is only worth what it exercises: if nobody changed chunk the
  // parity assertions above were checked against a standing population.
  REQUIRE(boundaryCrossings > playerCount);
}

TEST_CASE("A party that splits across a boundary is still one cluster",
          "[ParallelWorld]")
{
  // Eight players either side of x = 4096. Whatever cluster they land in,
  // they must land in the same one: they can see each other, so they must not
  // be processed as two independent areas.
  std::vector<ActorSnapshot> actors;
  for (int i = 0; i < 16; ++i) {
    ActorSnapshot actor;
    actor.formId = 0xff000001 + static_cast<uint32_t>(i);
    actor.worldOrCell = 0x3c;
    actor.currentPos[0] = (i < 8) ? 4090.f : 4102.f;
    actor.currentPos[1] = 100.f;
    actor.area = AreaKey{ 0x3c, ToChunkCoord(actor.currentPos[0]),
                          ToChunkCoord(actor.currentPos[1]) };
    actors.push_back(actor);
  }

  AreaPartitioner partitioner;
  std::vector<AreaCluster> clusters;
  partitioner.Partition(actors, 4, clusters);

  REQUIRE(partitioner.GetLastChunkCount() == 2);
  REQUIRE(clusters.size() == 1);
  REQUIRE(clusters.front().Size() == 16);
}

TEST_CASE("Every cluster of a scattered population is self-contained",
          "[ParallelWorld]")
{
  // The safety property the partitioner exists to provide, checked against
  // the definition rather than against an example: any two actors close
  // enough to relay to each other -- allowing for a chunk of movement in
  // either direction, which is what the separation margin is for -- share a
  // cluster.
  const std::vector<Mover> population = MakeProvince(400, 0xbeef0007ULL);

  std::vector<ActorSnapshot> actors;
  actors.reserve(population.size());
  for (const Mover& mover : population) {
    ActorSnapshot actor;
    actor.formId = mover.formId;
    actor.worldOrCell = mover.worldOrCell;
    actor.currentPos[0] = mover.pos[0];
    actor.currentPos[1] = mover.pos[1];
    actor.currentPos[2] = mover.pos[2];
    actor.area = AreaKey{ mover.worldOrCell, ToChunkCoord(mover.pos[0]),
                          ToChunkCoord(mover.pos[1]) };
    actors.push_back(actor);
  }

  AreaPartitioner partitioner;
  std::vector<AreaCluster> clusters;
  partitioner.Partition(actors, 4, clusters);

  REQUIRE(clusters.size() > 1);

  for (size_t a = 0; a < actors.size(); ++a) {
    for (size_t b = a + 1; b < actors.size(); ++b) {
      if (actors[a].worldOrCell != actors[b].worldOrCell) {
        continue;
      }
      const int32_t distance = actors[a].area.ChunkDistanceTo(actors[b].area);
      if (distance >= 0 && distance <= 2) {
        REQUIRE(actors[a].clusterIndex == actors[b].clusterIndex);
      }
    }
  }

  // And the partition is a partition: every actor placed, exactly once.
  std::set<uint32_t> seen;
  size_t members = 0;
  for (const AreaCluster& cluster : clusters) {
    for (const uint32_t index : cluster.actorIndices) {
      REQUIRE(seen.insert(index).second);
      ++members;
    }
  }
  REQUIRE(members == actors.size());
}

TEST_CASE("A chain of travellers merges into one cluster, and still shards",
          "[ParallelWorld]")
{
  // Worth pinning because it is the one way a scattered population can look
  // like a crowd to the partitioner. Clusters merge at a separation of four
  // chunks, so players strung along a road closer together than that join up,
  // however far the two ends are apart. That is not a bug -- the separation is
  // what makes a cluster self-contained -- but it does mean cluster count is
  // not a density signal, and that the parallelism has to come from sharding
  // the cluster rather than from having several.
  const size_t playerCount = 120;
  std::vector<Mover> population;
  population.reserve(playerCount);
  for (size_t i = 0; i < playerCount; ++i) {
    Mover mover;
    mover.formId = 0xff000001 + static_cast<uint32_t>(i);
    mover.userId = static_cast<Networking::UserId>(i);
    mover.worldOrCell = 0x3c;
    // Three chunks apart: inside the separation, outside the stencil.
    mover.pos[0] = static_cast<float>(i) * 3.f * 4096.f;
    mover.pos[1] = 0.f;
    population.push_back(mover);
  }

  const auto packets = BuildPackets(playerCount);
  OffloadDispatcher dispatcher(WorldConfig(4));
  WorldSink sink;

  PublishTargets(dispatcher, population);
  for (size_t i = 0; i < population.size(); ++i) {
    REQUIRE(dispatcher.WillAcceptThisTick());
    REQUIRE(dispatcher.SubmitMovement(
      SubmissionFor(population[i], packets[i], 5.f, 5.f)));
  }
  dispatcher.ExecuteTick(sink);

  REQUIRE_FALSE(sink.malformed);
  const ParallelMetrics& metrics = dispatcher.GetMetrics();
  REQUIRE(metrics.lastChunkCount == playerCount);
  REQUIRE(metrics.lastClusterCount == 1);
  REQUIRE(metrics.lastWorkUnitCount > 1);

  // Three chunks apart is outside the 3x3 stencil, so every one of these
  // travellers relays only to itself. Merged into one cluster or not, the
  // relay set is unchanged.
  REQUIRE(sink.edges.size() == playerCount);
  REQUIRE(Sorted(sink.edges) == ExpectedEdges(population, population));
}

TEST_CASE("Worker count does not change what a scattered population sends",
          "[ParallelWorld]")
{
  const size_t playerCount = 300;
  const std::vector<Mover> population = MakeProvince(playerCount, 0xf00d99ULL);
  const auto packets = BuildPackets(playerCount);
  const std::vector<Edge> expected = ExpectedEdges(population, population);

  std::vector<uint32_t> firstApplyOrder;

  for (const size_t workers : { size_t(1), size_t(3), size_t(8) }) {
    for (const bool fromWorkers : { false, true }) {
      ParallelConfig config = WorldConfig(workers);
      config.relayFromWorkers = fromWorkers;
      config.Normalize();

      OffloadDispatcher dispatcher(config);
      WorldSink sink;

      PublishTargets(dispatcher, population);
      for (size_t i = 0; i < population.size(); ++i) {
        REQUIRE(dispatcher.WillAcceptThisTick());
        REQUIRE(dispatcher.SubmitMovement(
          SubmissionFor(population[i], packets[i], 3.f, 3.f)));
      }
      dispatcher.ExecuteTick(sink);

      REQUIRE_FALSE(sink.malformed);
      REQUIRE(Sorted(sink.edges) == expected);

      // World writes are ordered by work unit, which is fixed before any task
      // starts. That order must not depend on the pool at all.
      if (firstApplyOrder.empty()) {
        firstApplyOrder = sink.applied;
      } else {
        REQUIRE(sink.applied == firstApplyOrder);
      }
    }
  }
}

TEST_CASE("A player changing worldspace is corrected, not relayed elsewhere",
          "[ParallelWorld]")
{
  // A movement update may not change which grid the actor is on: the inline
  // validator rejects that, and so must this path, or an actor could be
  // relayed into a cell it has not been streamed into.
  std::vector<Mover> population;
  for (int i = 0; i < 4; ++i) {
    Mover mover;
    mover.formId = 0xff000001 + static_cast<uint32_t>(i);
    mover.userId = static_cast<Networking::UserId>(i);
    mover.worldOrCell = 0x3c;
    mover.pos[0] = 100.f * static_cast<float>(i);
    population.push_back(mover);
  }
  // One more, already indoors, standing at the same coordinates.
  Mover indoors;
  indoors.formId = 0xff000010;
  indoors.userId = 4;
  indoors.worldOrCell = 0x10001;
  population.push_back(indoors);

  const auto packets = BuildPackets(population.size());
  OffloadDispatcher dispatcher(WorldConfig(2));
  WorldSink sink;

  PublishTargets(dispatcher, population);

  // The first four move normally; the first of them also claims to have
  // arrived in the interior.
  for (size_t i = 0; i < population.size(); ++i) {
    MovementSubmission submission =
      SubmissionFor(population[i], packets[i], 5.f, 5.f);
    if (i == 0) {
      submission.proposedWorldOrCell = 0x10001;
    }
    REQUIRE(dispatcher.WillAcceptThisTick());
    REQUIRE(dispatcher.SubmitMovement(submission));
  }
  dispatcher.ExecuteTick(sink);

  REQUIRE_FALSE(sink.malformed);
  REQUIRE(sink.corrected.size() == 1);
  REQUIRE(sink.corrected.front() == population[0].formId);
  REQUIRE(sink.applied.size() == population.size() - 1);

  // Rejected or not, the packet is still forwarded -- to the four outside,
  // never to the player indoors.
  const std::vector<Edge> expected = ExpectedEdges(population, population);
  REQUIRE(Sorted(sink.edges) == expected);
  for (const Edge& edge : sink.edges) {
    if (edge.second == 0) {
      REQUIRE(edge.first != indoors.userId);
    }
  }
}
