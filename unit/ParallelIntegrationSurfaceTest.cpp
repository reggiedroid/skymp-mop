#include "parallel/OffloadDispatcher.h"
#include "parallel/ParallelConfig.h"
#include "parallel/ParallelMetrics.h"
#include <catch2/catch_all.hpp>
#include <chrono>
#include <nlohmann/json.hpp>
#include <cstdint>
#include <vector>

// The API the live server uses, exercised from a file that builds without
// vcpkg.
//
// WHY THIS EXISTS
//
// PartOne.cpp, ActionListener.cpp and PartOneOffloadSink.cpp are the three
// translation units that drive this framework on a real server, and none of
// them can be compiled without slikenet, mongo-cxx-driver and the rest of the
// vcpkg tree. On a host without that tree the whole parallel suite can still
// be built and run -- which means a change can pass every test here and still
// leave the server unable to compile.
//
// That is not hypothetical. Merging two rival lines of this work produced a
// tree whose PartOne.cpp wrote `metrics.lastStaleActors` while the
// ParallelMetrics.h it was merged onto had no such field. Every test passed.
// The server did not build, and nothing in the suite could notice, because
// the only callers were in files the suite cannot compile.
//
// So this file makes the same calls, in the same order, with the same types,
// as those three do. It is a compile-time contract first and a behavioural
// test second: if a field or a signature the server depends on disappears,
// this stops building.
//
// KEEPING IT HONEST
//
// It is maintained by hand, so it is only as good as its last read of those
// three files. When the integration changes, change this with it. What it
// cannot do is verify the parts that need the real world -- an MpActor, an
// espm load order, a socket -- and unit/PartOne_MovementParallelTest.cpp
// covers those on a host that can build them.

using namespace MpParallel;

namespace {

// Mirrors PartOneOffloadSink: the same three overrides, the same parameter
// types, and the same bounds check on the byte range.
class SurfaceSink : public IOffloadSink
{
public:
  void ApplyMovement(const ActorSnapshot& actor) override
  {
    // Every field PartOneOffloadSink::ApplyMovement reads.
    applied.push_back(actor.formId);
    lastIdx = actor.idx;
    lastPos[0] = actor.proposedPos[0];
    lastPos[1] = actor.proposedPos[1];
    lastPos[2] = actor.proposedPos[2];
    lastRot[0] = actor.proposedRot[0];
    lastRot[1] = actor.proposedRot[1];
    lastRot[2] = actor.proposedRot[2];
    lastFlags = actor.isInJumpState || actor.isWeapDrawn || actor.isBlocking ||
      actor.isSneaking || actor.isStanding;
  }

  void SendCorrection(const ActorSnapshot& actor) override
  {
    // Every field PartOneOffloadSink::SendCorrection reads.
    if (actor.ownerUserId == Networking::InvalidUserId) {
      return;
    }
    corrected.push_back(actor.formId);
    correctionPos[0] = actor.currentPos[0];
    correctionPos[1] = actor.currentPos[1];
    correctionPos[2] = actor.currentPos[2];
    correctionRot[0] = actor.currentRot[0];
    correctionWorldOrCell = actor.currentWorldOrCell;
  }

  void SendRelayBatch(const OutboundSend* sends, size_t count,
                      const uint8_t* packetBytes,
                      size_t packetBytesLength) override
  {
    if (sends == nullptr || packetBytes == nullptr) {
      return;
    }
    for (size_t i = 0; i < count; ++i) {
      const OutboundSend& send = sends[i];
      if (send.byteLength == 0 ||
          static_cast<size_t>(send.byteOffset) + send.byteLength >
            packetBytesLength) {
        continue;
      }
      relayed.push_back(send.userId);
      reliableSeen = reliableSeen || send.reliable;
    }
  }

  // PartOneOffloadSink::BeginJoin has no dispatcher-facing signature, but the
  // tick sequence calls it, so the sequence below calls one too.
  void BeginJoin() { ++beginJoins; }

  [[nodiscard]] uint64_t GetStaleActorCount() const noexcept
  {
    return staleActorCount;
  }

  std::vector<uint32_t> applied;
  std::vector<uint32_t> corrected;
  std::vector<Networking::UserId> relayed;
  uint32_t lastIdx = 0;
  float lastPos[3] = { 0.f, 0.f, 0.f };
  float lastRot[3] = { 0.f, 0.f, 0.f };
  float correctionPos[3] = { 0.f, 0.f, 0.f };
  float correctionRot[3] = { 0.f, 0.f, 0.f };
  uint32_t correctionWorldOrCell = 0;
  bool lastFlags = false;
  bool reliableSeen = false;
  int beginJoins = 0;
  uint64_t staleActorCount = 0;
};

ParallelConfig SurfaceConfig()
{
  ParallelConfig config;
  config.enabled = true;
  config.workerThreads = 2;
  config.minActorsToOffload = 1;
  config.minClusterActors = 1;
  config.minShardActors = 1;
  config.minOffloadWorkMicros = 0;
  config.minOffloadSpeedup = 0.f;
  config.adaptiveThrottling = false;
  config.interestManagement = false;
  config.Normalize();
  return config;
}

// The shape of ActionListener::OnUpdateMovement, without the parts that need
// an MpActor: ask the gate, time the handling when the trial wants it timed,
// submit, and fall back inline when the submission is not taken on.
bool IngestOneUpdate(OffloadDispatcher& dispatcher,
                     const MovementSubmission& submission, bool& outInline)
{
  outInline = false;
  if (!dispatcher.IsEnabled()) {
    outInline = true;
    return false;
  }

  const bool accepting = dispatcher.WillAcceptThisTick();

  if (dispatcher.IsMeasuringThisTick()) {
    const auto started = std::chrono::steady_clock::now();
    const bool taken = accepting && dispatcher.SubmitMovement(submission);
    outInline = !taken;
    dispatcher.AddIngestNanos(static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now() - started)
        .count()));
    return taken;
  }

  const bool taken = accepting && dispatcher.SubmitMovement(submission);
  outInline = !taken;
  return taken;
}

// The shape of PartOne::Tick's parallel section.
void RunServerTick(OffloadDispatcher& dispatcher, SurfaceSink& sink,
                   const std::vector<RelayTarget>& connectedPlayers)
{
  if (dispatcher.IsEnabled()) {
    std::vector<RelayTarget>& potentialTargets =
      dispatcher.BeginPotentialTargets();
    if (dispatcher.GetPendingCount() > 0) {
      for (const RelayTarget& player : connectedPlayers) {
        RelayTarget target;
        target.userId = player.userId;
        target.listenerFormId = player.listenerFormId;
        target.pos[0] = player.pos[0];
        target.pos[1] = player.pos[1];
        target.pos[2] = player.pos[2];
        target.worldOrCell = player.worldOrCell;
        target.chunkX = ToChunkCoord(player.pos[0]);
        target.chunkY = ToChunkCoord(player.pos[1]);
        potentialTargets.push_back(target);
      }
    }
    dispatcher.CommitPotentialTargets();
  }

  sink.BeginJoin();
  const uint64_t staleBefore = sink.GetStaleActorCount();
  dispatcher.ExecuteTick(sink);

  // PartOne writes the sink's stale counts into the metrics block after the
  // join, through a const_cast, because only the sink knows them. If either
  // field stops existing this file stops compiling, which is the point.
  const uint64_t staleAfter = sink.GetStaleActorCount();
  auto& metrics = const_cast<ParallelMetrics&>(dispatcher.GetMetrics());
  metrics.lastStaleActors = staleAfter - staleBefore;
  metrics.totalStaleActors = staleAfter;
}

}

TEST_CASE("The server's tick sequence drives the dispatcher",
          "[ParallelSurface]")
{
  OffloadDispatcher dispatcher(SurfaceConfig());
  SurfaceSink sink;

  const std::vector<uint8_t> packet(48, 0x5a);

  std::vector<RelayTarget> players;
  for (int i = 0; i < 3; ++i) {
    RelayTarget player;
    player.userId = static_cast<Networking::UserId>(i);
    player.listenerFormId = 0xff000001 + static_cast<uint32_t>(i);
    player.worldOrCell = 0x3c;
    player.pos[0] = 100.f * static_cast<float>(i);
    player.pos[1] = 0.f;
    players.push_back(player);
  }

  // One mover, ingested the way ActionListener ingests it.
  MovementSubmission submission;
  submission.formId = players[0].listenerFormId;
  submission.idx = 7;
  submission.ownerUserId = players[0].userId;
  submission.currentPos[0] = players[0].pos[0];
  submission.currentPos[1] = players[0].pos[1];
  submission.currentPos[2] = players[0].pos[2];
  submission.currentRot[2] = 90.f;
  submission.currentWorldOrCell = 0x3c;
  submission.proposedPos[0] = players[0].pos[0] + 12.f;
  submission.proposedPos[1] = players[0].pos[1] + 4.f;
  submission.proposedRot[2] = 95.f;
  submission.proposedWorldOrCell = 0x3c;
  submission.teleportFlag = false;
  submission.isInJumpState = false;
  submission.isWeapDrawn = false;
  submission.isBlocking = false;
  submission.isSneaking = false;
  submission.isStanding = true;
  submission.packetData = packet.data();
  submission.packetLength = packet.size();

  bool wentInline = true;
  REQUIRE(IngestOneUpdate(dispatcher, submission, wentInline));
  REQUIRE_FALSE(wentInline);
  REQUIRE(dispatcher.GetPendingCount() == 1);

  RunServerTick(dispatcher, sink, players);

  REQUIRE(sink.beginJoins == 1);
  REQUIRE(sink.applied.size() == 1);
  REQUIRE(sink.applied.at(0) == submission.formId);
  REQUIRE(sink.lastIdx == 7);
  REQUIRE(sink.lastPos[0] == submission.proposedPos[0]);
  // All three players share a chunk, so each of them receives the relay.
  REQUIRE(sink.relayed.size() == 3);
  REQUIRE_FALSE(sink.reliableSeen);

  const ParallelMetrics& metrics = dispatcher.GetMetrics();
  REQUIRE(metrics.lastActorCount == 1);
  REQUIRE(metrics.lastRelayEdgesEmitted == 3);
  REQUIRE(metrics.lastStaleActors == 0);
  REQUIRE(metrics.totalStaleActors == 0);
}

TEST_CASE("A rejected update takes the correction path the server expects",
          "[ParallelSurface]")
{
  OffloadDispatcher dispatcher(SurfaceConfig());
  SurfaceSink sink;

  const std::vector<uint8_t> packet(48, 0x5a);

  std::vector<RelayTarget> players(1);
  players[0].userId = 0;
  players[0].listenerFormId = 0xff000001;
  players[0].worldOrCell = 0x3c;

  MovementSubmission submission;
  submission.formId = 0xff000001;
  submission.ownerUserId = 0;
  submission.currentPos[0] = 0.f;
  submission.currentRot[2] = 30.f;
  submission.currentWorldOrCell = 0x3c;
  // Further than one chunk: the same rule the inline validator applies.
  submission.proposedPos[0] = 40000.f;
  submission.proposedWorldOrCell = 0x3c;
  submission.packetData = packet.data();
  submission.packetLength = packet.size();

  bool wentInline = true;
  REQUIRE(IngestOneUpdate(dispatcher, submission, wentInline));

  RunServerTick(dispatcher, sink, players);

  REQUIRE(sink.applied.empty());
  REQUIRE(sink.corrected.size() == 1);
  REQUIRE(sink.correctionPos[0] == 0.f);
  REQUIRE(sink.correctionRot[0] == 0.f);
  REQUIRE(sink.correctionWorldOrCell == 0x3c);
  // A rejected update is still forwarded, exactly as the inline path does.
  REQUIRE(sink.relayed.size() == 1);
  REQUIRE(dispatcher.GetMetrics().lastRejectedMovements == 1);
}

TEST_CASE("A disabled dispatcher leaves the server on its original path",
          "[ParallelSurface]")
{
  ParallelConfig off;
  off.enabled = false;
  off.Normalize();

  OffloadDispatcher dispatcher(off);
  SurfaceSink sink;
  const std::vector<uint8_t> packet(48, 0x5a);

  MovementSubmission submission;
  submission.formId = 0xff000001;
  submission.ownerUserId = 0;
  submission.currentWorldOrCell = 0x3c;
  submission.proposedWorldOrCell = 0x3c;
  submission.packetData = packet.data();
  submission.packetLength = packet.size();

  bool wentInline = false;
  REQUIRE_FALSE(IngestOneUpdate(dispatcher, submission, wentInline));
  REQUIRE(wentInline);
  REQUIRE_FALSE(dispatcher.IsEnabled());
  REQUIRE(dispatcher.GetPendingCount() == 0);

  RunServerTick(dispatcher, sink, {});
  REQUIRE(sink.applied.empty());
  REQUIRE(sink.relayed.empty());
}

TEST_CASE("The settings an operator writes reach the dispatcher",
          "[ParallelSurface]")
{
  // ScampServer builds the config from the "parallelism" object and hands it
  // to PartOne::ConfigureParallelism, which calls Reconfigure and logs
  // Describe(). Both entry points are part of the server's surface.
  const nlohmann::json settings = nlohmann::json::parse(R"({
    "parallelism": {
      "enabled": true,
      "workerThreads": 3,
      "minActorsToOffload": 120,
      "interestManagement": true,
      "relayFromWorkers": false,
      "metricsLogIntervalTicks": 300
    }
  })");

  const ParallelConfig config = ParallelConfig::FromServerSettings(settings);
  REQUIRE(config.enabled);
  REQUIRE(config.workerThreads == 3);
  REQUIRE(config.minActorsToOffload == 120);
  REQUIRE(config.metricsLogIntervalTicks == 300);
  REQUIRE_FALSE(config.relayFromWorkers);

  OffloadDispatcher dispatcher(ParallelConfig{});
  REQUIRE_FALSE(dispatcher.IsEnabled());
  dispatcher.Reconfigure(config);
  REQUIRE(dispatcher.IsEnabled());
  REQUIRE_FALSE(dispatcher.GetConfig().Describe().empty());
}
