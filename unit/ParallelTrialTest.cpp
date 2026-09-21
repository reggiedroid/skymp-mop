#include "parallel/OffloadDispatcher.h"
#include <catch2/catch_all.hpp>
#include <cstdint>
#include <vector>

// The caller contract the A/B trial rests on.
//
// OffloadDispatcher does not decide whether to take a tick's movement on by
// comparing a statistic against a threshold. It runs both paths in short
// alternating blocks and keeps whichever measured cheaper per mover. That
// only works if the caller plays its part, because neither path keeps all of
// its cost in one place: an accepted tick is flattened during ingest and
// relayed in the join, a declined one does everything during ingest. The
// dispatcher can time its own phases and nothing else.
//
// So a caller must:
//
//   * call WillAcceptThisTick() once per movement packet, before building a
//     submission -- this is what counts the attempt and takes the tick's
//     decision;
//   * check IsMeasuringThisTick() and, when it is set, report what handling
//     the update cost through AddIngestNanos();
//   * have a real inline path for when the answer is no.
//
// Miss the second and the failure is silent and always in the same direction.
// A declined tick's whole cost is what the caller reported; report nothing and
// the declined arm costs zero per mover, no accepted arm can beat zero, and
// the trial declines for ever. The offload never engages, on any population,
// and nothing in the log says why.
//
// ActionListener implements the contract. Until this file existed nothing
// that builds without vcpkg checked that it has to, and the two files that
// did -- unit/ParallelBenchmark.cpp and unit/ParallelSimulation.cpp -- need
// the whole vcpkg tree to compile. That is how the trap stays a trap.

using namespace MpParallel;

namespace {

class SilentSink : public IOffloadSink
{
public:
  void ApplyMovement(const ActorSnapshot&) override { ++applied; }
  void SendCorrection(const ActorSnapshot&) override { ++corrected; }
  void SendRelayBatch(const OutboundSend*, size_t count, const uint8_t*,
                      size_t) override
  {
    relays += count;
  }

  size_t applied = 0;
  size_t corrected = 0;
  size_t relays = 0;
};

ParallelConfig TrialConfig()
{
  ParallelConfig config;
  config.enabled = true;
  config.workerThreads = 2;
  config.minActorsToOffload = 1;
  config.minClusterActors = 1;
  config.minShardActors = 1;
  // The work floor is consulted before the verdict is, so leaving it in place
  // would decline these populations without ever asking the trial.
  config.minOffloadWorkMicros = 0;
  config.minOffloadSpeedup = 0.f;
  config.adaptiveParallelism = true;
  // Short blocks: eight ticks per trial rather than forty-eight, so a case
  // can drive several trials without driving a thousand ticks.
  config.abTrialIntervalTicks = 1;
  config.abTrialBlockTicks = 2;
  config.abTrialBlocks = 4;
  config.adaptiveThrottling = false;
  config.interestManagement = false;
  config.Normalize();
  return config;
}

// A caller, with each part of the contract switchable so a case can leave one
// out and see what the trial concludes.
struct Driver
{
  bool asksFirst = true;   // WillAcceptThisTick before building a submission
  bool reportsIngest = true; // AddIngestNanos when asked to measure

  // What the caller reports for one update on each path. Synthetic: a unit
  // test cannot ask a real ActionListener what an update cost, and the trial
  // only ever sees what the caller hands it, so handing it a fixed number is
  // exactly as real as anything else it could be given.
  uint64_t acceptNanosPerUpdate = 1000;
  uint64_t declineNanosPerUpdate = 1000;

  size_t declinedUpdates = 0;
  size_t acceptedUpdates = 0;

  void RunTick(OffloadDispatcher& dispatcher, IOffloadSink& sink,
               size_t movers, const std::vector<uint8_t>& packet)
  {
    std::vector<RelayTarget>& targets = dispatcher.BeginPotentialTargets();
    for (size_t i = 0; i < movers; ++i) {
      RelayTarget target;
      target.userId = static_cast<Networking::UserId>(i);
      target.listenerFormId = 0xff000001 + static_cast<uint32_t>(i);
      target.worldOrCell = 0x3c;
      target.pos[0] = static_cast<float>(i % 32) * 100.f;
      target.pos[1] = static_cast<float>(i / 32) * 100.f;
      target.chunkX = ToChunkCoord(target.pos[0]);
      target.chunkY = ToChunkCoord(target.pos[1]);
      targets.push_back(target);
    }
    dispatcher.CommitPotentialTargets();

    for (size_t i = 0; i < movers; ++i) {
      const bool accepting =
        asksFirst ? dispatcher.WillAcceptThisTick() : true;

      MovementSubmission submission;
      submission.formId = 0xff000001 + static_cast<uint32_t>(i);
      submission.idx = static_cast<uint32_t>(i);
      submission.ownerUserId = static_cast<Networking::UserId>(i);
      submission.currentPos[0] = static_cast<float>(i % 32) * 100.f;
      submission.currentPos[1] = static_cast<float>(i / 32) * 100.f;
      submission.currentWorldOrCell = 0x3c;
      submission.proposedPos[0] = submission.currentPos[0] + 2.f;
      submission.proposedPos[1] = submission.currentPos[1] + 2.f;
      submission.proposedWorldOrCell = 0x3c;
      submission.isStanding = true;
      submission.packetData = packet.data();
      submission.packetLength = packet.size();

      const bool taken = accepting && dispatcher.SubmitMovement(submission);
      if (taken) {
        ++acceptedUpdates;
      } else {
        ++declinedUpdates;
      }

      if (reportsIngest && dispatcher.IsMeasuringThisTick()) {
        dispatcher.AddIngestNanos(taken ? acceptNanosPerUpdate
                                        : declineNanosPerUpdate);
      }
    }

    dispatcher.ExecuteTick(sink);
  }
};

}

TEST_CASE("A conforming caller gets a trial with both arms priced",
          "[ParallelTrial]")
{
  OffloadDispatcher dispatcher(TrialConfig());
  SilentSink sink;
  const std::vector<uint8_t> packet(64, 0xab);

  Driver driver;
  for (int tick = 0; tick < 24; ++tick) {
    driver.RunTick(dispatcher, sink, 40, packet);
  }

  const ParallelMetrics& metrics = dispatcher.GetMetrics();
  REQUIRE(metrics.totalTrials >= 1);
  // Both arms ran: some ticks were taken on and some were handed back.
  REQUIRE(driver.acceptedUpdates > 0);
  REQUIRE(driver.declinedUpdates > 0);
  // And both were priced. A zero here is the silent failure this file exists
  // for: it means one arm's cost was never reported and the comparison is
  // against nothing.
  REQUIRE(metrics.lastTrialAcceptMicrosPerMover > 0.0);
  REQUIRE(metrics.lastTrialDeclineMicrosPerMover > 0.0);
}

TEST_CASE("A caller that never reports ingest time declines for ever",
          "[ParallelTrial]")
{
  // Everything else about this caller is correct: it asks before submitting
  // and it falls back when told no. It simply never calls AddIngestNanos.
  OffloadDispatcher dispatcher(TrialConfig());
  SilentSink sink;
  const std::vector<uint8_t> packet(64, 0xab);

  Driver driver;
  driver.reportsIngest = false;
  for (int tick = 0; tick < 24; ++tick) {
    driver.RunTick(dispatcher, sink, 40, packet);
  }

  const ParallelMetrics& metrics = dispatcher.GetMetrics();
  REQUIRE(metrics.totalTrials >= 1);
  // The declined arm costs nothing, because nothing was reported, so it wins
  // whatever the accepted arm measured.
  REQUIRE(metrics.lastTrialDeclineMicrosPerMover == 0.0);
  REQUIRE(metrics.lastTrialAcceptMicrosPerMover > 0.0);
  REQUIRE_FALSE(metrics.lastTrialAccepted);

  // And that is not a one-off: the verdict is what the gate coasts on, so the
  // offload stays shut. Drive a stretch with no trial running and watch every
  // tick be handed back.
  const size_t declinedBefore = driver.declinedUpdates;
  const size_t acceptedBefore = driver.acceptedUpdates;
  for (int tick = 0; tick < 8; ++tick) {
    driver.RunTick(dispatcher, sink, 40, packet);
  }
  REQUIRE(driver.declinedUpdates > declinedBefore);
  // Some acceptance is expected even so -- a trial keeps alternating -- but
  // the standing verdict must be decline, which is what the gate uses between
  // trials.
  REQUIRE_FALSE(dispatcher.GetMetrics().lastTrialAccepted);
  REQUIRE(driver.acceptedUpdates - acceptedBefore <
          driver.declinedUpdates - declinedBefore);
}

TEST_CASE("A caller that never asks first is never trialled",
          "[ParallelTrial]")
{
  // attemptCountThisTick is incremented in WillAcceptThisTick and nowhere
  // else, so a caller that goes straight to SubmitMovement leaves the
  // dispatcher believing nobody tried to move. The trial declines to run on
  // an idle server, and this looks exactly like one.
  OffloadDispatcher dispatcher(TrialConfig());
  SilentSink sink;
  const std::vector<uint8_t> packet(64, 0xab);

  Driver driver;
  driver.asksFirst = false;
  for (int tick = 0; tick < 24; ++tick) {
    driver.RunTick(dispatcher, sink, 40, packet);
  }

  const ParallelMetrics& metrics = dispatcher.GetMetrics();
  REQUIRE(metrics.totalTrials == 0);
  REQUIRE(metrics.lastAttemptCount == 0);
  // The work still gets done -- SubmitMovement does not require the ask -- so
  // this is a measurement failure, not a correctness one.
  REQUIRE(driver.acceptedUpdates > 0);
  REQUIRE(sink.relays > 0);
}

TEST_CASE("The trial keeps whichever path the caller priced cheaper",
          "[ParallelTrial]")
{
  const std::vector<uint8_t> packet(64, 0xab);

  SECTION("declining is cheaper")
  {
    OffloadDispatcher dispatcher(TrialConfig());
    SilentSink sink;
    Driver driver;
    // Half a millisecond per update on the accepted path, a microsecond on
    // the declined one. Large enough that the dispatcher's own measured
    // phases cannot move the comparison.
    driver.acceptNanosPerUpdate = 500000;
    driver.declineNanosPerUpdate = 1000;
    for (int tick = 0; tick < 24; ++tick) {
      driver.RunTick(dispatcher, sink, 40, packet);
    }
    REQUIRE(dispatcher.GetMetrics().totalTrials >= 1);
    REQUIRE_FALSE(dispatcher.GetMetrics().lastTrialAccepted);
  }

  SECTION("accepting is cheaper")
  {
    OffloadDispatcher dispatcher(TrialConfig());
    SilentSink sink;
    Driver driver;
    driver.acceptNanosPerUpdate = 1000;
    driver.declineNanosPerUpdate = 500000;
    for (int tick = 0; tick < 24; ++tick) {
      driver.RunTick(dispatcher, sink, 40, packet);
    }
    REQUIRE(dispatcher.GetMetrics().totalTrials >= 1);
    REQUIRE(dispatcher.GetMetrics().lastTrialAccepted);
  }
}

TEST_CASE("A tie goes to engaging", "[ParallelTrial]")
{
  // The asymmetry is deliberate and measured: taking work on that did not
  // need it costs a few percent, missing a real crowd costs half the tick. So
  // equal prices -- and prices within 2% of each other -- keep the offload
  // engaged rather than shutting it.
  //
  // Ten milliseconds an update, which is absurd for a real update and is the
  // point: a tick's price is what the caller reported *plus* the dispatcher's
  // own measured phases, and only the first of those is under the test's
  // control. Reporting a number far larger than any parallel phase makes the
  // two arms tie to well inside the 2% band on a slow machine, under a
  // sanitizer, or on a host that is busy with something else.
  OffloadDispatcher dispatcher(TrialConfig());
  SilentSink sink;
  const std::vector<uint8_t> packet(64, 0xab);

  Driver driver;
  driver.acceptNanosPerUpdate = 10000000;
  driver.declineNanosPerUpdate = 10000000;
  for (int tick = 0; tick < 24; ++tick) {
    driver.RunTick(dispatcher, sink, 40, packet);
  }

  REQUIRE(dispatcher.GetMetrics().totalTrials >= 1);
  REQUIRE(dispatcher.GetMetrics().lastTrialAccepted);
}
