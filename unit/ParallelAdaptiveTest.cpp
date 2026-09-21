#include "parallel/OffloadDispatcher.h"
#include "parallel/ParallelConfig.h"

#include <catch2/catch_all.hpp>

using MpParallel::ParallelConfig;
using MpParallel::detail::AdaptiveState;
using MpParallel::detail::AdaptiveTickInput;
using MpParallel::detail::StepAdaptiveThreshold;

namespace {

constexpr size_t kFloor = 100;

AdaptiveTickInput Tick(uint64_t tickIndex)
{
  AdaptiveTickInput in;
  in.tickIndex = tickIndex;
  in.configuredThreshold = kFloor;
  in.bias = 1.05f;
  in.decayTicks = 10;
  return in;
}

// A tick that used the pool and spent `parallelMicros` of wall clock on
// `aggregateTaskMicros` of distributed work.
AdaptiveTickInput Offloaded(uint64_t tickIndex, size_t actors,
                            uint64_t parallelMicros,
                            uint64_t aggregateTaskMicros)
{
  AdaptiveTickInput in = Tick(tickIndex);
  in.offloaded = true;
  in.actorCount = actors;
  in.parallelMicros = parallelMicros;
  in.aggregateTaskMicros = aggregateTaskMicros;
  return in;
}

AdaptiveTickInput Inline(uint64_t tickIndex, size_t actors)
{
  AdaptiveTickInput in = Tick(tickIndex);
  in.offloaded = false;
  in.actorCount = actors;
  return in;
}

AdaptiveState Start(size_t threshold = kFloor)
{
  AdaptiveState state;
  state.threshold = threshold;
  return state;
}

}

TEST_CASE("Adaptive parallelism is off unless asked for", "[ParallelOffload]")
{
  // It is a control loop keyed on wall-clock timing, so with it on the
  // dispatcher's offload decision depends on how loaded the machine was a
  // tick ago. That is incompatible with "deterministic when enabled", which
  // is the property the rest of this subsystem is built to hold, so it is
  // opt-in until a benchmark on real hardware says otherwise.
  ParallelConfig config;
  REQUIRE(config.adaptiveParallelism == false);
}

TEST_CASE("A bias below 1.0 is clamped away", "[ParallelConfig]")
{
  // Below 1.0 the controller would demand the fork/join phase beat the work
  // it distributed, which nothing can do, so every offloaded tick would
  // count against the pool and the threshold would ratchet up for good.
  ParallelConfig config;
  config.adaptiveBias = 0.5f;
  config.adaptiveDecayTicks = 0;
  config.Normalize();
  REQUIRE(config.adaptiveBias >= 1.0f);
  REQUIRE(config.adaptiveDecayTicks >= 1);
}

TEST_CASE("A winning tick leaves the threshold alone", "[ParallelOffload]")
{
  // 400us of wall clock for 3000us of work is a 7.5x speedup.
  const AdaptiveState next =
    StepAdaptiveThreshold(Start(), Offloaded(1, 400, 400, 3000));
  REQUIRE(next.threshold == kFloor);
  REQUIRE(next.disappointingStreak == 0);
}

TEST_CASE("The join is not charged to the offload", "[ParallelOffload]")
{
  // The regression this guards: the original loop compared
  // parallelMicros + joinMicros against the task work alone, so a tick where
  // the fork/join phase turned 966us of work into 96us (a tenfold win, the
  // measured figure at 400 players) still looked like a loss as soon as the
  // join was large, and the join is largest exactly when the offload is
  // winning most. Here the parallel phase is a clear win and must be read as
  // one, whatever the join cost alongside it.
  AdaptiveState state = Start();
  for (uint64_t tick = 1; tick <= 10; ++tick) {
    state = StepAdaptiveThreshold(state, Offloaded(tick, 400, 96, 966));
  }
  REQUIRE(state.threshold == kFloor);
  REQUIRE(state.disappointingStreak == 0);
}

TEST_CASE("One disappointing tick is a hiccup, not a verdict",
          "[ParallelOffload]")
{
  // A GC pause on the Node thread, or another process taking a core for a
  // millisecond, must not suspend the pool.
  AdaptiveState state = Start();
  state = StepAdaptiveThreshold(state, Offloaded(1, 400, 3000, 1000));
  REQUIRE(state.threshold == kFloor);
  REQUIRE(state.disappointingStreak == 1);

  state = StepAdaptiveThreshold(state, Offloaded(2, 400, 3000, 1000));
  REQUIRE(state.threshold == kFloor);
  REQUIRE(state.disappointingStreak == 2);

  // Three in a row is a pattern.
  state = StepAdaptiveThreshold(state, Offloaded(3, 400, 3000, 1000));
  REQUIRE(state.threshold == 401);
  REQUIRE(state.disappointingStreak == 0);
}

TEST_CASE("A single good tick clears the streak", "[ParallelOffload]")
{
  AdaptiveState state = Start();
  state = StepAdaptiveThreshold(state, Offloaded(1, 400, 3000, 1000));
  state = StepAdaptiveThreshold(state, Offloaded(2, 400, 3000, 1000));
  REQUIRE(state.disappointingStreak == 2);

  state = StepAdaptiveThreshold(state, Offloaded(3, 400, 100, 1000));
  REQUIRE(state.disappointingStreak == 0);

  // So the next two bad ticks start over rather than tipping it.
  state = StepAdaptiveThreshold(state, Offloaded(4, 400, 3000, 1000));
  state = StepAdaptiveThreshold(state, Offloaded(5, 400, 3000, 1000));
  REQUIRE(state.threshold == kFloor);
}

TEST_CASE("The bias tolerates the offload being slightly slower",
          "[ParallelOffload]")
{
  // 1.05 means a fork/join phase costing up to about 5% more wall clock than
  // the work it distributed is still acceptable. Only the interior of the
  // tolerance is asserted: the exact boundary lands wherever float rounding
  // of the bias puts it, and a test that pinned it would be asserting an
  // artifact rather than a contract.
  AdaptiveState state = Start();
  for (uint64_t tick = 1; tick <= 6; ++tick) {
    state = StepAdaptiveThreshold(state, Offloaded(tick, 400, 1040, 1000));
  }
  REQUIRE(state.threshold == kFloor);
  REQUIRE(state.disappointingStreak == 0);

  for (uint64_t tick = 7; tick <= 9; ++tick) {
    state = StepAdaptiveThreshold(state, Offloaded(tick, 400, 1200, 1000));
  }
  REQUIRE(state.threshold == 401);
}

TEST_CASE("The controller is never less conservative than configured",
          "[ParallelOffload]")
{
  // The version this replaces decayed toward its own floor of 30, so an
  // operator who had measured 100 on their hardware and written it down
  // would silently get 30. A controller that can only ever raise the
  // threshold is a controller whose worst case an operator can reason about.
  AdaptiveState state = Start();
  for (uint64_t tick = 1; tick <= 400; ++tick) {
    state = StepAdaptiveThreshold(state, Inline(tick, 20));
    REQUIRE(state.threshold >= kFloor);
  }
  REQUIRE(state.threshold == kFloor);

  // And a threshold somehow below the floor is pulled back up to it.
  AdaptiveState low = Start(5);
  low = StepAdaptiveThreshold(low, Inline(1, 20));
  REQUIRE(low.threshold == kFloor);
}

TEST_CASE("Recovery halves the excess instead of crawling",
          "[ParallelOffload]")
{
  // Stepping down by one every ten ticks walks a 400-player back-off home in
  // about 3100 ticks, most of a minute at 60Hz, and `Cost of a wrong
  // offload threshold` measured that state at 15% to 19% worse than not
  // enabling the feature at all. Halving gets there in tens of ticks.
  AdaptiveState state = Start(401);

  // Only on a decay boundary.
  state = StepAdaptiveThreshold(state, Inline(11, 40));
  REQUIRE(state.threshold == 401);

  state = StepAdaptiveThreshold(state, Inline(20, 40));
  REQUIRE(state.threshold == 251);
  state = StepAdaptiveThreshold(state, Inline(30, 40));
  REQUIRE(state.threshold == 176);

  uint64_t tick = 40;
  while (state.threshold > kFloor && tick < 2000) {
    state = StepAdaptiveThreshold(state, Inline(tick, 40));
    tick += 10;
  }
  REQUIRE(state.threshold == kFloor);
  // Under 200 ticks, against roughly 3100 for the one-step-per-window
  // version.
  REQUIRE(tick < 240);
}

TEST_CASE("A raise cannot drop the threshold below the configured value",
          "[ParallelOffload]")
{
  // A disappointing tick at a population under the configured threshold
  // would otherwise "raise" it to actors + 1 and leave it lower than the
  // operator asked for.
  AdaptiveState state = Start();
  for (uint64_t tick = 1; tick <= 3; ++tick) {
    state = StepAdaptiveThreshold(state, Offloaded(tick, 10, 3000, 1000));
  }
  REQUIRE(state.threshold == kFloor);
}
