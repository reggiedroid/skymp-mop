#include "parallel/ThreadPool.h"
#include <array>
#include <atomic>
#include <catch2/catch_all.hpp>
#include <chrono>
#include <deque>
#include <mutex>
#include <numeric>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

using MpParallel::ThreadPool;

TEST_CASE("Zero workers still runs every task on the caller", "[ParallelPool]")
{
  ThreadPool pool(0);
  REQUIRE(pool.GetWorkerCount() == 0);
  REQUIRE(pool.GetSlotCount() == 1);

  std::vector<int> ran(16, 0);
  std::vector<ThreadPool::Task> tasks;
  for (size_t i = 0; i < ran.size(); ++i) {
    tasks.emplace_back([&ran, i](size_t slot) {
      REQUIRE(slot == 0);
      ran[i] = 1;
    });
  }

  pool.Run(tasks);

  REQUIRE(std::accumulate(ran.begin(), ran.end(), 0) ==
          static_cast<int>(ran.size()));
}

TEST_CASE("Every task runs exactly once", "[ParallelPool]")
{
  ThreadPool pool(4);

  constexpr size_t kTaskCount = 500;
  std::vector<std::atomic<int>> counters(kTaskCount);
  for (auto& counter : counters) {
    counter.store(0);
  }

  std::vector<ThreadPool::Task> tasks;
  tasks.reserve(kTaskCount);
  for (size_t i = 0; i < kTaskCount; ++i) {
    tasks.emplace_back(
      [&counters, i](size_t) { counters[i].fetch_add(1); });
  }

  pool.Run(tasks);

  for (size_t i = 0; i < kTaskCount; ++i) {
    REQUIRE(counters[i].load() == 1);
  }
}

TEST_CASE("Slot indices stay inside the advertised range", "[ParallelPool]")
{
  ThreadPool pool(3);
  REQUIRE(pool.GetSlotCount() == 4);

  std::mutex mutex;
  std::set<size_t> seenSlots;

  std::vector<ThreadPool::Task> tasks;
  for (size_t i = 0; i < 400; ++i) {
    tasks.emplace_back([&](size_t slot) {
      REQUIRE(slot < 4);
      std::lock_guard<std::mutex> lock(mutex);
      seenSlots.insert(slot);
    });
  }

  pool.Run(tasks);

  // The caller always participates, so slot 0 must show up.
  REQUIRE(seenSlots.count(0) == 1);
}

TEST_CASE("Repeated batches all complete", "[ParallelPool]")
{
  ThreadPool pool(4);
  std::atomic<int> total{ 0 };

  for (int round = 0; round < 50; ++round) {
    std::vector<ThreadPool::Task> tasks;
    for (int i = 0; i < 20; ++i) {
      tasks.emplace_back([&total](size_t) { total.fetch_add(1); });
    }
    pool.Run(tasks);
    // The barrier must have drained before Run returns.
    REQUIRE(total.load() == (round + 1) * 20);
  }
}

TEST_CASE("Back-to-back batches never bleed into each other",
          "[ParallelPool]")
{
  // Regression: Run used to return as soon as tasksRemaining hit zero, while
  // the worker that ran the final task was still looping on the cursor. The
  // next batch reset that cursor, so the straggler could re-run a task from
  // the previous vector and decrement the new batch's counter. Each round
  // here uses a fresh vector of fresh closures, so a straggler that reached
  // back into the old batch shows up as a wrong per-round total.
  ThreadPool pool(4);

  for (int round = 0; round < 400; ++round) {
    std::atomic<int> ran{ 0 };
    std::vector<ThreadPool::Task> tasks;
    // Deliberately tiny: the shorter the batch, the likelier a worker is
    // still in the drain loop when Run wants to return.
    for (int i = 0; i < 3; ++i) {
      tasks.emplace_back([&ran](size_t) { ran.fetch_add(1); });
    }
    pool.Run(tasks);
    REQUIRE(ran.load() == 3);
  }
}

TEST_CASE("Alternating batch sizes stay consistent", "[ParallelPool]")
{
  // A long batch followed by a very short one is the shape most likely to
  // leave a worker draining across the boundary.
  ThreadPool pool(4);

  for (int round = 0; round < 120; ++round) {
    const int count = (round % 2 == 0) ? 200 : 1;
    std::atomic<int> ran{ 0 };
    std::vector<ThreadPool::Task> tasks;
    for (int i = 0; i < count; ++i) {
      tasks.emplace_back([&ran](size_t) { ran.fetch_add(1); });
    }
    pool.Run(tasks);
    REQUIRE(ran.load() == count);
  }
}

TEST_CASE("Growing batches never let a straggler cross the boundary",
          "[ParallelPool]")
{
  // The shape `Alternating batch sizes stay consistent` cannot reach. Its
  // short batch is a single task, which Run executes inline without touching
  // the cursor, so every transition it puts through the claim protocol is
  // 200 -> 200. The dangerous transition is a pooled batch followed by a
  // *larger* one: a straggler from the small batch that loaded the large
  // batch's task count while the cursor still named its own generation used
  // to pass both claim tests, run a task of the next batch and bump that
  // batch's completion counter from outside the protocol. The counter then
  // overshot, the equality Run waits on was never true again, and the tick
  // hung: the failure this pool's claim protocol was built to rule out.
  //
  // A regression here is as likely to hang as to fail an assertion.
  //
  // Oversubscribed on purpose: the window needs a worker preempted between
  // reading the generation and reading the count, and a pool with twice as
  // many threads as cores makes that routine. Against the untagged count this
  // case hung on 20 of 20 runs on a 2-core host; at 300 rounds it was 12 of
  // 20, which is too weak to guard a regression with. It costs about 50ms.
  constexpr int kRounds = 2000;
  constexpr int kSmall = 2;
  constexpr int kLarge = 40;

  // Outlive every round and the pool itself, so a straggler reaching into a
  // finished batch shows up as a wrong count rather than as undefined
  // behaviour that could hide it.
  std::vector<std::array<std::atomic<int>, kLarge>> runs(kRounds);
  std::deque<std::vector<ThreadPool::Task>> batches;

  {
    const size_t cores =
      std::max<size_t>(1, std::thread::hardware_concurrency());
    // Capped at 32, the largest pool the configuration can ask for.
    ThreadPool pool(std::min<size_t>(cores * 2, 32));

    for (int round = 0; round < kRounds; ++round) {
      const int count = (round % 2 == 0) ? kSmall : kLarge;
      auto& tasks = batches.emplace_back();
      for (int i = 0; i < count; ++i) {
        tasks.emplace_back(
          [&runs, round, i](size_t) { runs[round][i].fetch_add(1); });
      }
      pool.Run(tasks);

      // Run may not return until every task of its own batch has finished,
      // and each of them exactly once.
      for (int i = 0; i < count; ++i) {
        REQUIRE(runs[round][i].load() == 1);
      }
    }
  }

  // The pool is gone, so every worker has been joined. A straggler that ran a
  // task after its round was checked would have left a count of two.
  for (int round = 0; round < kRounds; ++round) {
    const int count = (round % 2 == 0) ? kSmall : kLarge;
    for (int i = 0; i < count; ++i) {
      REQUIRE(runs[round][i].load() == 1);
    }
  }
}

TEST_CASE("A throwing task does not wedge the barrier", "[ParallelPool]")
{
  ThreadPool pool(3);

  std::atomic<int> completed{ 0 };
  std::vector<ThreadPool::Task> tasks;
  for (int i = 0; i < 60; ++i) {
    tasks.emplace_back([&completed, i](size_t) {
      if (i % 10 == 0) {
        throw std::runtime_error("boom");
      }
      completed.fetch_add(1);
    });
  }

  // Would hang rather than fail if the failure path skipped its decrement.
  pool.Run(tasks);

  REQUIRE(completed.load() == 54);
  REQUIRE(pool.GetFailedTaskCount() == 6);

  // The pool stays usable afterwards.
  std::atomic<int> after{ 0 };
  std::vector<ThreadPool::Task> more;
  for (int i = 0; i < 10; ++i) {
    more.emplace_back([&after](size_t) { after.fetch_add(1); });
  }
  pool.Run(more);
  REQUIRE(after.load() == 10);
}

TEST_CASE("Empty batch is a no-op", "[ParallelPool]")
{
  ThreadPool pool(2);
  std::vector<ThreadPool::Task> tasks;
  pool.Run(tasks);
  REQUIRE(pool.GetFailedTaskCount() == 0);
}

TEST_CASE("Priming does not change results", "[ParallelPool]")
{
  // Prime is a scheduling hint and nothing else. Every combination of primed
  // and not, correct hint and wrong hint, must produce the same work.
  ThreadPool pool(4);

  for (size_t hint : { size_t(0), size_t(1), size_t(2), size_t(8),
                       size_t(1000) }) {
    for (int round = 0; round < 30; ++round) {
      std::atomic<int> ran{ 0 };
      std::vector<ThreadPool::Task> tasks;
      for (int i = 0; i < 7; ++i) {
        tasks.emplace_back([&ran](size_t) { ran.fetch_add(1); });
      }
      pool.Prime(hint);
      pool.Run(tasks);
      REQUIRE(ran.load() == 7);
    }
  }
}

TEST_CASE("Priming without a batch leaves the pool usable", "[ParallelPool]")
{
  // A tick can prime and then submit nothing -- every movement packet may be
  // declined. Workers must spin out their budget and park again rather than
  // consuming a batch that never came.
  ThreadPool pool(3);

  for (int round = 0; round < 20; ++round) {
    pool.Prime(4);
  }

  std::atomic<int> ran{ 0 };
  std::vector<ThreadPool::Task> tasks;
  for (int i = 0; i < 12; ++i) {
    tasks.emplace_back([&ran](size_t) { ran.fetch_add(1); });
  }
  pool.Run(tasks);
  REQUIRE(ran.load() == 12);
  REQUIRE(pool.GetFailedTaskCount() == 0);
}

TEST_CASE("The claim protocol runs every task exactly once", "[ParallelPool]")
{
  // The claim protocol is a compare-exchange over a packed
  // (generation, index) word. A lost exchange must retry, not skip, and a
  // straggler from the previous batch must not consume an index from this
  // one. Both would show up here as a task index seen zero or twice.
  ThreadPool pool(6);

  for (int round = 0; round < 200; ++round) {
    constexpr int kCount = 37;
    std::vector<std::atomic<int>> seen(kCount);
    for (auto& counter : seen) {
      counter.store(0);
    }

    std::vector<ThreadPool::Task> tasks;
    for (int i = 0; i < kCount; ++i) {
      tasks.emplace_back([&seen, i](size_t) { seen[i].fetch_add(1); });
    }
    pool.Prime(kCount);
    pool.Run(tasks);

    for (int i = 0; i < kCount; ++i) {
      REQUIRE(seen[i].load() == 1);
    }
  }
}

TEST_CASE("A pool that never spins still completes", "[ParallelPool]")
{
  // workerSpinMicros of 0 is the documented escape hatch, and it takes the
  // pool down a different path: no worker is ever awake when Run publishes,
  // so every batch goes through the condition variable.
  ThreadPool pool(4, 0);

  for (int round = 0; round < 60; ++round) {
    std::atomic<int> ran{ 0 };
    std::vector<ThreadPool::Task> tasks;
    for (int i = 0; i < 9; ++i) {
      tasks.emplace_back([&ran](size_t) { ran.fetch_add(1); });
    }
    pool.Prime(9);
    pool.Run(tasks);
    REQUIRE(ran.load() == 9);
  }
}

TEST_CASE("Uneven task costs still drain", "[ParallelPool]")
{
  // Dynamic scheduling exists so that one long task cannot leave the other
  // slots idle. This checks the barrier copes with a lopsided batch.
  ThreadPool pool(4);

  std::atomic<int> done{ 0 };
  std::vector<ThreadPool::Task> tasks;
  tasks.emplace_back([&done](size_t) {
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    done.fetch_add(1);
  });
  for (int i = 0; i < 100; ++i) {
    tasks.emplace_back([&done](size_t) { done.fetch_add(1); });
  }

  pool.Run(tasks);
  REQUIRE(done.load() == 101);
}
