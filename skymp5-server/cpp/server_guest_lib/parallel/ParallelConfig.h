#pragma once
// For kMinSafeSeparationChunks, which Normalize clamps against.
#include "AreaKey.h"
#include <cstddef>
#include <cstdint>
#include <nlohmann/json_fwd.hpp>
#include <string>

namespace MpParallel {

constexpr size_t kMaxWorkerThreads = 32;

// Parsed from the "parallelism" object of server-settings.json.
//
// The framework is opt-in. With `enabled` false every entry point becomes a
// no-op and the server behaves exactly as it did before, which keeps the
// default deployment risk at zero.
struct ParallelConfig
{
  bool enabled = false;

  // 0 means "auto": hardware_concurrency() minus two reserved cores (one for
  // the Node/V8 main thread, one for the async save-storage thread), clamped
  // to [1, kMaxWorkerThreads].
  size_t workerThreads = 0;

  // Below this many tracked actors the fork/join barrier costs more than the
  // work it distributes, so the dispatcher runs everything inline.
  size_t minActorsToOffload = 24;

  // Clusters smaller than this are merged into the inline residual batch
  // rather than being scheduled as their own task.
  size_t minClusterActors = 4;

  // Fewest actors a shard of a large cluster may carry.
  //
  // A single crowded area is typically most of a tick's work, so a scheme
  // that could only parallelise across areas would be capped at whatever
  // fraction the quiet areas contribute. Splitting a busy cluster into
  // several ranges is what makes the offload scale with core count instead
  // of with the number of populated areas.
  size_t minShardActors = 4;

  // Upper bound on how many shards one cluster may be split into.
  // 0 means "auto": twice the slot count, which gives the dynamic scheduler
  // enough pieces to balance without paying for needless task overhead.
  size_t maxShardsPerCluster = 0;

  // Chebyshev distance, in 4096-unit chunks, that must separate two clusters.
  // Clamped up to kMinSafeSeparationChunks.
  int32_t clusterSeparationChunks = 4;

  // 0 means unlimited. Work units beyond the limit are processed inline on
  // the calling thread instead of going through the scheduler.
  size_t maxWorkUnitsPerTick = 0;

  // How often the partition is rebuilt. Between rebuilds the previous
  // partition is reused, which is safe because actors are re-bucketed by
  // their live chunk every tick and a stale cluster only costs balance
  // quality, never correctness.
  uint32_t repartitionIntervalTicks = 30;

  // Degrade relay frequency for distant neighbours when a cluster exceeds its
  // share of the tick budget, instead of letting the whole server stall.
  bool adaptiveThrottling = true;

  // Per-tick wall-clock target for the parallel phase, in microseconds.
  uint64_t targetTickBudgetMicros = 8000;

  // Squared distance beyond which a relay becomes eligible for throttling.
  // Default is one exterior cell (4096 units).
  float throttleDistanceUnits = 4096.f;

  // Maximum number of ticks a throttled relay may be held back.
  uint32_t maxThrottleSkipTicks = 3;

  // Emit a per-tick summary line at this interval. 0 disables reporting.
  uint32_t metricsLogIntervalTicks = 0;

  // Resolves workerThreads==0 to a concrete count and clamps every field to
  // its documented range. Idempotent.
  void Normalize();

  // Reads the "parallelism" object if present. Unknown keys are ignored,
  // missing keys keep their defaults, and a malformed value throws
  // std::runtime_error naming the offending key.
  static ParallelConfig FromServerSettings(const nlohmann::json& serverSettings);

  // Human-readable one-liner for the startup log.
  [[nodiscard]] std::string Describe() const;
};

}
