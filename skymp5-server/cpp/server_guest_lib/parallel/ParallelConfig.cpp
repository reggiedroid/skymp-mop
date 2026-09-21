#include "ParallelConfig.h"
#include "CoreCount.h"

#include <algorithm>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <thread>
#include <vector>

#ifdef _WIN32
#define NOMINMAX
#include <windows.h>
#if defined(_M_X64) || defined(_M_IX86)
#include <intrin.h>
#endif
#elif defined(__linux__)
#include <fstream>
#include <string>
#include <unordered_set>
#endif

namespace MpParallel {

namespace {

// Ceiling on the *auto-detected* worker count. An explicit workerThreads in
// server-settings.json is only bounded by kMaxWorkerThreads.
//
// This bounds two things at once, and the second is the one that matters.
//
// What actually decides the tick cost is how many workers a tick *involves*,
// which is the work-unit count, not how many threads exist. Measured by the
// `Idle threads` case on a 16-core/32-thread Ryzen 9950X3D at 400 players in
// one area, us/tick with the unit count pinned so only pool size varies:
//
//     pool size    4 units   8 units   16 units
//     4              622       610       615
//     8              628       544       551
//     16             628       547       583
//     24             627       550       572
//
// Down a column, 8 workers to 24 costs about 1%. Across a row, 4 units to 8 is
// worth 12%. Surplus threads park in the condition variable and are nearly
// free; at 150 players the residual is larger, around 10%, but still far below
// what the unit count is worth.
//
// So the cap earns its keep indirectly: the auto shard budget ceiling is
// `slots * 2`, so capping the pool at 8 caps the auto-sized unit count at 18,
// which measured at or near the optimum for every population tried. It also
// keeps the residual pool-size cost small on machines with many cores.
//
// The cap stays at 8, on measurement.
//
// It was briefly raised to 32, on the grounds that the 8 was an artefact of
// the wake-accounting bug and that "simulation and benchmarking on 16-core and
// AWS Graviton/Ice Lake systems" showed clean scaling past it. Neither holds:
// the 8 was measured *after* that bug was fixed, no Graviton or Ice Lake
// hardware was ever run, and perf_model.py -- the simulation in question --
// hardcodes `min(physical - 1, 8)` as its own worker rule, so it never modelled
// the change at all. Its per-platform IPC and barrier multipliers are
// hand-authored estimates, not calibrations.
//
// Re-measured on this machine, uncapping cost real time (us/tick, one chunk):
//
//     players            150     400
//     cap 8             150.6   615.4
//     cap 32            198.0   650.5
//
// At 400 players it raised the auto unit count from 18 to 31, which the
// `Idle threads` table already showed is the wrong direction.
//
// 8 is not a universal optimum and is not claimed to be one. It is the largest
// value with evidence behind it on the only hardware anyone has run. Operators
// on a bigger machine should run ParallelBenchmark and set workerThreads
// explicitly -- an explicit value is bounded only by kMaxWorkerThreads.
constexpr size_t kMaxAutoWorkerThreads = 8;

template <typename T>
T ReadNumber(const nlohmann::json& obj, const char* key, T fallback)
{
  auto it = obj.find(key);
  if (it == obj.end() || it->is_null()) {
    return fallback;
  }
  if (!it->is_number()) {
    throw std::runtime_error(
      fmt::format("parallelism.{} must be a number", key));
  }
  return it->get<T>();
}

bool ReadBool(const nlohmann::json& obj, const char* key, bool fallback)
{
  auto it = obj.find(key);
  if (it == obj.end() || it->is_null()) {
    return fallback;
  }
  if (!it->is_boolean()) {
    throw std::runtime_error(
      fmt::format("parallelism.{} must be a boolean", key));
  }
  return it->get<bool>();
}

}

void ParallelConfig::Normalize()
{
  if (workerThreads == 0) {
    // Physical cores, bounded by what this process may actually run on
    // (see CoreCount.h): an affinity mask or a cgroup quota makes the host's
    // core count the wrong answer. One core is left for the Node/V8 thread
    // that drives ScampServer::Tick.
    const size_t physical = GetPhysicalCoreCount();
    workerThreads = physical > 1 ? physical - 1 : 1;
    workerThreads = std::min(workerThreads, kMaxAutoWorkerThreads);
  }
  workerThreads = std::min(workerThreads, kMaxWorkerThreads);
  workerThreads = std::max<size_t>(workerThreads, 1);

  clusterSeparationChunks =
    std::max(clusterSeparationChunks, kMinSafeSeparationChunks);

  minClusterActors = std::max<size_t>(minClusterActors, 1);
  minActorsToOffload = std::max<size_t>(minActorsToOffload, 1);
  minShardActors = std::max<size_t>(minShardActors, 1);
  
  // No vendor branch here.
  //
  // A previous revision tripled minShardMicros on Intel parts, citing
  // "simulation and benchmarks show 55-95us is optimal for Ice Lake". No Intel
  // hardware was ever run; the figure comes from perf_model.py, whose Ice Lake
  // profile is a hand-written `ipc=0.75, barrier_scale=1.8` guess rather than
  // a calibration. Shipping a real behaviour change on a modelled constant is
  // how a server ends up slow for a reason nobody can reproduce.
  //
  // It was also unable to do what it claimed. The test `minShardMicros == 20`
  // cannot tell "the operator left the default" from "the operator measured
  // their hardware and chose 20", so it silently overrode explicit
  // configuration on every Intel host.
  //
  // The setting is already self-calibrating in the way that matters: the shard
  // budget divides *measured* per-actor cost by it, so a slower machine
  // naturally produces the same shard sizes in wall-clock terms. If a vendor
  // split turns out to be real, it needs a measurement on that vendor's
  // hardware first.
  minShardMicros = std::max<uint32_t>(minShardMicros, 1);

  // A spin longer than the tick period would keep every worker on a core for
  // the whole frame, which is the failure mode this is meant to avoid.
  workerSpinMicros = std::min<uint32_t>(workerSpinMicros, 5000);

  // Negative would accept everything; the disable value is exactly 0.
  minOffloadSpeedup = std::max(minOffloadSpeedup, 0.f);
  abTrialBlockTicks = std::max<uint32_t>(abTrialBlockTicks, 1);
  // At least one block per arm, or a "trial" would only ever measure one path.
  abTrialBlocks = std::max<uint32_t>(abTrialBlocks, 2);

  if (targetTickBudgetMicros == 0) {
    targetTickBudgetMicros = 8000;
  }

  throttleDistanceUnits = std::max(throttleDistanceUnits, 1.f);
  maxThrottleSkipTicks = std::min<uint32_t>(maxThrottleSkipTicks, 32);

  interestFullRateUnits = std::max(interestFullRateUnits, 1.f);
  maxInterestSkipTicks =
    std::min<uint32_t>(std::max<uint32_t>(maxInterestSkipTicks, 1), 32);
}

ParallelConfig ParallelConfig::FromServerSettings(
  const nlohmann::json& serverSettings)
{
  ParallelConfig config;

  auto it = serverSettings.find("parallelism");
  if (it == serverSettings.end() || it->is_null()) {
    config.Normalize();
    return config;
  }

  if (!it->is_object()) {
    throw std::runtime_error("parallelism must be an object");
  }

  const nlohmann::json& j = *it;

  config.enabled = ReadBool(j, "enabled", config.enabled);
  config.adaptiveParallelism =
    ReadBool(j, "adaptiveParallelism", config.adaptiveParallelism);
  config.minOffloadWorkMicros = ReadNumber<uint64_t>(
    j, "minOffloadWorkMicros", config.minOffloadWorkMicros);
  config.adaptiveProbeIntervalTicks = ReadNumber<uint32_t>(
    j, "adaptiveProbeIntervalTicks", config.adaptiveProbeIntervalTicks);
  config.minOffloadSpeedup =
    ReadNumber<float>(j, "minOffloadSpeedup", config.minOffloadSpeedup);
  config.abTrialIntervalTicks = ReadNumber<uint32_t>(
    j, "abTrialIntervalTicks", config.abTrialIntervalTicks);
  config.abTrialBlockTicks =
    ReadNumber<uint32_t>(j, "abTrialBlockTicks", config.abTrialBlockTicks);
  config.abTrialBlocks =
    ReadNumber<uint32_t>(j, "abTrialBlocks", config.abTrialBlocks);
  config.adaptiveThrottling =
    ReadBool(j, "adaptiveThrottling", config.adaptiveThrottling);
  config.interestManagement =
    ReadBool(j, "interestManagement", config.interestManagement);
  config.interestFullRateUnits = ReadNumber<float>(
    j, "interestFullRateUnits", config.interestFullRateUnits);
  config.maxInterestSkipTicks = ReadNumber<uint32_t>(
    j, "maxInterestSkipTicks", config.maxInterestSkipTicks);

  config.workerThreads =
    ReadNumber<size_t>(j, "workerThreads", config.workerThreads);
  config.minActorsToOffload =
    ReadNumber<size_t>(j, "minActorsToOffload", config.minActorsToOffload);
  config.minClusterActors =
    ReadNumber<size_t>(j, "minClusterActors", config.minClusterActors);
  config.minShardActors =
    ReadNumber<size_t>(j, "minShardActors", config.minShardActors);
  config.maxShardsPerCluster =
    ReadNumber<size_t>(j, "maxShardsPerCluster", config.maxShardsPerCluster);
  config.clusterSeparationChunks = ReadNumber<int32_t>(
    j, "clusterSeparationChunks", config.clusterSeparationChunks);
  config.maxWorkUnitsPerTick =
    ReadNumber<size_t>(j, "maxWorkUnitsPerTick", config.maxWorkUnitsPerTick);
  config.minShardMicros =
    ReadNumber<uint32_t>(j, "minShardMicros", config.minShardMicros);
  config.workerSpinMicros =
    ReadNumber<uint32_t>(j, "workerSpinMicros", config.workerSpinMicros);
  config.relayFromWorkers =
    ReadBool(j, "relayFromWorkers", config.relayFromWorkers);
  config.targetTickBudgetMicros = ReadNumber<uint64_t>(
    j, "targetTickBudgetMicros", config.targetTickBudgetMicros);
  config.throttleDistanceUnits =
    ReadNumber<float>(j, "throttleDistanceUnits", config.throttleDistanceUnits);
  config.maxThrottleSkipTicks =
    ReadNumber<uint32_t>(j, "maxThrottleSkipTicks", config.maxThrottleSkipTicks);
  config.metricsLogIntervalTicks = ReadNumber<uint32_t>(
    j, "metricsLogIntervalTicks", config.metricsLogIntervalTicks);

  config.Normalize();
  return config;
}

std::string ParallelConfig::Describe() const
{
  if (!enabled) {
    return "parallel area offload: disabled";
  }
  return fmt::format(
    "parallel area offload: enabled, workerThreads={}, "
    "minActorsToOffload={}, adaptiveParallelism={}, minClusterActors={}, minShardActors={}, "
    "minShardMicros={}, spin={}us, separation={} chunks, "
    "interestManagement={} (fullRate={}u, maxSkip={}), "
    "adaptiveThrottling={}, budget={}us",
    workerThreads, minActorsToOffload, adaptiveParallelism ? "on" : "off", minClusterActors, minShardActors,
    minShardMicros, workerSpinMicros, clusterSeparationChunks,
    interestManagement ? "on" : "off", interestFullRateUnits,
    maxInterestSkipTicks, adaptiveThrottling ? "on" : "off",
    targetTickBudgetMicros);
}

}
