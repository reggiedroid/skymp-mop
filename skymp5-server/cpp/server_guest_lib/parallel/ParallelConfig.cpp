#include "ParallelConfig.h"

#include <algorithm>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <thread>

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
// An earlier version of this comment claimed the fall-off past 8 was cache
// topology (two 8-core chiplets with separate L3). That was wrong: the
// evidence cited for it was measured before the wake-accounting fix in the
// same change, where Run re-woke workers Prime had already woken, so the
// large-pool figure was paying surplus thread wakeups rather than cross-die
// transfers. With the unit count pinned there is no such cliff.
//
// Operators on other hardware should run the benchmark and set workerThreads
// explicitly.
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
    // hardware_concurrency counts logical processors, so on an SMT machine
    // half of them share execution units with the other half. A worker here
    // alternates between a pause loop and streaming writes, which is close to
    // the worst case for an SMT sibling, so the estimate is in physical
    // cores. One of those is then left for the Node/V8 thread that drives
    // ScampServer::Tick.
    const size_t detected = std::thread::hardware_concurrency();
    const size_t physical = detected > 1 ? detected / 2 : 1;
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
  minShardMicros = std::max<uint32_t>(minShardMicros, 1);

  // A spin longer than the tick period would keep every worker on a core for
  // the whole frame, which is the failure mode this is meant to avoid.
  workerSpinMicros = std::min<uint32_t>(workerSpinMicros, 5000);

  // Prevent division-by-zero in the adaptive decay modulo check.
  adaptiveDecayTicks = std::max<uint32_t>(adaptiveDecayTicks, 1);
  // A bias below 1.0 would permanently disable offloading.
  adaptiveBias = std::max(adaptiveBias, 1.0f);
  adaptiveThresholdFloor = std::max<size_t>(adaptiveThresholdFloor, 1);

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
  config.adaptiveBias =
    ReadNumber<float>(j, "adaptiveBias", config.adaptiveBias);
  config.adaptiveDecayTicks =
    ReadNumber<uint32_t>(j, "adaptiveDecayTicks", config.adaptiveDecayTicks);
  config.adaptiveThresholdFloor =
    ReadNumber<size_t>(j, "adaptiveThresholdFloor", config.adaptiveThresholdFloor);
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
