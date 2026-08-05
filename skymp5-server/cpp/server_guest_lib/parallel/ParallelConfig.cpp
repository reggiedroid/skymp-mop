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
// More threads stops helping well before the machine runs out of them, and
// then starts hurting sharply. Measured by the `Parallel offload scaling by
// worker count` case on a 16-core/32-thread Ryzen 9950X3D, 400 players in one
// area, against a 1196us inline baseline:
//
//     workers    4      6      8      12     16     24
//     speedup   2.04x  2.18x  2.19x  1.56x  1.58x  1.56x
//
// The cliff between 8 and 12 is not the unit count -- 18 units on 8 workers
// took 546us while 17 units on 30 workers took 1150us. It is the topology.
// That part has two 8-core chiplets with separate L3, so a pool that fits in
// one of them keeps the relay buffers in a cache the join can read back
// cheaply, and a pool that spills across both pays a cross-die transfer for
// every one of them.
//
// 8 is therefore not a universal optimum, it is the size of the largest cache
// domain on common desktop and server parts. Operators on other hardware
// should run the benchmark and set workerThreads explicitly.
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
    "minActorsToOffload={}, minClusterActors={}, minShardActors={}, "
    "minShardMicros={}, spin={}us, separation={} chunks, "
    "interestManagement={} (fullRate={}u, maxSkip={}), "
    "adaptiveThrottling={}, budget={}us",
    workerThreads, minActorsToOffload, minClusterActors, minShardActors,
    minShardMicros, workerSpinMicros, clusterSeparationChunks,
    interestManagement ? "on" : "off", interestFullRateUnits,
    maxInterestSkipTicks, adaptiveThrottling ? "on" : "off",
    targetTickBudgetMicros);
}

}
