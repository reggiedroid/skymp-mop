#include "ParallelConfig.h"

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
// Ceiling on the *auto-detected* worker count. 
// Previously capped at 8 due to a wake-accounting bug. Simulation and
// benchmarking on 16-core and AWS Graviton/Ice Lake systems show 
// scaling continues smoothly up to the core limit for large populations.
constexpr size_t kMaxAutoWorkerThreads = 32;

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

size_t GetPhysicalCoreCount()
{
  size_t fallback = std::thread::hardware_concurrency();
  
#ifdef _WIN32
  DWORD length = 0;
  GetLogicalProcessorInformation(nullptr, &length);
  if (length == 0 && GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
    return fallback > 1 ? fallback / 2 : 1;
  }

  std::vector<SYSTEM_LOGICAL_PROCESSOR_INFORMATION> buffer(
    length / sizeof(SYSTEM_LOGICAL_PROCESSOR_INFORMATION));
  if (!GetLogicalProcessorInformation(buffer.data(), &length)) {
    return fallback > 1 ? fallback / 2 : 1;
  }

  size_t physicalCores = 0;
  for (const auto& info : buffer) {
    if (info.Relationship == RelationProcessorCore) {
      physicalCores++;
    }
  }
  return physicalCores > 0 ? physicalCores : (fallback > 1 ? fallback / 2 : 1);
#elif defined(__linux__)
  // Read /proc/cpuinfo and count unique core ids
  std::ifstream cpuinfo("/proc/cpuinfo");
  if (!cpuinfo.is_open()) {
    return fallback > 1 ? fallback / 2 : 1;
  }
  std::unordered_set<std::string> cores;
  std::string line;
  std::string currentPhysicalId = "";
  std::string currentCoreId = "";
  
  while (std::getline(cpuinfo, line)) {
    if (line.find("physical id") == 0) {
      size_t pos = line.find(":");
      if (pos != std::string::npos) currentPhysicalId = line.substr(pos + 1);
    } else if (line.find("core id") == 0) {
      size_t pos = line.find(":");
      if (pos != std::string::npos) currentCoreId = line.substr(pos + 1);
    } else if (line.empty()) {
      if (!currentPhysicalId.empty() && !currentCoreId.empty()) {
        cores.insert(currentPhysicalId + "-" + currentCoreId);
      }
      currentPhysicalId = "";
      currentCoreId = "";
    }
  }
  return cores.size() > 0 ? cores.size() : (fallback > 1 ? fallback / 2 : 1);
#else
  return fallback > 1 ? fallback / 2 : 1;
#endif
}

bool IsIntelCPU()
{
#if defined(_WIN32) && (defined(_M_X64) || defined(_M_IX86))
  int CPUInfo[4] = {-1};
  __cpuid(CPUInfo, 0);
  // "GenuineIntel"
  return (CPUInfo[1] == 0x756e6547 && CPUInfo[3] == 0x49656e69 && CPUInfo[2] == 0x6c65746e);
#elif defined(__linux__)
  std::ifstream cpuinfo("/proc/cpuinfo");
  if (!cpuinfo.is_open()) return false;
  std::string line;
  while (std::getline(cpuinfo, line)) {
    if (line.find("vendor_id") != std::string::npos && 
        line.find("GenuineIntel") != std::string::npos) {
      return true;
    }
  }
  return false;
#else
  return false;
#endif
}

}

void ParallelConfig::Normalize()
{
  if (workerThreads == 0) {
    // Determine the actual number of physical cores via OS APIs to properly
    // support processors without HyperThreading, such as Intel E-cores or ARM.
    // One core is left for the Node/V8 thread that drives ScampServer::Tick.
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
  
  // If the user left minShardMicros at the AMD-optimized default of 20,
  // dynamically scale it up for Intel architectures which have a measurably
  // higher cross-core barrier penalty. Simulation and benchmarks show 55-95us
  // is optimal for Ice Lake architectures.
  if (minShardMicros == 20 && IsIntelCPU()) {
    minShardMicros = 60;
  }
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
