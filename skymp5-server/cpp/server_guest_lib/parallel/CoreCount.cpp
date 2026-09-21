#include "CoreCount.h"

#include <algorithm>
#include <fstream>
#include <istream>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_set>

#ifdef _WIN32
#  ifndef NOMINMAX
#    define NOMINMAX
#  endif
#  include <windows.h>
#elif defined(__linux__)
#  include <sched.h>
#  include <unistd.h>
#elif defined(__APPLE__)
#  include <sys/sysctl.h>
#endif

namespace MpParallel {

namespace detail {

namespace {

std::string TrimmedValueAfterColon(const std::string& line)
{
  const size_t colon = line.find(':');
  if (colon == std::string::npos) {
    return std::string();
  }
  size_t begin = line.find_first_not_of(" \t", colon + 1);
  if (begin == std::string::npos) {
    return std::string();
  }
  const size_t end = line.find_last_not_of(" \t\r");
  return line.substr(begin, end - begin + 1);
}

bool StartsWithKey(const std::string& line, const char* key)
{
  return line.rfind(key, 0) == 0;
}

}

size_t CountCoresFromCpuInfo(std::istream& cpuinfo)
{
  std::unordered_set<std::string> cores;
  std::string line;
  std::string physicalId;
  std::string coreId;

  // A block ends at a blank line or at end of file. The end-of-file case is
  // the one worth spelling out: Linux does terminate /proc/cpuinfo with a
  // blank line, but a captured copy of one often does not, and dropping the
  // last processor silently undercounts by one core.
  const auto flush = [&]() {
    if (!physicalId.empty() && !coreId.empty()) {
      cores.insert(physicalId + "-" + coreId);
    }
    physicalId.clear();
    coreId.clear();
  };

  while (std::getline(cpuinfo, line)) {
    if (StartsWithKey(line, "physical id")) {
      physicalId = TrimmedValueAfterColon(line);
    } else if (StartsWithKey(line, "core id")) {
      coreId = TrimmedValueAfterColon(line);
    } else if (line.find_first_not_of(" \t\r") == std::string::npos) {
      flush();
    }
  }
  flush();

  return cores.size();
}

size_t CountCoresFromSiblingLists(const std::vector<std::string>& lists)
{
  std::unordered_set<std::string> distinct;
  for (const auto& list : lists) {
    const size_t begin = list.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
      continue;
    }
    const size_t end = list.find_last_not_of(" \t\r\n");
    distinct.insert(list.substr(begin, end - begin + 1));
  }
  return distinct.size();
}

size_t CgroupCpusFromQuota(long long quota, long long period)
{
  if (quota <= 0 || period <= 0) {
    return 0;
  }
  // Round up: a 1.5-CPU quota should not be read as one core.
  const long long cpus = (quota + period - 1) / period;
  return cpus > 0 ? static_cast<size_t>(cpus) : 0;
}

size_t ParseCgroupV2CpuMax(const std::string& contents)
{
  std::istringstream in(contents);
  std::string quotaText;
  long long period = 0;
  if (!(in >> quotaText)) {
    return 0;
  }
  if (quotaText == "max") {
    return 0;
  }
  if (!(in >> period)) {
    return 0;
  }
  try {
    return CgroupCpusFromQuota(std::stoll(quotaText), period);
  } catch (const std::exception&) {
    return 0;
  }
}

}

namespace {

size_t HardwareConcurrencyOrOne()
{
  const unsigned int hc = std::thread::hardware_concurrency();
  return hc > 0 ? static_cast<size_t>(hc) : 1;
}

#if defined(__linux__)

size_t ReadCgroupCpuLimit()
{
  // cgroup v2 first, then v1. Anything unreadable simply means "no limit".
  std::ifstream v2("/sys/fs/cgroup/cpu.max");
  if (v2.is_open()) {
    std::string contents;
    std::getline(v2, contents);
    const size_t cpus = detail::ParseCgroupV2CpuMax(contents);
    if (cpus > 0) {
      return cpus;
    }
  }

  std::ifstream quotaFile("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
  std::ifstream periodFile("/sys/fs/cgroup/cpu/cpu.cfs_period_us");
  if (quotaFile.is_open() && periodFile.is_open()) {
    long long quota = 0;
    long long period = 0;
    if ((quotaFile >> quota) && (periodFile >> period)) {
      return detail::CgroupCpusFromQuota(quota, period);
    }
  }
  return 0;
}

std::vector<int> OnlineCpusForThisProcess()
{
  std::vector<int> cpus;
  cpu_set_t set;
  CPU_ZERO(&set);
  if (sched_getaffinity(0, sizeof(set), &set) == 0) {
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
      if (CPU_ISSET(cpu, &set)) {
        cpus.push_back(cpu);
      }
    }
  }
  return cpus;
}

#endif

#if defined(__APPLE__)

// Apple Silicon has no SMT, so halving the logical count would be as wrong
// here as it was on aarch64 Linux. The kernel knows the answer; ask it.
size_t PhysicalCoresFromSysctl()
{
  int cores = 0;
  size_t size = sizeof(cores);
  if (sysctlbyname("hw.physicalcpu", &cores, &size, nullptr, 0) == 0 &&
      cores > 0) {
    return static_cast<size_t>(cores);
  }
  return 0;
}

#endif

}

size_t GetUsableCpuCount()
{
#if defined(__linux__)
  size_t usable = OnlineCpusForThisProcess().size();
  if (usable == 0) {
    usable = HardwareConcurrencyOrOne();
  }
  const size_t quota = ReadCgroupCpuLimit();
  if (quota > 0) {
    usable = std::min(usable, quota);
  }
  return std::max<size_t>(usable, 1);
#elif defined(_WIN32)
  const DWORD active = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
  if (active > 0) {
    return static_cast<size_t>(active);
  }
  return HardwareConcurrencyOrOne();
#else
  return HardwareConcurrencyOrOne();
#endif
}

size_t GetPhysicalCoreCount()
{
  const size_t usable = GetUsableCpuCount();

#if defined(_WIN32)
  // GetLogicalProcessorInformationEx, not GetLogicalProcessorInformation:
  // the older call reports only the caller's processor group, so it stops at
  // 64 logical processors and undercounts exactly the large machines an
  // auto-detected worker count is for.
  DWORD length = 0;
  GetLogicalProcessorInformationEx(RelationProcessorCore, nullptr, &length);
  if (length > 0 && GetLastError() == ERROR_INSUFFICIENT_BUFFER) {
    std::vector<char> buffer(length);
    if (GetLogicalProcessorInformationEx(
          RelationProcessorCore,
          reinterpret_cast<PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX>(
            buffer.data()),
          &length)) {
      size_t cores = 0;
      DWORD offset = 0;
      while (offset < length) {
        const auto* info =
          reinterpret_cast<const SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX*>(
            buffer.data() + offset);
        if (info->Size == 0) {
          break;
        }
        if (info->Relationship == RelationProcessorCore) {
          ++cores;
        }
        offset += info->Size;
      }
      if (cores > 0) {
        return std::min(cores, usable);
      }
    }
  }
  return usable;
#elif defined(__linux__)
  // sysfs topology first. It is the only source that is right on every
  // platform that matters here: aarch64 publishes it and /proc/cpuinfo does
  // not, and on a hybrid part the P-cores report two siblings while the
  // E-cores report one, which a physical-id/core-id count also gets right but
  // a logical/2 estimate does not.
  std::vector<std::string> siblingLists;
  for (int cpu : OnlineCpusForThisProcess()) {
    std::ifstream file("/sys/devices/system/cpu/cpu" + std::to_string(cpu) +
                       "/topology/thread_siblings_list");
    if (!file.is_open()) {
      continue;
    }
    std::string contents;
    std::getline(file, contents);
    if (!contents.empty()) {
      siblingLists.push_back(contents);
    }
  }
  const size_t fromSysfs = detail::CountCoresFromSiblingLists(siblingLists);
  if (fromSysfs > 0) {
    return std::min(fromSysfs, usable);
  }

  std::ifstream cpuinfo("/proc/cpuinfo");
  if (cpuinfo.is_open()) {
    const size_t fromCpuInfo = detail::CountCoresFromCpuInfo(cpuinfo);
    if (fromCpuInfo > 0) {
      return std::min(fromCpuInfo, usable);
    }
  }

  // No topology at all. Assume no SMT rather than assuming it: over-counting
  // costs the residual pool-size penalty, which measured at about 1% with the
  // unit count pinned, while under-counting halves the pool.
  return usable;
#elif defined(__APPLE__)
  const size_t fromSysctl = PhysicalCoresFromSysctl();
  if (fromSysctl > 0) {
    return std::min(fromSysctl, usable);
  }
  return usable;
#else
  return usable;
#endif
}

}
