#include "parallel/CoreCount.h"

#include <catch2/catch_all.hpp>
#include <sstream>
#include <string>
#include <vector>

using namespace MpParallel::detail;

namespace {

// A /proc/cpuinfo block as x86 kernels publish it: physical id and core id
// are present, and two SMT siblings share a core id.
std::string X86CpuInfo(int sockets, int coresPerSocket, int threadsPerCore,
                       bool trailingBlankLine = true)
{
  std::string out;
  int processor = 0;
  for (int socket = 0; socket < sockets; ++socket) {
    for (int core = 0; core < coresPerSocket; ++core) {
      for (int thread = 0; thread < threadsPerCore; ++thread) {
        out += "processor\t: " + std::to_string(processor++) + "\n";
        out += "vendor_id\t: AuthenticAMD\n";
        out += "physical id\t: " + std::to_string(socket) + "\n";
        out +=
          "siblings\t: " + std::to_string(coresPerSocket * threadsPerCore) +
          "\n";
        out += "core id\t\t: " + std::to_string(core) + "\n";
        out += "cpu cores\t: " + std::to_string(coresPerSocket) + "\n";
        out += "\n";
      }
    }
  }
  if (!trailingBlankLine && !out.empty()) {
    out.pop_back();
  }
  return out;
}

// aarch64 publishes none of it. This is the shape of a Graviton3
// /proc/cpuinfo, and the reason the old logic halved the core count on
// exactly the hardware it was written to support.
std::string Aarch64CpuInfo(int cpus)
{
  std::string out;
  for (int cpu = 0; cpu < cpus; ++cpu) {
    out += "processor\t: " + std::to_string(cpu) + "\n";
    out += "BogoMIPS\t: 2100.00\n";
    out += "Features\t: fp asimd aes\n";
    out += "CPU implementer\t: 0x41\n";
    out += "CPU architecture: 8\n";
    out += "CPU part\t: 0xd40\n";
    out += "\n";
  }
  return out;
}

size_t CoresIn(const std::string& cpuinfo)
{
  std::istringstream in(cpuinfo);
  return CountCoresFromCpuInfo(in);
}

}

TEST_CASE("cpuinfo core count sees through SMT siblings", "[ParallelConfig]")
{
  REQUIRE(CoresIn(X86CpuInfo(1, 16, 2)) == 16);
  REQUIRE(CoresIn(X86CpuInfo(2, 8, 2)) == 16);
  // Distinct sockets must not collapse into each other: both publish core
  // ids 0..7, so keying on core id alone would report 8.
  REQUIRE(CoresIn(X86CpuInfo(2, 8, 1)) == 16);
  REQUIRE(CoresIn(X86CpuInfo(1, 4, 1)) == 4);
}

TEST_CASE("cpuinfo core count keeps the last processor block",
          "[ParallelConfig]")
{
  // Linux terminates /proc/cpuinfo with a blank line, but a captured copy
  // usually does not, and a parser that only flushes on a blank line loses
  // one core without saying so.
  REQUIRE(CoresIn(X86CpuInfo(1, 4, 2, false)) == 4);
  REQUIRE(CoresIn(X86CpuInfo(1, 1, 1, false)) == 1);
}

TEST_CASE("cpuinfo without topology reports nothing rather than guessing",
          "[ParallelConfig]")
{
  // 0 means "this source says nothing", which is what lets the caller fall
  // through to sysfs and then to the usable CPU count. The bug this replaced
  // returned hardware_concurrency/2 here, so a 16-core Graviton3 with no SMT
  // at all came back as 8 cores and 7 workers.
  REQUIRE(CoresIn(Aarch64CpuInfo(16)) == 0);
  REQUIRE(CoresIn("") == 0);
  REQUIRE(CoresIn("processor\t: 0\n\n") == 0);
}

TEST_CASE("sibling lists count physical cores on any architecture",
          "[ParallelConfig]")
{
  // Two SMT siblings name the same list, so distinct lists are cores.
  const std::vector<std::string> smt = { "0,8", "1,9", "0,8", "1,9" };
  REQUIRE(CountCoresFromSiblingLists(smt) == 2);

  // No SMT: each CPU is its own core. This is the aarch64 and E-core case,
  // and the one a logical/2 estimate gets wrong.
  const std::vector<std::string> noSmt = { "0", "1", "2", "3" };
  REQUIRE(CountCoresFromSiblingLists(noSmt) == 4);

  // Hybrid: four P-cores with siblings, four E-cores without.
  const std::vector<std::string> hybrid = { "0-1", "0-1", "2-3", "2-3",
                                            "4",   "5",   "6",   "7" };
  REQUIRE(CountCoresFromSiblingLists(hybrid) == 6);

  // sysfs reads carry a trailing newline; two entries differing only by it
  // are the same core.
  const std::vector<std::string> ragged = { "0,8\n", "0,8", " 0,8 " };
  REQUIRE(CountCoresFromSiblingLists(ragged) == 1);

  REQUIRE(CountCoresFromSiblingLists({}) == 0);
  REQUIRE(CountCoresFromSiblingLists({ "", "  " }) == 0);
}

TEST_CASE("cgroup cpu quota bounds the worker estimate", "[ParallelConfig]")
{
  // The case that matters for this project specifically: the repository
  // ships a Dockerfile, and inside a container /proc/cpuinfo lists the
  // host's processors, not the ones the quota allows.
  REQUIRE(ParseCgroupV2CpuMax("200000 100000") == 2);
  REQUIRE(ParseCgroupV2CpuMax("800000 100000") == 8);

  // Rounded up: a 1.5-CPU quota is not one core.
  REQUIRE(ParseCgroupV2CpuMax("150000 100000") == 2);

  // "max" is the unlimited case, and 0 means "no limit to apply".
  REQUIRE(ParseCgroupV2CpuMax("max 100000") == 0);
  REQUIRE(ParseCgroupV2CpuMax("") == 0);
  REQUIRE(ParseCgroupV2CpuMax("nonsense") == 0);
  REQUIRE(ParseCgroupV2CpuMax("200000") == 0);

  // cgroup v1 spells unlimited as -1.
  REQUIRE(CgroupCpusFromQuota(-1, 100000) == 0);
  REQUIRE(CgroupCpusFromQuota(400000, 100000) == 4);
  REQUIRE(CgroupCpusFromQuota(100000, 0) == 0);
}

TEST_CASE("detected core count is self-consistent on this host",
          "[ParallelConfig]")
{
  const size_t usable = MpParallel::GetUsableCpuCount();
  const size_t physical = MpParallel::GetPhysicalCoreCount();

  REQUIRE(usable >= 1);
  REQUIRE(physical >= 1);
  // Physical cores can never exceed the CPUs this process may run on. The
  // old implementation could not violate this either, but it could sit at
  // half of it for no reason, which is the regression these cases guard.
  REQUIRE(physical <= usable);
}
