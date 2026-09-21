#pragma once
#include <cstddef>
#include <iosfwd>
#include <string>
#include <vector>

namespace MpParallel {

// How many CPUs this process may actually run on.
//
// Not the same question as "how many CPUs does this machine have". A server
// in a container is usually pinned by an affinity mask, a cgroup CPU quota,
// or both, and /proc/cpuinfo sees neither: it lists the host's processors.
// Spawning a worker per host core against a two-CPU quota puts every spinning
// worker on a runqueue it has to share, which is the opposite of the intent.
size_t GetUsableCpuCount();

// Physical cores rather than SMT siblings, bounded by GetUsableCpuCount.
//
// A worker here alternates between a pause loop and streaming writes, close
// to the worst case for an SMT sibling, so the estimate is in physical cores.
// Where no topology information is available the answer is the usable CPU
// count and not half of it. Machines with no SMT at all (aarch64, and
// Intel E-cores) publish no sibling data, and halving there would hand back
// half the machine on exactly the hardware the detection was added for.
size_t GetPhysicalCoreCount();

namespace detail {

// Each of these returns 0 for "this input says nothing", so a caller can try
// the next source. Exposed for unit tests, which must be able to ask about
// hardware the test machine is not.

// Distinct (physical id, core id) pairs in /proc/cpuinfo. Kernels on x86
// publish both; aarch64 and several virtualised platforms publish neither.
size_t CountCoresFromCpuInfo(std::istream& cpuinfo);

// Distinct entries among the per-CPU topology/thread_siblings_list files.
// Two SMT siblings name the same list, so the distinct count is the number
// of physical cores. This works on aarch64 and on hybrid parts, where
// /proc/cpuinfo does not.
size_t CountCoresFromSiblingLists(const std::vector<std::string>& lists);

// cgroup v2 cpu.max ("<quota> <period>", or "max <period>" for unlimited),
// as a whole number of CPUs, rounded up. 0 means unlimited or unparsable.
size_t ParseCgroupV2CpuMax(const std::string& contents);

// cgroup v1, from the quota and period files. Quota -1 means unlimited.
size_t CgroupCpusFromQuota(long long quota, long long period);

}

}
