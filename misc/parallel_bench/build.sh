#!/usr/bin/env bash
# Builds misc/parallel_bench without vcpkg and without cmake.
#
# The authoritative benchmark, unit/ParallelBenchmark.cpp, needs slikenet and
# the rest of the vcpkg tree. This one needs a C++17 compiler and four
# header-only libraries, so it can be built on a host where that tree cannot.
#
#   ./build.sh && ./parallel_bench 400
#
# Dependencies are taken from the system by default. Point DEPS_PREFIX at a
# local install to use one instead:
#
#   DEPS_PREFIX=$HOME/deps ./build.sh
#
# fmt, spdlog, nlohmann-json and their headers are the only requirement;
# Catch2 is not, since this is not a test binary.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
CXX=${CXX:-g++}
OUT=${OUT:-$HERE/parallel_bench}

INCLUDES=(
  -I"$ROOT/skymp5-server/cpp/server_guest_lib"
  -I"$ROOT/skymp5-server/cpp/mp_common"
  -I"$ROOT/skymp5-server/cpp/messages"
)
LIBS=()

if [ -n "${DEPS_PREFIX:-}" ]; then
  INCLUDES+=(-I"$DEPS_PREFIX/include")
  LIBS+=(-L"$DEPS_PREFIX/lib")
fi

# spdlog against an external fmt, which is how the server builds it.
"$CXX" -std=c++17 -O2 -g -pthread \
  -DSPDLOG_FMT_EXTERNAL=1 \
  "${INCLUDES[@]}" \
  "$HERE/parallel_bench.cpp" \
  "$ROOT"/skymp5-server/cpp/server_guest_lib/parallel/*.cpp \
  "$ROOT"/skymp5-server/cpp/server_guest_lib/FormDesc.cpp \
  "${LIBS[@]:-}" \
  -lspdlog -lfmt \
  -o "$OUT"

echo "built $OUT"
