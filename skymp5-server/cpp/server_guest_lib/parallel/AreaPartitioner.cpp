#include "AreaPartitioner.h"

#include <algorithm>
#include <cstddef>
#include <limits>

namespace MpParallel {

namespace {

constexpr int32_t kInt16Min = std::numeric_limits<int16_t>::min();
constexpr int32_t kInt16Max = std::numeric_limits<int16_t>::max();

}

uint32_t AreaPartitioner::Find(uint32_t node)
{
  // Iterative path halving: no recursion, so a pathological chain cannot
  // overflow the stack on a worker with a small default stack size.
  while (parent[node] != node) {
    parent[node] = parent[parent[node]];
    node = parent[node];
  }
  return node;
}

void AreaPartitioner::Unite(uint32_t a, uint32_t b)
{
  uint32_t rootA = Find(a);
  uint32_t rootB = Find(b);
  if (rootA == rootB) {
    return;
  }

  if (unionRank[rootA] < unionRank[rootB]) {
    std::swap(rootA, rootB);
  }
  parent[rootB] = rootA;
  if (unionRank[rootA] == unionRank[rootB]) {
    ++unionRank[rootA];
  }
}

void AreaPartitioner::Partition(std::vector<ActorSnapshot>& actors,
                                int32_t separationChunks,
                                std::vector<AreaCluster>& outClusters)
{
  outClusters.clear();
  chunkIndexByKey.clear();
  chunkKeys.clear();
  parent.clear();
  unionRank.clear();
  clusterOfRoot.clear();
  clusterOfChunk.clear();

  if (actors.empty()) {
    return;
  }

  const int32_t separation =
    std::max(separationChunks, kMinSafeSeparationChunks);

  // Pass 1: collect the distinct occupied chunks.
  //
  // Deduplicating through the hash map rather than by sorting the whole actor
  // list keeps this O(actors) instead of O(actors log actors). The difference
  // is the normal case, not the exotic one: a crowd is many actors in few
  // chunks, so sorting one key per actor was sorting the same value over and
  // over.
  actorChunkSlot.resize(actors.size());
  for (uint32_t i = 0; i < static_cast<uint32_t>(actors.size()); ++i) {
    const auto inserted = chunkIndexByKey.emplace(
      actors[i].area, static_cast<uint32_t>(chunkKeys.size()));
    if (inserted.second) {
      chunkKeys.push_back(actors[i].area);
    }
    actorChunkSlot[i] = inserted.first->second;
  }

  const uint32_t chunkCount = static_cast<uint32_t>(chunkKeys.size());

  // chunkKeys is in first-seen order, which depends on the order packets
  // happened to arrive in. Renumbering the chunks into sorted order here is
  // what makes everything downstream -- cluster indices, member lists, the
  // join sequence -- reproducible.
  chunkOrder.resize(chunkCount);
  for (uint32_t i = 0; i < chunkCount; ++i) {
    chunkOrder[i] = i;
  }
  std::sort(chunkOrder.begin(), chunkOrder.end(),
            [this](uint32_t lhs, uint32_t rhs) {
              return chunkKeys[lhs] < chunkKeys[rhs];
            });

  chunkRemap.resize(chunkCount);
  sortedChunkKeys.resize(chunkCount);
  for (uint32_t rank = 0; rank < chunkCount; ++rank) {
    chunkRemap[chunkOrder[rank]] = rank;
    sortedChunkKeys[rank] = chunkKeys[chunkOrder[rank]];
  }
  chunkKeys.swap(sortedChunkKeys);

  // chunkIndexByKey is not remapped with them. It exists to deduplicate
  // chunks in pass 1 and nothing reads it afterwards -- pass 2 searches the
  // sorted key list instead -- so rewriting one entry per occupied chunk
  // would be a pass over a hash table for no reader. It is cleared at the top
  // of every Partition, so no stale index survives the call.
  for (uint32_t& slot : actorChunkSlot) {
    slot = chunkRemap[slot];
  }

  // Pass 2: union chunks that are within `separation` of each other.
  //
  // Two properties make this cheaper than it looks. Pairs are symmetric, so
  // only the forward half of the window is walked: columns x..x+S, and in the
  // sender's own column only the rows below it. And chunkKeys is sorted by
  // (world, x, y), so each column of that half-window is a contiguous run --
  // one binary search finds where it starts and the scan walks sequential
  // memory until it leaves the row range.
  //
  // What it replaces was a lookup in chunkIndexByKey for each of the
  // (2S+1)^2 - 1 cells around every occupied chunk: at the default
  // separation, eighty random probes into a hash table per chunk. That is
  // invisible on a crowd, which occupies one chunk, and is the largest single
  // cost in the tick on a population spread across a map, which occupies
  // hundreds. Measured on this machine by `Partitioning alone` in
  // misc/parallel_bench, before and after; the numbers are in its README.
  parent.resize(chunkCount);
  unionRank.assign(chunkCount, 0);
  for (uint32_t i = 0; i < chunkCount; ++i) {
    parent[i] = i;
  }

  for (uint32_t i = 0; i < chunkCount; ++i) {
    const AreaKey& key = chunkKeys[i];
    const int32_t keyX = static_cast<int32_t>(key.chunkX);
    const int32_t keyY = static_cast<int32_t>(key.chunkY);

    for (int32_t dx = 0; dx <= separation; ++dx) {
      const int32_t nx = keyX + dx;
      if (nx > kInt16Max) {
        break;
      }

      // Own column: only the rows after this one, or the pair would be
      // united twice. Every other column: the full row range.
      const int32_t loY = (dx == 0) ? keyY + 1 : keyY - separation;
      const int32_t hiY = keyY + separation;
      if (loY > hiY || loY > kInt16Max || hiY < kInt16Min) {
        continue;
      }

      const AreaKey lo{ key.worldOrCell, static_cast<int16_t>(nx),
                        static_cast<int16_t>(std::max(loY, kInt16Min)) };

      // The column's run starts here. Searching only the part of the list
      // after i is safe because everything before it sorts lower, and keeps
      // the search shallow on the columns closest to home.
      const auto begin = chunkKeys.begin() + static_cast<std::ptrdiff_t>(i);
      auto it = std::lower_bound(begin, chunkKeys.end(), lo);

      for (; it != chunkKeys.end(); ++it) {
        if (it->worldOrCell != key.worldOrCell ||
            static_cast<int32_t>(it->chunkX) != nx) {
          break;
        }
        if (static_cast<int32_t>(it->chunkY) > hiY) {
          break;
        }
        Unite(i, static_cast<uint32_t>(it - chunkKeys.begin()));
      }
    }
  }

  // Pass 3: turn union-find roots into cluster indices. Scanning the sorted
  // chunk list and numbering roots on first sight orders clusters by their
  // smallest AreaKey.
  clusterOfChunk.assign(chunkCount, kUnassignedCluster);
  clusterOfRoot.assign(chunkCount, kUnassignedCluster);

  uint32_t nextClusterIndex = 0;
  for (uint32_t i = 0; i < chunkCount; ++i) {
    const uint32_t root = Find(i);
    if (clusterOfRoot[root] == kUnassignedCluster) {
      clusterOfRoot[root] = nextClusterIndex++;
    }
    clusterOfChunk[i] = clusterOfRoot[root];
  }

  outClusters.resize(nextClusterIndex);
  for (uint32_t i = 0; i < chunkCount; ++i) {
    AreaCluster& cluster = outClusters[clusterOfChunk[i]];
    if (cluster.chunkCount == 0) {
      // chunkKeys is sorted, so the first chunk seen for a cluster is its
      // smallest and therefore its stable identity.
      cluster.representative = chunkKeys[i];
    }
    ++cluster.chunkCount;
  }

  // Pass 4: hand the actors out. Iterating actors in index order keeps every
  // actorIndices list ascending without a second sort.
  for (uint32_t actorIndex = 0;
       actorIndex < static_cast<uint32_t>(actors.size()); ++actorIndex) {
    const uint32_t clusterIndex = clusterOfChunk[actorChunkSlot[actorIndex]];
    actors[actorIndex].clusterIndex = clusterIndex;
    outClusters[clusterIndex].actorIndices.push_back(actorIndex);
  }
}

}
