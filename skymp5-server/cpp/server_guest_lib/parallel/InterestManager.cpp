#include "InterestManager.h"

#include <algorithm>

namespace MpParallel {
namespace InterestManager {

namespace {

[[nodiscard]] float SqrDistance(const float a[3], const float b[3]) noexcept
{
  const float dx = a[0] - b[0];
  const float dy = a[1] - b[1];
  const float dz = a[2] - b[2];
  return dx * dx + dy * dy + dz * dz;
}

// splitmix64-style mix of the pair identity. Only needs to spread phases
// evenly across the skip window, so a cheap finalizer is enough.
[[nodiscard]] uint64_t MixPair(uint32_t senderFormId,
                               Networking::UserId recipientUserId) noexcept
{
  uint64_t h = (static_cast<uint64_t>(senderFormId) << 16) |
    static_cast<uint64_t>(recipientUserId);
  h ^= h >> 30;
  h *= 0xbf58476d1ce4e5b9ULL;
  h ^= h >> 27;
  h *= 0x94d049bb133111ebULL;
  h ^= h >> 31;
  return h;
}

}

bool ValidateMovement(const ActorSnapshot& actor) noexcept
{
  // The inline path substitutes an infinite target position when the
  // teleport flag is set, which makes the distance test below fail. Encoding
  // that directly is clearer and avoids relying on infinity arithmetic.
  if (actor.teleportFlag) {
    return false;
  }

  if (actor.currentWorldOrCell != actor.proposedWorldOrCell) {
    return false;
  }

  return SqrDistance(actor.currentPos, actor.proposedPos) <
    kSqrMaxMovementDistance;
}

uint32_t ComputeSkipFactor(float sqrDistance, uint32_t pressureLevel,
                           const ParallelConfig& config) noexcept
{
  if (!config.adaptiveThrottling || pressureLevel == 0) {
    return 1;
  }

  const float threshold = config.throttleDistanceUnits;
  const float sqrThreshold = threshold * threshold;

  // Inside the near band nothing is ever held back: those are the players
  // actually fighting or talking to each other, and they are what "smooth"
  // means to a user.
  if (sqrDistance <= sqrThreshold) {
    return 1;
  }

  // One extra step per doubling of the threshold distance. Comparing squared
  // distances against 4x keeps this branch free of square roots.
  uint32_t distanceBand = 1;
  if (sqrDistance > sqrThreshold * 4.f) {
    distanceBand = 2;
  }
  if (sqrDistance > sqrThreshold * 16.f) {
    distanceBand = 3;
  }

  const uint64_t factor =
    static_cast<uint64_t>(pressureLevel) * distanceBand + 1;
  return static_cast<uint32_t>(
    std::min<uint64_t>(factor, std::max<uint32_t>(config.maxThrottleSkipTicks, 1)));
}

bool ShouldRelayThisTick(uint64_t tickIndex, uint32_t senderFormId,
                         Networking::UserId recipientUserId,
                         uint32_t skipFactor) noexcept
{
  if (skipFactor <= 1) {
    return true;
  }
  const uint64_t phase = MixPair(senderFormId, recipientUserId) % skipFactor;
  return (tickIndex % skipFactor) == phase;
}

void ProcessRange(const TickSnapshot& snapshot, const uint32_t* actorIndices,
                  size_t actorCount, uint32_t pressureLevel,
                  const ParallelConfig& config, ClusterOutput& output)
{
  output.Reset();
  if (actorIndices == nullptr || actorCount == 0) {
    return;
  }
  output.verdicts.reserve(actorCount);

  for (size_t i = 0; i < actorCount; ++i) {
    const uint32_t actorIndex = actorIndices[i];
    if (actorIndex >= snapshot.actors.size()) {
      continue;
    }
    const ActorSnapshot& actor = snapshot.actors[actorIndex];

    const bool accepted = ValidateMovement(actor);

    MovementVerdict verdict;
    verdict.actorIndex = actorIndex;
    verdict.accepted = accepted;
    // Only the owning player gets pulled back; a hosted NPC has no client to
    // correct, matching the `isMe` guard in MovementValidation.
    verdict.needsCorrection =
      !accepted && actor.ownerUserId != Networking::InvalidUserId;
    output.verdicts.push_back(verdict);

    // The relay happens before validation on the inline path, so a rejected
    // update is still forwarded to neighbours. Preserving that ordering
    // keeps observable behaviour identical.
    if (actor.relayCount == 0 || actor.packetLength == 0) {
      continue;
    }

    const uint32_t relayEnd = actor.relayBegin + actor.relayCount;
    if (relayEnd > snapshot.relayTargets.size()) {
      continue;
    }

    for (uint32_t relayIndex = actor.relayBegin; relayIndex < relayEnd;
         ++relayIndex) {
      const RelayTarget& target = snapshot.relayTargets[relayIndex];
      if (target.userId == Networking::InvalidUserId) {
        continue;
      }

      const float sqrDistance = SqrDistance(actor.currentPos, target.pos);
      const uint32_t skipFactor =
        ComputeSkipFactor(sqrDistance, pressureLevel, config);

      if (!ShouldRelayThisTick(snapshot.tickIndex, actor.formId, target.userId,
                               skipFactor)) {
        ++output.throttledEdges;
        continue;
      }

      OutboundSend send;
      send.userId = target.userId;
      send.byteOffset = actor.packetOffset;
      send.byteLength = actor.packetLength;
      // Movement relays are unreliable on the inline path too: the next
      // update supersedes this one, so a retransmit would only add latency.
      send.reliable = false;
      output.sends.push_back(send);
      ++output.emittedEdges;
    }
  }
}

}
}
