package shard

import (
	"math"
	"time"

	"go.temporal.io/server/common"
)

// ShardOwnership has RangeID zero when the context has not acquired a range.
// Revision also changes across loss/reacquisition between snapshot reads.
type ShardOwnership struct {
	Revision uint64
	RangeID  int64
	Since    time.Time
}

// OwnershipSnapshot includes inactive contexts to retain their lifecycle revisions.
// Revision and ChangedAt cover controller insertions/removals, not acquisitions.
type OwnershipSnapshot struct {
	Revision  uint64
	ChangedAt time.Time
	Contexts  map[int32]ShardOwnership
}

// OwnershipSnapshotProvider is optional; the ordinary Controller API may acquire
// contexts and cannot establish read-only range ownership.
type OwnershipSnapshotProvider interface {
	OwnershipSnapshot(maxContexts int) (OwnershipSnapshot, bool)
}

func (s *ContextImpl) setOwnershipStateLocked(state contextState) {
	s.state = state
	if s.ownershipRevision < math.MaxUint64 {
		s.ownershipRevision++
	}
	s.ownershipChangedAt = time.Now()
}

func (s *ContextImpl) ownershipSnapshot() (ShardOwnership, bool) {
	if !s.stateLock.TryLock() {
		return ShardOwnership{}, false
	}
	defer s.stateLock.Unlock()
	if s.ownershipRevision == math.MaxUint64 {
		return ShardOwnership{}, false
	}
	result := ShardOwnership{Revision: s.ownershipRevision, Since: s.ownershipChangedAt}
	if s.state != contextStateAcquired {
		return result, true
	}
	// Range renewal can hold rwLock across persistence I/O. A busy read supplies
	// no authority instead of joining that I/O or acquiring a shard.
	if !s.rwLock.TryRLock() {
		return ShardOwnership{}, false
	}
	defer s.rwLock.RUnlock()
	if s.engineFuture == nil || !s.engineFuture.Ready() || s.shardInfo == nil || s.shardInfo.RangeId <= 0 || s.rangeAcquiredAt.IsZero() || result.Since.IsZero() {
		return ShardOwnership{}, false
	}
	result.RangeID = s.shardInfo.RangeId
	if result.Since.Before(s.rangeAcquiredAt) {
		result.Since = s.rangeAcquiredAt
	}
	return result, true
}

func (c *ControllerImpl) recordOwnershipChangeLocked() {
	if c.ownershipRevision < math.MaxUint64 {
		c.ownershipRevision++
	}
	c.ownershipChangedAt = time.Now()
}

// OwnershipSnapshot never creates a context or waits on shard/controller locks.
// False means the complete bounded view is unavailable, not that no shards are owned.
func (c *ControllerImpl) OwnershipSnapshot(maxContexts int) (OwnershipSnapshot, bool) {
	if maxContexts <= 0 || c.status.Load() != common.DaemonStatusStarted || !c.TryRLock() {
		return OwnershipSnapshot{}, false
	}
	defer c.RUnlock()
	if len(c.historyShards) > maxContexts || c.ownershipRevision == math.MaxUint64 {
		return OwnershipSnapshot{}, false
	}
	result := OwnershipSnapshot{Revision: c.ownershipRevision, ChangedAt: c.ownershipChangedAt,
		Contexts: make(map[int32]ShardOwnership, len(c.historyShards))}
	for id, context := range c.historyShards {
		reader, ok := context.(interface{ ownershipSnapshot() (ShardOwnership, bool) })
		if !ok {
			return OwnershipSnapshot{}, false
		}
		value, ok := reader.ownershipSnapshot()
		if !ok {
			return OwnershipSnapshot{}, false
		}
		result.Contexts[id] = value
	}
	// Equal revisions across both passes establish a common stable cut. The
	// controller lock keeps contexts fixed while their lifecycle can still change.
	for id, context := range c.historyShards {
		reader, ok := context.(interface{ ownershipSnapshot() (ShardOwnership, bool) })
		if !ok {
			return OwnershipSnapshot{}, false
		}
		value, ok := reader.ownershipSnapshot()
		if !ok || value != result.Contexts[id] {
			return OwnershipSnapshot{}, false
		}
	}
	if c.status.Load() != common.DaemonStatusStarted {
		return OwnershipSnapshot{}, false
	}
	return result, true
}
