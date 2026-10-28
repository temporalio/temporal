package workflow

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
)

// storageUsageDelta is the byte-seconds accrued by one execution over one interval between
// two state transitions.
type storageUsageDelta struct {
	MutableStateByteSeconds int64
	HistoryByteSeconds      int64
}

func (d storageUsageDelta) isZero() bool {
	return d.MutableStateByteSeconds == 0 && d.HistoryByteSeconds == 0
}

// computeStorageUsageDelta returns the byte-seconds for holding the given sizes from last to now.
// A zero last (no baseline yet) or a now before last (clock moved backwards) accrues nothing.
func computeStorageUsageDelta(
	msSizeBytes int64,
	historySizeBytes int64,
	last time.Time,
	now time.Time,
) storageUsageDelta {
	if last.IsZero() || !now.After(last) {
		return storageUsageDelta{}
	}
	seconds := now.Sub(last).Seconds()
	return storageUsageDelta{
		MutableStateByteSeconds: int64(float64(msSizeBytes) * seconds),
		HistoryByteSeconds:      int64(float64(historySizeBytes) * seconds),
	}
}

// addStorageUsage adds d to usage, allocating usage if it is nil.
func addStorageUsage(
	usage *persistencespb.StorageUsage,
	d storageUsageDelta,
) *persistencespb.StorageUsage {
	if usage == nil {
		usage = &persistencespb.StorageUsage{}
	}
	usage.MutableStateByteSeconds += d.MutableStateByteSeconds
	usage.HistoryByteSeconds += d.HistoryByteSeconds
	return usage
}

// CurrentStorageUsage returns this cluster's storage usage as of now: the stored counters plus the
// interval held since this cluster's last closed transaction, which is not accrued yet. A closed
// execution, or one with no local baseline yet, returns the stored counters. It does not modify the
// mutable state.
func (ms *MutableStateImpl) CurrentStorageUsage() *persistencespb.StorageUsage {
	usage := common.CloneProto(ms.executionInfo.GetStorageUsage())
	if !ms.runningAtLastClose {
		return usage
	}
	delta := computeStorageUsageDelta(
		ms.sizeAtLastClose,
		ms.executionInfo.GetExecutionStats().GetHistorySize(),
		ms.lastCloseTime,
		ms.timeSource.Now(),
	)
	if delta.isZero() {
		return usage
	}
	return addStorageUsage(usage, delta)
}
