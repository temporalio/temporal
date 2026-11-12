package workflow

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
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

// addStorageUsage returns usage + d. It does not modify usage.
func addStorageUsage(
	usage *persistencespb.StorageUsage,
	d storageUsageDelta,
) *persistencespb.StorageUsage {
	return &persistencespb.StorageUsage{
		MutableStateByteSeconds: usage.GetMutableStateByteSeconds() + d.MutableStateByteSeconds,
		HistoryByteSeconds:      usage.GetHistoryByteSeconds() + d.HistoryByteSeconds,
	}
}

// SetStorageUsage sets the storage usage in executionInfo's ExecutionStats, allocating ExecutionStats
// if it is nil. It leaves the other ExecutionStats fields unchanged.
func SetStorageUsage(
	executionInfo *persistencespb.WorkflowExecutionInfo,
	usage *persistencespb.StorageUsage,
) {
	if executionInfo.ExecutionStats == nil {
		executionInfo.ExecutionStats = &persistencespb.ExecutionStats{}
	}
	executionInfo.ExecutionStats.StorageUsage = usage
}
