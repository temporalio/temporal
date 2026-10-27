package workflow

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	historyi "go.temporal.io/server/service/history/interfaces"
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

func (ms *MutableStateImpl) ConsumeStorageUsageDelta() (mutableStateByteSeconds, historyByteSeconds int64) {
	delta := ms.pendingStorageUsageDelta
	ms.pendingStorageUsageDelta = storageUsageDelta{}
	return delta.MutableStateByteSeconds, delta.HistoryByteSeconds
}

// EmitStorageUsageDelta emits the byte-seconds this cluster accrued in the mutable state's last closed
// transaction. Call it only after that transaction is persisted.
func EmitStorageUsageDelta(
	metricsHandler metrics.Handler,
	chasmRegistry *chasm.Registry,
	mutableState historyi.MutableState,
) {
	if mutableState == nil {
		return
	}
	msByteSeconds, historyByteSeconds := mutableState.ConsumeStorageUsageDelta()
	if msByteSeconds == 0 && historyByteSeconds == 0 {
		return
	}
	handler := metricsHandler.WithTags(metrics.NamespaceTag(mutableState.GetNamespaceEntry().Name().String()))
	if archetypeTag, ok := getArchetypeMetricTag(chasmRegistry, mutableState.ChasmTree().ArchetypeID()); ok {
		handler = handler.WithTags(archetypeTag)
	}
	if msByteSeconds > 0 {
		metrics.MutableStateByteSeconds.With(handler).Record(msByteSeconds)
	}
	if historyByteSeconds > 0 {
		metrics.HistoryByteSeconds.With(handler).Record(historyByteSeconds)
	}
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
