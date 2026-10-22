package concurrency

import (
	"strings"
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
)

var (
	slotOperations = metrics.NewCounterDef("flowcontrol_concurrency_slot_operations",
		metrics.WithDescription("Slot operation results from successful CHASM Batch updates. Includes duplicate requests; excludes failed updates."))
	slots = metrics.NewGaugeDef("flowcontrol_concurrency_slots",
		metrics.WithDescription("Last successful Batch observation of whole-queue limiter slots, by state. Use the observation timestamp to select the newest host after ownership changes."))
	slotsObservedTimestamp = metrics.NewGaugeDef("flowcontrol_concurrency_slots_observed_timestamp",
		metrics.WithDescription("Unix timestamp in seconds of the last successful Batch slot observation. Gauges are event-driven, not periodic or namespace totals."))
	expirationAttempts = metrics.NewCounterDef("flowcontrol_concurrency_expiration_attempts",
		metrics.WithDescription("Expired reservations removed during CHASM transition attempts, including attempts that later fail persistence."))
	reservationToCommitLatency = metrics.NewTimerDef("flowcontrol_concurrency_reservation_to_commit_latency",
		metrics.WithDescription("Time from initial reservation to first commit in successful Batch updates. Excludes duplicate commits and slots without a reservation timestamp."))
	releaseLatency = metrics.NewTimerDef("flowcontrol_concurrency_release_latency",
		metrics.WithDescription("Time from scheduling an activity permit release to a successful release RPC response. Includes queueing, batching, and RPC latency; excludes failed attempts and tasks without a timestamp."))
)

type operationResult struct {
	operation string
	outcome   string
}

type batchMetrics struct {
	handler         metrics.Handler
	wholeQueue      bool
	operations      map[operationResult]int64
	commitLatencies []time.Duration
	reserved        int
	committed       int
	available       int32
	limit           int32
	observedAt      time.Time
}

func limiterMetricsHandler(handler metrics.Handler, key chasm.ExecutionKey) (metrics.Handler, bool) {
	handler = handler.WithTags(metrics.NamespaceIDTag(key.NamespaceID), metrics.ArchetypeTag("concurrency_limiter"), metrics.TaskQueueUnknownTag(), metrics.TaskTypeTag(""))
	rest, ok := strings.CutPrefix(key.BusinessID, "wholequeue/")
	if !ok {
		return handler, false
	}
	rest, ok = strings.CutSuffix(rest, "/0")
	if !ok {
		return handler, false
	}
	index := strings.LastIndexByte(rest, '/')
	if index <= 0 {
		return handler, false
	}
	var taskType string
	switch rest[index+1:] {
	case "1":
		taskType = "Workflow"
	case "2":
		taskType = "Activity"
	case "3":
		taskType = "Nexus"
	default:
		return handler, false
	}
	// These labels identify configured whole task queues, never per-task limiter or slot IDs.
	return handler.WithTags(metrics.UnsafeTaskQueueTag(rest[:index]), metrics.TaskTypeTag(taskType)), true
}

func (m *batchMetrics) recordOperation(operation, outcome string) {
	m.operations[operationResult{operation: operation, outcome: outcome}]++
}

func (m *batchMetrics) record() {
	for result, count := range m.operations {
		slotOperations.With(m.handler).Record(count, metrics.OperationTag(result.operation), metrics.OutcomeTag(result.outcome))
	}
	for _, latency := range m.commitLatencies {
		reservationToCommitLatency.With(m.handler).Record(latency)
	}
	if !m.wholeQueue {
		return
	}
	for state, count := range map[string]float64{
		"RESERVED": float64(m.reserved), "COMMITTED": float64(m.committed),
		"AVAILABLE": float64(m.available), "LIMIT": float64(m.limit),
	} {
		slots.With(m.handler).Record(count, metrics.StringTag("state", state))
	}
	slotsObservedTimestamp.With(m.handler).Record(float64(m.observedAt.UnixNano()) / float64(time.Second))
}

func recordExpirationAttempts(ctx chasm.MutableContext, count int) {
	if count == 0 {
		return
	}
	handler, _ := limiterMetricsHandler(ctx.MetricsHandler(), ctx.ExecutionKey())
	expirationAttempts.With(handler).Record(int64(count))
}

// RecordReleaseLatency records a successful activity release without labeling individual slot IDs.
func RecordReleaseLatency(handler metrics.Handler, namespaceID, key string, latency time.Duration) {
	if latency < 0 {
		return
	}
	handler, _ = limiterMetricsHandler(handler, chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: key})
	releaseLatency.With(handler).Record(latency)
}
