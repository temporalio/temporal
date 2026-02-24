package scheduler

import (
	"encoding/binary"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	schedulescommon "go.temporal.io/server/common/schedules"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func generateRequestID(scheduler *Scheduler, backfillID string, nominal, actual time.Time) string {
	return schedulescommon.GenerateRequestID(
		scheduler.NamespaceId,
		scheduler.ScheduleId,
		scheduler.ConflictToken,
		backfillID,
		nominal,
		actual,
	)
}

// serializeConflictToken serializes a conflict token as a byte slice.
func serializeConflictToken(conflictToken int64) []byte {
	token := make([]byte, 8)
	binary.LittleEndian.PutUint64(token, uint64(conflictToken))
	return token
}

// newTaggedLogger returns a logger tagged with the Scheduler's attributes.
func newTaggedLogger(baseLogger log.Logger, scheduler *Scheduler) log.Logger {
	return log.With(
		baseLogger,
		tag.WorkflowNamespace(scheduler.Namespace),
		tag.ScheduleID(scheduler.ScheduleId),
	)
}

// newTaggedMetricsHandler returns a metrics handler tagged with the Scheduler's namespace and backend.
func newTaggedMetricsHandler(baseHandler metrics.Handler, scheduler *Scheduler) metrics.Handler {
	return baseHandler.WithTags(
		metrics.NamespaceTag(scheduler.Namespace),
		metrics.StringTag(metrics.ScheduleBackendTag, metrics.ScheduleBackendChasm),
	)
}

// validateTaskHighWaterMark validates a component's lastProcessedTime against a
// task timestamp. A task is valid if its scheduled time is after the high water mark.
// Immediate tasks (zero scheduled time) are always valid since they execute inline.
func validateTaskHighWaterMark(
	lastProcessedTime *timestamppb.Timestamp,
	scheduledAt time.Time,
) (bool, error) {
	// Immediate tasks are always valid - they execute inline during the transaction.
	if scheduledAt.IsZero() {
		return true, nil
	}
	// If lastProcessedTime is not set, all scheduled tasks are valid.
	if lastProcessedTime == nil || (lastProcessedTime.GetSeconds() == 0 && lastProcessedTime.GetNanos() == 0) {
		return true, nil
	}
	// Scheduled tasks are valid if their time is after the high water mark.
	return scheduledAt.After(lastProcessedTime.AsTime()), nil
}

// jsonStringer wraps a proto.Message for lazy JSON serialization. Intended for
// debug logging structures.
type jsonStringer struct {
	proto.Message
}

func (j jsonStringer) String() string {
	json, _ := protojson.Marshal(j.Message)
	return string(json)
}
