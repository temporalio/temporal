package scheduler

import (
	"fmt"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// generateRequestID generates a deterministic request ID for a buffered action's
// time. The request ID is deterministic because the jittered actual time (as
// well as the spec's nominal time) is, in turn, also deterministic.
//
// backfillID should be left blank for actions that are being started
// automatically, based on the schedule spec. It must be set for backfills,
// as backfills may generate buffered actions that overlap with both
// automatically-buffered actions, as well as other requested backfills.
func generateRequestID(scheduler *Scheduler, backfillID string, nominal, actual time.Time) string {
	if backfillID == "" {
		backfillID = "auto"
	}
	return fmt.Sprintf(
		"sched-%s-%s-%s-%d-%d-%d",
		backfillID,
		scheduler.NamespaceId,
		scheduler.ScheduleId,
		scheduler.ConflictToken,
		nominal.UnixMilli(),
		actual.UnixMilli(),
	)
}

// newTaggedLogger returns a logger tagged with the Scheduler's attributes.
func newTaggedLogger(baseLogger log.Logger, scheduler *Scheduler) log.Logger {
	return log.With(
		baseLogger,
		tag.WorkflowNamespace(scheduler.Namespace),
		tag.ScheduleID(scheduler.ScheduleId),
	)
}

// validateTaskHighWaterMark validates a component's lastProcessedTime against a
// task timestamp. A task is valid if its scheduled time is after the high water mark.
//
// For immediate tasks (zero scheduled time), validation uses a logical clock
// comparison: the task's version must match the component's current version.
// This prevents re-execution of immediate tasks after the component has processed
// them and incremented its version.
func validateTaskHighWaterMark(
	lastProcessedTime *timestamppb.Timestamp,
	scheduledAt time.Time,
	componentTaskVersion int64,
	taskVersion int64,
) (bool, error) {
	// Immediate tasks are valid only if their version matches the component's version.
	if scheduledAt.IsZero() {
		return componentTaskVersion == taskVersion, nil
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
