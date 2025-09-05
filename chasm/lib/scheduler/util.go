package scheduler

import (
	"bytes"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
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
// current timestamp, returning ErrStaleReference for out-of-date tasks.
func validateTaskHighWaterMark(lastProcessedTime *timestamppb.Timestamp, scheduledAt time.Time) (bool, error) {
	if lastProcessedTime != nil && !scheduledAt.IsZero() {
		hwm := lastProcessedTime.AsTime()

		if hwm.After(scheduledAt) {
			return false, nil
		}
	}

	return true, nil
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

// mergeCustomSearchAttributes takes a map of existing search attributes
// (currentAttrs) and a map of incoming custom search attributes (customAttrs),
// mutating upsertAttrs with their merged result. Keys present in currentAttrs
// but not within customAttrs will be explicitly deleted in upsertAttrs. Keys
// with unchanged payloads will not be set in upsertAttrs.
func mergeCustomSearchAttributes(
	currentAttrs map[string]*commonpb.Payload,
	customAttrs map[string]*commonpb.Payload,
	upsertAttrs map[string]any,
) {
	// Insert new and updated values.
	for key, newPayload := range customAttrs {
		oldPayload, alreadySet := currentAttrs[key]

		if !alreadySet || !bytes.Equal(oldPayload.Data, newPayload.Data) {
			// New or out-of-date search attribute.
			upsertAttrs[key] = newPayload
		}
	}

	// Remove keys missing from customAttrs.
	for key := range currentAttrs {
		_, keep := customAttrs[key]
		if !keep {
			// Key isn't in the new map, delete it.
			upsertAttrs[key] = nil
		}
	}
}
