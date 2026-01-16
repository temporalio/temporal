package schedules

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// GenerateRequestID generates a deterministic request ID for a buffered action's
// time. The request ID is deterministic because the jittered actual time (as
// well as the spec's nominal time) is, in turn, also deterministic.
//
// backfillID should be left blank for actions that are being started
// automatically, based on the schedule spec. It must be set for backfills,
// as backfills may generate buffered actions that overlap with both
// automatically-buffered actions, as well as other requested backfills.
func GenerateRequestID(
	namespaceID string,
	scheduleID string,
	conflictToken int64,
	backfillID string,
	nominal time.Time,
	actual time.Time,
) string {
	if backfillID == "" {
		backfillID = "auto"
	}
	return fmt.Sprintf(
		"sched-%s-%s-%s-%d-%d-%d",
		backfillID,
		namespaceID,
		scheduleID,
		conflictToken,
		nominal.UnixMilli(),
		actual.UnixMilli(),
	)
}

// GenerateWorkflowID generates a deterministic workflow ID for a buffered
// action by combining the base workflow ID with the truncated nominal time.
func GenerateWorkflowID(baseWorkflowID string, nominalTime time.Time) string {
	nominalTimeSec := nominalTime.Truncate(time.Second)
	return fmt.Sprintf("%s-%s", baseWorkflowID, nominalTimeSec.UTC().Format(time.RFC3339))
}

// GenerateBackfillerID generates a unique ID for a Backfiller component.
// This ID is used to identify and deduplicate backfill requests.
func GenerateBackfillerID() string {
	return uuid.NewString()
}
