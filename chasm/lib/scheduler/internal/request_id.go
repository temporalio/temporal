package internal

import (
	"fmt"
	"strings"
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
		"sched-%s-%s",
		backfillID,
		uuid.NewSHA1(uuid.Nil, fmt.Appendf(nil,
			"%q-%q-%d-%d-%d",
			namespaceID,
			scheduleID,
			conflictToken,
			nominal.UnixMilli(),
			actual.UnixMilli(),
		)),
	)
}

// GenerateWorkflowID generates a deterministic workflow ID for a buffered
// action by combining the base workflow ID with the truncated nominal time.
//
// When appendTimestamp is false (the schedule set keep_original_workflow_id and the
// action's overlap policy permits it), the base workflow ID is used verbatim, so every
// action of the schedule reuses the same workflow ID.
func GenerateWorkflowID(baseWorkflowID string, nominalTime time.Time, appendTimestamp bool) string {
	if !appendTimestamp {
		return baseWorkflowID
	}
	nominalTimeSec := nominalTime.Truncate(time.Second)
	return fmt.Sprintf("%s-%s", baseWorkflowID, nominalTimeSec.UTC().Format(time.RFC3339))
}

// WorkflowIDHasTimestamp reports whether workflowID was generated with nominalTime
// appended to it, i.e. whether GenerateWorkflowID was called with appendTimestamp set.
//
// This is derived purely from the buffered start's own captured state, so it stays
// correct when the schedule's action workflow ID or keep_original_workflow_id policy
// changes after the start was buffered.
func WorkflowIDHasTimestamp(workflowID string, nominalTime time.Time) bool {
	return strings.HasSuffix(workflowID, "-"+nominalTime.Truncate(time.Second).UTC().Format(time.RFC3339))
}

// GenerateBackfillerID generates a unique ID for a Backfiller component.
// This ID is used to identify and deduplicate backfill requests.
func GenerateBackfillerID() string {
	return uuid.NewString()
}
