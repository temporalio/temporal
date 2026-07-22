package notification

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/namespace"
)

// FastForwardNotification is delivered to PollWorkflowExecutionTimeSkipping waiters.
// Closed is true when the run reached a terminal state without a continuation (no
// retry / cron / continue-as-new), meaning a pending fast-forward can no longer
// complete.
type FastForwardNotification struct {
	FastForwardInfo *commonpb.TimeSkippingFastForwardInfo
	Closed          bool
}

// FastForwardNotifier wakes fast-forward long polls keyed on namespace + workflowID.
type FastForwardNotifier = PubSubNotifier[*FastForwardNotification]

func NewFastForwardNotifier(workflowIDToShardID func(namespace.ID, string) int32) FastForwardNotifier {
	return NewPubSubNotifier[*FastForwardNotification](workflowIDToShardID)
}
