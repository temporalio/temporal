package notification

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/namespace"
)

type FastForwardNotification struct {
	FastForwardInfo            *commonpb.TimeSkippingFastForwardInfo
	WorkflowExecutionCompleted bool
}

type TimeSkippingFastForwardNotifier = PubSubNotifier[*FastForwardNotification]

const maxFastForwardWaitersPerExecution = 5

func NewTimeSkippingFastForwardNotifier(workflowIDToShardID func(namespace.ID, string) int32) TimeSkippingFastForwardNotifier {
	return NewPubSubNotifier[*FastForwardNotification](workflowIDToShardID, maxFastForwardWaitersPerExecution)
}

var NoopTimeSkippingFastForwardNotifier TimeSkippingFastForwardNotifier = NewNoopNotifier[*FastForwardNotification]()
