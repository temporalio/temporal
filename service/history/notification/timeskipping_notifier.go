package notification

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/namespace"
)

type FastForwardNotification struct {
	FastForwardInfo            *commonpb.TimeSkippingFastForwardInfo
	WorkflowExecutionCompleted bool
}

type FastForwardNotifier = PubSubNotifier[*FastForwardNotification]

const maxFastForwardWaitersPerExecution = 5

func NewFastForwardNotifier(workflowIDToShardID func(namespace.ID, string) int32) FastForwardNotifier {
	return NewPubSubNotifier[*FastForwardNotification](workflowIDToShardID, maxFastForwardWaitersPerExecution)
}

var NoopFastForwardNotifier FastForwardNotifier = NewNoopNotifier[*FastForwardNotification]()
