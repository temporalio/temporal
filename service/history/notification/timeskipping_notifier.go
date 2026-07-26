package notification

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
)

type TimeSkippingNotification struct {
	TimeSkippingInfo           *persistencespb.TimeSkippingInfo
	WorkflowExecutionCompleted bool
}

func (n *TimeSkippingNotification) GetTimeSkippingInfo() *persistencespb.TimeSkippingInfo {
	if n == nil {
		return nil
	}
	return n.TimeSkippingInfo
}

type TimeSkippingFastForwardNotifier = PubSubNotifier[*TimeSkippingNotification]

// NewTimeSkippingNotificationKey builds the subscription key for time-skipping notifications from
// namespace + workflowID with RunID left empty, so waiters and publishers key on the whole chain
// of runs (continue-as-new / retry / cron) of a workflow rather than a single run.
func NewTimeSkippingNotificationKey(namespaceID string, workflowID string) definition.WorkflowKey {
	return definition.NewWorkflowKey(namespaceID, workflowID, "")
}

const maxFastForwardWaitersPerExecution = 5

func NewTimeSkippingFastForwardNotifier(hashKey func(namespace.ID, string) int32) TimeSkippingFastForwardNotifier {
	return NewPubSubNotifier[*TimeSkippingNotification](hashKey, maxFastForwardWaitersPerExecution)
}

var NoopTimeSkippingFastForwardNotifier = NewNoopNotifier[*TimeSkippingNotification]()
