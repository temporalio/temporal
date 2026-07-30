package notification

import (
	"strconv"

	"github.com/dgryski/go-farm"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
)

// TimeSkippingFastForwardNotification is the payload delivered to subscribers waiting for
// fast-forward completion of an execution.
//
// Subscribers and publishers key on (namespace, workflowID, archetype) with no RunID, so one poll
// follows a chain of runs (e.g. continue-as-new / retry / cron); see TimeSkippingNotificationKey.
// The payload carries a clone of the persisted TimeSkippingInfo, so subscribers decide from it
// alone and never re-acquire the workflow lease — holding that lease would block the very
// execution being polled.
//
// The poll is sound because of the three properties below, and notably not because delivery is
// reliable:
//
//  1. Subscribe before read. Invoke watches the key before its initial state read, so an update
//     persisted in between still lands on the channel instead of being missed by both.
//  2. Filtered publishes. notifyFastForwardUpdate fires only when the transaction stamped the
//     fast-forward (init / update / completion) or the run chain ended, so every
//     delivered message is meaningful and the waiter can act on it without re-reading.
//  3. One buffered notification is usually enough. #2 reduces noise, so a poller usually
//     returns on the first notification: the fast-forward has either completed or lost its chance
//     to. The lone exception is the init in #2 inheriting a propagated target — a chain boundary
//     reinstalling the same fast-forward — which keeps waiting. A chain can emit several of those
//     back to back, so no buffer size removes the risk of a later notification being dropped behind
//     them. That edge case is accepted rather than solved here: the wait runs to its soft timeout
//     and the caller's next poll re-reads authoritative state, so treat POLL_TIMEOUT as "ask again".
type TimeSkippingFastForwardNotification struct {
	TimeSkippingInfo           *persistencespb.TimeSkippingInfo
	WorkflowExecutionCompleted bool
}

func (n *TimeSkippingFastForwardNotification) GetTimeSkippingInfo() *persistencespb.TimeSkippingInfo {
	if n == nil {
		return nil
	}
	return n.TimeSkippingInfo
}

func (n *TimeSkippingFastForwardNotification) GetWorkflowExecutionCompleted() bool {
	if n == nil {
		return false
	}
	return n.WorkflowExecutionCompleted
}

type TimeSkippingNotificationKey struct {
	NamespaceID string
	WorkflowID  string
	ArchetypeID chasm.ArchetypeID
}

type TimeSkippingFastForwardNotifier = PubSubNotifier[TimeSkippingNotificationKey, *TimeSkippingFastForwardNotification]

func NewTimeSkippingNotificationKey(
	namespaceID string,
	workflowID string,
	archetypeID chasm.ArchetypeID,
) TimeSkippingNotificationKey {
	if archetypeID == chasm.UnspecifiedArchetypeID {
		// A record persisted before archetype IDs existed, or a not-yet-initialized chasm tree,
		// is a workflow. Normalizing here is what keeps publisher and waiter on one key: the
		// waiter always names the workflow archetype, while the publisher forwards whatever the
		// execution carries.
		archetypeID = chasm.WorkflowArchetypeID
	}
	return TimeSkippingNotificationKey{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		ArchetypeID: archetypeID,
	}
}

const maxFastForwardWaitersPerExecution = 5

func NewTimeSkippingFastForwardNotifier() TimeSkippingFastForwardNotifier {
	return NewPubSubNotifier[TimeSkippingNotificationKey, *TimeSkippingFastForwardNotification](
		hashTimeSkippingNotificationKey,
		maxFastForwardWaitersPerExecution,
	)
}

func hashTimeSkippingNotificationKey(key TimeSkippingNotificationKey) uint32 {
	return farm.Fingerprint32([]byte(
		key.NamespaceID + "_" + key.WorkflowID + "_" + strconv.FormatUint(uint64(key.ArchetypeID), 10),
	))
}

var NoopTimeSkippingFastForwardNotifier = NewNoopNotifier[TimeSkippingNotificationKey, *TimeSkippingFastForwardNotification]()
