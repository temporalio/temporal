package notification

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
)

func TestTimeSkippingFastForwardNotifier(t *testing.T) {
	t.Run("archetype isolates executions sharing a business id", func(t *testing.T) {
		n := NewTimeSkippingFastForwardNotifier()
		workflowKey := NewTimeSkippingNotificationKey("ns", "wf", chasm.WorkflowArchetypeID)
		schedulerKey := NewTimeSkippingNotificationKey("ns", "wf", chasm.SchedulerArchetypeID)

		_, workflowCh, err := n.Watch(workflowKey)
		require.NoError(t, err)
		_, schedulerCh, err := n.Watch(schedulerKey)
		require.NoError(t, err)
		require.Equal(t, 2, liveKeys(t, n))

		notif := &TimeSkippingFastForwardNotification{WorkflowExecutionCompleted: true}
		n.Notify(schedulerKey, notif)

		require.Same(t, notif, <-schedulerCh)
		select {
		case got := <-workflowCh:
			require.Failf(t, "waiter woken by another archetype", "got %v", got)
		default:
		}
	})

	// The waiter always names the workflow archetype while the publisher forwards whatever the
	// execution carries, so a pre-archetype record (unspecified) must reach the workflow waiter.
	t.Run("unspecified archetype is normalized to workflow", func(t *testing.T) {
		n := NewTimeSkippingFastForwardNotifier()
		require.Equal(t,
			NewTimeSkippingNotificationKey("ns", "wf", chasm.WorkflowArchetypeID),
			NewTimeSkippingNotificationKey("ns", "wf", chasm.UnspecifiedArchetypeID))

		_, ch, err := n.Watch(NewTimeSkippingNotificationKey("ns", "wf", chasm.WorkflowArchetypeID))
		require.NoError(t, err)

		notif := &TimeSkippingFastForwardNotification{WorkflowExecutionCompleted: true}
		n.Notify(NewTimeSkippingNotificationKey("ns", "wf", chasm.UnspecifiedArchetypeID), notif)

		require.Same(t, notif, <-ch)
	})

	t.Run("nil hash key degrades instead of panicking", func(t *testing.T) {
		n := NewPubSubNotifier[TimeSkippingNotificationKey, *TimeSkippingFastForwardNotification](nil, 5)
		key := NewTimeSkippingNotificationKey("ns", "wf", chasm.WorkflowArchetypeID)

		notif := &TimeSkippingFastForwardNotification{WorkflowExecutionCompleted: true}
		require.NotPanics(t, func() {
			_, _, err := n.Watch(key)
			require.NoError(t, err)
			n.Notify(key, notif)
		})
	})

	t.Run("nil payload accessors are safe", func(t *testing.T) {
		var notif *TimeSkippingFastForwardNotification
		require.Nil(t, notif.GetTimeSkippingInfo())
		require.False(t, notif.GetWorkflowExecutionCompleted())
	})

	t.Run("keys match by value, not identity", func(t *testing.T) {
		n := NewTimeSkippingFastForwardNotifier()
		_, ch, err := n.Watch(NewTimeSkippingNotificationKey("ns", "wf", chasm.WorkflowArchetypeID))
		require.NoError(t, err)

		notif := &TimeSkippingFastForwardNotification{WorkflowExecutionCompleted: true}
		n.Notify(NewTimeSkippingNotificationKey("ns", "wf", chasm.WorkflowArchetypeID), notif)

		require.Same(t, notif, <-ch)
	})
}
