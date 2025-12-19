package history

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/testing/testvars"
)

func TestChasmNotifier_SubscribeAndNotify(t *testing.T) {
	tv := testvars.New(t)

	notifier := NewChasmNotifier()

	executionKey := chasm.ExecutionKey{
		NamespaceID: tv.NamespaceID().String(),
		BusinessID:  tv.WorkflowID(),
		RunID:       tv.RunID(),
	}

	// Multiple subscribers
	subscriberCount := 100
	subscribers := make([]struct {
		channel <-chan struct{}
	}, subscriberCount)

	for i := range subscriberCount {
		ch, unsubscribe := notifier.Subscribe(executionKey)
		defer unsubscribe() //nolint:revive
		subscribers[i].channel = ch
	}

	// Single notification
	notifier.Notify(executionKey)

	// All subscribers should receive it
	for i, sub := range subscribers {
		select {
		case <-sub.channel:
		case <-time.After(1 * time.Second):
			t.Fatalf("subscriber %d: timeout waiting for notification", i)
		}
	}
}

func TestChasmNotifier_KeyIsolation(t *testing.T) {
	tv := testvars.New(t)

	notifier := NewChasmNotifier()

	executionKey1 := chasm.ExecutionKey{
		NamespaceID: tv.NamespaceID().String(),
		BusinessID:  tv.WorkflowID(),
		RunID:       tv.RunID(),
	}
	executionKey2 := chasm.ExecutionKey{
		NamespaceID: "different-namespace-id",
		BusinessID:  "different-workflow-id",
		RunID:       "different-run-id",
	}

	channel, unsubscribe := notifier.Subscribe(executionKey1)
	defer unsubscribe()
	notifier.Notify(executionKey2)
	select {
	case <-channel:
		t.Fatal("should not receive notification for different entity")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestChasmNotifier_ConstantMemory(t *testing.T) {
	key := chasm.ExecutionKey{
		NamespaceID: "ns",
		BusinessID:  "wf",
		RunID:       "run",
	}
	notifier := NewChasmNotifier()
	require.Empty(t, notifier.executions)
	notifier.Subscribe(key)
	require.Len(t, notifier.executions, 1)
	notifier.Notify(key)
	require.Empty(t, notifier.executions)
	// Ignored: no subscribers
	notifier.Notify(key)
	require.Empty(t, notifier.executions)
}

func TestChasmNotifier_Unsubscribe(t *testing.T) {
	key := chasm.ExecutionKey{
		NamespaceID: "ns",
		BusinessID:  "wf",
		RunID:       "run",
	}

	t.Run("StaleUnsubscribeIsSafe", func(t *testing.T) {
		notifier := NewChasmNotifier()
		_, u1 := notifier.Subscribe(key)
		notifier.Notify(key)
		// The notify call closed and deleted the original channel.
		ch2, u2 := notifier.Subscribe(key)
		defer u2()
		// u1 should be a no-op.
		u1()
		select {
		case <-ch2:
			t.Fatal("notification channel was closed by stale unsubscribe function")
		case <-time.After(1 * time.Second):
		}
		notifier.Notify(key)
		select {
		case <-ch2:
		case <-time.After(1 * time.Second):
			t.Fatal("notification channel should have been closed")
		}
	})

	t.Run("IsIdempotent", func(t *testing.T) {
		notifier := NewChasmNotifier()
		_, u1 := notifier.Subscribe(key)
		ch2, u2 := notifier.Subscribe(key)
		defer u2()

		u1()
		u1()

		select {
		case <-ch2:
			t.Fatal("unsubscribe should be idempotent; notification channel was closed by second call")
		default:
		}
		notifier.Notify(key)
		<-ch2
	})
}
