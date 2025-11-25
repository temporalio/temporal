package history

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testvars"
)

func TestChasmNotifier_SubscribeAndNotify(t *testing.T) {
	tv := testvars.New(t)

	notifier := NewChasmNotifier(metrics.NoopMetricsHandler)

	entityKey := chasm.EntityKey{
		NamespaceID: tv.NamespaceID().String(),
		BusinessID:  tv.WorkflowID(),
		EntityID:    tv.RunID(),
	}

	// Multiple subscribers
	subscriberCount := 100
	subscribers := make([]struct {
		channel <-chan struct{}
	}, subscriberCount)

	for i := range subscriberCount {
		ch := notifier.Subscribe(entityKey)
		subscribers[i].channel = ch
	}

	// Single notification
	notifier.Notify(entityKey)

	// All subscribers should receive it
	for i, sub := range subscribers {
		select {
		case <-sub.channel:
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: timeout waiting for notification", i)
		}
	}
}

func TestChasmNotifier_KeyIsolation(t *testing.T) {
	tv := testvars.New(t)

	notifier := NewChasmNotifier(metrics.NoopMetricsHandler)

	entityKey1 := chasm.EntityKey{
		NamespaceID: tv.NamespaceID().String(),
		BusinessID:  tv.WorkflowID(),
		EntityID:    tv.RunID(),
	}
	entityKey2 := chasm.EntityKey{
		NamespaceID: "different-namespace-id",
		BusinessID:  "different-workflow-id",
		EntityID:    "different-run-id",
	}

	channel := notifier.Subscribe(entityKey1)
	notifier.Notify(entityKey2)
	select {
	case <-channel:
		t.Fatal("should not receive notification for different entity")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestChasmNotifier_ConstantMemory(t *testing.T) {
	key := chasm.EntityKey{
		NamespaceID: "ns",
		BusinessID:  "wf",
		EntityID:    "run",
	}
	notifier := NewChasmNotifier(metrics.NoopMetricsHandler)
	require.Empty(t, notifier.executions)
	notifier.Subscribe(key)
	require.Equal(t, 1, len(notifier.executions))
	notifier.Notify(key)
	require.Empty(t, notifier.executions)
	// Ignored: no subscribers
	notifier.Notify(key)
	require.Empty(t, notifier.executions)
}
