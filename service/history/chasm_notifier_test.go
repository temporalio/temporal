package history

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/configs"
)

func TestChasmNotifier_SubscribeAndNotify(t *testing.T) {
	tv := testvars.New(t)

	notifier := NewChasmNotifier(
		clock.NewRealTimeSource(),
		metrics.NoopMetricsHandler,
		configs.NewConfig(dynamicconfig.NewNoopCollection(), 1),
	)
	notifier.Start()
	defer notifier.Stop()

	entityKey := chasm.EntityKey{
		NamespaceID: tv.NamespaceID().String(),
		BusinessID:  tv.WorkflowID(),
		EntityID:    tv.RunID(),
	}

	// Multiple subscribers
	subscriberCount := 100
	subscribers := make([]struct {
		channel chan *ChasmExecutionNotification
		id      string
	}, subscriberCount)

	for i := range subscriberCount {
		ch, id, err := notifier.Subscribe(entityKey)
		require.NoError(t, err)
		subscribers[i].channel = ch
		subscribers[i].id = id
	}

	// Single notification
	expectedRef := []byte("test-ref")
	notifier.Notify(&ChasmExecutionNotification{
		Key: entityKey,
		Ref: expectedRef,
	})

	// All subscribers should receive it
	for i, sub := range subscribers {
		select {
		case received := <-sub.channel:
			require.NotNil(t, received, "subscriber %d", i)
			require.Equal(t, entityKey, received.Key, "subscriber %d", i)
			require.Equal(t, expectedRef, received.Ref, "subscriber %d", i)
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: timeout waiting for notification", i)
		}
	}

	for _, sub := range subscribers {
		err := notifier.Unsubscribe(entityKey, sub.id)
		require.NoError(t, err)
	}
}

func TestChasmNotifier_KeyIsolation(t *testing.T) {
	tv := testvars.New(t)

	notifier := NewChasmNotifier(
		clock.NewRealTimeSource(),
		metrics.NoopMetricsHandler,
		configs.NewConfig(dynamicconfig.NewNoopCollection(), 1),
	)
	notifier.Start()
	defer notifier.Stop()

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

	channel, subscriberID, err := notifier.Subscribe(entityKey1)
	require.NoError(t, err)
	notifier.Notify(&ChasmExecutionNotification{
		Key: entityKey2,
		Ref: []byte("wrong-entity"),
	})
	select {
	case <-channel:
		t.Fatal("should not receive notification for different entity")
	case <-time.After(50 * time.Millisecond):
	}

	err = notifier.Unsubscribe(entityKey1, subscriberID)
	require.NoError(t, err)
}

func TestChasmNotifier_UnsubscribeStopsDelivery(t *testing.T) {
	tv := testvars.New(t)

	notifier := NewChasmNotifier(
		clock.NewRealTimeSource(),
		metrics.NoopMetricsHandler,
		configs.NewConfig(dynamicconfig.NewNoopCollection(), 1),
	)
	notifier.Start()
	defer notifier.Stop()

	entityKey := chasm.EntityKey{
		NamespaceID: tv.NamespaceID().String(),
		BusinessID:  tv.WorkflowID(),
		EntityID:    tv.RunID(),
	}

	// First notification should arrive
	channel, subscriberID, err := notifier.Subscribe(entityKey)
	require.NoError(t, err)
	notifier.Notify(&ChasmExecutionNotification{
		Key: entityKey,
		Ref: []byte("before-unsubscribe"),
	})
	select {
	case received := <-channel:
		require.NotNil(t, received)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for notification")
	}

	// Notification after unsubscribe should not arrive
	err = notifier.Unsubscribe(entityKey, subscriberID)
	require.NoError(t, err)
	notifier.Notify(&ChasmExecutionNotification{
		Key: entityKey,
		Ref: []byte("after-unsubscribe"),
	})
	select {
	case <-channel:
		t.Fatal("should not receive notification after unsubscribe")
	case <-time.After(50 * time.Millisecond):
	}
}
