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

	componentKey := chasm.ComponentKey{
		EntityKey: chasm.EntityKey{
			NamespaceID: tv.NamespaceID().String(),
			BusinessID:  tv.WorkflowID(),
			EntityID:    tv.RunID(),
		},
		Path: "",
	}

	// Multiple subscribers
	subscriberCount := 100
	subscribers := make([]struct {
		channel chan *ChasmComponentNotification
		id      string
	}, subscriberCount)

	for i := range subscriberCount {
		ch, id, err := notifier.Subscribe(componentKey)
		require.NoError(t, err)
		subscribers[i].channel = ch
		subscribers[i].id = id
	}

	// Single notification
	expectedRef := []byte("test-ref")
	notifier.Notify(&ChasmComponentNotification{
		Key: componentKey,
		Ref: expectedRef,
	})

	// All subscribers should receive it
	for i, sub := range subscribers {
		select {
		case received := <-sub.channel:
			require.NotNil(t, received, "subscriber %d", i)
			require.Equal(t, componentKey, received.Key, "subscriber %d", i)
			require.Equal(t, expectedRef, received.Ref, "subscriber %d", i)
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d: timeout waiting for notification", i)
		}
	}

	for _, sub := range subscribers {
		err := notifier.Unsubscribe(componentKey, sub.id)
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

	entityKey := chasm.EntityKey{
		NamespaceID: tv.NamespaceID().String(),
		BusinessID:  tv.WorkflowID(),
		EntityID:    tv.RunID(),
	}
	componentKey1 := chasm.ComponentKey{
		EntityKey: entityKey,
		Path:      "component1",
	}
	componentKey2 := chasm.ComponentKey{
		EntityKey: entityKey,
		Path:      "component2",
	}

	channel, subscriberID, err := notifier.Subscribe(componentKey1)
	require.NoError(t, err)
	notifier.Notify(&ChasmComponentNotification{
		Key: componentKey2,
		Ref: []byte("wrong-entity"),
	})
	select {
	case <-channel:
		t.Fatal("should not receive notification for different entity")
	case <-time.After(50 * time.Millisecond):
	}

	err = notifier.Unsubscribe(componentKey1, subscriberID)
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

	componentKey := chasm.ComponentKey{
		EntityKey: chasm.EntityKey{
			NamespaceID: tv.NamespaceID().String(),
			BusinessID:  tv.WorkflowID(),
			EntityID:    tv.RunID(),
		},
		Path: "",
	}

	// First notification should arrive
	channel, subscriberID, err := notifier.Subscribe(componentKey)
	require.NoError(t, err)
	notifier.Notify(&ChasmComponentNotification{
		Key: componentKey,
		Ref: []byte("before-unsubscribe"),
	})
	select {
	case received := <-channel:
		require.NotNil(t, received)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for notification")
	}

	// Notification after unsubscribe should not arrive
	err = notifier.Unsubscribe(componentKey, subscriberID)
	require.NoError(t, err)
	notifier.Notify(&ChasmComponentNotification{
		Key: componentKey,
		Ref: []byte("after-unsubscribe"),
	})
	select {
	case <-channel:
		t.Fatal("should not receive notification after unsubscribe")
	case <-time.After(50 * time.Millisecond):
	}
}
