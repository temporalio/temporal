// This file implements ChasmNotifier, which allows subscribers to subscribe to notifications
// relating to components in a specified CHASM execution. It is based on the events.Notifier
// implementation.
package history

import (
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/configs"
)

type (
	// ChasmNotifier allows subscribers to subscribe to notifications relating to components in a
	// specified CHASM execution.
	ChasmNotifier struct {
		timeSource      clock.TimeSource
		metricsHandler  metrics.Handler
		status          int32
		notificationsCh chan *ChasmExecutionNotification
		stopCh          chan bool
		subscribers     collection.ConcurrentTxMap
	}

	// ChasmExecutionNotification is a notification relating to a CHASM component.
	ChasmExecutionNotification struct {
		// TODO(dan): confirm that we want Key in addition to the key in serialized ref
		Key       chasm.EntityKey
		Ref       []byte
		timestamp time.Time
	}
)

// NewChasmNotifier creates a new instance of ChasmNotifier allowing subscribers to subscribe to
// notifications relating to components in a specified CHASM execution.
func NewChasmNotifier(
	timeSource clock.TimeSource,
	metricsHandler metrics.Handler,
	config *configs.Config,
) *ChasmNotifier {
	return &ChasmNotifier{
		timeSource:      timeSource,
		metricsHandler:  metricsHandler.WithTags(metrics.OperationTag(metrics.ChasmComponentNotificationScope)),
		status:          common.DaemonStatusInitialized,
		stopCh:          make(chan bool),
		notificationsCh: make(chan *ChasmExecutionNotification, 1000),
		subscribers: collection.NewShardedConcurrentTxMap(1024, func(key any) uint32 {
			executionKey, ok := key.(chasm.EntityKey)
			if !ok {
				return 0
			}
			return farm.Fingerprint32([]byte(executionKey.NamespaceID + "_" + executionKey.BusinessID))
		}),
	}
}

// Start starts the ChasmNotifier.
func (n *ChasmNotifier) Start() {
	if !atomic.CompareAndSwapInt32(&n.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	go n.dequeueLoop()
}

// Stop stops the ChasmNotifier.
func (n *ChasmNotifier) Stop() {
	if !atomic.CompareAndSwapInt32(&n.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	close(n.stopCh)
}

// Notify sends a notification about a CHASM component.
func (n *ChasmNotifier) Notify(notification *ChasmExecutionNotification) {
	n.enqueue(notification)
}

// Subscribe returns a channel that will receive notifications relating to the execution, along with
// a subscriber ID that can be passed to UnsubscribeNotification.
//
// TODO(dan): support subscribing to notifications for a specific component only?
func (n *ChasmNotifier) Subscribe(key chasm.EntityKey) (chan *ChasmExecutionNotification, string, error) {
	channel := make(chan *ChasmExecutionNotification, 1)
	subscriberID := uuid.NewString()

	// TODO(dan): This allocates a value that will not be used if key already has subscribers (code
	// copied from events.Notifier)
	subscribers := map[string]chan *ChasmExecutionNotification{
		subscriberID: channel,
	}

	// If key exists then add new subscriber to that second-level map. Otherwise, add a new
	// second-level map containing the new subscriber.
	_, _, err := n.subscribers.PutOrDo(key, subscribers, func(key any, value any) error {
		subscribers := value.(map[string]chan *ChasmExecutionNotification)
		if _, ok := subscribers[subscriberID]; ok {
			// uuid collision
			return serviceerror.NewUnavailable("Unable to watch component.")
		}
		subscribers[subscriberID] = channel
		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return channel, subscriberID, nil
}

// Unsubscribe unsubscribes the subscriber from notifications relating to the execution.
func (n *ChasmNotifier) Unsubscribe(key chasm.EntityKey, subscriberID string) error {
	success := true
	n.subscribers.RemoveIf(key, func(key any, value any) bool {
		subscribers := value.(map[string]chan *ChasmExecutionNotification)
		if _, ok := subscribers[subscriberID]; !ok {
			success = false
		} else {
			delete(subscribers, subscriberID)
		}
		return len(subscribers) == 0
	})
	if !success {
		// This indicates a bug
		return serviceerror.NewInternal("Unable to unwatch component.")
	}
	return nil
}

func (n *ChasmNotifier) enqueue(notification *ChasmExecutionNotification) {
	// TODO(dan) This enqueues to an intermediate channel which might fill up, thus dropping
	// notifications for all subscribers. Consider broadcasting synchronously on enqueue instead.
	notification.timestamp = n.timeSource.Now()
	select {
	case n.notificationsCh <- notification:
	default:
		metrics.ChasmComponentNotificationFailDeliveryCount.With(n.metricsHandler).Record(1)
	}
}

func (n *ChasmNotifier) dequeueLoop() {
	for {
		metrics.ChasmComponentNotificationInFlightMessageGauge.With(n.metricsHandler).Record(float64(len(n.notificationsCh)))
		select {
		case notification := <-n.notificationsCh:
			metrics.ChasmComponentNotificationQueueingLatency.With(n.metricsHandler).Record(time.Since(notification.timestamp))
			n.broadcast(notification)
		case <-n.stopCh:
			return
		}
	}
}

func (n *ChasmNotifier) broadcast(notification *ChasmExecutionNotification) {
	startTime := time.Now().UTC()
	defer func() {
		// TODO(dan): retaining "Fanout" name for consistency with events.Notifier for now
		metrics.ChasmComponentNotificationFanoutLatency.With(n.metricsHandler).Record(time.Since(startTime))
	}()
	_, _, _ = n.subscribers.GetAndDo(notification.Key, func(key any, value any) error {
		subscribers := value.(map[string]chan *ChasmExecutionNotification)
		for _, ch := range subscribers {
			select {
			case ch <- notification:
			default:
				// Subscriber channel is full.
				// TODO(dan): metric? log?
			}
		}
		return nil
	})
}
