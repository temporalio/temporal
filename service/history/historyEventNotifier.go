// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package history

import (
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"

	gen "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/metrics"
)

const (
	eventsChanSize = 1000
)

type (
	historyEventNotification struct {
		id                     definition.WorkflowIdentifier
		lastFirstEventID       int64
		nextEventID            int64
		previousStartedEventID int64
		timestamp              time.Time
		currentBranchToken     []byte
		workflowState          int
		workflowCloseState     int
	}

	historyEventNotifierImpl struct {
		timeSource clock.TimeSource
		metrics    metrics.Client
		// internal status indicator
		status int32
		// stop signal channel
		closeChan chan bool
		// this channel will never close
		eventsChan chan *historyEventNotification
		// function which calculate the shard ID from given workflow ID
		workflowIDToShardID func(string) int

		// concurrent map with key workflowIdentifier, value map[string]chan *historyEventNotification.
		// the reason for the second map being non thread safe:
		// 1. expected number of subscriber per workflow is low, i.e. < 5
		// 2. update to this map is already guarded by GetAndDo API provided by ConcurrentTxMap
		eventsPubsubs collection.ConcurrentTxMap
	}
)

var _ historyEventNotifier = (*historyEventNotifierImpl)(nil)

func newHistoryEventNotification(
	domainID string,
	workflowExecution *gen.WorkflowExecution,
	lastFirstEventID int64,
	nextEventID int64,
	previousStartedEventID int64,
	currentBranchToken []byte,
	workflowState int,
	workflowCloseState int,
) *historyEventNotification {

	return &historyEventNotification{
		id: definition.NewWorkflowIdentifier(
			domainID,
			workflowExecution.GetWorkflowId(),
			workflowExecution.GetRunId(),
		),
		lastFirstEventID:       lastFirstEventID,
		nextEventID:            nextEventID,
		previousStartedEventID: previousStartedEventID,
		currentBranchToken:     currentBranchToken,
		workflowState:          workflowState,
		workflowCloseState:     workflowCloseState,
	}
}

func newHistoryEventNotifier(
	timeSource clock.TimeSource,
	metrics metrics.Client,
	workflowIDToShardID func(string) int,
) *historyEventNotifierImpl {

	hashFn := func(key interface{}) uint32 {
		notification, ok := key.(historyEventNotification)
		if !ok {
			return 0
		}
		return uint32(workflowIDToShardID(notification.id.WorkflowID))
	}
	return &historyEventNotifierImpl{
		timeSource: timeSource,
		metrics:    metrics,
		status:     common.DaemonStatusInitialized,
		closeChan:  make(chan bool),
		eventsChan: make(chan *historyEventNotification, eventsChanSize),

		workflowIDToShardID: workflowIDToShardID,

		eventsPubsubs: collection.NewShardedConcurrentTxMap(1024, hashFn),
	}
}

func (notifier *historyEventNotifierImpl) WatchHistoryEvent(
	identifier definition.WorkflowIdentifier) (string, chan *historyEventNotification, error) {

	channel := make(chan *historyEventNotification, 1)
	subscriberID := uuid.New()
	subscribers := map[string]chan *historyEventNotification{
		subscriberID: channel,
	}

	_, _, err := notifier.eventsPubsubs.PutOrDo(identifier, subscribers, func(key interface{}, value interface{}) error {
		subscribers := value.(map[string]chan *historyEventNotification)

		if _, ok := subscribers[subscriberID]; ok {
			// UUID collision
			return &gen.InternalServiceError{
				Message: "Unable to watch on workflow execution.",
			}
		}
		subscribers[subscriberID] = channel
		return nil
	})

	if err != nil {
		return "", nil, err
	}

	return subscriberID, channel, nil
}

func (notifier *historyEventNotifierImpl) UnwatchHistoryEvent(
	identifier definition.WorkflowIdentifier, subscriberID string) error {

	success := true
	notifier.eventsPubsubs.RemoveIf(identifier, func(key interface{}, value interface{}) bool {
		subscribers := value.(map[string]chan *historyEventNotification)

		if _, ok := subscribers[subscriberID]; !ok {
			// cannot find the subscribe ID, which means there is a bug
			success = false
		} else {
			delete(subscribers, subscriberID)
		}

		return len(subscribers) == 0
	})

	if !success {
		// cannot find the subscribe ID, which means there is a bug
		return &gen.InternalServiceError{
			Message: "Unable to unwatch on workflow execution.",
		}
	}

	return nil
}

func (notifier *historyEventNotifierImpl) dispatchHistoryEventNotification(event *historyEventNotification) {
	identifier := event.id

	timer := notifier.metrics.StartTimer(metrics.HistoryEventNotificationScope, metrics.HistoryEventNotificationFanoutLatency)
	defer timer.Stop()
	notifier.eventsPubsubs.GetAndDo(identifier, func(key interface{}, value interface{}) error { //nolint:errcheck
		subscribers := value.(map[string]chan *historyEventNotification)

		for _, channel := range subscribers {
			select {
			case channel <- event:
			default:
				// in case the channel is already filled with message
				// this should NOT happen, unless there is a bug or high load
			}
		}
		return nil
	})
}

func (notifier *historyEventNotifierImpl) enqueueHistoryEventNotification(event *historyEventNotification) {
	// set the timestamp just before enqueuing the event
	event.timestamp = notifier.timeSource.Now()
	select {
	case notifier.eventsChan <- event:
	default:
		// in case the channel is already filled with message
		// this can be caused by high load
		notifier.metrics.IncCounter(metrics.HistoryEventNotificationScope,
			metrics.HistoryEventNotificationFailDeliveryCount)
	}
}

func (notifier *historyEventNotifierImpl) dequeueHistoryEventNotifications() {
	for {
		// send out metrics about the current number of messages in flight
		notifier.metrics.UpdateGauge(metrics.HistoryEventNotificationScope,
			metrics.HistoryEventNotificationInFlightMessageGauge, float64(len(notifier.eventsChan)))
		select {
		case event := <-notifier.eventsChan:
			// send out metrics about message processing delay
			timeelapsed := time.Since(event.timestamp)
			notifier.metrics.RecordTimer(metrics.HistoryEventNotificationScope,
				metrics.HistoryEventNotificationQueueingLatency, timeelapsed)

			notifier.dispatchHistoryEventNotification(event)
		case <-notifier.closeChan:
			// shutdown
			return
		}
	}
}

func (notifier *historyEventNotifierImpl) Start() {
	if !atomic.CompareAndSwapInt32(&notifier.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	go notifier.dequeueHistoryEventNotifications()
}

func (notifier *historyEventNotifierImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&notifier.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	close(notifier.closeChan)
}

func (notifier *historyEventNotifierImpl) NotifyNewHistoryEvent(event *historyEventNotification) {
	notifier.enqueueHistoryEventNotification(event)
}
