// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package events

import (
	"sync/atomic"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

const (
	eventsChanSize = 1000
)

type (
	Notifier interface {
		common.Daemon
		NotifyNewHistoryEvent(event *Notification)
		WatchHistoryEvent(identifier definition.WorkflowKey) (string, chan *Notification, error)
		UnwatchHistoryEvent(identifier definition.WorkflowKey, subscriberID string) error
	}

	Notification struct {
		ID                     definition.WorkflowKey
		LastFirstEventID       int64
		LastFirstEventTxnID    int64
		NextEventID            int64
		PreviousStartedEventID int64
		Timestamp              time.Time
		CurrentBranchToken     []byte
		WorkflowState          enumsspb.WorkflowExecutionState
		WorkflowStatus         enumspb.WorkflowExecutionStatus
	}

	NotifierImpl struct {
		timeSource clock.TimeSource
		metrics    metrics.Client
		// internal status indicator
		status int32
		// stop signal channel
		closeChan chan bool
		// this channel will never close
		eventsChan chan *Notification
		// function which calculate the shard ID from given namespaceID and workflowID pair
		workflowIDToShardID func(namespace.ID, string) int32

		// concurrent map with key workflowKey, value map[string]chan *Notification.
		// the reason for the second map being non thread safe:
		// 1. expected number of subscriber per workflow is low, i.e. < 5
		// 2. update to this map is already guarded by GetAndDo API provided by ConcurrentTxMap
		eventsPubsubs collection.ConcurrentTxMap
	}
)

var _ Notifier = (*NotifierImpl)(nil)

func NewNotification(
	namespaceID string,
	workflowExecution *commonpb.WorkflowExecution,
	lastFirstEventID int64,
	lastFirstEventTxnID int64,
	nextEventID int64,
	previousStartedEventID int64,
	currentBranchToken []byte,
	workflowState enumsspb.WorkflowExecutionState,
	workflowStatus enumspb.WorkflowExecutionStatus,
) *Notification {

	return &Notification{
		ID: definition.NewWorkflowKey(
			namespaceID,
			workflowExecution.GetWorkflowId(),
			workflowExecution.GetRunId(),
		),
		LastFirstEventID:       lastFirstEventID,
		LastFirstEventTxnID:    lastFirstEventTxnID,
		NextEventID:            nextEventID,
		PreviousStartedEventID: previousStartedEventID,
		CurrentBranchToken:     currentBranchToken,
		WorkflowState:          workflowState,
		WorkflowStatus:         workflowStatus,
	}
}

func NewNotifier(
	timeSource clock.TimeSource,
	metrics metrics.Client,
	workflowIDToShardID func(namespace.ID, string) int32,
) *NotifierImpl {

	hashFn := func(key interface{}) uint32 {
		notification, ok := key.(Notification)
		if !ok {
			return 0
		}
		return uint32(workflowIDToShardID(namespace.ID(notification.ID.NamespaceID), notification.ID.WorkflowID))
	}
	return &NotifierImpl{
		timeSource: timeSource,
		metrics:    metrics,
		status:     common.DaemonStatusInitialized,
		closeChan:  make(chan bool),
		eventsChan: make(chan *Notification, eventsChanSize),

		workflowIDToShardID: workflowIDToShardID,

		eventsPubsubs: collection.NewShardedConcurrentTxMap(1024, hashFn),
	}
}

func (notifier *NotifierImpl) WatchHistoryEvent(
	identifier definition.WorkflowKey) (string, chan *Notification, error) {

	channel := make(chan *Notification, 1)
	subscriberID := uuid.New()
	subscribers := map[string]chan *Notification{
		subscriberID: channel,
	}

	_, _, err := notifier.eventsPubsubs.PutOrDo(identifier, subscribers, func(key interface{}, value interface{}) error {
		subscribers := value.(map[string]chan *Notification)

		if _, ok := subscribers[subscriberID]; ok {
			// UUID collision
			return serviceerror.NewUnavailable("Unable to watch on workflow execution.")
		}
		subscribers[subscriberID] = channel
		return nil
	})

	if err != nil {
		return "", nil, err
	}

	return subscriberID, channel, nil
}

func (notifier *NotifierImpl) UnwatchHistoryEvent(
	identifier definition.WorkflowKey, subscriberID string) error {

	success := true
	notifier.eventsPubsubs.RemoveIf(identifier, func(key interface{}, value interface{}) bool {
		subscribers := value.(map[string]chan *Notification)

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
		return serviceerror.NewInternal("Unable to unwatch on workflow execution.")
	}

	return nil
}

func (notifier *NotifierImpl) dispatchHistoryEventNotification(event *Notification) {
	identifier := event.ID

	timer := notifier.metrics.StartTimer(metrics.HistoryEventNotificationScope, metrics.HistoryEventNotificationFanoutLatency)
	defer timer.Stop()
	_, _, _ = notifier.eventsPubsubs.GetAndDo(identifier, func(key interface{}, value interface{}) error {
		subscribers := value.(map[string]chan *Notification)

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

func (notifier *NotifierImpl) enqueueHistoryEventNotification(event *Notification) {
	// set the Timestamp just before enqueuing the event
	event.Timestamp = notifier.timeSource.Now()
	select {
	case notifier.eventsChan <- event:
	default:
		// in case the channel is already filled with message
		// this can be caused by high load
		notifier.metrics.IncCounter(metrics.HistoryEventNotificationScope,
			metrics.HistoryEventNotificationFailDeliveryCount)
	}
}

func (notifier *NotifierImpl) dequeueHistoryEventNotifications() {
	for {
		// send out metrics about the current number of messages in flight
		notifier.metrics.UpdateGauge(metrics.HistoryEventNotificationScope,
			metrics.HistoryEventNotificationInFlightMessageGauge, float64(len(notifier.eventsChan)))
		select {
		case event := <-notifier.eventsChan:
			// send out metrics about message processing delay
			timeelapsed := time.Since(event.Timestamp)
			notifier.metrics.RecordTimer(metrics.HistoryEventNotificationScope,
				metrics.HistoryEventNotificationQueueingLatency, timeelapsed)

			notifier.dispatchHistoryEventNotification(event)
		case <-notifier.closeChan:
			// shutdown
			return
		}
	}
}

func (notifier *NotifierImpl) Start() {
	if !atomic.CompareAndSwapInt32(&notifier.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	go notifier.dequeueHistoryEventNotifications()
}

func (notifier *NotifierImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&notifier.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	close(notifier.closeChan)
}

func (notifier *NotifierImpl) NotifyNewHistoryEvent(event *Notification) {
	notifier.enqueueHistoryEventNotification(event)
}
