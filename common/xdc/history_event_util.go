// Copyright (c) 2019 Uber Technologies, Inc.
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

package xdc

import (
	"github.com/pborman/uuid"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"reflect"
	"time"
)

const (
	workflowType = "history event generation mock"
	domainID     = "00000000-0000-0000-0000-000000000000"
	tasklist     = "generation tasklist"
	timeout      = int32(1)
	identity     = "identity"
	runID        = "00000000-0000-0000-0000-000000000000"
)

type (
	// HistoryAttributesGenerator is to generator history attribute in history events
	HistoryAttributesGenerator interface {
		GenerateHistoryEvents([]batch) []*shared.History
	}

	// HistoryAttributesGeneratorImpl is to generator history attribute in history events
	HistoryAttributesGeneratorImpl struct {
		decisionTaskAttempts                    int64
		timerIDs                                map[string]bool
		timerStartEventIDs                      map[int64]bool
		childWorkflowInitialEventIDs            map[int64]int64
		childWorkflowStartEventIDs              map[int64]int64
		signalExternalWorkflowEventIDs          map[int64]int64
		requestExternalWorkflowCanceledEventIDs map[int64]int64
		activityScheduleEventIDs                map[int64]bool
		activityStartEventIDs                   map[int64]int64
		activityCancelRequestEventIDs           map[int64]int64
		decisionTaskScheduleEventID             int64
		decisionTaskStartEventID                int64
		decisionTaskCompleteEventID             int64
	}
)

// NewHistoryAttributesGenerator is initial a generator
func NewHistoryAttributesGenerator() HistoryAttributesGenerator {
	return &HistoryAttributesGeneratorImpl{
		decisionTaskAttempts:                    int64(0),
		timerIDs:                                make(map[string]bool),
		timerStartEventIDs:                      make(map[int64]bool),
		childWorkflowInitialEventIDs:            make(map[int64]int64),
		childWorkflowStartEventIDs:              make(map[int64]int64),
		signalExternalWorkflowEventIDs:          make(map[int64]int64),
		requestExternalWorkflowCanceledEventIDs: make(map[int64]int64),
		activityScheduleEventIDs:                make(map[int64]bool),
		activityStartEventIDs:                   make(map[int64]int64),
		activityCancelRequestEventIDs:           make(map[int64]int64),
		decisionTaskScheduleEventID:             -1,
		decisionTaskStartEventID:                -1,
		decisionTaskCompleteEventID:             -1,
	}
}

// GenerateHistoryEvents is to generator batches of history events
func (h *HistoryAttributesGeneratorImpl) GenerateHistoryEvents(batches []batch) []*shared.History {
	history := make([]*shared.History, 0, len(batches))
	historyEvents := make([]*shared.HistoryEvent, 0)
	eventID := int64(1)

	//TODO: Marker and EventTypeUpsertWorkflowSearchAttributes need to be added to the model and also to generate event attributes
	for _, batch := range batches {
		for _, event := range batch.events {
			historyEvent := h.generateEventAttribute(event, eventID)
			eventID++
			historyEvents = append(historyEvents, historyEvent)
		}
		nextHistoryBatch := &shared.History{
			Events: historyEvents,
		}
		history = append(history, nextHistoryBatch)
	}
	return history
}

func (h *HistoryAttributesGeneratorImpl) generateEventAttribute(vertex Vertex, eventID int64) *shared.HistoryEvent {
	historyEvent := getDefaultHistoryEvent(eventID)
	switch vertex.GetName() {
	case shared.EventTypeWorkflowExecutionStarted.String():
		historyEvent.EventType = shared.EventTypeWorkflowExecutionStarted.Ptr()
		historyEvent.WorkflowExecutionStartedEventAttributes = &shared.WorkflowExecutionStartedEventAttributes{
			WorkflowType: &shared.WorkflowType{
				Name: common.StringPtr(workflowType),
			},
			ParentWorkflowDomain:   common.StringPtr(domainID),
			ParentInitiatedEventId: common.Int64Ptr(common.EmptyEventID),
			TaskList: &shared.TaskList{
				Name: common.StringPtr(tasklist),
				Kind: shared.TaskListKindNormal.Ptr(),
			},
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(timeout),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(timeout),
			Identity:                            common.StringPtr(identity),
			FirstExecutionRunId:                 common.StringPtr(runID),
		}
	case shared.EventTypeWorkflowExecutionCompleted.String():
		historyEvent.EventType = shared.EventTypeWorkflowExecutionCompleted.Ptr()
		historyEvent.WorkflowExecutionCompletedEventAttributes = &shared.WorkflowExecutionCompletedEventAttributes{
			DecisionTaskCompletedEventId: common.Int64Ptr(h.decisionTaskCompleteEventID),
		}
	case shared.EventTypeWorkflowExecutionFailed.String():
		historyEvent.EventType = shared.EventTypeWorkflowExecutionFailed.Ptr()
		historyEvent.WorkflowExecutionFailedEventAttributes = &shared.WorkflowExecutionFailedEventAttributes{
			DecisionTaskCompletedEventId: common.Int64Ptr(h.decisionTaskCompleteEventID),
		}
	case shared.EventTypeWorkflowExecutionTerminated.String():
		historyEvent.EventType = shared.EventTypeWorkflowExecutionTerminated.Ptr()
		historyEvent.WorkflowExecutionTerminatedEventAttributes = &shared.WorkflowExecutionTerminatedEventAttributes{
			Identity: common.StringPtr(identity),
		}
	case shared.EventTypeWorkflowExecutionTimedOut.String():
		historyEvent.EventType = shared.EventTypeWorkflowExecutionTimedOut.Ptr()
		historyEvent.WorkflowExecutionTimedOutEventAttributes = &shared.WorkflowExecutionTimedOutEventAttributes{
			TimeoutType: shared.TimeoutTypeStartToClose.Ptr(),
		}
	case shared.EventTypeWorkflowExecutionCancelRequested.String():
		historyEvent.EventType = shared.EventTypeWorkflowExecutionCancelRequested.Ptr()
		historyEvent.WorkflowExecutionCancelRequestedEventAttributes = &shared.WorkflowExecutionCancelRequestedEventAttributes{
			Cause:                    common.StringPtr("cause"),
			ExternalInitiatedEventId: common.Int64Ptr(10),
			ExternalWorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("wid"),
				RunId:      common.StringPtr("rid"),
			},
			Identity: common.StringPtr(identity),
		}
	case shared.EventTypeWorkflowExecutionCanceled.String():
		historyEvent.EventType = shared.EventTypeWorkflowExecutionCanceled.Ptr()
		historyEvent.WorkflowExecutionCanceledEventAttributes = &shared.WorkflowExecutionCanceledEventAttributes{
			DecisionTaskCompletedEventId: common.Int64Ptr(eventID - 1),
		}
	case shared.EventTypeWorkflowExecutionContinuedAsNew.String():
		historyEvent.EventType = shared.EventTypeWorkflowExecutionContinuedAsNew.Ptr()
		historyEvent.WorkflowExecutionContinuedAsNewEventAttributes = &shared.WorkflowExecutionContinuedAsNewEventAttributes{
			NewExecutionRunId: common.StringPtr(runID),
			WorkflowType: &shared.WorkflowType{
				Name: common.StringPtr(workflowType),
			},
			TaskList: &shared.TaskList{
				Name: common.StringPtr(tasklist),
				Kind: shared.TaskListKindNormal.Ptr(),
			},
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(timeout),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(timeout),
			DecisionTaskCompletedEventId:        common.Int64Ptr(eventID - 1),
			Initiator:                           shared.ContinueAsNewInitiatorDecider.Ptr(),
		}
	case shared.EventTypeWorkflowExecutionSignaled.String():
		historyEvent.EventType = shared.EventTypeWorkflowExecutionSignaled.Ptr()
		historyEvent.WorkflowExecutionSignaledEventAttributes = &shared.WorkflowExecutionSignaledEventAttributes{
			SignalName: common.StringPtr("signal"),
			Identity:   common.StringPtr(identity),
		}
	case shared.EventTypeDecisionTaskScheduled.String():
		historyEvent.EventType = shared.EventTypeDecisionTaskScheduled.Ptr()
		historyEvent.DecisionTaskScheduledEventAttributes = &shared.DecisionTaskScheduledEventAttributes{
			TaskList: &shared.TaskList{
				Name: common.StringPtr(tasklist),
				Kind: shared.TaskListKindNormal.Ptr(),
			},
			StartToCloseTimeoutSeconds: common.Int32Ptr(timeout),
			Attempt:                    common.Int64Ptr(h.decisionTaskAttempts),
		}
		h.decisionTaskScheduleEventID = eventID
	case shared.EventTypeDecisionTaskStarted.String():
		historyEvent.EventType = shared.EventTypeDecisionTaskStarted.Ptr()
		historyEvent.DecisionTaskStartedEventAttributes = &shared.DecisionTaskStartedEventAttributes{
			ScheduledEventId: common.Int64Ptr(h.decisionTaskScheduleEventID),
			Identity:         common.StringPtr(identity),
			RequestId:        common.StringPtr(uuid.New()),
		}
		h.decisionTaskStartEventID = eventID
	case shared.EventTypeDecisionTaskTimedOut.String():
		historyEvent.EventType = shared.EventTypeDecisionTaskTimedOut.Ptr()
		historyEvent.DecisionTaskTimedOutEventAttributes = &shared.DecisionTaskTimedOutEventAttributes{
			ScheduledEventId: common.Int64Ptr(h.decisionTaskScheduleEventID),
			StartedEventId:   common.Int64Ptr(h.decisionTaskStartEventID),
			TimeoutType:      shared.TimeoutTypeScheduleToStart.Ptr(),
		}
		h.decisionTaskAttempts++
	case shared.EventTypeDecisionTaskFailed.String():
		historyEvent.EventType = shared.EventTypeDecisionTaskFailed.Ptr()
		historyEvent.DecisionTaskFailedEventAttributes = &shared.DecisionTaskFailedEventAttributes{
			ScheduledEventId: common.Int64Ptr(h.decisionTaskScheduleEventID),
			StartedEventId:   common.Int64Ptr(h.decisionTaskStartEventID),
			Cause:            common.DecisionTaskFailedCausePtr(shared.DecisionTaskFailedCauseUnhandledDecision),
			Identity:         common.StringPtr(identity),
			BaseRunId:        common.StringPtr(runID),
		}
		h.decisionTaskAttempts++
	case shared.EventTypeDecisionTaskCompleted.String():
		historyEvent.EventType = shared.EventTypeDecisionTaskCompleted.Ptr()
		h.decisionTaskAttempts = 0
		historyEvent.DecisionTaskCompletedEventAttributes = &shared.DecisionTaskCompletedEventAttributes{
			ScheduledEventId: common.Int64Ptr(h.decisionTaskScheduleEventID),
			StartedEventId:   common.Int64Ptr(h.decisionTaskStartEventID),
			Identity:         common.StringPtr(identity),
			BinaryChecksum:   common.StringPtr("checksum"),
		}
		h.decisionTaskCompleteEventID = eventID
	case shared.EventTypeActivityTaskScheduled.String():
		historyEvent.EventType = shared.EventTypeActivityTaskScheduled.Ptr()
		historyEvent.ActivityTaskScheduledEventAttributes = &shared.ActivityTaskScheduledEventAttributes{
			ActivityId:                    common.StringPtr(uuid.New()),
			ActivityType:                  common.ActivityTypePtr(shared.ActivityType{Name: common.StringPtr("activity")}),
			Domain:                        common.StringPtr("domain"),
			TaskList:                      common.TaskListPtr(shared.TaskList{Name: common.StringPtr(tasklist), Kind: common.TaskListKindPtr(shared.TaskListKindNormal)}),
			ScheduleToCloseTimeoutSeconds: common.Int32Ptr(timeout),
			ScheduleToStartTimeoutSeconds: common.Int32Ptr(timeout),
			StartToCloseTimeoutSeconds:    common.Int32Ptr(timeout),
			DecisionTaskCompletedEventId:  common.Int64Ptr(h.decisionTaskCompleteEventID),
		}
		h.activityScheduleEventIDs[eventID] = true
	case shared.EventTypeActivityTaskStarted.String():
		if len(h.activityScheduleEventIDs) == 0 {
			panic("No activity scheduled")
		}
		activityScheduleEventID := reflect.ValueOf(h.activityScheduleEventIDs).MapKeys()[0].Int()
		delete(h.activityScheduleEventIDs, activityScheduleEventID)

		historyEvent.EventType = shared.EventTypeActivityTaskStarted.Ptr()
		historyEvent.ActivityTaskStartedEventAttributes = &shared.ActivityTaskStartedEventAttributes{
			ScheduledEventId: common.Int64Ptr(activityScheduleEventID),
			Identity:         common.StringPtr(identity),
			RequestId:        common.StringPtr(uuid.New()),
			Attempt:          common.Int32Ptr(0),
		}
		h.activityStartEventIDs[eventID] = activityScheduleEventID
	case shared.EventTypeActivityTaskCompleted.String():
		if len(h.activityStartEventIDs) == 0 {
			panic("No activity started before complete")
		}
		activityStartEventID := reflect.ValueOf(h.activityStartEventIDs).MapKeys()[0].Int()
		activityScheduleEventID := h.activityStartEventIDs[activityStartEventID]
		delete(h.activityStartEventIDs, activityStartEventID)

		historyEvent.EventType = shared.EventTypeActivityTaskCompleted.Ptr()
		historyEvent.ActivityTaskCompletedEventAttributes = &shared.ActivityTaskCompletedEventAttributes{
			ScheduledEventId: common.Int64Ptr(activityScheduleEventID),
			StartedEventId:   common.Int64Ptr(activityStartEventID),
			Identity:         common.StringPtr(identity),
		}
	case shared.EventTypeActivityTaskTimedOut.String():
		if len(h.activityStartEventIDs) == 0 {
			panic("No activity started before timeout")
		}
		activityStartEventID := reflect.ValueOf(h.activityStartEventIDs).MapKeys()[0].Int()
		activityScheduleEventID := h.activityStartEventIDs[activityStartEventID]
		delete(h.activityStartEventIDs, activityStartEventID)

		historyEvent.EventType = shared.EventTypeActivityTaskTimedOut.Ptr()
		historyEvent.ActivityTaskTimedOutEventAttributes = &shared.ActivityTaskTimedOutEventAttributes{
			ScheduledEventId: common.Int64Ptr(activityScheduleEventID),
			StartedEventId:   common.Int64Ptr(activityStartEventID),
			TimeoutType:      common.TimeoutTypePtr(shared.TimeoutTypeScheduleToClose),
		}
	case shared.EventTypeActivityTaskFailed.String():
		if len(h.activityStartEventIDs) == 0 {
			panic("No activity started before fail")
		}
		activityStartEventID := reflect.ValueOf(h.activityStartEventIDs).MapKeys()[0].Int()
		activityScheduleEventID := h.activityStartEventIDs[activityStartEventID]
		delete(h.activityStartEventIDs, activityStartEventID)

		historyEvent.EventType = shared.EventTypeActivityTaskFailed.Ptr()
		historyEvent.ActivityTaskFailedEventAttributes = &shared.ActivityTaskFailedEventAttributes{
			ScheduledEventId: common.Int64Ptr(activityScheduleEventID),
			StartedEventId:   common.Int64Ptr(activityStartEventID),
			Identity:         common.StringPtr(identity),
			Reason:           common.StringPtr("failed"),
		}
	case shared.EventTypeActivityTaskCancelRequested.String():
		historyEvent.EventType = shared.EventTypeActivityTaskCancelRequested.Ptr()
		historyEvent.ActivityTaskCancelRequestedEventAttributes = &shared.ActivityTaskCancelRequestedEventAttributes{
			DecisionTaskCompletedEventId: common.Int64Ptr(h.decisionTaskCompleteEventID),
			ActivityId:                   common.StringPtr(uuid.New()),
		}
		h.activityCancelRequestEventIDs[eventID] = h.decisionTaskCompleteEventID
	case shared.EventTypeActivityTaskCanceled.String():
		if len(h.activityStartEventIDs) == 0 {
			panic("No activity started before canceled")
		}
		activityStartEventID := reflect.ValueOf(h.activityStartEventIDs).MapKeys()[0].Int()
		activityScheduleEventID := h.activityStartEventIDs[activityStartEventID]
		delete(h.activityStartEventIDs, activityStartEventID)
		delete(h.activityScheduleEventIDs, activityScheduleEventID)
		if len(h.activityCancelRequestEventIDs) == 0 {
			panic("No activity cancel requested before canceled")
		}
		activityCancelRequestEventID := reflect.ValueOf(h.activityCancelRequestEventIDs).MapKeys()[0].Int()
		delete(h.activityCancelRequestEventIDs, activityCancelRequestEventID)

		historyEvent.EventType = shared.EventTypeActivityTaskCanceled.Ptr()
		historyEvent.ActivityTaskCanceledEventAttributes = &shared.ActivityTaskCanceledEventAttributes{
			LatestCancelRequestedEventId: common.Int64Ptr(activityCancelRequestEventID),
			ScheduledEventId:             common.Int64Ptr(activityScheduleEventID),
			StartedEventId:               common.Int64Ptr(activityStartEventID),
			Identity:                     common.StringPtr(identity),
		}
	case shared.EventTypeRequestCancelActivityTaskFailed.String():
		if len(h.activityCancelRequestEventIDs) == 0 {
			panic("No activity cancel requested before failed")
		}
		activityCancelRequestEventID := reflect.ValueOf(h.activityCancelRequestEventIDs).MapKeys()[0].Int()
		completeEventID := h.activityCancelRequestEventIDs[activityCancelRequestEventID]
		delete(h.activityCancelRequestEventIDs, activityCancelRequestEventID)

		historyEvent.EventType = shared.EventTypeRequestCancelActivityTaskFailed.Ptr()
		historyEvent.RequestCancelActivityTaskFailedEventAttributes = &shared.RequestCancelActivityTaskFailedEventAttributes{
			ActivityId:                   common.StringPtr(uuid.New()),
			DecisionTaskCompletedEventId: common.Int64Ptr(completeEventID),
		}
	case shared.EventTypeTimerStarted.String():
		historyEvent.EventType = shared.EventTypeTimerStarted.Ptr()
		timerID := uuid.New()
		historyEvent.TimerStartedEventAttributes = &shared.TimerStartedEventAttributes{
			TimerId:                      common.StringPtr(timerID),
			StartToFireTimeoutSeconds:    common.Int64Ptr(10),
			DecisionTaskCompletedEventId: common.Int64Ptr(h.decisionTaskCompleteEventID),
		}
		h.timerStartEventIDs[eventID] = true
		h.timerIDs[timerID] = true
	case shared.EventTypeTimerFired.String():
		historyEvent.EventType = shared.EventTypeTimerFired.Ptr()
		if len(h.timerIDs) == 0 {
			panic("Timer fired before timer started")
		}
		timerID := reflect.ValueOf(h.timerIDs).MapKeys()[0].String()
		delete(h.timerIDs, timerID)

		if len(h.timerStartEventIDs) == 0 {
			panic("Timer fired before timer started")
		}
		timerStartEventID := reflect.ValueOf(h.timerStartEventIDs).MapKeys()[0].Int()
		delete(h.timerStartEventIDs, timerStartEventID)

		historyEvent.TimerFiredEventAttributes = &shared.TimerFiredEventAttributes{
			TimerId:        common.StringPtr(timerID),
			StartedEventId: common.Int64Ptr(timerStartEventID),
		}
	case shared.EventTypeTimerCanceled.String():
		historyEvent.EventType = shared.EventTypeTimerCanceled.Ptr()
		if len(h.timerIDs) == 0 {
			panic("Timer fired before timer started")
		}
		timerID := reflect.ValueOf(h.timerIDs).MapKeys()[0].String()
		delete(h.timerIDs, timerID)
		if len(h.timerStartEventIDs) == 0 {
			panic("Timer fired before timer started")
		}
		timerStartEventID := reflect.ValueOf(h.timerStartEventIDs).MapKeys()[0].Int()
		delete(h.timerStartEventIDs, timerStartEventID)

		historyEvent.TimerCanceledEventAttributes = &shared.TimerCanceledEventAttributes{
			TimerId:                      common.StringPtr(timerID),
			StartedEventId:               common.Int64Ptr(timerStartEventID),
			DecisionTaskCompletedEventId: common.Int64Ptr(h.decisionTaskCompleteEventID),
			Identity:                     common.StringPtr(identity),
		}
	case shared.EventTypeStartChildWorkflowExecutionInitiated.String():
		historyEvent.EventType = shared.EventTypeStartChildWorkflowExecutionInitiated.Ptr()
		historyEvent.StartChildWorkflowExecutionInitiatedEventAttributes = &shared.StartChildWorkflowExecutionInitiatedEventAttributes{
			Domain:                              common.StringPtr("domain"),
			WorkflowId:                          common.StringPtr("child_wid"),
			WorkflowType:                        common.WorkflowTypePtr(shared.WorkflowType{Name: common.StringPtr("child_wt")}),
			TaskList:                            common.TaskListPtr(shared.TaskList{Name: common.StringPtr(tasklist), Kind: common.TaskListKindPtr(shared.TaskListKindNormal)}),
			ExecutionStartToCloseTimeoutSeconds: common.Int32Ptr(timeout),
			TaskStartToCloseTimeoutSeconds:      common.Int32Ptr(timeout),
			DecisionTaskCompletedEventId:        common.Int64Ptr(h.decisionTaskCompleteEventID),
			WorkflowIdReusePolicy:               shared.WorkflowIdReusePolicyRejectDuplicate.Ptr(),
		}
		h.childWorkflowInitialEventIDs[eventID] = h.decisionTaskCompleteEventID
	case shared.EventTypeStartChildWorkflowExecutionFailed.String():
		if len(h.childWorkflowInitialEventIDs) == 0 {
			panic("Child workflow did not initial before failed")
		}
		childWorkflowInitialEventID := reflect.ValueOf(h.childWorkflowInitialEventIDs).MapKeys()[0].Int()
		completeEventID := h.childWorkflowInitialEventIDs[childWorkflowInitialEventID]
		delete(h.childWorkflowInitialEventIDs, childWorkflowInitialEventID)

		historyEvent.EventType = shared.EventTypeStartChildWorkflowExecutionFailed.Ptr()
		historyEvent.StartChildWorkflowExecutionFailedEventAttributes = &shared.StartChildWorkflowExecutionFailedEventAttributes{
			Domain:                       common.StringPtr("domain"),
			WorkflowId:                   common.StringPtr("child_wid"),
			WorkflowType:                 common.WorkflowTypePtr(shared.WorkflowType{Name: common.StringPtr("child_wt")}),
			Cause:                        shared.ChildWorkflowExecutionFailedCauseWorkflowAlreadyRunning.Ptr(),
			InitiatedEventId:             common.Int64Ptr(childWorkflowInitialEventID),
			DecisionTaskCompletedEventId: common.Int64Ptr(completeEventID),
		}
	case shared.EventTypeChildWorkflowExecutionStarted.String():
		if len(h.childWorkflowInitialEventIDs) == 0 {
			panic("Child workflow did not initial before start")
		}
		childWorkflowInitialEventID := reflect.ValueOf(h.childWorkflowInitialEventIDs).MapKeys()[0].Int()
		delete(h.childWorkflowInitialEventIDs, childWorkflowInitialEventID)

		historyEvent.EventType = shared.EventTypeChildWorkflowExecutionStarted.Ptr()
		historyEvent.ChildWorkflowExecutionStartedEventAttributes = &shared.ChildWorkflowExecutionStartedEventAttributes{
			Domain:           common.StringPtr("domain"),
			WorkflowType:     common.WorkflowTypePtr(shared.WorkflowType{Name: common.StringPtr("child_wt")}),
			InitiatedEventId: common.Int64Ptr(childWorkflowInitialEventID),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("child_wid"),
				RunId:      common.StringPtr("child_rid"),
			},
		}
		h.childWorkflowStartEventIDs[eventID] = childWorkflowInitialEventID
	case shared.EventTypeChildWorkflowExecutionCompleted.String():
		if len(h.childWorkflowStartEventIDs) == 0 {
			panic("Child workflow did not start before complete")
		}
		childWorkflowStartEventID := reflect.ValueOf(h.childWorkflowStartEventIDs).MapKeys()[0].Int()
		childWorkflowInitialEventID := h.childWorkflowStartEventIDs[childWorkflowStartEventID]
		delete(h.childWorkflowStartEventIDs, childWorkflowStartEventID)

		historyEvent.EventType = shared.EventTypeChildWorkflowExecutionCompleted.Ptr()
		historyEvent.ChildWorkflowExecutionCompletedEventAttributes = &shared.ChildWorkflowExecutionCompletedEventAttributes{
			Domain:           common.StringPtr("domain"),
			WorkflowType:     common.WorkflowTypePtr(shared.WorkflowType{Name: common.StringPtr("child_wt")}),
			InitiatedEventId: common.Int64Ptr(childWorkflowInitialEventID),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("child_wid"),
				RunId:      common.StringPtr("child_rid"),
			},
			StartedEventId: common.Int64Ptr(childWorkflowStartEventID),
		}
	case shared.EventTypeChildWorkflowExecutionTimedOut.String():
		if len(h.childWorkflowStartEventIDs) == 0 {
			panic("Child workflow did not start before timeout")
		}
		childWorkflowStartEventID := reflect.ValueOf(h.childWorkflowStartEventIDs).MapKeys()[0].Int()
		childWorkflowInitialEventID := h.childWorkflowStartEventIDs[childWorkflowStartEventID]
		delete(h.childWorkflowStartEventIDs, childWorkflowStartEventID)

		historyEvent.EventType = shared.EventTypeChildWorkflowExecutionTimedOut.Ptr()
		historyEvent.ChildWorkflowExecutionTimedOutEventAttributes = &shared.ChildWorkflowExecutionTimedOutEventAttributes{
			Domain:           common.StringPtr("domain"),
			WorkflowType:     common.WorkflowTypePtr(shared.WorkflowType{Name: common.StringPtr("child_wt")}),
			InitiatedEventId: common.Int64Ptr(childWorkflowInitialEventID),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("child_wid"),
				RunId:      common.StringPtr("child_rid"),
			},
			StartedEventId: common.Int64Ptr(childWorkflowStartEventID),
			TimeoutType:    common.TimeoutTypePtr(shared.TimeoutTypeScheduleToClose),
		}
	case shared.EventTypeChildWorkflowExecutionTerminated.String():

		if len(h.childWorkflowStartEventIDs) == 0 {
			panic("Child workflow did not start before terminate ")
		}
		childWorkflowStartEventID := reflect.ValueOf(h.childWorkflowStartEventIDs).MapKeys()[0].Int()
		childWorkflowInitialEventID := h.childWorkflowStartEventIDs[childWorkflowStartEventID]
		delete(h.childWorkflowStartEventIDs, childWorkflowStartEventID)

		historyEvent.EventType = shared.EventTypeChildWorkflowExecutionTerminated.Ptr()
		historyEvent.ChildWorkflowExecutionTerminatedEventAttributes = &shared.ChildWorkflowExecutionTerminatedEventAttributes{
			Domain:           common.StringPtr("domain"),
			WorkflowType:     common.WorkflowTypePtr(shared.WorkflowType{Name: common.StringPtr("child_wt")}),
			InitiatedEventId: common.Int64Ptr(childWorkflowInitialEventID),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("child_wid"),
				RunId:      common.StringPtr("child_rid"),
			},
			StartedEventId: common.Int64Ptr(childWorkflowStartEventID),
		}
	case shared.EventTypeChildWorkflowExecutionFailed.String():
		if len(h.childWorkflowStartEventIDs) == 0 {
			panic("Child workflow did not start before fail")
		}
		childWorkflowStartEventID := reflect.ValueOf(h.childWorkflowStartEventIDs).MapKeys()[0].Int()
		childWorkflowInitialEventID := h.childWorkflowStartEventIDs[childWorkflowStartEventID]
		delete(h.childWorkflowStartEventIDs, childWorkflowStartEventID)

		historyEvent.EventType = shared.EventTypeChildWorkflowExecutionFailed.Ptr()
		historyEvent.ChildWorkflowExecutionFailedEventAttributes = &shared.ChildWorkflowExecutionFailedEventAttributes{
			Domain:           common.StringPtr("domain"),
			WorkflowType:     common.WorkflowTypePtr(shared.WorkflowType{Name: common.StringPtr("child_wt")}),
			InitiatedEventId: common.Int64Ptr(childWorkflowInitialEventID),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("child_wid"),
				RunId:      common.StringPtr("child_rid"),
			},
			StartedEventId: common.Int64Ptr(childWorkflowStartEventID),
		}
	case shared.EventTypeChildWorkflowExecutionCanceled.String():
		if len(h.childWorkflowStartEventIDs) == 0 {
			panic("Child workflow did not start before cancel")
		}
		childWorkflowStartEventID := reflect.ValueOf(h.childWorkflowStartEventIDs).MapKeys()[0].Int()
		childWorkflowInitialEventID := h.childWorkflowStartEventIDs[childWorkflowStartEventID]
		delete(h.childWorkflowStartEventIDs, childWorkflowStartEventID)

		historyEvent.EventType = shared.EventTypeChildWorkflowExecutionCanceled.Ptr()
		historyEvent.ChildWorkflowExecutionCanceledEventAttributes = &shared.ChildWorkflowExecutionCanceledEventAttributes{
			Domain:           common.StringPtr("domain"),
			WorkflowType:     common.WorkflowTypePtr(shared.WorkflowType{Name: common.StringPtr("child_wt")}),
			InitiatedEventId: common.Int64Ptr(childWorkflowInitialEventID),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("child_wid"),
				RunId:      common.StringPtr("child_rid"),
			},
			StartedEventId: common.Int64Ptr(childWorkflowStartEventID),
		}
	case shared.EventTypeSignalExternalWorkflowExecutionInitiated.String():
		historyEvent.EventType = shared.EventTypeSignalExternalWorkflowExecutionInitiated.Ptr()
		historyEvent.SignalExternalWorkflowExecutionInitiatedEventAttributes = &shared.SignalExternalWorkflowExecutionInitiatedEventAttributes{
			DecisionTaskCompletedEventId: common.Int64Ptr(h.decisionTaskCompleteEventID),
			Domain:                       common.StringPtr("domain"),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("external_wid"),
				RunId:      common.StringPtr("external_rid"),
			},
			SignalName:        common.StringPtr("signal"),
			ChildWorkflowOnly: common.BoolPtr(false),
		}
		h.signalExternalWorkflowEventIDs[eventID] = h.decisionTaskCompleteEventID
	case shared.EventTypeSignalExternalWorkflowExecutionFailed.String():
		if len(h.signalExternalWorkflowEventIDs) == 0 {
			panic("No external workflow signaled")
		}
		signalExternalWorkflowEventID := reflect.ValueOf(h.signalExternalWorkflowEventIDs).MapKeys()[0].Int()
		completeEventID := h.signalExternalWorkflowEventIDs[signalExternalWorkflowEventID]
		delete(h.signalExternalWorkflowEventIDs, signalExternalWorkflowEventID)

		historyEvent.EventType = shared.EventTypeSignalExternalWorkflowExecutionFailed.Ptr()
		historyEvent.SignalExternalWorkflowExecutionFailedEventAttributes = &shared.SignalExternalWorkflowExecutionFailedEventAttributes{
			Cause:                        common.SignalExternalWorkflowExecutionFailedCausePtr(shared.SignalExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution),
			DecisionTaskCompletedEventId: common.Int64Ptr(completeEventID),
			Domain:                       common.StringPtr("domain"),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("external_wid"),
				RunId:      common.StringPtr("external_rid"),
			},
			InitiatedEventId: common.Int64Ptr(signalExternalWorkflowEventID),
		}
	case shared.EventTypeExternalWorkflowExecutionSignaled.String():
		if len(h.signalExternalWorkflowEventIDs) == 0 {
			panic("No external workflow signaled")
		}
		signalExternalWorkflowEventID := reflect.ValueOf(h.signalExternalWorkflowEventIDs).MapKeys()[0].Int()
		delete(h.signalExternalWorkflowEventIDs, signalExternalWorkflowEventID)

		historyEvent.EventType = shared.EventTypeExternalWorkflowExecutionSignaled.Ptr()
		historyEvent.ExternalWorkflowExecutionSignaledEventAttributes = &shared.ExternalWorkflowExecutionSignaledEventAttributes{
			InitiatedEventId: common.Int64Ptr(signalExternalWorkflowEventID),
			Domain:           common.StringPtr("domain"),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("external_wid"),
				RunId:      common.StringPtr("external_rid"),
			},
		}
	case shared.EventTypeRequestCancelExternalWorkflowExecutionInitiated.String():
		historyEvent.EventType = shared.EventTypeRequestCancelExternalWorkflowExecutionInitiated.Ptr()
		historyEvent.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes = &shared.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes{
			DecisionTaskCompletedEventId: common.Int64Ptr(h.decisionTaskCompleteEventID),
			Domain:                       common.StringPtr("domain"),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("external_wid"),
				RunId:      common.StringPtr("external_rid"),
			},
			ChildWorkflowOnly: common.BoolPtr(false),
		}
		h.requestExternalWorkflowCanceledEventIDs[eventID] = h.decisionTaskCompleteEventID
	case shared.EventTypeRequestCancelExternalWorkflowExecutionFailed.String():
		if len(h.requestExternalWorkflowCanceledEventIDs) == 0 {
			panic("No cancel request external workflow")
		}
		requestExternalWorkflowCanceledEventID := reflect.ValueOf(h.requestExternalWorkflowCanceledEventIDs).MapKeys()[0].Int()
		completeEventID := h.requestExternalWorkflowCanceledEventIDs[requestExternalWorkflowCanceledEventID]
		delete(h.requestExternalWorkflowCanceledEventIDs, requestExternalWorkflowCanceledEventID)

		historyEvent.EventType = shared.EventTypeRequestCancelExternalWorkflowExecutionFailed.Ptr()
		historyEvent.RequestCancelExternalWorkflowExecutionFailedEventAttributes = &shared.RequestCancelExternalWorkflowExecutionFailedEventAttributes{
			Cause:                        common.CancelExternalWorkflowExecutionFailedCausePtr(shared.CancelExternalWorkflowExecutionFailedCauseUnknownExternalWorkflowExecution),
			DecisionTaskCompletedEventId: common.Int64Ptr(completeEventID),
			Domain:                       common.StringPtr("domain"),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("external_wid"),
				RunId:      common.StringPtr("external_rid"),
			},
			InitiatedEventId: common.Int64Ptr(requestExternalWorkflowCanceledEventID),
		}
	case shared.EventTypeExternalWorkflowExecutionCancelRequested.String():
		if len(h.requestExternalWorkflowCanceledEventIDs) == 0 {
			panic("No cancel request external workflow")
		}
		requestExternalWorkflowCanceledEventID := reflect.ValueOf(h.requestExternalWorkflowCanceledEventIDs).MapKeys()[0].Int()
		delete(h.requestExternalWorkflowCanceledEventIDs, requestExternalWorkflowCanceledEventID)

		historyEvent.EventType = shared.EventTypeExternalWorkflowExecutionCancelRequested.Ptr()
		historyEvent.ExternalWorkflowExecutionCancelRequestedEventAttributes = &shared.ExternalWorkflowExecutionCancelRequestedEventAttributes{
			InitiatedEventId: common.Int64Ptr(requestExternalWorkflowCanceledEventID),
			Domain:           common.StringPtr("domain"),
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: common.StringPtr("external_wid"),
				RunId:      common.StringPtr("external_rid"),
			},
		}
	}
	return historyEvent
}

func getDefaultHistoryEvent(eventID int64) *shared.HistoryEvent {
	return &shared.HistoryEvent{
		EventId:   common.Int64Ptr(eventID),
		Timestamp: common.Int64Ptr(time.Now().Unix()),
		TaskId:    common.Int64Ptr(common.EmptyEventTaskID),
		Version:   common.Int64Ptr(common.EmptyVersion),
	}
}

type (
	// batch is the struct for history event batch
	batch struct {
		events []Vertex
	}

	// branch is a branch of new history events
	branch struct {
		next    []*branch
		batches []batch
	}
)

func (b *branch) split(resetIdx int) *branch {
	curr := getBranchToSplit(b, resetIdx)
	return updateCurrentBranchWithSplit(curr, resetIdx)
}

func getBranchToSplit(root *branch, resetIdx int) *branch {
	curr := root
	for curr.next != nil {
		length := len(curr.batches)
		if length > resetIdx {
			break
		}
		curr = curr.next[len(curr.next)-1]
		resetIdx -= length
	}
	return curr
}

func updateCurrentBranchWithSplit(curr *branch, resetIdx int) *branch {
	firstBatches := make([]batch, resetIdx+1)
	copy(firstBatches, curr.batches[:resetIdx+1])
	secondBatches := make([]batch, len(curr.batches)-resetIdx-1)
	copy(secondBatches, curr.batches[resetIdx+1:])
	splitBranch := &branch{
		next:    curr.next,
		batches: secondBatches,
	}
	newBranch := &branch{
		batches: make([]batch, 0),
	}
	curr.batches = firstBatches
	curr.next = []*branch{splitBranch, newBranch}
	return newBranch
}
