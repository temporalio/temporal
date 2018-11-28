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
	"time"

	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"

	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/persistence"
)

type (
	// TODO: This should be part of persistence layer
	decisionInfo struct {
		Version         int64
		ScheduleID      int64
		StartedID       int64
		RequestID       string
		DecisionTimeout int32
		TaskList        string // This is only needed to communicate tasklist used after AddDecisionTaskScheduledEvent
		Attempt         int64
		Timestamp       int64
	}

	mutableState interface {
		AddActivityTaskCancelRequestedEvent(int64, string, string) (*workflow.HistoryEvent, *persistence.ActivityInfo, bool)
		AddActivityTaskCanceledEvent(int64, int64, int64, []uint8, string) *workflow.HistoryEvent
		AddActivityTaskCompletedEvent(int64, int64, *workflow.RespondActivityTaskCompletedRequest) *workflow.HistoryEvent
		AddActivityTaskFailedEvent(int64, int64, *workflow.RespondActivityTaskFailedRequest) *workflow.HistoryEvent
		AddActivityTaskScheduledEvent(int64, *workflow.ScheduleActivityTaskDecisionAttributes) (*workflow.HistoryEvent, *persistence.ActivityInfo)
		AddActivityTaskStartedEvent(*persistence.ActivityInfo, int64, string, string) *workflow.HistoryEvent
		AddActivityTaskTimedOutEvent(int64, int64, workflow.TimeoutType, []uint8) *workflow.HistoryEvent
		AddCancelTimerFailedEvent(int64, *workflow.CancelTimerDecisionAttributes, string) *workflow.HistoryEvent
		AddChildWorkflowExecutionCanceledEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionCanceledEventAttributes) *workflow.HistoryEvent
		AddChildWorkflowExecutionCompletedEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionCompletedEventAttributes) *workflow.HistoryEvent
		AddChildWorkflowExecutionFailedEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionFailedEventAttributes) *workflow.HistoryEvent
		AddChildWorkflowExecutionStartedEvent(*string, *workflow.WorkflowExecution, *workflow.WorkflowType, int64) *workflow.HistoryEvent
		AddChildWorkflowExecutionTerminatedEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionTerminatedEventAttributes) *workflow.HistoryEvent
		AddChildWorkflowExecutionTimedOutEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionTimedOutEventAttributes) *workflow.HistoryEvent
		AddCompletedWorkflowEvent(int64, *workflow.CompleteWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent
		AddContinueAsNewEvent(int64, *cache.DomainCacheEntry, string, *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, mutableState, error)
		AddDecisionTaskCompletedEvent(int64, int64, *workflow.RespondDecisionTaskCompletedRequest) *workflow.HistoryEvent
		AddDecisionTaskFailedEvent(int64, int64, workflow.DecisionTaskFailedCause, []uint8, string) *workflow.HistoryEvent
		AddDecisionTaskScheduleToStartTimeoutEvent(int64) *workflow.HistoryEvent
		AddDecisionTaskScheduledEvent() *decisionInfo
		AddDecisionTaskStartedEvent(int64, string, *workflow.PollForDecisionTaskRequest) (*workflow.HistoryEvent, *decisionInfo)
		AddDecisionTaskTimedOutEvent(int64, int64) *workflow.HistoryEvent
		AddExternalWorkflowExecutionCancelRequested(int64, string, string, string) *workflow.HistoryEvent
		AddExternalWorkflowExecutionSignaled(int64, string, string, string, []uint8) *workflow.HistoryEvent
		AddFailWorkflowEvent(int64, *workflow.FailWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent
		AddRecordMarkerEvent(int64, *workflow.RecordMarkerDecisionAttributes) *workflow.HistoryEvent
		AddRequestCancelActivityTaskFailedEvent(int64, string, string) *workflow.HistoryEvent
		AddRequestCancelExternalWorkflowExecutionFailedEvent(int64, int64, string, string, string, workflow.CancelExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent
		AddRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, string, *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *persistence.RequestCancelInfo)
		AddSignalExternalWorkflowExecutionFailedEvent(int64, int64, string, string, string, []uint8, workflow.SignalExternalWorkflowExecutionFailedCause) *workflow.HistoryEvent
		AddSignalExternalWorkflowExecutionInitiatedEvent(int64, string, *workflow.SignalExternalWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *persistence.SignalInfo)
		AddSignalRequested(requestID string)
		AddStartChildWorkflowExecutionFailedEvent(int64, workflow.ChildWorkflowExecutionFailedCause, *workflow.StartChildWorkflowExecutionInitiatedEventAttributes) *workflow.HistoryEvent
		AddStartChildWorkflowExecutionInitiatedEvent(int64, string, *workflow.StartChildWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *persistence.ChildExecutionInfo)
		AddTimeoutWorkflowEvent() *workflow.HistoryEvent
		AddTimerCanceledEvent(int64, *workflow.CancelTimerDecisionAttributes, string) *workflow.HistoryEvent
		AddTimerFiredEvent(int64, string) *workflow.HistoryEvent
		AddTimerStartedEvent(int64, *workflow.StartTimerDecisionAttributes) (*workflow.HistoryEvent, *persistence.TimerInfo)
		AddWorkflowExecutionCancelRequestedEvent(string, *h.RequestCancelWorkflowExecutionRequest) *workflow.HistoryEvent
		AddWorkflowExecutionCanceledEvent(int64, *workflow.CancelWorkflowExecutionDecisionAttributes) *workflow.HistoryEvent
		AddWorkflowExecutionSignaled(*workflow.SignalWorkflowExecutionRequest) *workflow.HistoryEvent
		AddWorkflowExecutionStartedEvent(workflow.WorkflowExecution, *h.StartWorkflowExecutionRequest) *workflow.HistoryEvent
		AddWorkflowExecutionTerminatedEvent(*workflow.TerminateWorkflowExecutionRequest) *workflow.HistoryEvent
		AfterAddDecisionTaskCompletedEvent(int64)
		BeforeAddDecisionTaskCompletedEvent()
		BufferReplicationTask(*h.ReplicateEventsRequest) error
		ClearStickyness()
		CloseUpdateSession() (*mutableStateSessionUpdates, error)
		CopyToPersistence() *persistence.WorkflowMutableState
		CreateActivityRetryTimer(*persistence.ActivityInfo, string) persistence.Task
		CreateNewHistoryEvent(eventType workflow.EventType) *workflow.HistoryEvent
		CreateNewHistoryEventWithTimestamp(eventType workflow.EventType, timestamp int64) *workflow.HistoryEvent
		CreateReplicationTask(int32, []byte) *persistence.HistoryReplicationTask
		CreateTransientDecisionEvents(di *decisionInfo, identity string) (*workflow.HistoryEvent, *workflow.HistoryEvent)
		DeleteActivity(int64) error
		DeleteBufferedReplicationTask(int64)
		DeleteDecision()
		DeletePendingChildExecution(int64)
		DeletePendingRequestCancel(int64)
		DeleteSignalRequested(requestID string)
		DeletePendingSignal(int64)
		DeleteUserTimer(string)
		FailDecision(bool)
		FlushBufferedEvents() error
		GetActivityByActivityID(string) (*persistence.ActivityInfo, bool)
		GetActivityInfo(int64) (*persistence.ActivityInfo, bool)
		GetActivityScheduledEvent(int64) (*workflow.HistoryEvent, bool)
		GetActivityStartedEvent(int64) (*workflow.HistoryEvent, bool)
		GetBufferedReplicationTask(int64) (*persistence.BufferedReplicationTask, bool)
		GetChildExecutionInfo(int64) (*persistence.ChildExecutionInfo, bool)
		GetChildExecutionInitiatedEvent(int64) (*workflow.HistoryEvent, bool)
		GetChildExecutionStartedEvent(int64) (*workflow.HistoryEvent, bool)
		GetCompletionEvent() (*workflow.HistoryEvent, bool)
		GetContinueAsNew() *persistence.CreateWorkflowExecutionRequest
		GetCurrentBranch() []byte
		GetCurrentVersion() int64
		GetExecutionInfo() *persistence.WorkflowExecutionInfo
		GetEventStoreVersion() int32
		GetHistoryBuilder() *historyBuilder
		GetHistorySize() int64
		GetInFlightDecisionTask() (*decisionInfo, bool)
		GetLastFirstEventID() int64
		GetLastUpdatedTimestamp() int64
		GetLastWriteVersion() int64
		GetNextEventID() int64
		GetPendingDecision(int64) (*decisionInfo, bool)
		GetPendingActivityInfos() map[int64]*persistence.ActivityInfo
		GetPendingTimerInfos() map[string]*persistence.TimerInfo
		GetPendingChildExecutionInfos() map[int64]*persistence.ChildExecutionInfo
		GetReplicationState() *persistence.ReplicationState
		GetRequestCancelInfo(int64) (*persistence.RequestCancelInfo, bool)
		GetRetryBackoffDuration(errReason string) time.Duration
		GetScheduleIDByActivityID(string) (int64, bool)
		GetSignalInfo(int64) (*persistence.SignalInfo, bool)
		GetStartVersion() int64
		GetUserTimer(string) (bool, *persistence.TimerInfo)
		GetWorkflowType() *workflow.WorkflowType
		HasBufferedEvents() bool
		HasBufferedReplicationTasks() bool
		HasInFlightDecisionTask() bool
		HasParentExecution() bool
		HasPendingDecisionTask() bool
		IncrementHistorySize(int)
		IsCancelRequested() (bool, string)
		IsSignalRequested(requestID string) bool
		IsStickyTaskListEnabled() bool
		IsWorkflowExecutionRunning() bool
		Load(*persistence.WorkflowMutableState)
		ReplicateActivityInfo(*h.SyncActivityRequest, bool) error
		ReplicateActivityTaskCancelRequestedEvent(*workflow.HistoryEvent)
		ReplicateActivityTaskCanceledEvent(*workflow.HistoryEvent) error
		ReplicateActivityTaskCompletedEvent(*workflow.HistoryEvent) error
		ReplicateActivityTaskFailedEvent(*workflow.HistoryEvent) error
		ReplicateActivityTaskScheduledEvent(*workflow.HistoryEvent) *persistence.ActivityInfo
		ReplicateActivityTaskStartedEvent(*workflow.HistoryEvent)
		ReplicateActivityTaskTimedOutEvent(*workflow.HistoryEvent) error
		ReplicateChildWorkflowExecutionCanceledEvent(*workflow.HistoryEvent)
		ReplicateChildWorkflowExecutionCompletedEvent(*workflow.HistoryEvent)
		ReplicateChildWorkflowExecutionFailedEvent(*workflow.HistoryEvent)
		ReplicateChildWorkflowExecutionStartedEvent(*workflow.HistoryEvent) error
		ReplicateChildWorkflowExecutionTerminatedEvent(*workflow.HistoryEvent)
		ReplicateChildWorkflowExecutionTimedOutEvent(*workflow.HistoryEvent)
		ReplicateDecisionTaskCompletedEvent(int64, int64)
		ReplicateDecisionTaskFailedEvent()
		ReplicateDecisionTaskScheduledEvent(int64, int64, string, int32, int64) *decisionInfo
		ReplicateDecisionTaskStartedEvent(*decisionInfo, int64, int64, int64, string, int64) *decisionInfo
		ReplicateDecisionTaskTimedOutEvent(workflow.TimeoutType)
		ReplicateExternalWorkflowExecutionCancelRequested(*workflow.HistoryEvent)
		ReplicateExternalWorkflowExecutionSignaled(*workflow.HistoryEvent)
		ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(*workflow.HistoryEvent)
		ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(*workflow.HistoryEvent, string) *persistence.RequestCancelInfo
		ReplicateSignalExternalWorkflowExecutionFailedEvent(*workflow.HistoryEvent)
		ReplicateSignalExternalWorkflowExecutionInitiatedEvent(*workflow.HistoryEvent, string) *persistence.SignalInfo
		ReplicateStartChildWorkflowExecutionFailedEvent(*workflow.HistoryEvent)
		ReplicateStartChildWorkflowExecutionInitiatedEvent(*workflow.HistoryEvent, string) *persistence.ChildExecutionInfo
		ReplicateTimerCanceledEvent(*workflow.HistoryEvent)
		ReplicateTimerFiredEvent(*workflow.HistoryEvent)
		ReplicateTimerStartedEvent(*workflow.HistoryEvent) *persistence.TimerInfo
		ReplicateTransientDecisionTaskScheduled() *decisionInfo
		ReplicateWorkflowExecutionCancelRequestedEvent(*workflow.HistoryEvent)
		ReplicateWorkflowExecutionCanceledEvent(*workflow.HistoryEvent)
		ReplicateWorkflowExecutionCompletedEvent(*workflow.HistoryEvent)
		ReplicateWorkflowExecutionContinuedAsNewEvent(string, string, *workflow.HistoryEvent, *workflow.HistoryEvent, *decisionInfo, mutableState)
		ReplicateWorkflowExecutionFailedEvent(*workflow.HistoryEvent)
		ReplicateWorkflowExecutionSignaled(*workflow.HistoryEvent)
		ReplicateWorkflowExecutionStartedEvent(string, *string, workflow.WorkflowExecution, string, *workflow.WorkflowExecutionStartedEventAttributes)
		ReplicateWorkflowExecutionTerminatedEvent(*workflow.HistoryEvent)
		ReplicateWorkflowExecutionTimedoutEvent(*workflow.HistoryEvent)
		ResetSnapshot(string) *persistence.ResetMutableStateRequest
		SetHistoryBuilder(hBuilder *historyBuilder)
		SetHistoryTree(treeID string) error
		SetNewRunSize(size int)
		UpdateActivity(*persistence.ActivityInfo) error
		UpdateActivityProgress(ai *persistence.ActivityInfo, request *workflow.RecordActivityTaskHeartbeatRequest)
		UpdateDecision(*decisionInfo)
		UpdateReplicationStateVersion(int64, bool)
		UpdateReplicationStateLastEventID(string, int64, int64)
		UpdateUserTimer(string, *persistence.TimerInfo)
	}
)
