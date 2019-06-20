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
		// They are useful for transient decision: when transient decision finally completes, use these timestamp to create scheduled/started events.
		// Also used for recording latency metrics
		ScheduledTimestamp int64
		StartedTimestamp   int64
	}

	mutableState interface {
		AddActivityTaskCancelRequestedEvent(int64, string, string) (*workflow.HistoryEvent, *persistence.ActivityInfo, error)
		AddActivityTaskCanceledEvent(int64, int64, int64, []uint8, string) (*workflow.HistoryEvent, error)
		AddActivityTaskCompletedEvent(int64, int64, *workflow.RespondActivityTaskCompletedRequest) (*workflow.HistoryEvent, error)
		AddActivityTaskFailedEvent(int64, int64, *workflow.RespondActivityTaskFailedRequest) (*workflow.HistoryEvent, error)
		AddActivityTaskScheduledEvent(int64, *workflow.ScheduleActivityTaskDecisionAttributes) (*workflow.HistoryEvent, *persistence.ActivityInfo, error)
		AddActivityTaskStartedEvent(*persistence.ActivityInfo, int64, string, string) (*workflow.HistoryEvent, error)
		AddActivityTaskTimedOutEvent(int64, int64, workflow.TimeoutType, []uint8) (*workflow.HistoryEvent, error)
		AddCancelTimerFailedEvent(int64, *workflow.CancelTimerDecisionAttributes, string) (*workflow.HistoryEvent, error)
		AddChildWorkflowExecutionCanceledEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionCanceledEventAttributes) (*workflow.HistoryEvent, error)
		AddChildWorkflowExecutionCompletedEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionCompletedEventAttributes) (*workflow.HistoryEvent, error)
		AddChildWorkflowExecutionFailedEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionFailedEventAttributes) (*workflow.HistoryEvent, error)
		AddChildWorkflowExecutionStartedEvent(*string, *workflow.WorkflowExecution, *workflow.WorkflowType, int64, *workflow.Header) (*workflow.HistoryEvent, error)
		AddChildWorkflowExecutionTerminatedEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionTerminatedEventAttributes) (*workflow.HistoryEvent, error)
		AddChildWorkflowExecutionTimedOutEvent(int64, *workflow.WorkflowExecution, *workflow.WorkflowExecutionTimedOutEventAttributes) (*workflow.HistoryEvent, error)
		AddCompletedWorkflowEvent(int64, *workflow.CompleteWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, error)
		AddContinueAsNewEvent(int64, int64, *cache.DomainCacheEntry, string, *workflow.ContinueAsNewWorkflowExecutionDecisionAttributes, int32) (*workflow.HistoryEvent, mutableState, error)
		AddDecisionTaskCompletedEvent(int64, int64, *workflow.RespondDecisionTaskCompletedRequest, int) (*workflow.HistoryEvent, error)
		AddDecisionTaskFailedEvent(scheduleEventID int64, startedEventID int64, cause workflow.DecisionTaskFailedCause, details []byte, identity, reason, baseRunID, newRunID string, forkEventVersion int64) (*workflow.HistoryEvent, error)
		AddDecisionTaskScheduleToStartTimeoutEvent(int64) (*workflow.HistoryEvent, error)
		AddDecisionTaskScheduledEvent() (*decisionInfo, error)
		AddDecisionTaskStartedEvent(int64, string, *workflow.PollForDecisionTaskRequest) (*workflow.HistoryEvent, *decisionInfo, error)
		AddDecisionTaskTimedOutEvent(int64, int64) (*workflow.HistoryEvent, error)
		AddExternalWorkflowExecutionCancelRequested(int64, string, string, string) (*workflow.HistoryEvent, error)
		AddExternalWorkflowExecutionSignaled(int64, string, string, string, []uint8) (*workflow.HistoryEvent, error)
		AddFailWorkflowEvent(int64, *workflow.FailWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, error)
		AddRecordMarkerEvent(int64, *workflow.RecordMarkerDecisionAttributes) (*workflow.HistoryEvent, error)
		AddRequestCancelActivityTaskFailedEvent(int64, string, string) (*workflow.HistoryEvent, error)
		AddRequestCancelExternalWorkflowExecutionFailedEvent(int64, int64, string, string, string, workflow.CancelExternalWorkflowExecutionFailedCause) (*workflow.HistoryEvent, error)
		AddRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, string, *workflow.RequestCancelExternalWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *persistence.RequestCancelInfo, error)
		AddSignalExternalWorkflowExecutionFailedEvent(int64, int64, string, string, string, []uint8, workflow.SignalExternalWorkflowExecutionFailedCause) (*workflow.HistoryEvent, error)
		AddSignalExternalWorkflowExecutionInitiatedEvent(int64, string, *workflow.SignalExternalWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *persistence.SignalInfo, error)
		AddSignalRequested(requestID string)
		AddStartChildWorkflowExecutionFailedEvent(int64, workflow.ChildWorkflowExecutionFailedCause, *workflow.StartChildWorkflowExecutionInitiatedEventAttributes) (*workflow.HistoryEvent, error)
		AddStartChildWorkflowExecutionInitiatedEvent(int64, string, *workflow.StartChildWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, *persistence.ChildExecutionInfo, error)
		AddTimeoutWorkflowEvent() (*workflow.HistoryEvent, error)
		AddTimerCanceledEvent(int64, *workflow.CancelTimerDecisionAttributes, string) (*workflow.HistoryEvent, error)
		AddTimerFiredEvent(int64, string) (*workflow.HistoryEvent, error)
		AddTimerStartedEvent(int64, *workflow.StartTimerDecisionAttributes) (*workflow.HistoryEvent, *persistence.TimerInfo, error)
		AddWorkflowExecutionCancelRequestedEvent(string, *h.RequestCancelWorkflowExecutionRequest) (*workflow.HistoryEvent, error)
		AddWorkflowExecutionCanceledEvent(int64, *workflow.CancelWorkflowExecutionDecisionAttributes) (*workflow.HistoryEvent, error)
		AddWorkflowExecutionSignaled(signalName string, input []byte, identity string) (*workflow.HistoryEvent, error)
		AddWorkflowExecutionStartedEvent(workflow.WorkflowExecution, *h.StartWorkflowExecutionRequest) (*workflow.HistoryEvent, error)
		AddWorkflowExecutionTerminatedEvent(reason string, details []byte, identity string) (*workflow.HistoryEvent, error)
		ClearStickyness()
		CloseUpdateSession() (*mutableStateSessionUpdates, error)
		CheckResettable() error
		CopyToPersistence() *persistence.WorkflowMutableState
		CreateActivityRetryTimer(*persistence.ActivityInfo, string) persistence.Task
		CreateNewHistoryEvent(eventType workflow.EventType) *workflow.HistoryEvent
		CreateNewHistoryEventWithTimestamp(eventType workflow.EventType, timestamp int64) *workflow.HistoryEvent
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
		GetAllBufferedReplicationTasks() map[int64]*persistence.BufferedReplicationTask
		GetChildExecutionInfo(int64) (*persistence.ChildExecutionInfo, bool)
		GetChildExecutionInitiatedEvent(int64) (*workflow.HistoryEvent, bool)
		GetCompletionEvent() (*workflow.HistoryEvent, bool)
		GetStartEvent() (*workflow.HistoryEvent, bool)
		GetContinueAsNew() *persistence.CreateWorkflowExecutionRequest
		GetCurrentBranch() []byte
		GetVersionHistories() *persistence.VersionHistories
		GetCurrentVersion() int64
		GetExecutionInfo() *persistence.WorkflowExecutionInfo
		GetEventStoreVersion() int32
		GetHistoryBuilder() *historyBuilder
		GetHistorySize() int64
		GetInFlightDecisionTask() (*decisionInfo, bool)
		GetLastFirstEventID() int64
		GetLastWriteVersion() int64
		GetNextEventID() int64
		GetPreviousStartedEventID() int64
		GetPendingDecision(int64) (*decisionInfo, bool)
		GetPendingActivityInfos() map[int64]*persistence.ActivityInfo
		GetPendingTimerInfos() map[string]*persistence.TimerInfo
		GetPendingChildExecutionInfos() map[int64]*persistence.ChildExecutionInfo
		GetReplicationState() *persistence.ReplicationState
		GetRequestCancelInfo(int64) (*persistence.RequestCancelInfo, bool)
		GetRetryBackoffDuration(errReason string) time.Duration
		GetCronBackoffDuration() time.Duration
		GetScheduleIDByActivityID(string) (int64, bool)
		GetSignalInfo(int64) (*persistence.SignalInfo, bool)
		GetAllSignalsToSend() map[int64]*persistence.SignalInfo
		GetAllRequestCancels() map[int64]*persistence.RequestCancelInfo
		GetStartVersion() int64
		GetUserTimer(string) (bool, *persistence.TimerInfo)
		GetWorkflowType() *workflow.WorkflowType
		GetWorkflowStateCloseStatus() (int, int)
		HasBufferedEvents() bool
		HasBufferedReplicationTasks() bool
		HasInFlightDecisionTask() bool
		HasParentExecution() bool
		HasPendingDecisionTask() bool
		HasProcessedOrPendingDecisionTask() bool
		IncrementHistorySize(int)
		IsCancelRequested() (bool, string)
		IsSignalRequested(requestID string) bool
		IsStickyTaskListEnabled() bool
		IsWorkflowExecutionRunning() bool
		Load(*persistence.WorkflowMutableState)
		ReplicateActivityInfo(*h.SyncActivityRequest, bool) error
		ReplicateActivityTaskCancelRequestedEvent(*workflow.HistoryEvent) error
		ReplicateActivityTaskCanceledEvent(*workflow.HistoryEvent) error
		ReplicateActivityTaskCompletedEvent(*workflow.HistoryEvent) error
		ReplicateActivityTaskFailedEvent(*workflow.HistoryEvent) error
		ReplicateActivityTaskScheduledEvent(int64, *workflow.HistoryEvent) (*persistence.ActivityInfo, error)
		ReplicateActivityTaskStartedEvent(*workflow.HistoryEvent) error
		ReplicateActivityTaskTimedOutEvent(*workflow.HistoryEvent) error
		ReplicateChildWorkflowExecutionCanceledEvent(*workflow.HistoryEvent) error
		ReplicateChildWorkflowExecutionCompletedEvent(*workflow.HistoryEvent) error
		ReplicateChildWorkflowExecutionFailedEvent(*workflow.HistoryEvent) error
		ReplicateChildWorkflowExecutionStartedEvent(*workflow.HistoryEvent) error
		ReplicateChildWorkflowExecutionTerminatedEvent(*workflow.HistoryEvent) error
		ReplicateChildWorkflowExecutionTimedOutEvent(*workflow.HistoryEvent) error
		ReplicateDecisionTaskCompletedEvent(*workflow.HistoryEvent) error
		ReplicateDecisionTaskFailedEvent() error
		ReplicateDecisionTaskScheduledEvent(int64, int64, string, int32, int64, int64) (*decisionInfo, error)
		ReplicateDecisionTaskStartedEvent(*decisionInfo, int64, int64, int64, string, int64) (*decisionInfo, error)
		ReplicateDecisionTaskTimedOutEvent(workflow.TimeoutType) error
		ReplicateExternalWorkflowExecutionCancelRequested(*workflow.HistoryEvent) error
		ReplicateExternalWorkflowExecutionSignaled(*workflow.HistoryEvent) error
		ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(*workflow.HistoryEvent) error
		ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(*workflow.HistoryEvent, string) (*persistence.RequestCancelInfo, error)
		ReplicateSignalExternalWorkflowExecutionFailedEvent(*workflow.HistoryEvent) error
		ReplicateSignalExternalWorkflowExecutionInitiatedEvent(*workflow.HistoryEvent, string) (*persistence.SignalInfo, error)
		ReplicateStartChildWorkflowExecutionFailedEvent(*workflow.HistoryEvent) error
		ReplicateStartChildWorkflowExecutionInitiatedEvent(int64, *workflow.HistoryEvent, string) (*persistence.ChildExecutionInfo, error)
		ReplicateTimerCanceledEvent(*workflow.HistoryEvent) error
		ReplicateTimerFiredEvent(*workflow.HistoryEvent) error
		ReplicateTimerStartedEvent(*workflow.HistoryEvent) (*persistence.TimerInfo, error)
		ReplicateTransientDecisionTaskScheduled() (*decisionInfo, error)
		ReplicateWorkflowExecutionCancelRequestedEvent(*workflow.HistoryEvent) error
		ReplicateWorkflowExecutionCanceledEvent(int64, *workflow.HistoryEvent) error
		ReplicateWorkflowExecutionCompletedEvent(int64, *workflow.HistoryEvent) error
		ReplicateWorkflowExecutionContinuedAsNewEvent(int64, string, string, *workflow.HistoryEvent, *workflow.HistoryEvent, *decisionInfo, mutableState, int32, int32) error
		ReplicateWorkflowExecutionFailedEvent(int64, *workflow.HistoryEvent) error
		ReplicateWorkflowExecutionSignaled(*workflow.HistoryEvent) error
		ReplicateWorkflowExecutionStartedEvent(string, *string, workflow.WorkflowExecution, string, *workflow.HistoryEvent) error
		ReplicateWorkflowExecutionTerminatedEvent(int64, *workflow.HistoryEvent) error
		ReplicateWorkflowExecutionTimedoutEvent(int64, *workflow.HistoryEvent) error
		ResetSnapshot(string, int64, int, []persistence.Task, []persistence.Task, []persistence.Task) *persistence.ResetMutableStateRequest
		SetHistoryBuilder(hBuilder *historyBuilder)
		SetHistoryTree(treeID string) error
		SetNewRunSize(size int)
		SetVersionHistories(*persistence.VersionHistories) error
		UpdateActivity(*persistence.ActivityInfo) error
		UpdateActivityProgress(ai *persistence.ActivityInfo, request *workflow.RecordActivityTaskHeartbeatRequest)
		UpdateDecision(*decisionInfo)
		UpdateReplicationStateVersion(int64, bool)
		UpdateReplicationStateLastEventID(string, int64, int64)
		UpdateUserTimer(string, *persistence.TimerInfo)
		UpdateWorkflowStateCloseStatus(state int, closeStatus int) error
	}
)
