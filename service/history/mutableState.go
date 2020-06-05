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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination mutableState_mock.go

package history

import (
	"time"

	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	failurepb "go.temporal.io/temporal-proto/failure"
	"go.temporal.io/temporal-proto/workflowservice"

	executiongenpb "github.com/temporalio/temporal/.gen/proto/execution"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/persistence"
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
		// Scheduled and Started timestamps are useful for transient decision: when transient decision finally completes,
		// use these timestamp to create scheduled/started events.
		// Also used for recording latency metrics
		ScheduledTimestamp int64
		StartedTimestamp   int64
		// OriginalScheduledTimestamp is to record the first scheduled decision during decision heartbeat.
		// Client may heartbeat decision by RespondDecisionTaskComplete with ForceCreateNewDecisionTask == true
		// In this case, OriginalScheduledTimestamp won't change. Then when current time - OriginalScheduledTimestamp exceeds
		// some threshold, server can interrupt the heartbeat by enforcing to timeout the decision.
		OriginalScheduledTimestamp int64
	}

	mutableState interface {
		AddActivityTaskCancelRequestedEvent(int64, int64, string) (*eventpb.HistoryEvent, *persistence.ActivityInfo, error)
		AddActivityTaskCanceledEvent(int64, int64, int64, *commonpb.Payloads, string) (*eventpb.HistoryEvent, error)
		AddActivityTaskCompletedEvent(int64, int64, *workflowservice.RespondActivityTaskCompletedRequest) (*eventpb.HistoryEvent, error)
		AddActivityTaskFailedEvent(int64, int64, *failurepb.Failure, commonpb.RetryStatus, string) (*eventpb.HistoryEvent, error)
		AddActivityTaskScheduledEvent(int64, *decisionpb.ScheduleActivityTaskDecisionAttributes) (*eventpb.HistoryEvent, *persistence.ActivityInfo, error)
		AddActivityTaskStartedEvent(*persistence.ActivityInfo, int64, string, string) (*eventpb.HistoryEvent, error)
		AddActivityTaskTimedOutEvent(int64, int64, *failurepb.Failure, commonpb.RetryStatus) (*eventpb.HistoryEvent, error)
		AddCancelTimerFailedEvent(int64, *decisionpb.CancelTimerDecisionAttributes, string) (*eventpb.HistoryEvent, error)
		AddChildWorkflowExecutionCanceledEvent(int64, *commonpb.WorkflowExecution, *eventpb.WorkflowExecutionCanceledEventAttributes) (*eventpb.HistoryEvent, error)
		AddChildWorkflowExecutionCompletedEvent(int64, *commonpb.WorkflowExecution, *eventpb.WorkflowExecutionCompletedEventAttributes) (*eventpb.HistoryEvent, error)
		AddChildWorkflowExecutionFailedEvent(int64, *commonpb.WorkflowExecution, *eventpb.WorkflowExecutionFailedEventAttributes) (*eventpb.HistoryEvent, error)
		AddChildWorkflowExecutionStartedEvent(string, *commonpb.WorkflowExecution, *commonpb.WorkflowType, int64, *commonpb.Header) (*eventpb.HistoryEvent, error)
		AddChildWorkflowExecutionTerminatedEvent(int64, *commonpb.WorkflowExecution, *eventpb.WorkflowExecutionTerminatedEventAttributes) (*eventpb.HistoryEvent, error)
		AddChildWorkflowExecutionTimedOutEvent(int64, *commonpb.WorkflowExecution, *eventpb.WorkflowExecutionTimedOutEventAttributes) (*eventpb.HistoryEvent, error)
		AddCompletedWorkflowEvent(int64, *decisionpb.CompleteWorkflowExecutionDecisionAttributes) (*eventpb.HistoryEvent, error)
		AddContinueAsNewEvent(int64, int64, string, *decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes) (*eventpb.HistoryEvent, mutableState, error)
		AddDecisionTaskCompletedEvent(int64, int64, *workflowservice.RespondDecisionTaskCompletedRequest, int) (*eventpb.HistoryEvent, error)
		AddDecisionTaskFailedEvent(scheduleEventID int64, startedEventID int64, cause eventpb.DecisionTaskFailedCause, failure *failurepb.Failure, identity, binChecksum, baseRunID, newRunID string, forkEventVersion int64) (*eventpb.HistoryEvent, error)
		AddDecisionTaskScheduleToStartTimeoutEvent(int64) (*eventpb.HistoryEvent, error)
		AddFirstDecisionTaskScheduled(*eventpb.HistoryEvent) error
		AddDecisionTaskScheduledEvent(bypassTaskGeneration bool) (*decisionInfo, error)
		AddDecisionTaskScheduledEventAsHeartbeat(bypassTaskGeneration bool, originalScheduledTimestamp int64) (*decisionInfo, error)
		AddDecisionTaskStartedEvent(int64, string, *workflowservice.PollForDecisionTaskRequest) (*eventpb.HistoryEvent, *decisionInfo, error)
		AddDecisionTaskTimedOutEvent(int64, int64) (*eventpb.HistoryEvent, error)
		AddExternalWorkflowExecutionCancelRequested(int64, string, string, string) (*eventpb.HistoryEvent, error)
		AddExternalWorkflowExecutionSignaled(int64, string, string, string, string) (*eventpb.HistoryEvent, error)
		AddFailWorkflowEvent(int64, commonpb.RetryStatus, *decisionpb.FailWorkflowExecutionDecisionAttributes) (*eventpb.HistoryEvent, error)
		AddRecordMarkerEvent(int64, *decisionpb.RecordMarkerDecisionAttributes) (*eventpb.HistoryEvent, error)
		AddRequestCancelExternalWorkflowExecutionFailedEvent(int64, int64, string, string, string, eventpb.CancelExternalWorkflowExecutionFailedCause) (*eventpb.HistoryEvent, error)
		AddRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, string, *decisionpb.RequestCancelExternalWorkflowExecutionDecisionAttributes) (*eventpb.HistoryEvent, *persistenceblobs.RequestCancelInfo, error)
		AddSignalExternalWorkflowExecutionFailedEvent(int64, int64, string, string, string, string, eventpb.SignalExternalWorkflowExecutionFailedCause) (*eventpb.HistoryEvent, error)
		AddSignalExternalWorkflowExecutionInitiatedEvent(int64, string, *decisionpb.SignalExternalWorkflowExecutionDecisionAttributes) (*eventpb.HistoryEvent, *persistenceblobs.SignalInfo, error)
		AddSignalRequested(requestID string)
		AddStartChildWorkflowExecutionFailedEvent(int64, eventpb.StartChildWorkflowExecutionFailedCause, *eventpb.StartChildWorkflowExecutionInitiatedEventAttributes) (*eventpb.HistoryEvent, error)
		AddStartChildWorkflowExecutionInitiatedEvent(int64, string, *decisionpb.StartChildWorkflowExecutionDecisionAttributes) (*eventpb.HistoryEvent, *persistence.ChildExecutionInfo, error)
		AddTimeoutWorkflowEvent(int64, commonpb.RetryStatus) (*eventpb.HistoryEvent, error)
		AddTimerCanceledEvent(int64, *decisionpb.CancelTimerDecisionAttributes, string) (*eventpb.HistoryEvent, error)
		AddTimerFiredEvent(string) (*eventpb.HistoryEvent, error)
		AddTimerStartedEvent(int64, *decisionpb.StartTimerDecisionAttributes) (*eventpb.HistoryEvent, *persistenceblobs.TimerInfo, error)
		AddUpsertWorkflowSearchAttributesEvent(int64, *decisionpb.UpsertWorkflowSearchAttributesDecisionAttributes) (*eventpb.HistoryEvent, error)
		AddWorkflowExecutionCancelRequestedEvent(string, *historyservice.RequestCancelWorkflowExecutionRequest) (*eventpb.HistoryEvent, error)
		AddWorkflowExecutionCanceledEvent(int64, *decisionpb.CancelWorkflowExecutionDecisionAttributes) (*eventpb.HistoryEvent, error)
		AddWorkflowExecutionSignaled(signalName string, input *commonpb.Payloads, identity string) (*eventpb.HistoryEvent, error)
		AddWorkflowExecutionStartedEvent(commonpb.WorkflowExecution, *historyservice.StartWorkflowExecutionRequest) (*eventpb.HistoryEvent, error)
		AddWorkflowExecutionTerminatedEvent(firstEventID int64, reason string, details *commonpb.Payloads, identity string) (*eventpb.HistoryEvent, error)
		ClearStickyness()
		CheckResettable() error
		CopyToPersistence() *persistence.WorkflowMutableState
		RetryActivity(ai *persistence.ActivityInfo, failure *failurepb.Failure) (commonpb.RetryStatus, error)
		CreateNewHistoryEvent(eventType eventpb.EventType) *eventpb.HistoryEvent
		CreateNewHistoryEventWithTimestamp(eventType eventpb.EventType, timestamp int64) *eventpb.HistoryEvent
		CreateTransientDecisionEvents(di *decisionInfo, identity string) (*eventpb.HistoryEvent, *eventpb.HistoryEvent)
		DeleteDecision()
		DeleteSignalRequested(requestID string)
		FailDecision(bool)
		FlushBufferedEvents() error
		GetActivityByActivityID(string) (*persistence.ActivityInfo, bool)
		GetActivityInfo(int64) (*persistence.ActivityInfo, bool)
		GetActivityScheduledEvent(int64) (*eventpb.HistoryEvent, error)
		GetChildExecutionInfo(int64) (*persistence.ChildExecutionInfo, bool)
		GetChildExecutionInitiatedEvent(int64) (*eventpb.HistoryEvent, error)
		GetCompletionEvent() (*eventpb.HistoryEvent, error)
		GetDecisionInfo(int64) (*decisionInfo, bool)
		GetNamespaceEntry() *cache.NamespaceCacheEntry
		GetStartEvent() (*eventpb.HistoryEvent, error)
		GetCurrentBranchToken() ([]byte, error)
		GetVersionHistories() *persistence.VersionHistories
		GetCurrentVersion() int64
		GetExecutionInfo() *persistence.WorkflowExecutionInfo
		GetHistoryBuilder() *historyBuilder
		GetInFlightDecision() (*decisionInfo, bool)
		GetPendingDecision() (*decisionInfo, bool)
		GetLastFirstEventID() int64
		GetLastWriteVersion() (int64, error)
		GetNextEventID() int64
		GetPreviousStartedEventID() int64
		GetPendingActivityInfos() map[int64]*persistence.ActivityInfo
		GetPendingTimerInfos() map[string]*persistenceblobs.TimerInfo
		GetPendingChildExecutionInfos() map[int64]*persistence.ChildExecutionInfo
		GetPendingRequestCancelExternalInfos() map[int64]*persistenceblobs.RequestCancelInfo
		GetPendingSignalExternalInfos() map[int64]*persistenceblobs.SignalInfo
		GetReplicationState() *persistence.ReplicationState
		GetRequestCancelInfo(int64) (*persistenceblobs.RequestCancelInfo, bool)
		GetRetryBackoffDuration(failure *failurepb.Failure) (time.Duration, commonpb.RetryStatus)
		GetCronBackoffDuration() (time.Duration, error)
		GetSignalInfo(int64) (*persistenceblobs.SignalInfo, bool)
		GetStartVersion() (int64, error)
		GetUserTimerInfoByEventID(int64) (*persistenceblobs.TimerInfo, bool)
		GetUserTimerInfo(string) (*persistenceblobs.TimerInfo, bool)
		GetWorkflowType() *commonpb.WorkflowType
		GetWorkflowStateStatus() (executiongenpb.WorkflowExecutionState, executionpb.WorkflowExecutionStatus)
		GetQueryRegistry() queryRegistry
		HasBufferedEvents() bool
		HasInFlightDecision() bool
		HasParentExecution() bool
		HasPendingDecision() bool
		HasProcessedOrPendingDecision() bool
		IsCancelRequested() (bool, string)
		IsCurrentWorkflowGuaranteed() bool
		IsSignalRequested(requestID string) bool
		IsStickyTaskListEnabled() bool
		IsWorkflowExecutionRunning() bool
		IsResourceDuplicated(resourceDedupKey definition.DeduplicationID) bool
		UpdateDuplicatedResource(resourceDedupKey definition.DeduplicationID)
		Load(*persistence.WorkflowMutableState)
		ReplicateActivityInfo(*historyservice.SyncActivityRequest, bool) error
		ReplicateActivityTaskCancelRequestedEvent(*eventpb.HistoryEvent) error
		ReplicateActivityTaskCanceledEvent(*eventpb.HistoryEvent) error
		ReplicateActivityTaskCompletedEvent(*eventpb.HistoryEvent) error
		ReplicateActivityTaskFailedEvent(*eventpb.HistoryEvent) error
		ReplicateActivityTaskScheduledEvent(int64, *eventpb.HistoryEvent) (*persistence.ActivityInfo, error)
		ReplicateActivityTaskStartedEvent(*eventpb.HistoryEvent) error
		ReplicateActivityTaskTimedOutEvent(*eventpb.HistoryEvent) error
		ReplicateChildWorkflowExecutionCanceledEvent(*eventpb.HistoryEvent) error
		ReplicateChildWorkflowExecutionCompletedEvent(*eventpb.HistoryEvent) error
		ReplicateChildWorkflowExecutionFailedEvent(*eventpb.HistoryEvent) error
		ReplicateChildWorkflowExecutionStartedEvent(*eventpb.HistoryEvent) error
		ReplicateChildWorkflowExecutionTerminatedEvent(*eventpb.HistoryEvent) error
		ReplicateChildWorkflowExecutionTimedOutEvent(*eventpb.HistoryEvent) error
		ReplicateDecisionTaskCompletedEvent(*eventpb.HistoryEvent) error
		ReplicateDecisionTaskFailedEvent() error
		ReplicateDecisionTaskScheduledEvent(int64, int64, string, int32, int64, int64, int64) (*decisionInfo, error)
		ReplicateDecisionTaskStartedEvent(*decisionInfo, int64, int64, int64, string, int64) (*decisionInfo, error)
		ReplicateDecisionTaskTimedOutEvent(commonpb.TimeoutType) error
		ReplicateExternalWorkflowExecutionCancelRequested(*eventpb.HistoryEvent) error
		ReplicateExternalWorkflowExecutionSignaled(*eventpb.HistoryEvent) error
		ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(*eventpb.HistoryEvent) error
		ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, *eventpb.HistoryEvent, string) (*persistenceblobs.RequestCancelInfo, error)
		ReplicateSignalExternalWorkflowExecutionFailedEvent(*eventpb.HistoryEvent) error
		ReplicateSignalExternalWorkflowExecutionInitiatedEvent(int64, *eventpb.HistoryEvent, string) (*persistenceblobs.SignalInfo, error)
		ReplicateStartChildWorkflowExecutionFailedEvent(*eventpb.HistoryEvent) error
		ReplicateStartChildWorkflowExecutionInitiatedEvent(int64, *eventpb.HistoryEvent, string) (*persistence.ChildExecutionInfo, error)
		ReplicateTimerCanceledEvent(*eventpb.HistoryEvent) error
		ReplicateTimerFiredEvent(*eventpb.HistoryEvent) error
		ReplicateTimerStartedEvent(*eventpb.HistoryEvent) (*persistenceblobs.TimerInfo, error)
		ReplicateTransientDecisionTaskScheduled() (*decisionInfo, error)
		ReplicateUpsertWorkflowSearchAttributesEvent(*eventpb.HistoryEvent)
		ReplicateWorkflowExecutionCancelRequestedEvent(*eventpb.HistoryEvent) error
		ReplicateWorkflowExecutionCanceledEvent(int64, *eventpb.HistoryEvent) error
		ReplicateWorkflowExecutionCompletedEvent(int64, *eventpb.HistoryEvent) error
		ReplicateWorkflowExecutionContinuedAsNewEvent(int64, string, *eventpb.HistoryEvent) error
		ReplicateWorkflowExecutionFailedEvent(int64, *eventpb.HistoryEvent) error
		ReplicateWorkflowExecutionSignaled(*eventpb.HistoryEvent) error
		ReplicateWorkflowExecutionStartedEvent(string, commonpb.WorkflowExecution, string, *eventpb.HistoryEvent) error
		ReplicateWorkflowExecutionTerminatedEvent(int64, *eventpb.HistoryEvent) error
		ReplicateWorkflowExecutionTimedoutEvent(int64, *eventpb.HistoryEvent) error
		SetCurrentBranchToken(branchToken []byte) error
		SetHistoryBuilder(hBuilder *historyBuilder)
		SetHistoryTree(treeID string) error
		SetVersionHistories(*persistence.VersionHistories) error
		UpdateActivity(*persistence.ActivityInfo) error
		UpdateActivityProgress(ai *persistence.ActivityInfo, request *workflowservice.RecordActivityTaskHeartbeatRequest)
		UpdateDecision(*decisionInfo)
		UpdateReplicationStateVersion(int64, bool)
		UpdateReplicationStateLastEventID(int64, int64)
		UpdateUserTimer(*persistenceblobs.TimerInfo) error
		UpdateCurrentVersion(version int64, forceUpdate bool) error
		UpdateWorkflowStateStatus(state executiongenpb.WorkflowExecutionState, status executionpb.WorkflowExecutionStatus) error

		AddTransferTasks(transferTasks ...persistence.Task)
		AddTimerTasks(timerTasks ...persistence.Task)
		SetUpdateCondition(int64)
		GetUpdateCondition() int64

		StartTransaction(entry *cache.NamespaceCacheEntry) (bool, error)
		StartTransactionSkipDecisionFail(entry *cache.NamespaceCacheEntry) error
		CloseTransactionAsMutation(now time.Time, transactionPolicy transactionPolicy) (*persistence.WorkflowMutation, []*persistence.WorkflowEvents, error)
		CloseTransactionAsSnapshot(now time.Time, transactionPolicy transactionPolicy) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error)
	}
)
