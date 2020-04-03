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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination mutableState_mock.go

package history

import (
	"time"

	commonpb "go.temporal.io/temporal-proto/common"
	decisionpb "go.temporal.io/temporal-proto/decision"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	filterpb "go.temporal.io/temporal-proto/filter"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	querypb "go.temporal.io/temporal-proto/query"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"
	versionpb "go.temporal.io/temporal-proto/version"
	"go.temporal.io/temporal-proto/workflowservice"

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
		AddActivityTaskCancelRequestedEvent(int64, string, string) (*historypb.HistoryEvent, *persistence.ActivityInfo, error)
		AddActivityTaskCanceledEvent(int64, int64, int64, []uint8, string) (*historypb.HistoryEvent, error)
		AddActivityTaskCompletedEvent(int64, int64, *workflowservice.RespondActivityTaskCompletedRequest) (*historypb.HistoryEvent, error)
		AddActivityTaskFailedEvent(int64, int64, *workflowservice.RespondActivityTaskFailedRequest) (*historypb.HistoryEvent, error)
		AddActivityTaskScheduledEvent(int64, *decisionpb.ScheduleActivityTaskDecisionAttributes) (*historypb.HistoryEvent, *persistence.ActivityInfo, error)
		AddActivityTaskStartedEvent(*persistence.ActivityInfo, int64, string, string) (*historypb.HistoryEvent, error)
		AddActivityTaskTimedOutEvent(int64, int64, eventpb.TimeoutType, []uint8) (*historypb.HistoryEvent, error)
		AddCancelTimerFailedEvent(int64, *decisionpb.CancelTimerDecisionAttributes, string) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionCanceledEvent(int64, *executionpb.WorkflowExecution, *historypb.WorkflowExecutionCanceledEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionCompletedEvent(int64, *executionpb.WorkflowExecution, *historypb.WorkflowExecutionCompletedEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionFailedEvent(int64, *executionpb.WorkflowExecution, *historypb.WorkflowExecutionFailedEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionStartedEvent(string, *executionpb.WorkflowExecution, *commonpb.WorkflowType, int64, *commonpb.Header) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionTerminatedEvent(int64, *executionpb.WorkflowExecution, *historypb.WorkflowExecutionTerminatedEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionTimedOutEvent(int64, *executionpb.WorkflowExecution, *historypb.WorkflowExecutionTimedOutEventAttributes) (*historypb.HistoryEvent, error)
		AddCompletedWorkflowEvent(int64, *decisionpb.CompleteWorkflowExecutionDecisionAttributes) (*historypb.HistoryEvent, error)
		AddContinueAsNewEvent(int64, int64, string, *decisionpb.ContinueAsNewWorkflowExecutionDecisionAttributes) (*historypb.HistoryEvent, mutableState, error)
		AddDecisionTaskCompletedEvent(int64, int64, *workflowservice.RespondDecisionTaskCompletedRequest, int) (*historypb.HistoryEvent, error)
		AddDecisionTaskFailedEvent(scheduleEventID int64, startedEventID int64, cause eventpb.DecisionTaskFailedCause, details []byte, identity, reason, binChecksum, baseRunID, newRunID string, forkEventVersion int64) (*historypb.HistoryEvent, error)
		AddDecisionTaskScheduleToStartTimeoutEvent(int64) (*historypb.HistoryEvent, error)
		AddFirstDecisionTaskScheduled(*historypb.HistoryEvent) error
		AddDecisionTaskScheduledEvent(bypassTaskGeneration bool) (*decisionInfo, error)
		AddDecisionTaskScheduledEventAsHeartbeat(bypassTaskGeneration bool, originalScheduledTimestamp int64) (*decisionInfo, error)
		AddDecisionTaskStartedEvent(int64, string, *workflowservice.PollForDecisionTaskRequest) (*historypb.HistoryEvent, *decisionInfo, error)
		AddDecisionTaskTimedOutEvent(int64, int64) (*historypb.HistoryEvent, error)
		AddExternalWorkflowExecutionCancelRequested(int64, string, string, string) (*historypb.HistoryEvent, error)
		AddExternalWorkflowExecutionSignaled(int64, string, string, string, []uint8) (*historypb.HistoryEvent, error)
		AddFailWorkflowEvent(int64, *decisionpb.FailWorkflowExecutionDecisionAttributes) (*historypb.HistoryEvent, error)
		AddRecordMarkerEvent(int64, *decisionpb.RecordMarkerDecisionAttributes) (*historypb.HistoryEvent, error)
		AddRequestCancelActivityTaskFailedEvent(int64, string, string) (*historypb.HistoryEvent, error)
		AddRequestCancelExternalWorkflowExecutionFailedEvent(int64, int64, string, string, string, eventpb.CancelExternalWorkflowExecutionFailedCause) (*historypb.HistoryEvent, error)
		AddRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, string, *decisionpb.RequestCancelExternalWorkflowExecutionDecisionAttributes) (*historypb.HistoryEvent, *persistenceblobs.RequestCancelInfo, error)
		AddSignalExternalWorkflowExecutionFailedEvent(int64, int64, string, string, string, []uint8, eventpb.SignalExternalWorkflowExecutionFailedCause) (*historypb.HistoryEvent, error)
		AddSignalExternalWorkflowExecutionInitiatedEvent(int64, string, *decisionpb.SignalExternalWorkflowExecutionDecisionAttributes) (*historypb.HistoryEvent, *persistenceblobs.SignalInfo, error)
		AddSignalRequested(requestID string)
		AddStartChildWorkflowExecutionFailedEvent(int64, eventpb.ChildWorkflowExecutionFailedCause, *historypb.StartChildWorkflowExecutionInitiatedEventAttributes) (*historypb.HistoryEvent, error)
		AddStartChildWorkflowExecutionInitiatedEvent(int64, string, *decisionpb.StartChildWorkflowExecutionDecisionAttributes) (*historypb.HistoryEvent, *persistence.ChildExecutionInfo, error)
		AddTimeoutWorkflowEvent(int64) (*historypb.HistoryEvent, error)
		AddTimerCanceledEvent(int64, *decisionpb.CancelTimerDecisionAttributes, string) (*historypb.HistoryEvent, error)
		AddTimerFiredEvent(string) (*historypb.HistoryEvent, error)
		AddTimerStartedEvent(int64, *decisionpb.StartTimerDecisionAttributes) (*historypb.HistoryEvent, *persistenceblobs.TimerInfo, error)
		AddUpsertWorkflowSearchAttributesEvent(int64, *decisionpb.UpsertWorkflowSearchAttributesDecisionAttributes) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionCancelRequestedEvent(string, *historyservice.RequestCancelWorkflowExecutionRequest) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionCanceledEvent(int64, *decisionpb.CancelWorkflowExecutionDecisionAttributes) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionSignaled(signalName string, input []byte, identity string) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionStartedEvent(executionpb.WorkflowExecution, *historyservice.StartWorkflowExecutionRequest) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionTerminatedEvent(firstEventID int64, reason string, details []byte, identity string) (*historypb.HistoryEvent, error)
		ClearStickyness()
		CheckResettable() error
		CopyToPersistence() *persistence.WorkflowMutableState
		RetryActivity(ai *persistence.ActivityInfo, failureReason string, failureDetails []byte) (bool, error)
		CreateNewHistoryEvent(eventType eventpb.EventType) *historypb.HistoryEvent
		CreateNewHistoryEventWithTimestamp(eventType eventpb.EventType, timestamp int64) *historypb.HistoryEvent
		CreateTransientDecisionEvents(di *decisionInfo, identity string) (*historypb.HistoryEvent, *historypb.HistoryEvent)
		DeleteDecision()
		DeleteSignalRequested(requestID string)
		FailDecision(bool)
		FlushBufferedEvents() error
		GetActivityByActivityID(string) (*persistence.ActivityInfo, bool)
		GetActivityInfo(int64) (*persistence.ActivityInfo, bool)
		GetActivityScheduledEvent(int64) (*historypb.HistoryEvent, error)
		GetChildExecutionInfo(int64) (*persistence.ChildExecutionInfo, bool)
		GetChildExecutionInitiatedEvent(int64) (*historypb.HistoryEvent, error)
		GetCompletionEvent() (*historypb.HistoryEvent, error)
		GetDecisionInfo(int64) (*decisionInfo, bool)
		GetNamespaceEntry() *cache.NamespaceCacheEntry
		GetStartEvent() (*historypb.HistoryEvent, error)
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
		GetRetryBackoffDuration(errReason string) time.Duration
		GetCronBackoffDuration() (time.Duration, error)
		GetSignalInfo(int64) (*persistenceblobs.SignalInfo, bool)
		GetStartVersion() (int64, error)
		GetUserTimerInfoByEventID(int64) (*persistenceblobs.TimerInfo, bool)
		GetUserTimerInfo(string) (*persistenceblobs.TimerInfo, bool)
		GetWorkflowType() *commonpb.WorkflowType
		GetWorkflowStateStatus() (int, executionpb.WorkflowExecutionStatus)
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
		ReplicateActivityTaskCancelRequestedEvent(*historypb.HistoryEvent) error
		ReplicateActivityTaskCanceledEvent(*historypb.HistoryEvent) error
		ReplicateActivityTaskCompletedEvent(*historypb.HistoryEvent) error
		ReplicateActivityTaskFailedEvent(*historypb.HistoryEvent) error
		ReplicateActivityTaskScheduledEvent(int64, *historypb.HistoryEvent) (*persistence.ActivityInfo, error)
		ReplicateActivityTaskStartedEvent(*historypb.HistoryEvent) error
		ReplicateActivityTaskTimedOutEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionCanceledEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionCompletedEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionFailedEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionStartedEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionTerminatedEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionTimedOutEvent(*historypb.HistoryEvent) error
		ReplicateDecisionTaskCompletedEvent(*historypb.HistoryEvent) error
		ReplicateDecisionTaskFailedEvent() error
		ReplicateDecisionTaskScheduledEvent(int64, int64, string, int32, int64, int64, int64) (*decisionInfo, error)
		ReplicateDecisionTaskStartedEvent(*decisionInfo, int64, int64, int64, string, int64) (*decisionInfo, error)
		ReplicateDecisionTaskTimedOutEvent(eventpb.TimeoutType) error
		ReplicateExternalWorkflowExecutionCancelRequested(*historypb.HistoryEvent) error
		ReplicateExternalWorkflowExecutionSignaled(*historypb.HistoryEvent) error
		ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(*historypb.HistoryEvent) error
		ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, *historypb.HistoryEvent, string) (*persistenceblobs.RequestCancelInfo, error)
		ReplicateSignalExternalWorkflowExecutionFailedEvent(*historypb.HistoryEvent) error
		ReplicateSignalExternalWorkflowExecutionInitiatedEvent(int64, *historypb.HistoryEvent, string) (*persistenceblobs.SignalInfo, error)
		ReplicateStartChildWorkflowExecutionFailedEvent(*historypb.HistoryEvent) error
		ReplicateStartChildWorkflowExecutionInitiatedEvent(int64, *historypb.HistoryEvent, string) (*persistence.ChildExecutionInfo, error)
		ReplicateTimerCanceledEvent(*historypb.HistoryEvent) error
		ReplicateTimerFiredEvent(*historypb.HistoryEvent) error
		ReplicateTimerStartedEvent(*historypb.HistoryEvent) (*persistenceblobs.TimerInfo, error)
		ReplicateTransientDecisionTaskScheduled() (*decisionInfo, error)
		ReplicateUpsertWorkflowSearchAttributesEvent(*historypb.HistoryEvent)
		ReplicateWorkflowExecutionCancelRequestedEvent(*historypb.HistoryEvent) error
		ReplicateWorkflowExecutionCanceledEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionCompletedEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionContinuedAsNewEvent(int64, string, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionFailedEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionSignaled(*historypb.HistoryEvent) error
		ReplicateWorkflowExecutionStartedEvent(string, executionpb.WorkflowExecution, string, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionTerminatedEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionTimedoutEvent(int64, *historypb.HistoryEvent) error
		SetCurrentBranchToken(branchToken []byte) error
		SetHistoryBuilder(hBuilder *historyBuilder)
		SetHistoryTree(treeID []byte) error
		SetVersionHistories(*persistence.VersionHistories) error
		UpdateActivity(*persistence.ActivityInfo) error
		UpdateActivityProgress(ai *persistence.ActivityInfo, request *workflowservice.RecordActivityTaskHeartbeatRequest)
		UpdateDecision(*decisionInfo)
		UpdateReplicationStateVersion(int64, bool)
		UpdateReplicationStateLastEventID(int64, int64)
		UpdateUserTimer(*persistenceblobs.TimerInfo) error
		UpdateCurrentVersion(version int64, forceUpdate bool) error
		UpdateWorkflowStateStatus(state int, status executionpb.WorkflowExecutionStatus) error

		AddTransferTasks(transferTasks ...persistence.Task)
		AddTimerTasks(timerTasks ...persistence.Task)
		SetUpdateCondition(int64)
		GetUpdateCondition() int64

		StartTransaction(entry *cache.NamespaceCacheEntry) (bool, error)
		CloseTransactionAsMutation(now time.Time, transactionPolicy transactionPolicy) (*persistence.WorkflowMutation, []*persistence.WorkflowEvents, error)
		CloseTransactionAsSnapshot(now time.Time, transactionPolicy transactionPolicy) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error)
	}
)
