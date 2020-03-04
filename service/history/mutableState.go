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

	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
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
		AddActivityTaskCancelRequestedEvent(int64, string, string) (*commonproto.HistoryEvent, *persistence.ActivityInfo, error)
		AddActivityTaskCanceledEvent(int64, int64, int64, []uint8, string) (*commonproto.HistoryEvent, error)
		AddActivityTaskCompletedEvent(int64, int64, *workflowservice.RespondActivityTaskCompletedRequest) (*commonproto.HistoryEvent, error)
		AddActivityTaskFailedEvent(int64, int64, *workflowservice.RespondActivityTaskFailedRequest) (*commonproto.HistoryEvent, error)
		AddActivityTaskScheduledEvent(int64, *commonproto.ScheduleActivityTaskDecisionAttributes) (*commonproto.HistoryEvent, *persistence.ActivityInfo, error)
		AddActivityTaskStartedEvent(*persistence.ActivityInfo, int64, string, string) (*commonproto.HistoryEvent, error)
		AddActivityTaskTimedOutEvent(int64, int64, enums.TimeoutType, []uint8) (*commonproto.HistoryEvent, error)
		AddCancelTimerFailedEvent(int64, *commonproto.CancelTimerDecisionAttributes, string) (*commonproto.HistoryEvent, error)
		AddChildWorkflowExecutionCanceledEvent(int64, *commonproto.WorkflowExecution, *commonproto.WorkflowExecutionCanceledEventAttributes) (*commonproto.HistoryEvent, error)
		AddChildWorkflowExecutionCompletedEvent(int64, *commonproto.WorkflowExecution, *commonproto.WorkflowExecutionCompletedEventAttributes) (*commonproto.HistoryEvent, error)
		AddChildWorkflowExecutionFailedEvent(int64, *commonproto.WorkflowExecution, *commonproto.WorkflowExecutionFailedEventAttributes) (*commonproto.HistoryEvent, error)
		AddChildWorkflowExecutionStartedEvent(string, *commonproto.WorkflowExecution, *commonproto.WorkflowType, int64, *commonproto.Header) (*commonproto.HistoryEvent, error)
		AddChildWorkflowExecutionTerminatedEvent(int64, *commonproto.WorkflowExecution, *commonproto.WorkflowExecutionTerminatedEventAttributes) (*commonproto.HistoryEvent, error)
		AddChildWorkflowExecutionTimedOutEvent(int64, *commonproto.WorkflowExecution, *commonproto.WorkflowExecutionTimedOutEventAttributes) (*commonproto.HistoryEvent, error)
		AddCompletedWorkflowEvent(int64, *commonproto.CompleteWorkflowExecutionDecisionAttributes) (*commonproto.HistoryEvent, error)
		AddContinueAsNewEvent(int64, int64, string, *commonproto.ContinueAsNewWorkflowExecutionDecisionAttributes) (*commonproto.HistoryEvent, mutableState, error)
		AddDecisionTaskCompletedEvent(int64, int64, *workflowservice.RespondDecisionTaskCompletedRequest, int) (*commonproto.HistoryEvent, error)
		AddDecisionTaskFailedEvent(scheduleEventID int64, startedEventID int64, cause enums.DecisionTaskFailedCause, details []byte, identity, reason, binChecksum, baseRunID, newRunID string, forkEventVersion int64) (*commonproto.HistoryEvent, error)
		AddDecisionTaskScheduleToStartTimeoutEvent(int64) (*commonproto.HistoryEvent, error)
		AddFirstDecisionTaskScheduled(*commonproto.HistoryEvent) error
		AddDecisionTaskScheduledEvent(bypassTaskGeneration bool) (*decisionInfo, error)
		AddDecisionTaskScheduledEventAsHeartbeat(bypassTaskGeneration bool, originalScheduledTimestamp int64) (*decisionInfo, error)
		AddDecisionTaskStartedEvent(int64, string, *workflowservice.PollForDecisionTaskRequest) (*commonproto.HistoryEvent, *decisionInfo, error)
		AddDecisionTaskTimedOutEvent(int64, int64) (*commonproto.HistoryEvent, error)
		AddExternalWorkflowExecutionCancelRequested(int64, string, string, string) (*commonproto.HistoryEvent, error)
		AddExternalWorkflowExecutionSignaled(int64, string, string, string, []uint8) (*commonproto.HistoryEvent, error)
		AddFailWorkflowEvent(int64, *commonproto.FailWorkflowExecutionDecisionAttributes) (*commonproto.HistoryEvent, error)
		AddRecordMarkerEvent(int64, *commonproto.RecordMarkerDecisionAttributes) (*commonproto.HistoryEvent, error)
		AddRequestCancelActivityTaskFailedEvent(int64, string, string) (*commonproto.HistoryEvent, error)
		AddRequestCancelExternalWorkflowExecutionFailedEvent(int64, int64, string, string, string, enums.CancelExternalWorkflowExecutionFailedCause) (*commonproto.HistoryEvent, error)
		AddRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, string, *commonproto.RequestCancelExternalWorkflowExecutionDecisionAttributes) (*commonproto.HistoryEvent, *persistenceblobs.RequestCancelInfo, error)
		AddSignalExternalWorkflowExecutionFailedEvent(int64, int64, string, string, string, []uint8, enums.SignalExternalWorkflowExecutionFailedCause) (*commonproto.HistoryEvent, error)
		AddSignalExternalWorkflowExecutionInitiatedEvent(int64, string, *commonproto.SignalExternalWorkflowExecutionDecisionAttributes) (*commonproto.HistoryEvent, *persistenceblobs.SignalInfo, error)
		AddSignalRequested(requestID string)
		AddStartChildWorkflowExecutionFailedEvent(int64, enums.ChildWorkflowExecutionFailedCause, *commonproto.StartChildWorkflowExecutionInitiatedEventAttributes) (*commonproto.HistoryEvent, error)
		AddStartChildWorkflowExecutionInitiatedEvent(int64, string, *commonproto.StartChildWorkflowExecutionDecisionAttributes) (*commonproto.HistoryEvent, *persistence.ChildExecutionInfo, error)
		AddTimeoutWorkflowEvent(int64) (*commonproto.HistoryEvent, error)
		AddTimerCanceledEvent(int64, *commonproto.CancelTimerDecisionAttributes, string) (*commonproto.HistoryEvent, error)
		AddTimerFiredEvent(string) (*commonproto.HistoryEvent, error)
		AddTimerStartedEvent(int64, *commonproto.StartTimerDecisionAttributes) (*commonproto.HistoryEvent, *persistenceblobs.TimerInfo, error)
		AddUpsertWorkflowSearchAttributesEvent(int64, *commonproto.UpsertWorkflowSearchAttributesDecisionAttributes) (*commonproto.HistoryEvent, error)
		AddWorkflowExecutionCancelRequestedEvent(string, *historyservice.RequestCancelWorkflowExecutionRequest) (*commonproto.HistoryEvent, error)
		AddWorkflowExecutionCanceledEvent(int64, *commonproto.CancelWorkflowExecutionDecisionAttributes) (*commonproto.HistoryEvent, error)
		AddWorkflowExecutionSignaled(signalName string, input []byte, identity string) (*commonproto.HistoryEvent, error)
		AddWorkflowExecutionStartedEvent(commonproto.WorkflowExecution, *historyservice.StartWorkflowExecutionRequest) (*commonproto.HistoryEvent, error)
		AddWorkflowExecutionTerminatedEvent(firstEventID int64, reason string, details []byte, identity string) (*commonproto.HistoryEvent, error)
		ClearStickyness()
		CheckResettable() error
		CopyToPersistence() *persistence.WorkflowMutableState
		RetryActivity(ai *persistence.ActivityInfo, failureReason string, failureDetails []byte) (bool, error)
		CreateNewHistoryEvent(eventType enums.EventType) *commonproto.HistoryEvent
		CreateNewHistoryEventWithTimestamp(eventType enums.EventType, timestamp int64) *commonproto.HistoryEvent
		CreateTransientDecisionEvents(di *decisionInfo, identity string) (*commonproto.HistoryEvent, *commonproto.HistoryEvent)
		DeleteDecision()
		DeleteSignalRequested(requestID string)
		FailDecision(bool)
		FlushBufferedEvents() error
		GetActivityByActivityID(string) (*persistence.ActivityInfo, bool)
		GetActivityInfo(int64) (*persistence.ActivityInfo, bool)
		GetActivityScheduledEvent(int64) (*commonproto.HistoryEvent, error)
		GetChildExecutionInfo(int64) (*persistence.ChildExecutionInfo, bool)
		GetChildExecutionInitiatedEvent(int64) (*commonproto.HistoryEvent, error)
		GetCompletionEvent() (*commonproto.HistoryEvent, error)
		GetDecisionInfo(int64) (*decisionInfo, bool)
		GetDomainEntry() *cache.DomainCacheEntry
		GetStartEvent() (*commonproto.HistoryEvent, error)
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
		GetWorkflowType() *commonproto.WorkflowType
		GetWorkflowStateCloseStatus() (int, int)
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
		ReplicateActivityTaskCancelRequestedEvent(*commonproto.HistoryEvent) error
		ReplicateActivityTaskCanceledEvent(*commonproto.HistoryEvent) error
		ReplicateActivityTaskCompletedEvent(*commonproto.HistoryEvent) error
		ReplicateActivityTaskFailedEvent(*commonproto.HistoryEvent) error
		ReplicateActivityTaskScheduledEvent(int64, *commonproto.HistoryEvent) (*persistence.ActivityInfo, error)
		ReplicateActivityTaskStartedEvent(*commonproto.HistoryEvent) error
		ReplicateActivityTaskTimedOutEvent(*commonproto.HistoryEvent) error
		ReplicateChildWorkflowExecutionCanceledEvent(*commonproto.HistoryEvent) error
		ReplicateChildWorkflowExecutionCompletedEvent(*commonproto.HistoryEvent) error
		ReplicateChildWorkflowExecutionFailedEvent(*commonproto.HistoryEvent) error
		ReplicateChildWorkflowExecutionStartedEvent(*commonproto.HistoryEvent) error
		ReplicateChildWorkflowExecutionTerminatedEvent(*commonproto.HistoryEvent) error
		ReplicateChildWorkflowExecutionTimedOutEvent(*commonproto.HistoryEvent) error
		ReplicateDecisionTaskCompletedEvent(*commonproto.HistoryEvent) error
		ReplicateDecisionTaskFailedEvent() error
		ReplicateDecisionTaskScheduledEvent(int64, int64, string, int32, int64, int64, int64) (*decisionInfo, error)
		ReplicateDecisionTaskStartedEvent(*decisionInfo, int64, int64, int64, string, int64) (*decisionInfo, error)
		ReplicateDecisionTaskTimedOutEvent(enums.TimeoutType) error
		ReplicateExternalWorkflowExecutionCancelRequested(*commonproto.HistoryEvent) error
		ReplicateExternalWorkflowExecutionSignaled(*commonproto.HistoryEvent) error
		ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(*commonproto.HistoryEvent) error
		ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, *commonproto.HistoryEvent, string) (*persistenceblobs.RequestCancelInfo, error)
		ReplicateSignalExternalWorkflowExecutionFailedEvent(*commonproto.HistoryEvent) error
		ReplicateSignalExternalWorkflowExecutionInitiatedEvent(int64, *commonproto.HistoryEvent, string) (*persistenceblobs.SignalInfo, error)
		ReplicateStartChildWorkflowExecutionFailedEvent(*commonproto.HistoryEvent) error
		ReplicateStartChildWorkflowExecutionInitiatedEvent(int64, *commonproto.HistoryEvent, string) (*persistence.ChildExecutionInfo, error)
		ReplicateTimerCanceledEvent(*commonproto.HistoryEvent) error
		ReplicateTimerFiredEvent(*commonproto.HistoryEvent) error
		ReplicateTimerStartedEvent(*commonproto.HistoryEvent) (*persistenceblobs.TimerInfo, error)
		ReplicateTransientDecisionTaskScheduled() (*decisionInfo, error)
		ReplicateUpsertWorkflowSearchAttributesEvent(*commonproto.HistoryEvent)
		ReplicateWorkflowExecutionCancelRequestedEvent(*commonproto.HistoryEvent) error
		ReplicateWorkflowExecutionCanceledEvent(int64, *commonproto.HistoryEvent) error
		ReplicateWorkflowExecutionCompletedEvent(int64, *commonproto.HistoryEvent) error
		ReplicateWorkflowExecutionContinuedAsNewEvent(int64, string, *commonproto.HistoryEvent) error
		ReplicateWorkflowExecutionFailedEvent(int64, *commonproto.HistoryEvent) error
		ReplicateWorkflowExecutionSignaled(*commonproto.HistoryEvent) error
		ReplicateWorkflowExecutionStartedEvent(string, commonproto.WorkflowExecution, string, *commonproto.HistoryEvent) error
		ReplicateWorkflowExecutionTerminatedEvent(int64, *commonproto.HistoryEvent) error
		ReplicateWorkflowExecutionTimedoutEvent(int64, *commonproto.HistoryEvent) error
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
		UpdateWorkflowStateCloseStatus(state int, closeStatus int) error

		AddTransferTasks(transferTasks ...persistence.Task)
		AddTimerTasks(timerTasks ...persistence.Task)
		SetUpdateCondition(int64)
		GetUpdateCondition() int64

		StartTransaction(entry *cache.DomainCacheEntry) (bool, error)
		CloseTransactionAsMutation(now time.Time, transactionPolicy transactionPolicy) (*persistence.WorkflowMutation, []*persistence.WorkflowEvents, error)
		CloseTransactionAsSnapshot(now time.Time, transactionPolicy transactionPolicy) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error)
	}
)
