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

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/mutablestate"
)

type (
	// TODO: This should be part of persistence layer
	workflowTaskInfo struct {
		Version             int64
		ScheduleID          int64
		StartedID           int64
		RequestID           string
		WorkflowTaskTimeout *time.Duration
		TaskQueue           *taskqueuepb.TaskQueue // This is only needed to communicate task queue used after AddWorkflowTaskScheduledEvent
		Attempt             int32
		// Scheduled and Started timestamps are useful for transient workflow task: when transient workflow task finally completes,
		// use these timestamp to create scheduled/started events.
		// Also used for recording latency metrics
		ScheduledTime *time.Time
		StartedTime   *time.Time
		// OriginalScheduledTime is to record the first scheduled workflow task during workflow task heartbeat.
		// Client may heartbeat workflow task by RespondWorkflowTaskComplete with ForceCreateNewWorkflowTask == true
		// In this case, OriginalScheduledTime won't change. Then when current time - OriginalScheduledTime exceeds
		// some threshold, server can interrupt the heartbeat by enforcing to timeout the workflow task.
		OriginalScheduledTime *time.Time
	}

	mutableState interface {
		AddActivityTaskCancelRequestedEvent(int64, int64, string) (*historypb.HistoryEvent, *persistencespb.ActivityInfo, error)
		AddActivityTaskCanceledEvent(int64, int64, int64, *commonpb.Payloads, string) (*historypb.HistoryEvent, error)
		AddActivityTaskCompletedEvent(int64, int64, *workflowservice.RespondActivityTaskCompletedRequest) (*historypb.HistoryEvent, error)
		AddActivityTaskFailedEvent(int64, int64, *failurepb.Failure, enumspb.RetryState, string) (*historypb.HistoryEvent, error)
		AddActivityTaskScheduledEvent(int64, *commandpb.ScheduleActivityTaskCommandAttributes) (*historypb.HistoryEvent, *persistencespb.ActivityInfo, error)
		AddActivityTaskStartedEvent(*persistencespb.ActivityInfo, int64, string, string) (*historypb.HistoryEvent, error)
		AddActivityTaskTimedOutEvent(int64, int64, *failurepb.Failure, enumspb.RetryState) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionCanceledEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionCanceledEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionCompletedEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionCompletedEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionFailedEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionFailedEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionStartedEvent(string, *commonpb.WorkflowExecution, *commonpb.WorkflowType, int64, *commonpb.Header) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionTerminatedEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionTerminatedEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionTimedOutEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionTimedOutEventAttributes) (*historypb.HistoryEvent, error)
		AddCompletedWorkflowEvent(int64, *commandpb.CompleteWorkflowExecutionCommandAttributes) (*historypb.HistoryEvent, error)
		AddContinueAsNewEvent(int64, int64, string, *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes) (*historypb.HistoryEvent, mutableState, error)
		AddWorkflowTaskCompletedEvent(int64, int64, *workflowservice.RespondWorkflowTaskCompletedRequest, int) (*historypb.HistoryEvent, error)
		AddWorkflowTaskFailedEvent(scheduleEventID int64, startedEventID int64, cause enumspb.WorkflowTaskFailedCause, failure *failurepb.Failure, identity, binChecksum, baseRunID, newRunID string, forkEventVersion int64) (*historypb.HistoryEvent, error)
		AddWorkflowTaskScheduleToStartTimeoutEvent(int64) (*historypb.HistoryEvent, error)
		AddFirstWorkflowTaskScheduled(*historypb.HistoryEvent) error
		AddWorkflowTaskScheduledEvent(bypassTaskGeneration bool) (*workflowTaskInfo, error)
		AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration bool, originalScheduledTimestamp *time.Time) (*workflowTaskInfo, error)
		AddWorkflowTaskStartedEvent(int64, string, *taskqueuepb.TaskQueue, string) (*historypb.HistoryEvent, *workflowTaskInfo, error)
		AddWorkflowTaskTimedOutEvent(int64, int64) (*historypb.HistoryEvent, error)
		AddExternalWorkflowExecutionCancelRequested(int64, string, string, string) (*historypb.HistoryEvent, error)
		AddExternalWorkflowExecutionSignaled(int64, string, string, string, string) (*historypb.HistoryEvent, error)
		AddFailWorkflowEvent(int64, enumspb.RetryState, *commandpb.FailWorkflowExecutionCommandAttributes) (*historypb.HistoryEvent, error)
		AddRecordMarkerEvent(int64, *commandpb.RecordMarkerCommandAttributes) (*historypb.HistoryEvent, error)
		AddRequestCancelExternalWorkflowExecutionFailedEvent(int64, string, string, string, enumspb.CancelExternalWorkflowExecutionFailedCause) (*historypb.HistoryEvent, error)
		AddRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, string, *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes) (*historypb.HistoryEvent, *persistencespb.RequestCancelInfo, error)
		AddSignalExternalWorkflowExecutionFailedEvent(int64, string, string, string, string, enumspb.SignalExternalWorkflowExecutionFailedCause) (*historypb.HistoryEvent, error)
		AddSignalExternalWorkflowExecutionInitiatedEvent(int64, string, *commandpb.SignalExternalWorkflowExecutionCommandAttributes) (*historypb.HistoryEvent, *persistencespb.SignalInfo, error)
		AddSignalRequested(requestID string)
		AddStartChildWorkflowExecutionFailedEvent(int64, enumspb.StartChildWorkflowExecutionFailedCause, *historypb.StartChildWorkflowExecutionInitiatedEventAttributes) (*historypb.HistoryEvent, error)
		AddStartChildWorkflowExecutionInitiatedEvent(int64, string, *commandpb.StartChildWorkflowExecutionCommandAttributes) (*historypb.HistoryEvent, *persistencespb.ChildExecutionInfo, error)
		AddTimeoutWorkflowEvent(int64, enumspb.RetryState) (*historypb.HistoryEvent, error)
		AddTimerCanceledEvent(int64, *commandpb.CancelTimerCommandAttributes, string) (*historypb.HistoryEvent, error)
		AddTimerFiredEvent(string) (*historypb.HistoryEvent, error)
		AddTimerStartedEvent(int64, *commandpb.StartTimerCommandAttributes) (*historypb.HistoryEvent, *persistencespb.TimerInfo, error)
		AddUpsertWorkflowSearchAttributesEvent(int64, *commandpb.UpsertWorkflowSearchAttributesCommandAttributes) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionCancelRequestedEvent(*historyservice.RequestCancelWorkflowExecutionRequest) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionCanceledEvent(int64, *commandpb.CancelWorkflowExecutionCommandAttributes) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionSignaled(signalName string, input *commonpb.Payloads, identity string) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionStartedEvent(commonpb.WorkflowExecution, *historyservice.StartWorkflowExecutionRequest) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionTerminatedEvent(firstEventID int64, reason string, details *commonpb.Payloads, identity string) (*historypb.HistoryEvent, error)
		ClearStickyness()
		CheckResettable() error
		CloneToProto() *persistencespb.WorkflowMutableState
		RetryActivity(ai *persistencespb.ActivityInfo, failure *failurepb.Failure) (enumspb.RetryState, error)
		CreateTransientWorkflowTaskEvents(di *workflowTaskInfo, identity string) (*historypb.HistoryEvent, *historypb.HistoryEvent)
		DeleteWorkflowTask()
		DeleteSignalRequested(requestID string)
		FailWorkflowTask(bool)
		FlushBufferedEvents()
		GetActivityByActivityID(string) (*persistencespb.ActivityInfo, bool)
		GetActivityInfo(int64) (*persistencespb.ActivityInfo, bool)
		GetActivityInfoWithTimerHeartbeat(scheduleEventID int64) (*persistencespb.ActivityInfo, time.Time, bool)
		GetActivityScheduledEvent(int64) (*historypb.HistoryEvent, error)
		GetChildExecutionInfo(int64) (*persistencespb.ChildExecutionInfo, bool)
		GetChildExecutionInitiatedEvent(int64) (*historypb.HistoryEvent, error)
		GetCompletionEvent() (*historypb.HistoryEvent, error)
		GetWorkflowTaskInfo(int64) (*workflowTaskInfo, bool)
		GetNamespaceEntry() *cache.NamespaceCacheEntry
		GetStartEvent() (*historypb.HistoryEvent, error)
		GetCurrentBranchToken() ([]byte, error)
		GetCurrentVersion() int64
		GetExecutionInfo() *persistencespb.WorkflowExecutionInfo
		GetExecutionState() *persistencespb.WorkflowExecutionState
		GetInFlightWorkflowTask() (*workflowTaskInfo, bool)
		GetPendingWorkflowTask() (*workflowTaskInfo, bool)
		GetLastFirstEventIDTxnID() (int64, int64)
		GetLastWriteVersion() (int64, error)
		GetNextEventID() int64
		GetPreviousStartedEventID() int64
		GetPendingActivityInfos() map[int64]*persistencespb.ActivityInfo
		GetPendingTimerInfos() map[string]*persistencespb.TimerInfo
		GetPendingChildExecutionInfos() map[int64]*persistencespb.ChildExecutionInfo
		GetPendingRequestCancelExternalInfos() map[int64]*persistencespb.RequestCancelInfo
		GetPendingSignalExternalInfos() map[int64]*persistencespb.SignalInfo
		GetRequestCancelInfo(int64) (*persistencespb.RequestCancelInfo, bool)
		GetRetryBackoffDuration(failure *failurepb.Failure) (time.Duration, enumspb.RetryState)
		GetCronBackoffDuration() (time.Duration, error)
		GetSignalInfo(int64) (*persistencespb.SignalInfo, bool)
		GetStartVersion() (int64, error)
		GetUserTimerInfoByEventID(int64) (*persistencespb.TimerInfo, bool)
		GetUserTimerInfo(string) (*persistencespb.TimerInfo, bool)
		GetWorkflowType() *commonpb.WorkflowType
		GetWorkflowStateStatus() (enumsspb.WorkflowExecutionState, enumspb.WorkflowExecutionStatus)
		GetQueryRegistry() queryRegistry
		HasBufferedEvents() bool
		HasInFlightWorkflowTask() bool
		HasParentExecution() bool
		HasPendingWorkflowTask() bool
		HasProcessedOrPendingWorkflowTask() bool
		IsCancelRequested() bool
		IsCurrentWorkflowGuaranteed() bool
		IsSignalRequested(requestID string) bool
		IsStickyTaskQueueEnabled() bool
		IsWorkflowExecutionRunning() bool
		IsResourceDuplicated(resourceDedupKey definition.DeduplicationID) bool
		UpdateDuplicatedResource(resourceDedupKey definition.DeduplicationID)
		ReplicateActivityInfo(*historyservice.SyncActivityRequest, bool) error
		ReplicateActivityTaskCancelRequestedEvent(*historypb.HistoryEvent) error
		ReplicateActivityTaskCanceledEvent(*historypb.HistoryEvent) error
		ReplicateActivityTaskCompletedEvent(*historypb.HistoryEvent) error
		ReplicateActivityTaskFailedEvent(*historypb.HistoryEvent) error
		ReplicateActivityTaskScheduledEvent(int64, *historypb.HistoryEvent) (*persistencespb.ActivityInfo, error)
		ReplicateActivityTaskStartedEvent(*historypb.HistoryEvent) error
		ReplicateActivityTaskTimedOutEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionCanceledEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionCompletedEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionFailedEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionStartedEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionTerminatedEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionTimedOutEvent(*historypb.HistoryEvent) error
		ReplicateWorkflowTaskCompletedEvent(*historypb.HistoryEvent) error
		ReplicateWorkflowTaskFailedEvent() error
		ReplicateWorkflowTaskScheduledEvent(int64, int64, *taskqueuepb.TaskQueue, int32, int32, *time.Time, *time.Time) (*workflowTaskInfo, error)
		ReplicateWorkflowTaskStartedEvent(*workflowTaskInfo, int64, int64, int64, string, time.Time) (*workflowTaskInfo, error)
		ReplicateWorkflowTaskTimedOutEvent(enumspb.TimeoutType) error
		ReplicateExternalWorkflowExecutionCancelRequested(*historypb.HistoryEvent) error
		ReplicateExternalWorkflowExecutionSignaled(*historypb.HistoryEvent) error
		ReplicateRequestCancelExternalWorkflowExecutionFailedEvent(*historypb.HistoryEvent) error
		ReplicateRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, *historypb.HistoryEvent, string) (*persistencespb.RequestCancelInfo, error)
		ReplicateSignalExternalWorkflowExecutionFailedEvent(*historypb.HistoryEvent) error
		ReplicateSignalExternalWorkflowExecutionInitiatedEvent(int64, *historypb.HistoryEvent, string) (*persistencespb.SignalInfo, error)
		ReplicateStartChildWorkflowExecutionFailedEvent(*historypb.HistoryEvent) error
		ReplicateStartChildWorkflowExecutionInitiatedEvent(int64, *historypb.HistoryEvent, string) (*persistencespb.ChildExecutionInfo, error)
		ReplicateTimerCanceledEvent(*historypb.HistoryEvent) error
		ReplicateTimerFiredEvent(*historypb.HistoryEvent) error
		ReplicateTimerStartedEvent(*historypb.HistoryEvent) (*persistencespb.TimerInfo, error)
		ReplicateTransientWorkflowTaskScheduled() (*workflowTaskInfo, error)
		ReplicateUpsertWorkflowSearchAttributesEvent(*historypb.HistoryEvent)
		ReplicateWorkflowExecutionCancelRequestedEvent(*historypb.HistoryEvent) error
		ReplicateWorkflowExecutionCanceledEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionCompletedEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionContinuedAsNewEvent(int64, string, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionFailedEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionSignaled(*historypb.HistoryEvent) error
		ReplicateWorkflowExecutionStartedEvent(string, commonpb.WorkflowExecution, string, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionTerminatedEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionTimedoutEvent(int64, *historypb.HistoryEvent) error
		SetCurrentBranchToken(branchToken []byte) error
		SetHistoryBuilder(hBuilder *mutablestate.HistoryBuilder)
		SetHistoryTree(treeID string) error
		UpdateActivity(*persistencespb.ActivityInfo) error
		UpdateActivityWithTimerHeartbeat(*persistencespb.ActivityInfo, time.Time) error
		UpdateActivityProgress(ai *persistencespb.ActivityInfo, request *workflowservice.RecordActivityTaskHeartbeatRequest)
		UpdateWorkflowTask(*workflowTaskInfo)
		UpdateUserTimer(*persistencespb.TimerInfo) error
		UpdateCurrentVersion(version int64, forceUpdate bool) error
		UpdateWorkflowStateStatus(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) error

		AddTransferTasks(transferTasks ...persistence.Task)
		AddTimerTasks(timerTasks ...persistence.Task)
		AddVisibilityTasks(visibilityTasks ...persistence.Task)
		SetUpdateCondition(int64, int64)
		GetUpdateCondition() (int64, int64)

		StartTransaction(entry *cache.NamespaceCacheEntry) (bool, error)
		StartTransactionSkipWorkflowTaskFail(entry *cache.NamespaceCacheEntry) error
		CloseTransactionAsMutation(now time.Time, transactionPolicy transactionPolicy) (*persistence.WorkflowMutation, []*persistence.WorkflowEvents, error)
		CloseTransactionAsSnapshot(now time.Time, transactionPolicy transactionPolicy) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error)
	}
)
