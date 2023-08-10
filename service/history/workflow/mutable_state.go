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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination mutable_state_mock.go

package workflow

import (
	"context"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	updatespb "go.temporal.io/server/api/update/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
)

type TransactionPolicy int

const (
	TransactionPolicyActive  TransactionPolicy = 0
	TransactionPolicyPassive TransactionPolicy = 1
)

func (policy TransactionPolicy) Ptr() *TransactionPolicy {
	return &policy
}

var emptyTasks = []tasks.Task{}

type (
	// TODO: This should be part of persistence layer
	WorkflowTaskInfo struct {
		Version             int64
		ScheduledEventID    int64
		StartedEventID      int64
		RequestID           string
		WorkflowTaskTimeout *time.Duration
		// This is only needed to communicate task queue used after AddWorkflowTaskScheduledEvent.
		TaskQueue *taskqueuepb.TaskQueue
		Attempt   int32
		// Scheduled and Started timestamps are useful for transient workflow task: when transient workflow task finally completes,
		// use these Timestamp to create scheduled/started events.
		// Also used for recording latency metrics
		ScheduledTime *time.Time
		StartedTime   *time.Time
		// OriginalScheduledTime is to record the first scheduled workflow task during workflow task heartbeat.
		// Client may to heartbeat workflow task by RespondWorkflowTaskComplete with ForceCreateNewWorkflowTask == true
		// In this case, OriginalScheduledTime won't change. Then when time.Now().UTC()-OriginalScheduledTime exceeds
		// some threshold, server can interrupt the heartbeat by enforcing to time out the workflow task.
		OriginalScheduledTime *time.Time

		// Indicate type of the current workflow task (normal, transient, or speculative).
		Type enumsspb.WorkflowTaskType

		// These two fields are sent to workers in the WorkflowTaskStarted event. We need to save a
		// copy in mutable state to know the last values we sent (which might have been in a
		// transient event), otherwise a dynamic config change of the suggestion threshold could
		// cause the WorkflowTaskStarted event that the worker used to not match the event we saved
		// in history.
		SuggestContinueAsNew bool
		HistorySizeBytes     int64
	}

	WorkflowTaskCompletionLimits struct {
		MaxResetPoints              int
		MaxSearchAttributeValueSize int
	}

	MutableState interface {
		AddActivityTaskCancelRequestedEvent(int64, int64, string) (*historypb.HistoryEvent, *persistencespb.ActivityInfo, error)
		AddActivityTaskCanceledEvent(int64, int64, int64, *commonpb.Payloads, string) (*historypb.HistoryEvent, error)
		AddActivityTaskCompletedEvent(int64, int64, *workflowservice.RespondActivityTaskCompletedRequest) (*historypb.HistoryEvent, error)
		AddActivityTaskFailedEvent(int64, int64, *failurepb.Failure, enumspb.RetryState, string) (*historypb.HistoryEvent, error)
		AddActivityTaskScheduledEvent(int64, *commandpb.ScheduleActivityTaskCommandAttributes, bool) (*historypb.HistoryEvent, *persistencespb.ActivityInfo, error)
		AddActivityTaskStartedEvent(*persistencespb.ActivityInfo, int64, string, string) (*historypb.HistoryEvent, error)
		AddActivityTaskTimedOutEvent(int64, int64, *failurepb.Failure, enumspb.RetryState) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionCanceledEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionCanceledEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionCompletedEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionCompletedEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionFailedEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionFailedEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionStartedEvent(*commonpb.WorkflowExecution, *commonpb.WorkflowType, int64, *commonpb.Header, *clockspb.VectorClock) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionTerminatedEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionTerminatedEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionTimedOutEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionTimedOutEventAttributes) (*historypb.HistoryEvent, error)
		AddCompletedWorkflowEvent(int64, *commandpb.CompleteWorkflowExecutionCommandAttributes, string) (*historypb.HistoryEvent, error)
		AddContinueAsNewEvent(context.Context, int64, int64, namespace.Name, *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes) (*historypb.HistoryEvent, MutableState, error)
		AddWorkflowTaskCompletedEvent(*WorkflowTaskInfo, *workflowservice.RespondWorkflowTaskCompletedRequest, WorkflowTaskCompletionLimits) (*historypb.HistoryEvent, error)
		AddWorkflowTaskFailedEvent(workflowTask *WorkflowTaskInfo, cause enumspb.WorkflowTaskFailedCause, failure *failurepb.Failure, identity, binChecksum, baseRunID, newRunID string, forkEventVersion int64) (*historypb.HistoryEvent, error)
		AddWorkflowTaskScheduleToStartTimeoutEvent(workflowTask *WorkflowTaskInfo) (*historypb.HistoryEvent, error)
		AddFirstWorkflowTaskScheduled(parentClock *clockspb.VectorClock, event *historypb.HistoryEvent, bypassTaskGeneration bool) (int64, error)
		AddWorkflowTaskScheduledEvent(bypassTaskGeneration bool, workflowTaskType enumsspb.WorkflowTaskType) (*WorkflowTaskInfo, error)
		AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration bool, originalScheduledTimestamp *time.Time, workflowTaskType enumsspb.WorkflowTaskType) (*WorkflowTaskInfo, error)
		AddWorkflowTaskStartedEvent(int64, string, *taskqueuepb.TaskQueue, string) (*historypb.HistoryEvent, *WorkflowTaskInfo, error)
		AddWorkflowTaskTimedOutEvent(workflowTask *WorkflowTaskInfo) (*historypb.HistoryEvent, error)
		AddExternalWorkflowExecutionCancelRequested(int64, namespace.Name, namespace.ID, string, string) (*historypb.HistoryEvent, error)
		AddExternalWorkflowExecutionSignaled(int64, namespace.Name, namespace.ID, string, string, string) (*historypb.HistoryEvent, error)
		AddFailWorkflowEvent(int64, enumspb.RetryState, *commandpb.FailWorkflowExecutionCommandAttributes, string) (*historypb.HistoryEvent, error)
		AddRecordMarkerEvent(int64, *commandpb.RecordMarkerCommandAttributes) (*historypb.HistoryEvent, error)
		AddRequestCancelExternalWorkflowExecutionFailedEvent(int64, namespace.Name, namespace.ID, string, string, enumspb.CancelExternalWorkflowExecutionFailedCause) (*historypb.HistoryEvent, error)
		AddRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, string, *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes, namespace.ID) (*historypb.HistoryEvent, *persistencespb.RequestCancelInfo, error)
		AddSignalExternalWorkflowExecutionFailedEvent(int64, namespace.Name, namespace.ID, string, string, string, enumspb.SignalExternalWorkflowExecutionFailedCause) (*historypb.HistoryEvent, error)
		AddSignalExternalWorkflowExecutionInitiatedEvent(int64, string, *commandpb.SignalExternalWorkflowExecutionCommandAttributes, namespace.ID) (*historypb.HistoryEvent, *persistencespb.SignalInfo, error)
		AddSignalRequested(requestID string)
		AddStartChildWorkflowExecutionFailedEvent(int64, enumspb.StartChildWorkflowExecutionFailedCause, *historypb.StartChildWorkflowExecutionInitiatedEventAttributes) (*historypb.HistoryEvent, error)
		AddStartChildWorkflowExecutionInitiatedEvent(int64, string, *commandpb.StartChildWorkflowExecutionCommandAttributes, namespace.ID) (*historypb.HistoryEvent, *persistencespb.ChildExecutionInfo, error)
		AddTimeoutWorkflowEvent(int64, enumspb.RetryState, string) (*historypb.HistoryEvent, error)
		AddTimerCanceledEvent(int64, *commandpb.CancelTimerCommandAttributes, string) (*historypb.HistoryEvent, error)
		AddTimerFiredEvent(string) (*historypb.HistoryEvent, error)
		AddTimerStartedEvent(int64, *commandpb.StartTimerCommandAttributes) (*historypb.HistoryEvent, *persistencespb.TimerInfo, error)
		AddUpsertWorkflowSearchAttributesEvent(int64, *commandpb.UpsertWorkflowSearchAttributesCommandAttributes) (*historypb.HistoryEvent, error)
		AddWorkflowPropertiesModifiedEvent(int64, *commandpb.ModifyWorkflowPropertiesCommandAttributes) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionCancelRequestedEvent(*historyservice.RequestCancelWorkflowExecutionRequest) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionCanceledEvent(int64, *commandpb.CancelWorkflowExecutionCommandAttributes) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionSignaled(signalName string, input *commonpb.Payloads, identity string, header *commonpb.Header, skipGenerateWorkflowTask bool) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionStartedEvent(commonpb.WorkflowExecution, *historyservice.StartWorkflowExecutionRequest) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionStartedEventWithOptions(commonpb.WorkflowExecution, *historyservice.StartWorkflowExecutionRequest, *workflowpb.ResetPoints, string, string) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionTerminatedEvent(firstEventID int64, reason string, details *commonpb.Payloads, identity string, deleteAfterTerminate bool) (*historypb.HistoryEvent, error)

		AddWorkflowExecutionUpdateAcceptedEvent(protocolInstanceID string, acceptedRequestMessageId string, acceptedRequestSequencingEventId int64, acceptedRequest *updatepb.Request) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionUpdateCompletedEvent(acceptedEventID int64, updResp *updatepb.Response) (*historypb.HistoryEvent, error)
		RejectWorkflowExecutionUpdate(protocolInstanceID string, updRejection *updatepb.Rejection) error
		VisitUpdates(visitor func(updID string, updInfo *updatespb.UpdateInfo))
		GetUpdateOutcome(ctx context.Context, updateID string) (*updatepb.Outcome, error)

		CheckResettable() error
		CloneToProto() *persistencespb.WorkflowMutableState
		RetryActivity(ai *persistencespb.ActivityInfo, failure *failurepb.Failure) (enumspb.RetryState, error)
		GetTransientWorkflowTaskInfo(workflowTask *WorkflowTaskInfo, identity string) *historyspb.TransientWorkflowTaskInfo
		DeleteSignalRequested(requestID string)
		FlushBufferedEvents()
		GetWorkflowKey() definition.WorkflowKey
		GetActivityByActivityID(string) (*persistencespb.ActivityInfo, bool)
		GetActivityInfo(int64) (*persistencespb.ActivityInfo, bool)
		GetActivityInfoWithTimerHeartbeat(scheduledEventID int64) (*persistencespb.ActivityInfo, time.Time, bool)
		GetActivityType(context.Context, *persistencespb.ActivityInfo) (*commonpb.ActivityType, error)
		GetActivityScheduledEvent(context.Context, int64) (*historypb.HistoryEvent, error)
		GetRequesteCancelExternalInitiatedEvent(context.Context, int64) (*historypb.HistoryEvent, error)
		GetChildExecutionInfo(int64) (*persistencespb.ChildExecutionInfo, bool)
		GetChildExecutionInitiatedEvent(context.Context, int64) (*historypb.HistoryEvent, error)
		GetCompletionEvent(context.Context) (*historypb.HistoryEvent, error)
		GetWorkflowCloseTime(ctx context.Context) (*time.Time, error)
		GetWorkflowTaskByID(scheduledEventID int64) *WorkflowTaskInfo
		GetNamespaceEntry() *namespace.Namespace
		GetStartEvent(context.Context) (*historypb.HistoryEvent, error)
		GetSignalExternalInitiatedEvent(context.Context, int64) (*historypb.HistoryEvent, error)
		GetFirstRunID(ctx context.Context) (string, error)
		GetCurrentBranchToken() ([]byte, error)
		GetCurrentVersion() int64
		GetExecutionInfo() *persistencespb.WorkflowExecutionInfo
		GetExecutionState() *persistencespb.WorkflowExecutionState
		GetStartedWorkflowTask() *WorkflowTaskInfo
		GetPendingWorkflowTask() *WorkflowTaskInfo
		GetLastFirstEventIDTxnID() (int64, int64)
		GetLastWriteVersion() (int64, error)
		GetNextEventID() int64
		GetLastWorkflowTaskStartedEventID() int64
		GetPendingActivityInfos() map[int64]*persistencespb.ActivityInfo
		GetPendingTimerInfos() map[string]*persistencespb.TimerInfo
		GetPendingChildExecutionInfos() map[int64]*persistencespb.ChildExecutionInfo
		GetPendingRequestCancelExternalInfos() map[int64]*persistencespb.RequestCancelInfo
		GetPendingSignalExternalInfos() map[int64]*persistencespb.SignalInfo
		GetRequestCancelInfo(int64) (*persistencespb.RequestCancelInfo, bool)
		GetRetryBackoffDuration(failure *failurepb.Failure) (time.Duration, enumspb.RetryState)
		GetCronBackoffDuration() time.Duration
		GetSignalInfo(int64) (*persistencespb.SignalInfo, bool)
		GetStartVersion() (int64, error)
		GetUserTimerInfoByEventID(int64) (*persistencespb.TimerInfo, bool)
		GetUserTimerInfo(string) (*persistencespb.TimerInfo, bool)
		GetWorkflowType() *commonpb.WorkflowType
		GetWorkflowStateStatus() (enumsspb.WorkflowExecutionState, enumspb.WorkflowExecutionStatus)
		GetQueryRegistry() QueryRegistry
		GetBaseWorkflowInfo() *workflowspb.BaseExecutionInfo
		GetWorkerVersionStamp() *commonpb.WorkerVersionStamp
		IsTransientWorkflowTask() bool
		ClearTransientWorkflowTask() error
		HasBufferedEvents() bool
		HasAnyBufferedEvent(filter BufferedEventFilter) bool
		HasStartedWorkflowTask() bool
		HasParentExecution() bool
		HasPendingWorkflowTask() bool
		HadOrHasWorkflowTask() bool
		IsCancelRequested() bool
		IsWorkflowCloseAttempted() bool
		IsCurrentWorkflowGuaranteed() bool
		IsSignalRequested(requestID string) bool
		GetApproximatePersistedSize() int

		CurrentTaskQueue() *taskqueuepb.TaskQueue
		SetStickyTaskQueue(name string, scheduleToStartTimeout *time.Duration)
		ClearStickyTaskQueue()
		IsStickyTaskQueueSet() bool
		TaskQueueScheduleToStartTimeout(name string) (*taskqueuepb.TaskQueue, *time.Duration)

		IsWorkflowExecutionRunning() bool
		IsResourceDuplicated(resourceDedupKey definition.DeduplicationID) bool
		IsWorkflowPendingOnWorkflowTaskBackoff() bool
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
		ReplicateChildWorkflowExecutionStartedEvent(*historypb.HistoryEvent, *clockspb.VectorClock) error
		ReplicateChildWorkflowExecutionTerminatedEvent(*historypb.HistoryEvent) error
		ReplicateChildWorkflowExecutionTimedOutEvent(*historypb.HistoryEvent) error
		ReplicateWorkflowTaskCompletedEvent(*historypb.HistoryEvent) error
		ReplicateWorkflowTaskFailedEvent() error
		ReplicateWorkflowTaskScheduledEvent(int64, int64, *taskqueuepb.TaskQueue, *time.Duration, int32, *time.Time, *time.Time, enumsspb.WorkflowTaskType) (*WorkflowTaskInfo, error)
		ReplicateWorkflowTaskStartedEvent(*WorkflowTaskInfo, int64, int64, int64, string, time.Time, bool, int64) (*WorkflowTaskInfo, error)
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
		ReplicateTransientWorkflowTaskScheduled() (*WorkflowTaskInfo, error)
		ReplicateWorkflowPropertiesModifiedEvent(*historypb.HistoryEvent)
		ReplicateUpsertWorkflowSearchAttributesEvent(*historypb.HistoryEvent)
		ReplicateWorkflowExecutionCancelRequestedEvent(*historypb.HistoryEvent) error
		ReplicateWorkflowExecutionCanceledEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionCompletedEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionContinuedAsNewEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionFailedEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionSignaled(*historypb.HistoryEvent) error
		ReplicateWorkflowExecutionStartedEvent(*clockspb.VectorClock, commonpb.WorkflowExecution, string, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionTerminatedEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionTimedoutEvent(int64, *historypb.HistoryEvent) error
		ReplicateWorkflowExecutionUpdateAcceptedEvent(*historypb.HistoryEvent) error
		ReplicateWorkflowExecutionUpdateCompletedEvent(event *historypb.HistoryEvent, batchID int64) error
		SetCurrentBranchToken(branchToken []byte) error
		SetHistoryBuilder(hBuilder *HistoryBuilder)
		SetHistoryTree(ctx context.Context, executionTimeout *time.Duration, runTimeout *time.Duration, treeID string) error
		SetBaseWorkflow(
			baseRunID string,
			baseRunLowestCommonAncestorEventID int64,
			baseRunLowestCommonAncestorEventVersion int64,
		)
		UpdateActivity(*persistencespb.ActivityInfo) error
		UpdateActivityWithTimerHeartbeat(*persistencespb.ActivityInfo, time.Time) error
		UpdateActivityProgress(ai *persistencespb.ActivityInfo, request *workflowservice.RecordActivityTaskHeartbeatRequest)
		UpdateUserTimer(*persistencespb.TimerInfo) error
		UpdateCurrentVersion(version int64, forceUpdate bool) error
		UpdateWorkflowStateStatus(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) error

		GetHistorySize() int64
		AddHistorySize(size int64)

		AddTasks(tasks ...tasks.Task)
		PopTasks() map[tasks.Category][]tasks.Task
		SetUpdateCondition(int64, int64)
		GetUpdateCondition() (int64, int64)

		SetSpeculativeWorkflowTaskTimeoutTask(task *tasks.WorkflowTaskTimeoutTask) error
		CheckSpeculativeWorkflowTaskTimeoutTask(task *tasks.WorkflowTaskTimeoutTask) bool
		RemoveSpeculativeWorkflowTaskTimeoutTask()

		StartTransaction(entry *namespace.Namespace) (bool, error)
		CloseTransactionAsMutation(transactionPolicy TransactionPolicy) (*persistence.WorkflowMutation, []*persistence.WorkflowEvents, error)
		CloseTransactionAsSnapshot(transactionPolicy TransactionPolicy) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error)
		GenerateMigrationTasks() (tasks.Task, int64, error)

		// ContinueAsNewMinBackoff calculate minimal backoff for next ContinueAsNew run.
		// Input backoffDuration is current backoff for next run.
		// If current backoff comply with minimal ContinueAsNew interval requirement, current backoff will be returned.
		// Current backoff could be nil which means it does not have a backoff.
		ContinueAsNewMinBackoff(backoffDuration *time.Duration) *time.Duration
	}
)
