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
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/service/history/historybuilder"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TransactionPolicy int

const (
	TransactionPolicyActive  TransactionPolicy = 0
	TransactionPolicyPassive TransactionPolicy = 1
	// Mutable state is a top-level state machine in the state machines framework.
	StateMachineType = "workflow.MutableState"
)

func (policy TransactionPolicy) Ptr() *TransactionPolicy {
	return &policy
}

var emptyTasks = []tasks.Task{}

type stateMachineDefinition struct{}

// TODO: Remove this implementation once transition history is fully implemented.
func (s stateMachineDefinition) CompareState(any, any) (int, error) {
	return 0, serviceerror.NewUnimplemented("CompareState not implemented for workflow mutable state")
}

func (stateMachineDefinition) Deserialize([]byte) (any, error) {
	return nil, serviceerror.NewUnimplemented("workflow mutable state persistence is not supported in the HSM framework")
}

// Serialize is a noop as Deserialize is not supported.
func (stateMachineDefinition) Serialize(any) ([]byte, error) {
	return nil, nil
}

func (stateMachineDefinition) Type() string {
	return StateMachineType
}

func RegisterStateMachine(reg *hsm.Registry) error {
	return reg.RegisterMachine(stateMachineDefinition{})
}

type (
	// TODO: This should be part of persistence layer
	WorkflowTaskInfo struct {
		Version             int64
		ScheduledEventID    int64
		StartedEventID      int64
		RequestID           string
		WorkflowTaskTimeout time.Duration
		// This is only needed to communicate task queue used after AddWorkflowTaskScheduledEvent.
		TaskQueue *taskqueuepb.TaskQueue
		Attempt   int32
		// Scheduled and Started timestamps are useful for transient workflow task: when transient workflow task finally completes,
		// use these Timestamp to create scheduled/started events.
		// Also used for recording latency metrics
		ScheduledTime time.Time
		StartedTime   time.Time
		// OriginalScheduledTime is to record the first scheduled workflow task during workflow task heartbeat.
		// Client may to heartbeat workflow task by RespondWorkflowTaskComplete with ForceCreateNewWorkflowTask == true
		// In this case, OriginalScheduledTime won't change. Then when time.Now().UTC()-OriginalScheduledTime exceeds
		// some threshold, server can interrupt the heartbeat by enforcing to time out the workflow task.
		OriginalScheduledTime time.Time

		// Indicate type of the current workflow task (normal, transient, or speculative).
		Type enumsspb.WorkflowTaskType

		// These two fields are sent to workers in the WorkflowTaskStarted event. We need to save a
		// copy in mutable state to know the last values we sent (which might have been in a
		// transient event), otherwise a dynamic config change of the suggestion threshold could
		// cause the WorkflowTaskStarted event that the worker used to not match the event we saved
		// in history.
		SuggestContinueAsNew bool
		HistorySizeBytes     int64
		// BuildIdRedirectCounter tracks the started build ID redirect counter for transient/speculative WFT. This
		// info is to make sure the right redirect counter is used in the WFT started event created later
		// for a transient/speculative WFT.
		BuildIdRedirectCounter int64
		// BuildId tracks the started build ID for transient/speculative WFT. This info is used for two purposes:
		// - verify WFT completes by the same Build ID that started in the latest attempt
		// - when persisting transient/speculative WFT, the right Build ID is used in the WFT started event
		BuildId string
	}

	WorkflowTaskCompletionLimits struct {
		MaxResetPoints              int
		MaxSearchAttributeValueSize int
	}

	MutableState interface {
		callbacks.CanGetNexusCompletion
		callbacks.CanGetHSMCompletionCallbackArg
		AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent
		LoadHistoryEvent(ctx context.Context, token []byte) (*historypb.HistoryEvent, error)

		AddActivityTaskCancelRequestedEvent(int64, int64, string) (*historypb.HistoryEvent, *persistencespb.ActivityInfo, error)
		AddActivityTaskCanceledEvent(int64, int64, int64, *commonpb.Payloads, string) (*historypb.HistoryEvent, error)
		AddActivityTaskCompletedEvent(int64, int64, *workflowservice.RespondActivityTaskCompletedRequest) (*historypb.HistoryEvent, error)
		AddActivityTaskFailedEvent(int64, int64, *failurepb.Failure, enumspb.RetryState, string, *commonpb.WorkerVersionStamp) (*historypb.HistoryEvent, error)
		AddActivityTaskScheduledEvent(int64, *commandpb.ScheduleActivityTaskCommandAttributes, bool) (*historypb.HistoryEvent, *persistencespb.ActivityInfo, error)
		AddActivityTaskStartedEvent(*persistencespb.ActivityInfo, int64, string, string, *commonpb.WorkerVersionStamp, *taskqueuespb.BuildIdRedirectInfo) (*historypb.HistoryEvent, error)
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
		AddWorkflowTaskFailedEvent(workflowTask *WorkflowTaskInfo, cause enumspb.WorkflowTaskFailedCause, failure *failurepb.Failure, identity string, versioningStamp *commonpb.WorkerVersionStamp, binChecksum, baseRunID, newRunID string, forkEventVersion int64) (*historypb.HistoryEvent, error)
		AddWorkflowTaskScheduleToStartTimeoutEvent(workflowTask *WorkflowTaskInfo) (*historypb.HistoryEvent, error)
		AddFirstWorkflowTaskScheduled(parentClock *clockspb.VectorClock, event *historypb.HistoryEvent, bypassTaskGeneration bool) (int64, error)
		AddWorkflowTaskScheduledEvent(bypassTaskGeneration bool, workflowTaskType enumsspb.WorkflowTaskType) (*WorkflowTaskInfo, error)
		AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration bool, originalScheduledTimestamp *timestamppb.Timestamp, workflowTaskType enumsspb.WorkflowTaskType) (*WorkflowTaskInfo, error)
		AddWorkflowTaskStartedEvent(int64, string, *taskqueuepb.TaskQueue, string, *commonpb.WorkerVersionStamp, *taskqueuespb.BuildIdRedirectInfo, bool) (*historypb.HistoryEvent, *WorkflowTaskInfo, error)
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
		AddWorkflowExecutionSignaled(
			signalName string,
			input *commonpb.Payloads,
			identity string,
			header *commonpb.Header,
			skipGenerateWorkflowTask bool,
			links []*commonpb.Link,
		) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionSignaledEvent(
			signalName string,
			input *commonpb.Payloads,
			identity string,
			header *commonpb.Header,
			skipGenerateWorkflowTask bool,
			externalWorkflowExecution *commonpb.WorkflowExecution,
			links []*commonpb.Link,
		) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionStartedEvent(*commonpb.WorkflowExecution, *historyservice.StartWorkflowExecutionRequest) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionStartedEventWithOptions(*commonpb.WorkflowExecution, *historyservice.StartWorkflowExecutionRequest, *workflowpb.ResetPoints, string, string) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionTerminatedEvent(firstEventID int64, reason string, details *commonpb.Payloads, identity string, deleteAfterTerminate bool, links []*commonpb.Link) (*historypb.HistoryEvent, error)

		AddWorkflowExecutionUpdateAcceptedEvent(protocolInstanceID string, acceptedRequestMessageId string, acceptedRequestSequencingEventId int64, acceptedRequest *updatepb.Request) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionUpdateCompletedEvent(acceptedEventID int64, updResp *updatepb.Response) (*historypb.HistoryEvent, error)
		RejectWorkflowExecutionUpdate(protocolInstanceID string, updRejection *updatepb.Rejection) error
		AddWorkflowExecutionUpdateAdmittedEvent(request *updatepb.Request, origin enumspb.UpdateAdmittedEventOrigin) (*historypb.HistoryEvent, error)
		ApplyWorkflowExecutionUpdateAdmittedEvent(event *historypb.HistoryEvent, batchId int64) error
		VisitUpdates(visitor func(updID string, updInfo *persistencespb.UpdateInfo))
		GetUpdateOutcome(ctx context.Context, updateID string) (*updatepb.Outcome, error)

		CheckResettable() error
		// UpdateResetRunID saves the runID that resulted when this execution was reset.
		UpdateResetRunID(runID string)

		CloneToProto() *persistencespb.WorkflowMutableState
		RetryActivity(ai *persistencespb.ActivityInfo, failure *failurepb.Failure) (enumspb.RetryState, error)
		RecordLastActivityStarted(ai *persistencespb.ActivityInfo)
		RegenerateActivityRetryTask(ai *persistencespb.ActivityInfo) error
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
		GetWorkflowCloseTime(ctx context.Context) (time.Time, error)
		GetWorkflowExecutionDuration(ctx context.Context) (time.Duration, error)
		GetWorkflowTaskByID(scheduledEventID int64) *WorkflowTaskInfo
		GetNamespaceEntry() *namespace.Namespace
		GetStartEvent(context.Context) (*historypb.HistoryEvent, error)
		GetSignalExternalInitiatedEvent(context.Context, int64) (*historypb.HistoryEvent, error)
		GetFirstRunID(ctx context.Context) (string, error)
		GetCurrentBranchToken() ([]byte, error)
		GetCurrentVersion() int64
		GetStartVersion() (int64, error)
		GetCloseVersion() (int64, error)
		GetLastWriteVersion() (int64, error)
		GetLastEventVersion() (int64, error)
		GetExecutionInfo() *persistencespb.WorkflowExecutionInfo
		GetExecutionState() *persistencespb.WorkflowExecutionState
		GetStartedWorkflowTask() *WorkflowTaskInfo
		GetPendingWorkflowTask() *WorkflowTaskInfo
		GetLastFirstEventIDTxnID() (int64, int64)
		GetNextEventID() int64
		GetLastCompletedWorkflowTaskStartedEventId() int64
		GetPendingActivityInfos() map[int64]*persistencespb.ActivityInfo
		GetPendingTimerInfos() map[string]*persistencespb.TimerInfo
		GetPendingChildExecutionInfos() map[int64]*persistencespb.ChildExecutionInfo
		GetPendingRequestCancelExternalInfos() map[int64]*persistencespb.RequestCancelInfo
		GetPendingSignalExternalInfos() map[int64]*persistencespb.SignalInfo
		GetPendingSignalRequestedIds() []string
		GetRequestCancelInfo(int64) (*persistencespb.RequestCancelInfo, bool)
		GetRetryBackoffDuration(failure *failurepb.Failure) (time.Duration, enumspb.RetryState)
		GetCronBackoffDuration() time.Duration
		GetSignalInfo(int64) (*persistencespb.SignalInfo, bool)
		GetUserTimerInfoByEventID(int64) (*persistencespb.TimerInfo, bool)
		GetUserTimerInfo(string) (*persistencespb.TimerInfo, bool)
		GetWorkflowType() *commonpb.WorkflowType
		GetWorkflowStateStatus() (enumsspb.WorkflowExecutionState, enumspb.WorkflowExecutionStatus)
		GetQueryRegistry() QueryRegistry
		GetBaseWorkflowInfo() *workflowspb.BaseExecutionInfo
		GetAssignedBuildId() string
		GetInheritedBuildId() string
		GetMostRecentWorkerVersionStamp() *commonpb.WorkerVersionStamp
		IsTransientWorkflowTask() bool
		ClearTransientWorkflowTask() error
		HasBufferedEvents() bool
		HasAnyBufferedEvent(filter historybuilder.BufferedEventFilter) bool
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
		SetStickyTaskQueue(name string, scheduleToStartTimeout *durationpb.Duration)
		ClearStickyTaskQueue()
		IsStickyTaskQueueSet() bool
		TaskQueueScheduleToStartTimeout(name string) (*taskqueuepb.TaskQueue, *durationpb.Duration)

		IsWorkflowExecutionRunning() bool
		IsResourceDuplicated(resourceDedupKey definition.DeduplicationID) bool
		IsWorkflowPendingOnWorkflowTaskBackoff() bool
		UpdateDuplicatedResource(resourceDedupKey definition.DeduplicationID)
		UpdateActivityInfo(*historyservice.ActivitySyncInfo, bool) error
		ApplyMutation(mutation *persistencespb.WorkflowMutableStateMutation) error
		ApplySnapshot(snapshot *persistencespb.WorkflowMutableState) error
		ApplyActivityTaskCancelRequestedEvent(*historypb.HistoryEvent) error
		ApplyActivityTaskCanceledEvent(*historypb.HistoryEvent) error
		ApplyActivityTaskCompletedEvent(*historypb.HistoryEvent) error
		ApplyActivityTaskFailedEvent(*historypb.HistoryEvent) error
		ApplyActivityTaskScheduledEvent(int64, *historypb.HistoryEvent) (*persistencespb.ActivityInfo, error)
		ApplyActivityTaskStartedEvent(*historypb.HistoryEvent) error
		ApplyActivityTaskTimedOutEvent(*historypb.HistoryEvent) error
		ApplyChildWorkflowExecutionCanceledEvent(*historypb.HistoryEvent) error
		ApplyChildWorkflowExecutionCompletedEvent(*historypb.HistoryEvent) error
		ApplyChildWorkflowExecutionFailedEvent(*historypb.HistoryEvent) error
		ApplyChildWorkflowExecutionStartedEvent(*historypb.HistoryEvent, *clockspb.VectorClock) error
		ApplyChildWorkflowExecutionTerminatedEvent(*historypb.HistoryEvent) error
		ApplyChildWorkflowExecutionTimedOutEvent(*historypb.HistoryEvent) error
		ApplyWorkflowTaskCompletedEvent(*historypb.HistoryEvent) error
		ApplyWorkflowTaskFailedEvent() error
		ApplyWorkflowTaskScheduledEvent(int64, int64, *taskqueuepb.TaskQueue, *durationpb.Duration, int32, *timestamppb.Timestamp, *timestamppb.Timestamp, enumsspb.WorkflowTaskType) (*WorkflowTaskInfo, error)
		ApplyWorkflowTaskStartedEvent(*WorkflowTaskInfo, int64, int64, int64, string, time.Time, bool, int64, *commonpb.WorkerVersionStamp, int64) (*WorkflowTaskInfo, error)
		ApplyWorkflowTaskTimedOutEvent(enumspb.TimeoutType) error
		ApplyExternalWorkflowExecutionCancelRequested(*historypb.HistoryEvent) error
		ApplyExternalWorkflowExecutionSignaled(*historypb.HistoryEvent) error
		ApplyRequestCancelExternalWorkflowExecutionFailedEvent(*historypb.HistoryEvent) error
		ApplyRequestCancelExternalWorkflowExecutionInitiatedEvent(int64, *historypb.HistoryEvent, string) (*persistencespb.RequestCancelInfo, error)
		ApplySignalExternalWorkflowExecutionFailedEvent(*historypb.HistoryEvent) error
		ApplySignalExternalWorkflowExecutionInitiatedEvent(int64, *historypb.HistoryEvent, string) (*persistencespb.SignalInfo, error)
		ApplyStartChildWorkflowExecutionFailedEvent(*historypb.HistoryEvent) error
		ApplyStartChildWorkflowExecutionInitiatedEvent(int64, *historypb.HistoryEvent, string) (*persistencespb.ChildExecutionInfo, error)
		ApplyTimerCanceledEvent(*historypb.HistoryEvent) error
		ApplyTimerFiredEvent(*historypb.HistoryEvent) error
		ApplyTimerStartedEvent(*historypb.HistoryEvent) (*persistencespb.TimerInfo, error)
		ApplyTransientWorkflowTaskScheduled() (*WorkflowTaskInfo, error)
		ApplyWorkflowPropertiesModifiedEvent(*historypb.HistoryEvent)
		ApplyUpsertWorkflowSearchAttributesEvent(*historypb.HistoryEvent)
		ApplyWorkflowExecutionCancelRequestedEvent(*historypb.HistoryEvent) error
		ApplyWorkflowExecutionCanceledEvent(int64, *historypb.HistoryEvent) error
		ApplyWorkflowExecutionCompletedEvent(int64, *historypb.HistoryEvent) error
		ApplyWorkflowExecutionContinuedAsNewEvent(int64, *historypb.HistoryEvent) error
		ApplyWorkflowExecutionFailedEvent(int64, *historypb.HistoryEvent) error
		ApplyWorkflowExecutionSignaled(*historypb.HistoryEvent) error
		ApplyWorkflowExecutionStartedEvent(*clockspb.VectorClock, *commonpb.WorkflowExecution, string, *historypb.HistoryEvent) error
		ApplyWorkflowExecutionTerminatedEvent(int64, *historypb.HistoryEvent) error
		ApplyWorkflowExecutionTimedoutEvent(int64, *historypb.HistoryEvent) error
		ApplyWorkflowExecutionUpdateAcceptedEvent(*historypb.HistoryEvent) error
		ApplyWorkflowExecutionUpdateCompletedEvent(event *historypb.HistoryEvent, batchID int64) error
		SetCurrentBranchToken(branchToken []byte) error
		SetHistoryBuilder(hBuilder *historybuilder.HistoryBuilder)
		SetHistoryTree(executionTimeout *durationpb.Duration, runTimeout *durationpb.Duration, treeID string) error
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
		UpdateBuildIdAssignment(buildId string) error
		ApplyBuildIdRedirect(startingTaskScheduledEventId int64, buildId string, redirectCounter int64) error
		RefreshExpirationTimeoutTask(ctx context.Context) error

		GetHistorySize() int64
		AddHistorySize(size int64)

		AddTasks(tasks ...tasks.Task)
		PopTasks() map[tasks.Category][]tasks.Task
		SetUpdateCondition(int64, int64)
		GetUpdateCondition() (int64, int64)

		SetSpeculativeWorkflowTaskTimeoutTask(task *tasks.WorkflowTaskTimeoutTask) error
		CheckSpeculativeWorkflowTaskTimeoutTask(task *tasks.WorkflowTaskTimeoutTask) bool
		RemoveSpeculativeWorkflowTaskTimeoutTask()

		IsDirty() bool
		IsTransitionHistoryEnabled() bool
		StartTransaction(entry *namespace.Namespace) (bool, error)
		CloseTransactionAsMutation(transactionPolicy TransactionPolicy) (*persistence.WorkflowMutation, []*persistence.WorkflowEvents, error)
		CloseTransactionAsSnapshot(transactionPolicy TransactionPolicy) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error)
		GenerateMigrationTasks() ([]tasks.Task, int64, error)

		// ContinueAsNewMinBackoff calculate minimal backoff for next ContinueAsNew run.
		// Input backoffDuration is current backoff for next run.
		// If current backoff comply with minimal ContinueAsNew interval requirement, current backoff will be returned.
		// Current backoff could be nil which means it does not have a backoff.
		ContinueAsNewMinBackoff(backoffDuration *durationpb.Duration) *durationpb.Duration
		HasCompletedAnyWorkflowTask() bool

		HSM() *hsm.Node

		// NextTransitionCount returns the next state transition count from the state transition history.
		// If state transition history is empty (e.g. when disabled or fresh mutable state), returns 0.
		NextTransitionCount() int64

		InitTransitionHistory()

		ShouldResetActivityTimerTaskMask(current, incoming *persistencespb.ActivityInfo) bool
	}
)
