//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination mutable_state_mock.go

package interfaces

import (
	"context"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
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
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/historybuilder"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow/update"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	ActivityUpdater func(*persistencespb.ActivityInfo, MutableState) error

	MutableState interface {
		AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent
		LoadHistoryEvent(ctx context.Context, token []byte) (*historypb.HistoryEvent, error)

		AddActivityTaskCancelRequestedEvent(int64, int64, string) (*historypb.HistoryEvent, *persistencespb.ActivityInfo, error)
		AddActivityTaskCanceledEvent(int64, int64, int64, *commonpb.Payloads, string) (*historypb.HistoryEvent, error)
		AddActivityTaskCompletedEvent(int64, int64, *workflowservice.RespondActivityTaskCompletedRequest) (*historypb.HistoryEvent, error)
		AddActivityTaskFailedEvent(int64, int64, *failurepb.Failure, enumspb.RetryState, string, *commonpb.WorkerVersionStamp) (*historypb.HistoryEvent, error)
		AddActivityTaskScheduledEvent(int64, *commandpb.ScheduleActivityTaskCommandAttributes, bool) (*historypb.HistoryEvent, *persistencespb.ActivityInfo, error)
		AddActivityTaskStartedEvent(
			*persistencespb.ActivityInfo,
			int64,
			string,
			string,
			*commonpb.WorkerVersionStamp,
			*deploymentpb.Deployment,
			*taskqueuespb.BuildIdRedirectInfo,
		) (*historypb.HistoryEvent, error)
		AddActivityTaskTimedOutEvent(int64, int64, *failurepb.Failure, enumspb.RetryState) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionCanceledEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionCanceledEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionCompletedEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionCompletedEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionFailedEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionFailedEventAttributes) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionStartedEvent(*commonpb.WorkflowExecution, *commonpb.WorkflowType, int64, *commonpb.Header, *clockspb.VectorClock) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionTerminatedEvent(int64, *commonpb.WorkflowExecution) (*historypb.HistoryEvent, error)
		AddChildWorkflowExecutionTimedOutEvent(int64, *commonpb.WorkflowExecution, *historypb.WorkflowExecutionTimedOutEventAttributes) (*historypb.HistoryEvent, error)
		AddCompletedWorkflowEvent(int64, *commandpb.CompleteWorkflowExecutionCommandAttributes, string) (*historypb.HistoryEvent, error)
		AddContinueAsNewEvent(context.Context, int64, int64, namespace.Name, *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes, worker_versioning.IsWFTaskQueueInVersionDetector) (*historypb.HistoryEvent, MutableState, error)
		AddWorkflowTaskCompletedEvent(*WorkflowTaskInfo, *workflowservice.RespondWorkflowTaskCompletedRequest, WorkflowTaskCompletionLimits) (*historypb.HistoryEvent, error)
		AddWorkflowTaskFailedEvent(workflowTask *WorkflowTaskInfo, cause enumspb.WorkflowTaskFailedCause, failure *failurepb.Failure, identity string, versioningStamp *commonpb.WorkerVersionStamp, binChecksum, baseRunID, newRunID string, forkEventVersion int64) (*historypb.HistoryEvent, error)
		AddWorkflowTaskScheduleToStartTimeoutEvent(workflowTask *WorkflowTaskInfo) (*historypb.HistoryEvent, error)
		AddFirstWorkflowTaskScheduled(parentClock *clockspb.VectorClock, event *historypb.HistoryEvent, bypassTaskGeneration bool) (int64, error)
		AddWorkflowTaskScheduledEvent(bypassTaskGeneration bool, workflowTaskType enumsspb.WorkflowTaskType) (*WorkflowTaskInfo, error)
		AddWorkflowTaskScheduledEventAsHeartbeat(bypassTaskGeneration bool, originalScheduledTimestamp *timestamppb.Timestamp, workflowTaskType enumsspb.WorkflowTaskType) (*WorkflowTaskInfo, error)
		AddWorkflowTaskStartedEvent(int64, string, *taskqueuepb.TaskQueue, string, *commonpb.WorkerVersionStamp, *taskqueuespb.BuildIdRedirectInfo, update.Registry, bool) (*historypb.HistoryEvent, *WorkflowTaskInfo, error)
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
		AddStartChildWorkflowExecutionInitiatedEvent(int64, *commandpb.StartChildWorkflowExecutionCommandAttributes, namespace.ID) (*historypb.HistoryEvent, *persistencespb.ChildExecutionInfo, error)
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
			links []*commonpb.Link,
		) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionSignaledEvent(
			signalName string,
			input *commonpb.Payloads,
			identity string,
			header *commonpb.Header,
			externalWorkflowExecution *commonpb.WorkflowExecution,
			links []*commonpb.Link,
		) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionStartedEvent(*commonpb.WorkflowExecution, *historyservice.StartWorkflowExecutionRequest) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionStartedEventWithOptions(*commonpb.WorkflowExecution, *historyservice.StartWorkflowExecutionRequest, *workflowpb.ResetPoints, string, string) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionTerminatedEvent(firstEventID int64, reason string, details *commonpb.Payloads, identity string, deleteAfterTerminate bool, links []*commonpb.Link) (*historypb.HistoryEvent, error)
		AddWorkflowExecutionOptionsUpdatedEvent(
			versioningOverride *workflowpb.VersioningOverride,
			unsetVersioningOverride bool,
			attachRequestID string,
			attachCompletionCallbacks []*commonpb.Callback,
			links []*commonpb.Link,
		) (*historypb.HistoryEvent, error)
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
		// IsResetRun returns true if this run is result of a reset operation.
		IsResetRun() bool
		SetChildrenInitializedPostResetPoint(children map[string]*persistencespb.ResetChildInfo)
		GetChildrenInitializedPostResetPoint() map[string]*persistencespb.ResetChildInfo
		AttachRequestID(requestID string, eventType enumspb.EventType, eventID int64)

		CloneToProto() *persistencespb.WorkflowMutableState
		RetryActivity(ai *persistencespb.ActivityInfo, failure *failurepb.Failure) (enumspb.RetryState, error)
		RecordLastActivityCompleteTime(ai *persistencespb.ActivityInfo)
		RegenerateActivityRetryTask(ai *persistencespb.ActivityInfo, newScheduledTime time.Time) error
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
		GetPendingChildIds() map[int64]struct{}
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
		IsNonCurrentWorkflowGuaranteed() (bool, error)
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
		ApplyStartChildWorkflowExecutionInitiatedEvent(int64, *historypb.HistoryEvent) (*persistencespb.ChildExecutionInfo, error)
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
		ApplyWorkflowExecutionOptionsUpdatedEvent(event *historypb.HistoryEvent) error
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
		UpdateActivity(int64, ActivityUpdater) error
		UpdateActivityTaskStatusWithTimerHeartbeat(scheduleEventId int64, timerTaskStatus int32, heartbeatTimeoutVisibility *time.Time) error
		UpdateActivityProgress(ai *persistencespb.ActivityInfo, request *workflowservice.RecordActivityTaskHeartbeatRequest)
		UpdateUserTimer(*persistencespb.TimerInfo) error
		UpdateUserTimerTaskStatus(timerId string, status int64) error
		UpdateCurrentVersion(version int64, forceUpdate bool) error
		UpdateWorkflowStateStatus(state enumsspb.WorkflowExecutionState, status enumspb.WorkflowExecutionStatus) (bool, error)
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

		SetWorkflowTaskScheduleToStartTimeoutTask(task *tasks.WorkflowTaskTimeoutTask)
		SetWorkflowTaskStartToCloseTimeoutTask(task *tasks.WorkflowTaskTimeoutTask)
		GetWorkflowTaskScheduleToStartTimeoutTask() *tasks.WorkflowTaskTimeoutTask
		GetWorkflowTaskStartToCloseTimeoutTask() *tasks.WorkflowTaskTimeoutTask

		IsDirty() bool
		IsTransitionHistoryEnabled() bool
		// StartTransaction sets up the mutable state for transacting.
		StartTransaction(entry *namespace.Namespace) (bool, error)
		// CloseTransactionAsMutation closes the mutable state transaction (different from DB transaction) and prepares the whole state mutation to be persisted and bumps the DBRecordVersion.
		// You should ideally not make any changes to the mutable state after this call.
		CloseTransactionAsMutation(transactionPolicy TransactionPolicy) (*persistence.WorkflowMutation, []*persistence.WorkflowEvents, error)
		// CloseTransactionAsSnapshot closes the mutable state transaction (different from DB transaction) and prepares the current snapshot of the state to be persisted and bumps the DBRecordVersion.
		// You should ideally not make any changes to the mutable state after this call.
		CloseTransactionAsSnapshot(transactionPolicy TransactionPolicy) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error)
		GenerateMigrationTasks(targetClusters []string) ([]tasks.Task, int64, error)

		// ContinueAsNewMinBackoff calculate minimal backoff for next ContinueAsNew run.
		// Input backoffDuration is current backoff for next run.
		// If current backoff comply with minimal ContinueAsNew interval requirement, current backoff will be returned.
		// Current backoff could be nil which means it does not have a backoff.
		ContinueAsNewMinBackoff(backoffDuration *durationpb.Duration) *durationpb.Duration
		HasCompletedAnyWorkflowTask() bool

		HSM() *hsm.Node

		IsWorkflow() bool
		ChasmTree() ChasmTree

		// NextTransitionCount returns the next state transition count from the state transition history.
		// If state transition history is empty (e.g. when disabled or fresh mutable state), returns 0.
		NextTransitionCount() int64

		InitTransitionHistory()

		ShouldResetActivityTimerTaskMask(current, incoming *persistencespb.ActivityInfo) bool
		// GetEffectiveDeployment returns the effective deployment in the following order:
		//  1. DeploymentVersionTransition.Deployment: this is returned when the wf is transitioning to a
		//     new deployment
		//  2. VersioningOverride.Deployment: this is returned when user has set a PINNED override
		//     at wf start time, or later via UpdateWorkflowExecutionOptions.
		//  3. Deployment: this is returned when there is no transition and no override (the most
		//     common case). Deployment is set based on the worker-sent deployment in the latest WFT
		//     completion. Exception: if Deployment is set but the workflow's effective behavior is
		//     UNSPECIFIED, it means the workflow is unversioned, so effective deployment will be nil.
		// Note: Deployment objects are immutable, never change their fields.
		GetEffectiveDeployment() *deploymentpb.Deployment
		// GetEffectiveVersioningBehavior returns the effective versioning behavior in the following
		// order:
		//  1. DeploymentVersionTransition: if there is a transition, then effective behavior is AUTO_UPGRADE.
		//  2. VersioningOverride.Behavior: this is returned when user has set a behavior override
		//     at wf start time, or later via UpdateWorkflowExecutionOptions.
		//  3. Behavior: this is returned when there is no override (most common case). Behavior is
		//     set based on the worker-sent deployment in the latest WFT completion.
		GetEffectiveVersioningBehavior() enumspb.VersioningBehavior
		GetDeploymentTransition() *workflowpb.DeploymentTransition
		// StartDeploymentTransition starts a transition to the given deployment which must be
		// different from workflows effective deployment. Will fail if the workflow is pinned.
		// Starting a new transition replaces current transition, if present, without rescheduling
		// activities.
		// If there is a pending workflow task that is not started yet, it'll be rescheduled after
		// transition start.
		StartDeploymentTransition(deployment *deploymentpb.Deployment) error

		AddReapplyCandidateEvent(event *historypb.HistoryEvent)
		GetReapplyCandidateEvents() []*historypb.HistoryEvent

		CurrentVersionedTransition() *persistencespb.VersionedTransition

		DeleteSubStateMachine(path *persistencespb.StateMachinePath) error
		IsSubStateMachineDeleted() bool

		HasRequestID(requestID string) bool
		SetSuccessorRunID(runID string)
	}
)
