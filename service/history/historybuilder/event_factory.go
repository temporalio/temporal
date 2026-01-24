package historybuilder

import (
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	sdkpb "go.temporal.io/api/sdk/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type EventFactory struct {
	timeSource clock.TimeSource
	version    int64
}

func (b *EventFactory) CreateWorkflowExecutionStartedEvent(
	startTime time.Time,
	request *historyservice.StartWorkflowExecutionRequest,
	resetPoints *workflowpb.ResetPoints,
	prevRunID string,
	firstRunID string,
	originalRunID string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, startTime)
	req := request.GetStartRequest()

	// Versioning override might be set on the workflow service request if a user passes it to
	// StartWorkflow options, or it might be set on the history service request if a workflow is
	// continuing-as-new and inheriting a Pinned override. Use whichever of the two is non-nil.
	nonNilVersioningOverride := req.GetVersioningOverride() // From user.
	if nonNilVersioningOverride == nil {
		nonNilVersioningOverride = request.GetVersioningOverride() // From server during continue-as-new.
	}

	attributes := historypb.WorkflowExecutionStartedEventAttributes_builder{
		WorkflowType:                    req.GetWorkflowType(),
		TaskQueue:                       req.GetTaskQueue(),
		Header:                          req.GetHeader(),
		Input:                           req.GetInput(),
		WorkflowRunTimeout:              req.GetWorkflowRunTimeout(),
		WorkflowExecutionTimeout:        req.GetWorkflowExecutionTimeout(),
		WorkflowTaskTimeout:             req.GetWorkflowTaskTimeout(),
		ContinuedExecutionRunId:         prevRunID,
		PrevAutoResetPoints:             resetPoints,
		Identity:                        req.GetIdentity(),
		RetryPolicy:                     req.GetRetryPolicy(),
		Attempt:                         request.GetAttempt(),
		WorkflowExecutionExpirationTime: request.GetWorkflowExecutionExpirationTime(),
		CronSchedule:                    req.GetCronSchedule(),
		LastCompletionResult:            request.GetLastCompletionResult(),
		ContinuedFailure:                request.GetContinuedFailure(),
		Initiator:                       request.GetContinueAsNewInitiator(),
		FirstWorkflowTaskBackoff:        request.GetFirstWorkflowTaskBackoff(),
		FirstExecutionRunId:             firstRunID,
		OriginalExecutionRunId:          originalRunID,
		Memo:                            req.GetMemo(),
		SearchAttributes:                req.GetSearchAttributes(),
		WorkflowId:                      req.GetWorkflowId(),
		SourceVersionStamp:              request.GetSourceVersionStamp(),
		CompletionCallbacks:             req.GetCompletionCallbacks(),
		RootWorkflowExecution:           request.GetRootExecutionInfo().GetExecution(),
		InheritedBuildId:                request.GetInheritedBuildId(),
		VersioningOverride:              worker_versioning.ConvertOverrideToV32(nonNilVersioningOverride),
		Priority:                        req.GetPriority(),
		InheritedPinnedVersion:          request.GetInheritedPinnedVersion(),
		// We expect the API handler to unset RequestEagerExecution if eager execution cannot be accepted.
		EagerExecutionAccepted:   req.GetRequestEagerExecution(),
		InheritedAutoUpgradeInfo: request.GetInheritedAutoUpgradeInfo(),
	}.Build()

	parentInfo := request.GetParentExecutionInfo()
	if parentInfo != nil {
		attributes.SetParentWorkflowNamespaceId(parentInfo.GetNamespaceId())
		attributes.SetParentWorkflowNamespace(parentInfo.GetNamespace())
		attributes.SetParentWorkflowExecution(parentInfo.GetExecution())
		attributes.SetParentInitiatedEventId(parentInfo.GetInitiatedId())
		attributes.SetParentInitiatedEventVersion(parentInfo.GetInitiatedVersion())
	}

	event.SetWorkflowExecutionStartedEventAttributes(proto.ValueOrDefault(attributes))
	return event
}

func (b *EventFactory) CreateWorkflowTaskScheduledEvent(
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeout *durationpb.Duration,
	attempt int32,
	scheduleTime time.Time,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, scheduleTime)
	event.SetWorkflowTaskScheduledEventAttributes(historypb.WorkflowTaskScheduledEventAttributes_builder{
		TaskQueue:           taskQueue,
		StartToCloseTimeout: startToCloseTimeout,
		Attempt:             attempt,
	}.Build())

	return event
}

func (b *EventFactory) CreateWorkflowTaskStartedEvent(
	scheduledEventID int64,
	requestID string,
	identity string,
	startTime time.Time,
	suggestContinueAsNew bool,
	historySizeBytes int64,
	versioningStamp *commonpb.WorkerVersionStamp,
	buildIdRedirectCounter int64,
	suggestContinueAsNewReasons []enumspb.SuggestContinueAsNewReason,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, startTime)
	event.SetWorkflowTaskStartedEventAttributes(historypb.WorkflowTaskStartedEventAttributes_builder{
		ScheduledEventId:            scheduledEventID,
		Identity:                    identity,
		RequestId:                   requestID,
		SuggestContinueAsNew:        suggestContinueAsNew,
		SuggestContinueAsNewReasons: suggestContinueAsNewReasons,
		HistorySizeBytes:            historySizeBytes,
		WorkerVersion:               versioningStamp,
		BuildIdRedirectCounter:      buildIdRedirectCounter,
	}.Build())
	return event
}

func (b *EventFactory) CreateWorkflowTaskCompletedEvent(
	scheduledEventID int64,
	startedEventID int64,
	identity string,
	checksum string,
	workerVersionStamp *commonpb.WorkerVersionStamp,
	sdkMetadata *sdkpb.WorkflowTaskCompletedMetadata,
	meteringMetadata *commonpb.MeteringMetadata,
	deploymentName string,
	deployment *deploymentpb.Deployment,
	behavior enumspb.VersioningBehavior,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, b.timeSource.Now())
	event.SetWorkflowTaskCompletedEventAttributes(historypb.WorkflowTaskCompletedEventAttributes_builder{
		ScheduledEventId:     scheduledEventID,
		StartedEventId:       startedEventID,
		Identity:             identity,
		BinaryChecksum:       checksum,
		WorkerVersion:        workerVersionStamp,
		SdkMetadata:          sdkMetadata,
		MeteringMetadata:     meteringMetadata,
		WorkerDeploymentName: deploymentName,
		DeploymentVersion:    worker_versioning.ExternalWorkerDeploymentVersionFromDeployment(deployment),
		VersioningBehavior:   behavior,
	}.Build())

	return event
}

func (b *EventFactory) CreateWorkflowTaskTimedOutEvent(
	scheduledEventID int64,
	startedEventID int64,
	timeoutType enumspb.TimeoutType,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT, b.timeSource.Now())
	event.SetWorkflowTaskTimedOutEventAttributes(historypb.WorkflowTaskTimedOutEventAttributes_builder{
		ScheduledEventId: scheduledEventID,
		StartedEventId:   startedEventID,
		TimeoutType:      timeoutType,
	}.Build())

	return event
}

func (b *EventFactory) CreateWorkflowTaskFailedEvent(
	scheduledEventID int64,
	startedEventID int64,
	cause enumspb.WorkflowTaskFailedCause,
	failure *failurepb.Failure,
	identity string,
	baseRunID string,
	newRunID string,
	forkEventVersion int64,
	checksum string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED, b.timeSource.Now())
	event.SetWorkflowTaskFailedEventAttributes(historypb.WorkflowTaskFailedEventAttributes_builder{
		ScheduledEventId: scheduledEventID,
		StartedEventId:   startedEventID,
		Cause:            cause,
		Failure:          failure,
		Identity:         identity,
		BaseRunId:        baseRunID,
		NewRunId:         newRunID,
		ForkEventVersion: forkEventVersion,
		BinaryChecksum:   checksum,
	}.Build())
	return event
}

func (b *EventFactory) CreateActivityTaskScheduledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.ScheduleActivityTaskCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED, b.timeSource.Now())
	event.SetActivityTaskScheduledEventAttributes(historypb.ActivityTaskScheduledEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		ActivityId:                   command.GetActivityId(),
		ActivityType:                 command.GetActivityType(),
		TaskQueue:                    command.GetTaskQueue(),
		Header:                       command.GetHeader(),
		Input:                        command.GetInput(),
		ScheduleToCloseTimeout:       command.GetScheduleToCloseTimeout(),
		ScheduleToStartTimeout:       command.GetScheduleToStartTimeout(),
		StartToCloseTimeout:          command.GetStartToCloseTimeout(),
		HeartbeatTimeout:             command.GetHeartbeatTimeout(),
		RetryPolicy:                  command.GetRetryPolicy(),
		UseWorkflowBuildId:           command.GetUseWorkflowBuildId(),
		Priority:                     command.GetPriority(),
	}.Build())
	return event
}

func (b *EventFactory) CreateActivityTaskStartedEvent(
	scheduledEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailure *failurepb.Failure,
	versioningStamp *commonpb.WorkerVersionStamp,
	redirectCounter int64,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, b.timeSource.Now())
	event.SetActivityTaskStartedEventAttributes(historypb.ActivityTaskStartedEventAttributes_builder{
		ScheduledEventId:       scheduledEventID,
		Attempt:                attempt,
		Identity:               identity,
		RequestId:              requestID,
		LastFailure:            lastFailure,
		WorkerVersion:          versioningStamp,
		BuildIdRedirectCounter: redirectCounter,
	}.Build())
	return event
}

func (b *EventFactory) CreateActivityTaskCompletedEvent(
	scheduledEventID int64,
	startedEventID int64,
	identity string,
	result *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, b.timeSource.Now())
	event.SetActivityTaskCompletedEventAttributes(historypb.ActivityTaskCompletedEventAttributes_builder{
		ScheduledEventId: scheduledEventID,
		StartedEventId:   startedEventID,
		Result:           result,
		Identity:         identity,
	}.Build())
	return event
}

func (b *EventFactory) CreateActivityTaskFailedEvent(
	scheduledEventID int64,
	startedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
	identity string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, b.timeSource.Now())
	event.SetActivityTaskFailedEventAttributes(historypb.ActivityTaskFailedEventAttributes_builder{
		ScheduledEventId: scheduledEventID,
		StartedEventId:   startedEventID,
		Failure:          failure,
		RetryState:       retryState,
		Identity:         identity,
	}.Build())
	return event
}

func (b *EventFactory) CreateActivityTaskTimedOutEvent(
	scheduledEventID int64,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, b.timeSource.Now())
	event.SetActivityTaskTimedOutEventAttributes(historypb.ActivityTaskTimedOutEventAttributes_builder{
		ScheduledEventId: scheduledEventID,
		StartedEventId:   startedEventID,
		Failure:          timeoutFailure,
		RetryState:       retryState,
	}.Build())
	return event
}

func (b *EventFactory) CreateCompletedWorkflowEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CompleteWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, b.timeSource.Now())
	event.SetWorkflowExecutionCompletedEventAttributes(historypb.WorkflowExecutionCompletedEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Result:                       command.GetResult(),
		NewExecutionRunId:            newExecutionRunID,
	}.Build())
	return event
}

func (b *EventFactory) CreateFailWorkflowEvent(
	workflowTaskCompletedEventID int64,
	retryState enumspb.RetryState,
	command *commandpb.FailWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.SetWorkflowExecutionFailedEventAttributes(historypb.WorkflowExecutionFailedEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Failure:                      command.GetFailure(),
		RetryState:                   retryState,
		NewExecutionRunId:            newExecutionRunID,
	}.Build())
	return event
}

func (b *EventFactory) CreateTimeoutWorkflowEvent(
	retryState enumspb.RetryState,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT, b.timeSource.Now())
	event.SetWorkflowExecutionTimedOutEventAttributes(historypb.WorkflowExecutionTimedOutEventAttributes_builder{
		RetryState:        retryState,
		NewExecutionRunId: newExecutionRunID,
	}.Build())
	return event
}

func (b *EventFactory) CreateWorkflowExecutionTerminatedEvent(
	reason string,
	details *commonpb.Payloads,
	identity string,
	links []*commonpb.Link,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED, b.timeSource.Now())
	event.SetWorkflowExecutionTerminatedEventAttributes(historypb.WorkflowExecutionTerminatedEventAttributes_builder{
		Reason:   reason,
		Details:  details,
		Identity: identity,
	}.Build())
	event.SetLinks(links)
	return event
}

func (b *EventFactory) CreateWorkflowExecutionOptionsUpdatedEvent(
	versioningOverride *workflowpb.VersioningOverride,
	unsetVersioningOverride bool,
	attachRequestID string,
	attachCompletionCallbacks []*commonpb.Callback,
	links []*commonpb.Link,
	identity string,
	priority *commonpb.Priority,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, b.timeSource.Now())
	event.SetWorkflowExecutionOptionsUpdatedEventAttributes(historypb.WorkflowExecutionOptionsUpdatedEventAttributes_builder{
		VersioningOverride:          versioningOverride,
		UnsetVersioningOverride:     unsetVersioningOverride,
		AttachedRequestId:           attachRequestID,
		AttachedCompletionCallbacks: attachCompletionCallbacks,
		Identity:                    identity,
		Priority:                    priority,
	}.Build())
	event.SetLinks(links)
	event.SetWorkerMayIgnore(true)
	return event
}

func (b *EventFactory) CreateWorkflowExecutionUpdateAcceptedEvent(
	protocolInstanceID string,
	acceptedRequestMessageId string,
	acceptedRequestSequencingEventId int64,
	acceptedRequest *updatepb.Request,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, b.timeSource.Now())
	event.SetWorkflowExecutionUpdateAcceptedEventAttributes(historypb.WorkflowExecutionUpdateAcceptedEventAttributes_builder{
		ProtocolInstanceId:               protocolInstanceID,
		AcceptedRequestMessageId:         acceptedRequestMessageId,
		AcceptedRequestSequencingEventId: acceptedRequestSequencingEventId,
		AcceptedRequest:                  acceptedRequest,
	}.Build())
	return event
}

func (b *EventFactory) CreateWorkflowExecutionUpdateCompletedEvent(
	acceptedEventID int64,
	updResp *updatepb.Response,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_COMPLETED, b.timeSource.Now())
	event.SetWorkflowExecutionUpdateCompletedEventAttributes(historypb.WorkflowExecutionUpdateCompletedEventAttributes_builder{
		AcceptedEventId: acceptedEventID,
		Meta:            updResp.GetMeta(),
		Outcome:         updResp.GetOutcome(),
	}.Build())
	return event
}

func (b *EventFactory) CreateWorkflowExecutionUpdateAdmittedEvent(request *updatepb.Request, origin enumspb.UpdateAdmittedEventOrigin) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED, b.timeSource.Now())
	event.SetWorkflowExecutionUpdateAdmittedEventAttributes(historypb.WorkflowExecutionUpdateAdmittedEventAttributes_builder{
		Request: request,
		Origin:  origin,
	}.Build())
	return event
}

func (b EventFactory) CreateContinuedAsNewEvent(
	workflowTaskCompletedEventID int64,
	newRunID string,
	command *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, b.timeSource.Now())
	attributes := historypb.WorkflowExecutionContinuedAsNewEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		NewExecutionRunId:            newRunID,
		WorkflowType:                 command.GetWorkflowType(),
		TaskQueue:                    command.GetTaskQueue(),
		Header:                       command.GetHeader(),
		Input:                        command.GetInput(),
		WorkflowRunTimeout:           command.GetWorkflowRunTimeout(),
		WorkflowTaskTimeout:          command.GetWorkflowTaskTimeout(),
		BackoffStartInterval:         command.GetBackoffStartInterval(),
		Initiator:                    command.GetInitiator(),
		Failure:                      command.GetFailure(),
		LastCompletionResult:         command.GetLastCompletionResult(),
		Memo:                         command.GetMemo(),
		SearchAttributes:             command.GetSearchAttributes(),
		InheritBuildId:               command.GetInheritBuildId(),
	}.Build()
	event.SetWorkflowExecutionContinuedAsNewEventAttributes(proto.ValueOrDefault(attributes))
	return event
}

func (b *EventFactory) CreateTimerStartedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartTimerCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_TIMER_STARTED, b.timeSource.Now())
	event.SetTimerStartedEventAttributes(historypb.TimerStartedEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		TimerId:                      command.GetTimerId(),
		StartToFireTimeout:           command.GetStartToFireTimeout(),
	}.Build())
	return event
}

func (b *EventFactory) CreateTimerFiredEvent(startedEventID int64, timerID string) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_TIMER_FIRED, b.timeSource.Now())
	event.SetTimerFiredEventAttributes(historypb.TimerFiredEventAttributes_builder{
		TimerId:        timerID,
		StartedEventId: startedEventID,
	}.Build())
	return event
}

func (b *EventFactory) CreateActivityTaskCancelRequestedEvent(
	workflowTaskCompletedEventID int64,
	scheduledEventID int64,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED, b.timeSource.Now())
	event.SetActivityTaskCancelRequestedEventAttributes(historypb.ActivityTaskCancelRequestedEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		ScheduledEventId:             scheduledEventID,
	}.Build())
	return event
}

func (b *EventFactory) CreateActivityTaskCanceledEvent(
	scheduledEventID int64,
	startedEventID int64,
	latestCancelRequestedEventID int64,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, b.timeSource.Now())
	event.SetActivityTaskCanceledEventAttributes(historypb.ActivityTaskCanceledEventAttributes_builder{
		ScheduledEventId:             scheduledEventID,
		StartedEventId:               startedEventID,
		LatestCancelRequestedEventId: latestCancelRequestedEventID,
		Details:                      details,
		Identity:                     identity,
	}.Build())
	return event
}

func (b *EventFactory) CreateTimerCanceledEvent(
	workflowTaskCompletedEventID int64,
	startedEventID int64,
	timerID string,
	identity string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_TIMER_CANCELED, b.timeSource.Now())
	event.SetTimerCanceledEventAttributes(historypb.TimerCanceledEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		StartedEventId:               startedEventID,
		TimerId:                      timerID,
		Identity:                     identity,
	}.Build())
	return event
}

func (b *EventFactory) CreateWorkflowExecutionCancelRequestedEvent(request *historyservice.RequestCancelWorkflowExecutionRequest) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED, b.timeSource.Now())
	event.SetWorkflowExecutionCancelRequestedEventAttributes(historypb.WorkflowExecutionCancelRequestedEventAttributes_builder{
		Cause:                     request.GetCancelRequest().GetReason(),
		Identity:                  request.GetCancelRequest().GetIdentity(),
		ExternalInitiatedEventId:  request.GetExternalInitiatedEventId(),
		ExternalWorkflowExecution: request.GetExternalWorkflowExecution(),
	}.Build())
	event.SetLinks(request.GetCancelRequest().GetLinks())
	return event
}

func (b *EventFactory) CreateWorkflowExecutionCanceledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CancelWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED, b.timeSource.Now())
	event.SetWorkflowExecutionCanceledEventAttributes(historypb.WorkflowExecutionCanceledEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Details:                      command.GetDetails(),
	}.Build())
	return event
}

func (b *EventFactory) CreateRequestCancelExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		b.timeSource.Now(),
	)
	event.SetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes(historypb.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Namespace:                    command.GetNamespace(),
		NamespaceId:                  targetNamespaceID.String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: command.GetWorkflowId(),
			RunId:      command.GetRunId(),
		}.Build(),
		Control:           command.GetControl(),
		ChildWorkflowOnly: command.GetChildWorkflowOnly(),
		Reason:            command.GetReason(),
	}.Build())
	return event
}

func (b *EventFactory) CreateRequestCancelExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	cause enumspb.CancelExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		b.timeSource.Now(),
	)
	event.SetRequestCancelExternalWorkflowExecutionFailedEventAttributes(historypb.RequestCancelExternalWorkflowExecutionFailedEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		InitiatedEventId:             initiatedEventID,
		Namespace:                    targetNamespace.String(),
		NamespaceId:                  targetNamespaceID.String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: workflowID,
			RunId:      runID,
		}.Build(),
		Cause:   cause,
		Control: "",
	}.Build())
	return event
}

func (b *EventFactory) CreateExternalWorkflowExecutionCancelRequested(
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		b.timeSource.Now(),
	)
	event.SetExternalWorkflowExecutionCancelRequestedEventAttributes(historypb.ExternalWorkflowExecutionCancelRequestedEventAttributes_builder{
		InitiatedEventId: initiatedEventID,
		Namespace:        targetNamespace.String(),
		NamespaceId:      targetNamespaceID.String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: workflowID,
			RunId:      runID,
		}.Build(),
	}.Build())
	return event
}

func (b *EventFactory) CreateSignalExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_INITIATED,
		b.timeSource.Now(),
	)
	event.SetSignalExternalWorkflowExecutionInitiatedEventAttributes(historypb.SignalExternalWorkflowExecutionInitiatedEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Namespace:                    command.GetNamespace(),
		NamespaceId:                  targetNamespaceID.String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: command.GetExecution().GetWorkflowId(),
			RunId:      command.GetExecution().GetRunId(),
		}.Build(),
		SignalName:        command.GetSignalName(),
		Input:             command.GetInput(),
		Control:           command.GetControl(),
		ChildWorkflowOnly: command.GetChildWorkflowOnly(),
		Header:            command.GetHeader(),
	}.Build())
	return event
}

func (b *EventFactory) CreateUpsertWorkflowSearchAttributesEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES, b.timeSource.Now())
	event.SetUpsertWorkflowSearchAttributesEventAttributes(historypb.UpsertWorkflowSearchAttributesEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		SearchAttributes:             command.GetSearchAttributes(),
	}.Build())
	return event
}

func (b *EventFactory) CreateWorkflowPropertiesModifiedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.ModifyWorkflowPropertiesCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED, b.timeSource.Now())
	event.SetWorkflowPropertiesModifiedEventAttributes(historypb.WorkflowPropertiesModifiedEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		UpsertedMemo:                 command.GetUpsertedMemo(),
	}.Build())
	return event
}

func (b *EventFactory) CreateSignalExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	control string,
	cause enumspb.SignalExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.SetSignalExternalWorkflowExecutionFailedEventAttributes(historypb.SignalExternalWorkflowExecutionFailedEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		InitiatedEventId:             initiatedEventID,
		Namespace:                    targetNamespace.String(),
		NamespaceId:                  targetNamespaceID.String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: workflowID,
			RunId:      runID,
		}.Build(),
		Cause:   cause,
		Control: control,
	}.Build())
	return event
}

func (b *EventFactory) CreateExternalWorkflowExecutionSignaled(
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	control string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED, b.timeSource.Now())
	event.SetExternalWorkflowExecutionSignaledEventAttributes(historypb.ExternalWorkflowExecutionSignaledEventAttributes_builder{
		InitiatedEventId: initiatedEventID,
		Namespace:        targetNamespace.String(),
		NamespaceId:      targetNamespaceID.String(),
		WorkflowExecution: commonpb.WorkflowExecution_builder{
			WorkflowId: workflowID,
			RunId:      runID,
		}.Build(),
		Control: control,
	}.Build())
	return event
}

func (b *EventFactory) CreateMarkerRecordedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.RecordMarkerCommandAttributes,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_MARKER_RECORDED, b.timeSource.Now())
	event.SetMarkerRecordedEventAttributes(historypb.MarkerRecordedEventAttributes_builder{
		MarkerName:                   command.GetMarkerName(),
		Details:                      command.GetDetails(),
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Header:                       command.GetHeader(),
		Failure:                      command.GetFailure(),
	}.Build())
	return event
}

func (b *EventFactory) CreateWorkflowExecutionSignaledEvent(
	signalName string,
	input *commonpb.Payloads,
	identity string,
	header *commonpb.Header,
	externalWorkflowExecution *commonpb.WorkflowExecution,
	links []*commonpb.Link,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, b.timeSource.Now())
	event.SetWorkflowExecutionSignaledEventAttributes(historypb.WorkflowExecutionSignaledEventAttributes_builder{
		SignalName:                signalName,
		Input:                     input,
		Identity:                  identity,
		Header:                    header,
		ExternalWorkflowExecution: externalWorkflowExecution,
	}.Build())
	event.SetLinks(links)
	return event
}

func (b *EventFactory) CreateStartChildWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartChildWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED, b.timeSource.Now())
	event.SetStartChildWorkflowExecutionInitiatedEventAttributes(historypb.StartChildWorkflowExecutionInitiatedEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		Namespace:                    command.GetNamespace(),
		NamespaceId:                  targetNamespaceID.String(),
		WorkflowId:                   command.GetWorkflowId(),
		WorkflowType:                 command.GetWorkflowType(),
		TaskQueue:                    command.GetTaskQueue(),
		Header:                       command.GetHeader(),
		Input:                        command.GetInput(),
		WorkflowExecutionTimeout:     command.GetWorkflowExecutionTimeout(),
		WorkflowRunTimeout:           command.GetWorkflowRunTimeout(),
		WorkflowTaskTimeout:          command.GetWorkflowTaskTimeout(),
		Control:                      command.GetControl(),
		WorkflowIdReusePolicy:        command.GetWorkflowIdReusePolicy(),
		RetryPolicy:                  command.GetRetryPolicy(),
		CronSchedule:                 command.GetCronSchedule(),
		Memo:                         command.GetMemo(),
		SearchAttributes:             command.GetSearchAttributes(),
		ParentClosePolicy:            command.GetParentClosePolicy(),
		InheritBuildId:               command.GetInheritBuildId(),
		Priority:                     command.GetPriority(),
	}.Build())
	return event
}

func (b *EventFactory) CreateChildWorkflowExecutionStartedEvent(
	initiatedID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, b.timeSource.Now())
	event.SetChildWorkflowExecutionStartedEventAttributes(historypb.ChildWorkflowExecutionStartedEventAttributes_builder{
		InitiatedEventId:  initiatedID,
		Namespace:         targetNamespace.String(),
		NamespaceId:       targetNamespaceID.String(),
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		Header:            header,
	}.Build())
	return event
}

func (b *EventFactory) CreateStartChildWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedID int64,
	cause enumspb.StartChildWorkflowExecutionFailedCause,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	workflowType *commonpb.WorkflowType,
	control string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.SetStartChildWorkflowExecutionFailedEventAttributes(historypb.StartChildWorkflowExecutionFailedEventAttributes_builder{
		WorkflowTaskCompletedEventId: workflowTaskCompletedEventID,
		InitiatedEventId:             initiatedID,
		Namespace:                    targetNamespace.String(),
		NamespaceId:                  targetNamespaceID.String(),
		WorkflowId:                   workflowID,
		WorkflowType:                 workflowType,
		Control:                      control,
		Cause:                        cause,
	}.Build())
	return event
}

func (b *EventFactory) CreateChildWorkflowExecutionCompletedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	result *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED, b.timeSource.Now())
	event.SetChildWorkflowExecutionCompletedEventAttributes(historypb.ChildWorkflowExecutionCompletedEventAttributes_builder{
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedEventID,
		Namespace:         targetNamespace.String(),
		NamespaceId:       targetNamespaceID.String(),
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		Result:            result,
	}.Build())
	return event
}

func (b *EventFactory) CreateChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED, b.timeSource.Now())
	event.SetChildWorkflowExecutionFailedEventAttributes(historypb.ChildWorkflowExecutionFailedEventAttributes_builder{
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedEventID,
		Namespace:         targetNamespace.String(),
		NamespaceId:       targetNamespaceID.String(),
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		Failure:           failure,
		RetryState:        retryState,
	}.Build())
	return event
}

func (b *EventFactory) CreateChildWorkflowExecutionCanceledEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	details *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED, b.timeSource.Now())
	event.SetChildWorkflowExecutionCanceledEventAttributes(historypb.ChildWorkflowExecutionCanceledEventAttributes_builder{
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedEventID,
		Namespace:         targetNamespace.String(),
		NamespaceId:       targetNamespaceID.String(),
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		Details:           details,
	}.Build())
	return event
}

func (b *EventFactory) CreateChildWorkflowExecutionTerminatedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED, b.timeSource.Now())
	event.SetChildWorkflowExecutionTerminatedEventAttributes(historypb.ChildWorkflowExecutionTerminatedEventAttributes_builder{
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedEventID,
		Namespace:         targetNamespace.String(),
		NamespaceId:       targetNamespaceID.String(),
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
	}.Build())
	return event
}

func (b *EventFactory) CreateChildWorkflowExecutionTimedOutEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT, b.timeSource.Now())
	event.SetChildWorkflowExecutionTimedOutEventAttributes(historypb.ChildWorkflowExecutionTimedOutEventAttributes_builder{
		InitiatedEventId:  initiatedID,
		StartedEventId:    startedEventID,
		Namespace:         targetNamespace.String(),
		NamespaceId:       targetNamespaceID.String(),
		WorkflowExecution: execution,
		WorkflowType:      workflowType,
		RetryState:        retryState,
	}.Build())
	return event
}

func (b *EventFactory) CreateWorkflowExecutionPausedEvent(
	identity string,
	reason string,
	requestID string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED, b.timeSource.Now())
	event.SetWorkflowExecutionPausedEventAttributes(historypb.WorkflowExecutionPausedEventAttributes_builder{
		Identity:  identity,
		Reason:    reason,
		RequestId: requestID,
	}.Build())
	return event
}

func (b *EventFactory) CreateWorkflowExecutionUnpausedEvent(
	identity string,
	reason string,
	requestID string,
) *historypb.HistoryEvent {
	event := b.createHistoryEvent(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED, b.timeSource.Now())
	event.SetWorkflowExecutionUnpausedEventAttributes(historypb.WorkflowExecutionUnpausedEventAttributes_builder{
		Identity:  identity,
		Reason:    reason,
		RequestId: requestID,
	}.Build())
	return event
}

func (b *EventFactory) createHistoryEvent(
	eventType enumspb.EventType,
	time time.Time,
) *historypb.HistoryEvent {
	historyEvent := &historypb.HistoryEvent{}
	historyEvent.SetEventTime(timestamppb.New(time.UTC()))
	historyEvent.SetEventType(eventType)
	historyEvent.SetVersion(b.version)
	historyEvent.SetTaskId(common.EmptyEventTaskID)

	return historyEvent
}
