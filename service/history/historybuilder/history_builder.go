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
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	HistoryBuilderStateMutable HistoryBuilderState = iota
	HistoryBuilderStateImmutable
	HistoryBuilderStateSealed
)

type (
	HistoryBuilder struct {
		EventStore
		EventFactory
	}

	HistoryBuilderState int

	HistoryMutation struct {
		// events to be persisted to events table
		DBEventsBatches [][]*historypb.HistoryEvent
		// events to be buffer in execution table
		DBBufferBatch []*historypb.HistoryEvent
		// whether to clear buffer events on DB
		DBClearBuffer bool
		// accumulated buffered events, equal to all buffer events from execution table
		MemBufferBatch []*historypb.HistoryEvent
		// scheduled to started event ID mapping for flushed buffered event
		ScheduledIDToStartedID map[int64]int64
		// request id to event ID mapping for flushed buffered event
		RequestIDToEventID map[string]int64
	}

	TaskIDGenerator func(number int) ([]int64, error)

	BufferedEventFilter func(*historypb.HistoryEvent) bool
)

func New(
	timeSource clock.TimeSource,
	taskIDGenerator TaskIDGenerator,
	version int64,
	nextEventID int64,
	dbBufferBatch []*historypb.HistoryEvent,
	metricsHandler metrics.Handler,
) *HistoryBuilder {
	return &HistoryBuilder{
		EventStore: EventStore{
			state:           HistoryBuilderStateMutable,
			timeSource:      timeSource,
			taskIDGenerator: taskIDGenerator,

			version:     version,
			nextEventID: nextEventID,

			workflowFinished: false,

			dbBufferBatch:          dbBufferBatch,
			dbClearBuffer:          false,
			memEventsBatches:       nil,
			memLatestBatch:         nil,
			memBufferBatch:         nil,
			scheduledIDToStartedID: make(map[int64]int64),
			requestIDToEventID:     make(map[string]int64),

			metricsHandler: metricsHandler,
		},
		EventFactory: EventFactory{timeSource: timeSource, version: version},
	}
}

func NewImmutable(histories ...[]*historypb.HistoryEvent) *HistoryBuilder {
	lastHistory := histories[len(histories)-1]
	lastEvent := lastHistory[len(lastHistory)-1]
	return &HistoryBuilder{
		EventStore: EventStore{
			state:           HistoryBuilderStateImmutable,
			timeSource:      nil,
			taskIDGenerator: nil,

			version:     lastEvent.GetVersion(),
			nextEventID: lastEvent.GetEventId() + 1,

			workflowFinished: false,

			dbBufferBatch:          nil,
			dbClearBuffer:          false,
			memEventsBatches:       histories,
			memLatestBatch:         nil,
			memBufferBatch:         nil,
			scheduledIDToStartedID: nil,
			requestIDToEventID:     nil,

			metricsHandler: nil,
		},
		EventFactory: EventFactory{},
	}
}

func NewImmutableForUpdateNextEventID(lastVersionHistoryItem *historyspb.VersionHistoryItem) *HistoryBuilder {
	return &HistoryBuilder{
		EventStore: EventStore{
			state:           HistoryBuilderStateImmutable,
			timeSource:      nil,
			taskIDGenerator: nil,

			version:     lastVersionHistoryItem.GetVersion(),
			nextEventID: lastVersionHistoryItem.GetEventId() + 1,

			workflowFinished: false,

			dbBufferBatch:          nil,
			dbClearBuffer:          false,
			memEventsBatches:       nil,
			memLatestBatch:         nil,
			memBufferBatch:         nil,
			scheduledIDToStartedID: nil,
			requestIDToEventID:     nil,

			metricsHandler: nil,
		},
		EventFactory: EventFactory{},
	}
}

func (b *HistoryBuilder) IsDirty() bool {
	return b.EventStore.IsDirty()
}

// AddWorkflowExecutionStartedEvent
// firstInChainRunID is the runID of the first run in a workflow chain (continueAsNew, cron & workflow retry)
// originalRunID is the base workflow's runID upon workflow reset. If the current run is the base (i.e. no reset),
// then originalRunID is current run's runID.
func (b *HistoryBuilder) AddWorkflowExecutionStartedEvent(
	startTime time.Time,
	request *historyservice.StartWorkflowExecutionRequest,
	resetPoints *workflowpb.ResetPoints,
	prevRunID string,
	firstInChainRunID string,
	originalRunID string,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateWorkflowExecutionStartedEvent(
		startTime,
		request,
		resetPoints,
		prevRunID,
		firstInChainRunID,
		originalRunID,
	)
	if request.StartRequest.GetUserMetadata() != nil {
		event.UserMetadata = request.StartRequest.GetUserMetadata()
	}
	if len(request.StartRequest.GetLinks()) > 0 {
		event.Links = request.StartRequest.GetLinks()
	}
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowTaskScheduledEvent(
	taskQueue *taskqueuepb.TaskQueue,
	startToCloseTimeout *durationpb.Duration,
	attempt int32,
	scheduleTime time.Time,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateWorkflowTaskScheduledEvent(taskQueue, startToCloseTimeout, attempt, scheduleTime)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowTaskStartedEvent(
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
	event := b.EventFactory.CreateWorkflowTaskStartedEvent(
		scheduledEventID,
		requestID,
		identity,
		startTime,
		suggestContinueAsNew,
		historySizeBytes,
		versioningStamp,
		buildIdRedirectCounter,
		suggestContinueAsNewReasons,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowTaskCompletedEvent(
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
	event := b.EventFactory.CreateWorkflowTaskCompletedEvent(
		scheduledEventID,
		startedEventID,
		identity,
		checksum,
		workerVersionStamp,
		sdkMetadata,
		meteringMetadata,
		deploymentName,
		deployment,
		behavior,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowTaskTimedOutEvent(
	scheduledEventID int64,
	startedEventID int64,
	timeoutType enumspb.TimeoutType,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateWorkflowTaskTimedOutEvent(scheduledEventID, startedEventID, timeoutType)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowTaskFailedEvent(
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
	event := b.EventFactory.CreateWorkflowTaskFailedEvent(
		scheduledEventID,
		startedEventID,
		cause,
		failure,
		identity,
		baseRunID,
		newRunID,
		forkEventVersion,
		checksum,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionPausedEvent(
	identity string,
	reason string,
	requestID string,
) *historypb.HistoryEvent {
	event := b.CreateWorkflowExecutionPausedEvent(identity, reason, requestID)
	// Mark the event as 'worker may ignore' so that older SDKs can safely ignore it.
	event.WorkerMayIgnore = true
	event, _ = b.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionUnpausedEvent(
	identity string,
	reason string,
	requestID string,
) *historypb.HistoryEvent {
	event := b.CreateWorkflowExecutionUnpausedEvent(identity, reason, requestID)
	// Mark the event as 'worker may ignore' so that older SDKs can safely ignore it.
	event.WorkerMayIgnore = true
	event, _ = b.add(event)
	return event
}

func (b *HistoryBuilder) AddActivityTaskScheduledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.ScheduleActivityTaskCommandAttributes,
	ns namespace.Name,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateActivityTaskScheduledEvent(workflowTaskCompletedEventID, command)
	event, _ = b.EventStore.add(event)

	if payloadSize := command.Input.Size(); payloadSize > 0 {
		b.metricsHandler.Counter(metrics.ActivityPayloadSize.Name()).Record(
			int64(payloadSize),
			metrics.OperationTag(metrics.HistoryRecordActivityTaskStartedScope),
			metrics.NamespaceTag(ns.String()))
	}

	return event
}

func (b *HistoryBuilder) AddActivityTaskStartedEvent(
	scheduledEventID int64,
	attempt int32,
	requestID string,
	identity string,
	lastFailure *failurepb.Failure,
	versioningStamp *commonpb.WorkerVersionStamp,
	redirectCounter int64,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateActivityTaskStartedEvent(scheduledEventID, attempt, requestID, identity, lastFailure, versioningStamp, redirectCounter)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddActivityTaskCompletedEvent(
	scheduledEventID int64,
	startedEventID int64,
	identity string,
	result *commonpb.Payloads,
	ns namespace.Name,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateActivityTaskCompletedEvent(scheduledEventID, startedEventID, identity, result)
	event, _ = b.EventStore.add(event)

	if payloadSize := result.Size(); payloadSize > 0 {
		b.metricsHandler.Counter(metrics.ActivityPayloadSize.Name()).Record(
			int64(payloadSize),
			metrics.OperationTag(metrics.HistoryRespondActivityTaskCompletedScope),
			metrics.NamespaceTag(ns.String()))
	}

	return event
}

func (b *HistoryBuilder) AddActivityTaskFailedEvent(
	scheduledEventID int64,
	startedEventID int64,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
	identity string,
	ns namespace.Name,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateActivityTaskFailedEvent(
		scheduledEventID,
		startedEventID,
		failure,
		retryState,
		identity,
	)

	event, _ = b.EventStore.add(event)

	if payloadSize := failure.Size(); payloadSize > 0 {
		b.metricsHandler.Counter(metrics.ActivityPayloadSize.Name()).Record(
			int64(payloadSize),
			metrics.OperationTag(metrics.HistoryRespondActivityTaskFailedScope),
			metrics.NamespaceTag(ns.String()))
	}

	return event
}

func (b *HistoryBuilder) AddActivityTaskTimedOutEvent(
	scheduledEventID,
	startedEventID int64,
	timeoutFailure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateActivityTaskTimedOutEvent(
		scheduledEventID,
		startedEventID,
		timeoutFailure,
		retryState,
	)

	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddCompletedWorkflowEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CompleteWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateCompletedWorkflowEvent(workflowTaskCompletedEventID, command, newExecutionRunID)

	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddFailWorkflowEvent(
	workflowTaskCompletedEventID int64,
	retryState enumspb.RetryState,
	command *commandpb.FailWorkflowExecutionCommandAttributes,
	newExecutionRunID string,
) (*historypb.HistoryEvent, int64) {
	event := b.EventFactory.CreateFailWorkflowEvent(
		workflowTaskCompletedEventID,
		retryState,
		command,
		newExecutionRunID,
	)

	return b.EventStore.add(event)
}

func (b *HistoryBuilder) AddTimeoutWorkflowEvent(
	retryState enumspb.RetryState,
	newExecutionRunID string,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateTimeoutWorkflowEvent(retryState, newExecutionRunID)

	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionTerminatedEvent(
	reason string,
	details *commonpb.Payloads,
	identity string,
	links []*commonpb.Link,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateWorkflowExecutionTerminatedEvent(reason, details, identity, links)

	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionOptionsUpdatedEvent(
	versioningOverride *workflowpb.VersioningOverride,
	unsetVersioningOverride bool,
	attachRequestID string,
	attachCompletionCallbacks []*commonpb.Callback,
	links []*commonpb.Link,
	identity string,
	priority *commonpb.Priority,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateWorkflowExecutionOptionsUpdatedEvent(
		worker_versioning.ConvertOverrideToV32(versioningOverride),
		unsetVersioningOverride,
		attachRequestID,
		attachCompletionCallbacks,
		links,
		identity,
		priority,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionUpdateAcceptedEvent(
	protocolInstanceID string,
	acceptedRequestMessageId string,
	acceptedRequestSequencingEventId int64,
	acceptedRequest *updatepb.Request,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateWorkflowExecutionUpdateAcceptedEvent(
		protocolInstanceID,
		acceptedRequestMessageId,
		acceptedRequestSequencingEventId,
		acceptedRequest,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionUpdateCompletedEvent(
	acceptedEventID int64,
	updResp *updatepb.Response,
) (*historypb.HistoryEvent, int64) {
	event := b.EventFactory.CreateWorkflowExecutionUpdateCompletedEvent(acceptedEventID, updResp)
	return b.EventStore.add(event)
}

func (b *HistoryBuilder) AddWorkflowExecutionUpdateAdmittedEvent(request *updatepb.Request, origin enumspb.UpdateAdmittedEventOrigin) (*historypb.HistoryEvent, int64) {
	event := b.EventFactory.CreateWorkflowExecutionUpdateAdmittedEvent(request, origin)
	return b.EventStore.add(event)
}

func (b *HistoryBuilder) AddContinuedAsNewEvent(
	workflowTaskCompletedEventID int64,
	newRunID string,
	command *commandpb.ContinueAsNewWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateContinuedAsNewEvent(workflowTaskCompletedEventID, newRunID, command)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddTimerStartedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartTimerCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateTimerStartedEvent(workflowTaskCompletedEventID, command)

	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddTimerFiredEvent(
	startedEventID int64,
	timerID string,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateTimerFiredEvent(startedEventID, timerID)

	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddActivityTaskCancelRequestedEvent(
	workflowTaskCompletedEventID int64,
	scheduledEventID int64,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateActivityTaskCancelRequestedEvent(workflowTaskCompletedEventID, scheduledEventID)

	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddActivityTaskCanceledEvent(
	scheduledEventID int64,
	startedEventID int64,
	latestCancelRequestedEventID int64,
	details *commonpb.Payloads,
	identity string,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateActivityTaskCanceledEvent(
		scheduledEventID,
		startedEventID,
		latestCancelRequestedEventID,
		details,
		identity,
	)

	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddTimerCanceledEvent(
	workflowTaskCompletedEventID int64,
	startedEventID int64,
	timerID string,
	identity string,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateTimerCanceledEvent(workflowTaskCompletedEventID, startedEventID, timerID, identity)

	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionCancelRequestedEvent(
	request *historyservice.RequestCancelWorkflowExecutionRequest,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateWorkflowExecutionCancelRequestedEvent(request)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionCanceledEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.CancelWorkflowExecutionCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateWorkflowExecutionCanceledEvent(workflowTaskCompletedEventID, command)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddRequestCancelExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.RequestCancelExternalWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateRequestCancelExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		command,
		targetNamespaceID,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddRequestCancelExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	cause enumspb.CancelExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateRequestCancelExternalWorkflowExecutionFailedEvent(
		workflowTaskCompletedEventID,
		initiatedEventID,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		runID,
		cause,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddExternalWorkflowExecutionCancelRequested(
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateExternalWorkflowExecutionCancelRequested(
		initiatedEventID,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		runID,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddSignalExternalWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.SignalExternalWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateSignalExternalWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		command,
		targetNamespaceID,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddUpsertWorkflowSearchAttributesEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.UpsertWorkflowSearchAttributesCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateUpsertWorkflowSearchAttributesEvent(workflowTaskCompletedEventID, command)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowPropertiesModifiedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.ModifyWorkflowPropertiesCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateWorkflowPropertiesModifiedEvent(workflowTaskCompletedEventID, command)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddSignalExternalWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	control string,
	cause enumspb.SignalExternalWorkflowExecutionFailedCause,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateSignalExternalWorkflowExecutionFailedEvent(
		workflowTaskCompletedEventID,
		initiatedEventID,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		runID,
		control,
		cause,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddExternalWorkflowExecutionSignaled(
	initiatedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	runID string,
	control string,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateExternalWorkflowExecutionSignaled(
		initiatedEventID,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		runID,
		control,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddMarkerRecordedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.RecordMarkerCommandAttributes,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateMarkerRecordedEvent(workflowTaskCompletedEventID, command)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddWorkflowExecutionSignaledEvent(
	signalName string,
	input *commonpb.Payloads,
	identity string,
	header *commonpb.Header,
	externalWorkflowExecution *commonpb.WorkflowExecution,
	links []*commonpb.Link,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateWorkflowExecutionSignaledEvent(
		signalName,
		input,
		identity,
		header,
		externalWorkflowExecution,
		links,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddStartChildWorkflowExecutionInitiatedEvent(
	workflowTaskCompletedEventID int64,
	command *commandpb.StartChildWorkflowExecutionCommandAttributes,
	targetNamespaceID namespace.ID,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateStartChildWorkflowExecutionInitiatedEvent(
		workflowTaskCompletedEventID,
		command,
		targetNamespaceID,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddChildWorkflowExecutionStartedEvent(
	initiatedID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	header *commonpb.Header,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateChildWorkflowExecutionStartedEvent(
		initiatedID,
		targetNamespace,
		targetNamespaceID,
		execution,
		workflowType,
		header,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddChildWorkflowExecutionFailedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	failure *failurepb.Failure,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateChildWorkflowExecutionFailedEvent(
		initiatedID,
		startedEventID,
		targetNamespace,
		targetNamespaceID,
		execution,
		workflowType,
		failure,
		retryState,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddChildWorkflowExecutionCompletedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	result *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateChildWorkflowExecutionCompletedEvent(
		initiatedID,
		startedEventID,
		targetNamespace,
		targetNamespaceID,
		execution,
		workflowType,
		result,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddStartChildWorkflowExecutionFailedEvent(
	workflowTaskCompletedEventID int64,
	initiatedID int64,
	cause enumspb.StartChildWorkflowExecutionFailedCause,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	workflowID string,
	workflowType *commonpb.WorkflowType,
	control string,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateStartChildWorkflowExecutionFailedEvent(
		workflowTaskCompletedEventID,
		initiatedID,
		cause,
		targetNamespace,
		targetNamespaceID,
		workflowID,
		workflowType,
		control,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddChildWorkflowExecutionCanceledEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	details *commonpb.Payloads,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateChildWorkflowExecutionCanceledEvent(
		initiatedID,
		startedEventID,
		targetNamespace,
		targetNamespaceID,
		execution,
		workflowType,
		details,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddChildWorkflowExecutionTerminatedEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateChildWorkflowExecutionTerminatedEvent(
		initiatedID,
		startedEventID,
		targetNamespace,
		targetNamespaceID,
		execution,
		workflowType,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddChildWorkflowExecutionTimedOutEvent(
	initiatedID int64,
	startedEventID int64,
	targetNamespace namespace.Name,
	targetNamespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	workflowType *commonpb.WorkflowType,
	retryState enumspb.RetryState,
) *historypb.HistoryEvent {
	event := b.EventFactory.CreateChildWorkflowExecutionTimedOutEvent(
		initiatedID,
		startedEventID,
		targetNamespace,
		targetNamespaceID,
		execution,
		workflowType,
		retryState,
	)
	event, _ = b.EventStore.add(event)
	return event
}

func (b *HistoryBuilder) AddHistoryEvent(
	eventType enumspb.EventType,
	setAttributes func(*historypb.HistoryEvent),
) *historypb.HistoryEvent {
	event := b.EventFactory.createHistoryEvent(eventType, b.EventFactory.timeSource.Now())
	setAttributes(event)
	event, _ = b.EventStore.add(event)
	return event
}
