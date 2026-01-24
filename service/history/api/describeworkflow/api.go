package describeworkflow

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/sony/gobreaker"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	chasmcallback "go.temporal.io/server/chasm/lib/callback"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/circuitbreakerpool"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	errCallbackStateUnspecified       = errors.New("callback with UNSPECIFIED state")
	errNexusOperationStateUnspecified = errors.New("Nexus operation with UNSPECIFIED state")
)

func clonePayloadMap(source map[string]*commonpb.Payload) map[string]*commonpb.Payload {
	target := make(map[string]*commonpb.Payload, len(source))
	for k, v := range source {
		metadata := make(map[string][]byte, len(v.GetMetadata()))
		for mk, mv := range v.GetMetadata() {
			metadata[mk] = mv
		}
		target[k] = commonpb.Payload_builder{
			Metadata: metadata,
			Data:     v.GetData(),
		}.Build()
	}
	return target
}

func Invoke(
	ctx context.Context,
	req *historyservice.DescribeWorkflowExecutionRequest,
	shard historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	persistenceVisibilityMgr manager.VisibilityManager,
	outboundQueueCBPool *circuitbreakerpool.OutboundQueueCircuitBreakerPool,
) (_ *historyservice.DescribeWorkflowExecutionResponse, retError error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}

	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(
			req.GetNamespaceId(),
			req.GetRequest().GetExecution().GetWorkflowId(),
			req.GetRequest().GetExecution().GetRunId(),
		),
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	// We release the lock on this workflow just before we return from this method, at which point mutable state might
	// be mutated. Take extra care to clone all response methods as marshalling happens after we return and it is unsafe
	// to mutate proto fields during marshalling.
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	mutableState := workflowLease.GetMutableState()
	namespaceName := mutableState.GetNamespaceEntry().Name().String()
	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()

	// fetch the start event to get the associated user metadata.
	startEvent, err := mutableState.GetStartEvent(ctx)
	if err != nil {
		return nil, err
	}
	result := historyservice.DescribeWorkflowExecutionResponse_builder{
		ExecutionConfig: workflowpb.WorkflowExecutionConfig_builder{
			TaskQueue: taskqueuepb.TaskQueue_builder{
				Name: executionInfo.GetTaskQueue(),
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			}.Build(),
			WorkflowExecutionTimeout:   executionInfo.GetWorkflowExecutionTimeout(),
			WorkflowRunTimeout:         executionInfo.GetWorkflowRunTimeout(),
			DefaultWorkflowTaskTimeout: executionInfo.GetDefaultWorkflowTaskTimeout(),
			UserMetadata:               startEvent.GetUserMetadata(),
		}.Build(),
		WorkflowExecutionInfo: workflowpb.WorkflowExecutionInfo_builder{
			Execution: commonpb.WorkflowExecution_builder{
				WorkflowId: executionInfo.GetWorkflowId(),
				RunId:      executionState.GetRunId(),
			}.Build(),
			Type:          commonpb.WorkflowType_builder{Name: executionInfo.GetWorkflowTypeName()}.Build(),
			StartTime:     executionState.GetStartTime(),
			Status:        executionState.GetStatus(),
			HistoryLength: mutableState.GetNextEventID() - common.FirstEventID,
			ExecutionTime: executionInfo.GetExecutionTime(),
			// Memo and SearchAttributes are set below
			AutoResetPoints:      common.CloneProto(executionInfo.GetAutoResetPoints()),
			TaskQueue:            executionInfo.GetTaskQueue(),
			StateTransitionCount: executionInfo.GetStateTransitionCount(),
			HistorySizeBytes:     executionInfo.GetExecutionStats().GetHistorySize(),
			RootExecution: commonpb.WorkflowExecution_builder{
				WorkflowId: executionInfo.GetRootWorkflowId(),
				RunId:      executionInfo.GetRootRunId(),
			}.Build(),

			MostRecentWorkerVersionStamp: executionInfo.GetMostRecentWorkerVersionStamp(),
			AssignedBuildId:              executionInfo.GetAssignedBuildId(),
			InheritedBuildId:             executionInfo.GetInheritedBuildId(),
			FirstRunId:                   executionInfo.GetFirstExecutionRunId(),
			VersioningInfo:               common.CloneProto(executionInfo.GetVersioningInfo()),
			WorkerDeploymentName:         executionInfo.GetWorkerDeploymentName(),
			Priority:                     executionInfo.GetPriority(),
		}.Build(),
		WorkflowExtendedInfo: workflowpb.WorkflowExecutionExtendedInfo_builder{
			ExecutionExpirationTime: executionInfo.GetWorkflowExecutionExpirationTime(),
			RunExpirationTime:       executionInfo.GetWorkflowRunExpirationTime(),
			OriginalStartTime:       startEvent.GetEventTime(),
			CancelRequested:         executionInfo.GetCancelRequested(),
			ResetRunId:              executionInfo.GetResetRunId(),
			RequestIdInfos:          make(map[string]*workflowpb.RequestIdInfo),
		}.Build(),
	}.Build()

	// copy pause info to the response if it exists
	if executionInfo.HasPauseInfo() {
		result.GetWorkflowExtendedInfo().SetPauseInfo(workflowpb.WorkflowExecutionPauseInfo_builder{
			PausedTime: executionInfo.GetPauseInfo().GetPauseTime(),
			Identity:   executionInfo.GetPauseInfo().GetIdentity(),
			Reason:     executionInfo.GetPauseInfo().GetReason(),
		}.Build())
	}

	if mutableState.IsResetRun() {
		result.GetWorkflowExtendedInfo().SetLastResetTime(executionState.GetStartTime())
	}

	if shard.GetConfig().ExternalPayloadsEnabled(namespaceName) {
		executionStats := executionInfo.GetExecutionStats()
		result.GetWorkflowExecutionInfo().SetExternalPayloadSizeBytes(executionStats.GetExternalPayloadSize())
		result.GetWorkflowExecutionInfo().SetExternalPayloadCount(executionStats.GetExternalPayloadCount())
	}

	for requestID, requestIDInfo := range mutableState.GetExecutionState().GetRequestIds() {
		info := workflowpb.RequestIdInfo_builder{
			EventType: requestIDInfo.GetEventType(),
			Buffered:  requestIDInfo.GetEventId() == common.BufferedEventID,
		}.Build()
		if !info.GetBuffered() {
			info.SetEventId(requestIDInfo.GetEventId())
		}
		result.GetWorkflowExtendedInfo().GetRequestIdInfos()[requestID] = info
	}

	if executionInfo.GetParentRunId() != "" {
		result.GetWorkflowExecutionInfo().SetParentExecution(commonpb.WorkflowExecution_builder{
			WorkflowId: executionInfo.GetParentWorkflowId(),
			RunId:      executionInfo.GetParentRunId(),
		}.Build())
		result.GetWorkflowExecutionInfo().SetParentNamespaceId(executionInfo.GetParentNamespaceId())
	}
	if executionState.GetState() == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// for closed workflow
		result.GetWorkflowExecutionInfo().SetStatus(executionState.GetStatus())
		closeTime, err := mutableState.GetWorkflowCloseTime(ctx)
		if err != nil {
			return nil, err
		}
		executionDuration, err := mutableState.GetWorkflowExecutionDuration(ctx)
		if err != nil {
			return nil, err
		}
		result.GetWorkflowExecutionInfo().SetCloseTime(timestamppb.New(closeTime))
		result.GetWorkflowExecutionInfo().SetExecutionDuration(durationpb.New(executionDuration))
	}

	for _, ai := range mutableState.GetPendingActivityInfos() {
		p, err := workflow.GetPendingActivityInfo(ctx, shard, mutableState, ai)
		if err != nil {
			return nil, err
		}

		result.SetPendingActivities(append(result.GetPendingActivities(), p))
	}

	for _, ch := range mutableState.GetPendingChildExecutionInfos() {
		p := workflowpb.PendingChildExecutionInfo_builder{
			WorkflowId:        ch.GetStartedWorkflowId(),
			RunId:             ch.GetStartedRunId(),
			WorkflowTypeName:  ch.GetWorkflowTypeName(),
			InitiatedId:       ch.GetInitiatedEventId(),
			ParentClosePolicy: ch.GetParentClosePolicy(),
		}.Build()
		result.SetPendingChildren(append(result.GetPendingChildren(), p))
	}

	if pendingWorkflowTask := mutableState.GetPendingWorkflowTask(); pendingWorkflowTask != nil {
		result.SetPendingWorkflowTask(workflowpb.PendingWorkflowTaskInfo_builder{
			State:                 enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED,
			ScheduledTime:         timestamppb.New(pendingWorkflowTask.ScheduledTime),
			OriginalScheduledTime: timestamppb.New(pendingWorkflowTask.OriginalScheduledTime),
			Attempt:               pendingWorkflowTask.Attempt,
		}.Build())
		if pendingWorkflowTask.StartedEventID != common.EmptyEventID {
			result.GetPendingWorkflowTask().SetState(enumspb.PENDING_WORKFLOW_TASK_STATE_STARTED)
			result.GetPendingWorkflowTask().SetStartedTime(timestamppb.New(pendingWorkflowTask.StartedTime))
		}
	}

	relocatableAttrsFetcher := workflow.RelocatableAttributesFetcherProvider(
		shard.GetConfig(),
		persistenceVisibilityMgr,
	)
	relocatableAttributes, err := relocatableAttrsFetcher.Fetch(ctx, mutableState)
	if err != nil {
		shard.GetLogger().Error(
			"Failed to fetch relocatable attributes",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(executionInfo.GetWorkflowId()),
			tag.WorkflowRunID(executionState.GetRunId()),
			tag.Error(err),
		)
		return nil, serviceerror.NewInternal("Failed to fetch memo and search attributes")
	}
	result.GetWorkflowExecutionInfo().SetMemo(commonpb.Memo_builder{
		Fields: clonePayloadMap(relocatableAttributes.Memo.GetFields()),
	}.Build())
	result.GetWorkflowExecutionInfo().SetSearchAttributes(commonpb.SearchAttributes_builder{
		IndexedFields: clonePayloadMap(relocatableAttributes.SearchAttributes.GetIndexedFields()),
	}.Build())

	// Check for CHASM callbacks (regardless of feature flag setting)
	// Only process CHASM callbacks if we have an actual chasm.Node (not a noopChasmTree)
	if mutableState.ChasmEnabled() {
		chasmCallbackInfos, err := buildCallbackInfosFromChasm(
			ctx,
			namespaceID,
			mutableState,
			executionInfo,
			executionState,
			outboundQueueCBPool,
			shard.GetLogger(),
		)
		if err != nil {
			return nil, err
		}
		result.SetCallbacks(append(result.GetCallbacks(), chasmCallbackInfos...))
	}

	// Check for HSM callbacks
	hsmCallbackInfos, err := buildCallbackInfosFromHSM(
		namespaceID,
		mutableState,
		executionInfo,
		executionState,
		outboundQueueCBPool,
		shard.GetLogger(),
	)
	if err != nil {
		return nil, err
	}
	result.SetCallbacks(append(result.GetCallbacks(), hsmCallbackInfos...))

	opColl := nexusoperations.MachineCollection(mutableState.HSM())
	ops := opColl.List()
	result.SetPendingNexusOperations(make([]*workflowpb.PendingNexusOperationInfo, 0, len(ops)))
	for _, node := range ops {
		op, err := opColl.Data(node.Key.ID)
		if err != nil {
			shard.GetLogger().Error(
				"failed to load Nexus operation data while building describe response",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.GetWorkflowId()),
				tag.WorkflowRunID(executionState.GetRunId()),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
		}

		operationInfo, err := buildPendingNexusOperationInfo(namespaceID, node, op, outboundQueueCBPool)
		if err != nil {
			shard.GetLogger().Error(
				"failed to build Nexus operation info while building describe response",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.GetWorkflowId()),
				tag.WorkflowRunID(executionState.GetRunId()),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
		}
		if operationInfo == nil {
			// Operation is not pending
			continue
		}
		result.SetPendingNexusOperations(append(result.GetPendingNexusOperations(), operationInfo))
	}

	return result, nil
}

func buildCallbackInfoFromHSM(
	namespaceID namespace.ID,
	callback callbacks.Callback,
	outboundQueueCBPool *circuitbreakerpool.OutboundQueueCircuitBreakerPool,
) (*workflowpb.CallbackInfo, error) {
	if callback.GetCallback().GetNexus() == nil {
		// Ignore non-nexus callbacks for now (there aren't any just yet).
		return nil, nil
	}

	cbSpec, err := workflow.PersistenceCallbackToAPICallback(callback.GetCallback())
	if err != nil {
		return nil, err
	}

	var state enumspb.CallbackState
	switch callback.State() {
	case enumsspb.CALLBACK_STATE_UNSPECIFIED:
		return nil, errCallbackStateUnspecified
	case enumsspb.CALLBACK_STATE_STANDBY:
		state = enumspb.CALLBACK_STATE_STANDBY
	case enumsspb.CALLBACK_STATE_SCHEDULED:
		state = enumspb.CALLBACK_STATE_SCHEDULED
	case enumsspb.CALLBACK_STATE_BACKING_OFF:
		state = enumspb.CALLBACK_STATE_BACKING_OFF
	case enumsspb.CALLBACK_STATE_FAILED:
		state = enumspb.CALLBACK_STATE_FAILED
	case enumsspb.CALLBACK_STATE_SUCCEEDED:
		state = enumspb.CALLBACK_STATE_SUCCEEDED
	default:
		return nil, fmt.Errorf("unknown callback state: %v", callback.State())
	}

	blockedReason := ""
	if state == enumspb.CALLBACK_STATE_SCHEDULED {
		cb := outboundQueueCBPool.Get(tasks.TaskGroupNamespaceIDAndDestination{
			TaskGroup:   callbacks.TaskTypeInvocation,
			NamespaceID: namespaceID.String(),
			Destination: cbSpec.GetNexus().GetUrl(),
		})
		if cb.State() != gobreaker.StateClosed {
			state = enumspb.CALLBACK_STATE_BLOCKED
			blockedReason = "The circuit breaker is open."
		}
	}

	trigger := workflowpb.CallbackInfo_Trigger_builder{}.Build()
	switch callback.GetTrigger().WhichVariant() {
	case persistencespb.CallbackInfo_Trigger_WorkflowClosed_case:
		trigger.SetWorkflowClosed(&workflowpb.CallbackInfo_WorkflowClosed{})
	}

	return workflowpb.CallbackInfo_builder{
		Callback:                cbSpec,
		Trigger:                 trigger,
		RegistrationTime:        callback.GetRegistrationTime(),
		State:                   state,
		Attempt:                 callback.GetAttempt(),
		LastAttemptCompleteTime: callback.GetLastAttemptCompleteTime(),
		LastAttemptFailure:      callback.GetLastAttemptFailure(),
		NextAttemptScheduleTime: callback.GetNextAttemptScheduleTime(),
		BlockedReason:           blockedReason,
	}.Build(), nil
}

// buildCallbackInfosFromHSM reads callbacks from HSM and converts them to API format.
func buildCallbackInfosFromHSM(
	namespaceID namespace.ID,
	mutableState historyi.MutableState,
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	outboundQueueCBPool *circuitbreakerpool.OutboundQueueCircuitBreakerPool,
	logger log.Logger,
) ([]*workflowpb.CallbackInfo, error) {
	cbColl := callbacks.MachineCollection(mutableState.HSM())
	cbs := cbColl.List()
	result := make([]*workflowpb.CallbackInfo, 0, len(cbs))

	for _, node := range cbs {
		callback, err := cbColl.Data(node.Key.ID)
		if err != nil {
			logger.Error(
				"failed to load callback data while building describe response",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.GetWorkflowId()),
				tag.WorkflowRunID(executionState.GetRunId()),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
		}

		callbackInfo, err := buildCallbackInfoFromHSM(namespaceID, callback, outboundQueueCBPool)
		if err != nil {
			logger.Error(
				"failed to build callback info while building describe response",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.GetWorkflowId()),
				tag.WorkflowRunID(executionState.GetRunId()),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
		}
		if callbackInfo == nil {
			continue
		}
		result = append(result, callbackInfo)
	}

	return result, nil
}

// buildCallbackInfosFromChasm reads callbacks from the CHASM tree and converts them to API format.
func buildCallbackInfosFromChasm(
	ctx context.Context,
	namespaceID namespace.ID,
	mutableState historyi.MutableState,
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	outboundQueueCBPool *circuitbreakerpool.OutboundQueueCircuitBreakerPool,
	logger log.Logger,
) ([]*workflowpb.CallbackInfo, error) {
	wf, chasmCtx, err := mutableState.ChasmWorkflowComponentReadOnly(ctx)
	if err != nil {
		logger.Error(
			"failed to get workflow component from CHASM tree",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(executionInfo.GetWorkflowId()),
			tag.WorkflowRunID(executionState.GetRunId()),
			tag.Error(err),
		)
		return nil, serviceerror.NewInternal("failed to construct describe response")
	}

	result := make([]*workflowpb.CallbackInfo, 0, len(wf.Callbacks))
	for _, field := range wf.Callbacks {
		callback := field.Get(chasmCtx)

		callbackInfo, err := buildCallbackInfoFromChasm(ctx, namespaceID, callback, outboundQueueCBPool)
		if err != nil {
			logger.Error(
				"failed to build callback info from CHASM callback",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.GetWorkflowId()),
				tag.WorkflowRunID(executionState.GetRunId()),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
		}
		if callbackInfo == nil {
			continue
		}
		result = append(result, callbackInfo)
	}

	return result, nil
}

// buildCallbackInfoFromChasm converts a single CHASM callback to API format.
func buildCallbackInfoFromChasm(
	ctx context.Context,
	namespaceID namespace.ID,
	callback *chasmcallback.Callback,
	outboundQueueCBPool *circuitbreakerpool.OutboundQueueCircuitBreakerPool,
) (*workflowpb.CallbackInfo, error) {
	// Create a circuit breaker state checker function
	circuitBreakerState := func(destination string) bool {
		cb := outboundQueueCBPool.Get(tasks.TaskGroupNamespaceIDAndDestination{
			TaskGroup:   callbacks.TaskTypeInvocation,
			NamespaceID: namespaceID.String(),
			Destination: destination,
		})
		return cb.State() != gobreaker.StateClosed
	}

	return buildChasmCallbackInfo(ctx, namespaceID.String(), callback, circuitBreakerState)
}

// buildChasmCallbackInfo converts a single CHASM callback to API CallbackInfo format.
// Returns nil if the callback should not be included in the response.
func buildChasmCallbackInfo(
	ctx context.Context,
	namespaceID string,
	cb *chasmcallback.Callback,
	circuitBreakerState func(destination string) bool,
) (*workflowpb.CallbackInfo, error) {
	nexusVariant := cb.GetCallback().GetNexus()
	if nexusVariant == nil {
		// Only Nexus callbacks are supported
		return nil, nil
	}

	cbSpec, err := cb.ToAPICallback()
	if err != nil {
		return nil, err
	}

	var state enumspb.CallbackState
	switch cb.GetStatus() {
	case callbackspb.CALLBACK_STATUS_UNSPECIFIED:
		return nil, serviceerror.NewInternal("callback with UNSPECIFIED state")
	case callbackspb.CALLBACK_STATUS_STANDBY:
		state = enumspb.CALLBACK_STATE_STANDBY
	case callbackspb.CALLBACK_STATUS_SCHEDULED:
		state = enumspb.CALLBACK_STATE_SCHEDULED
	case callbackspb.CALLBACK_STATUS_BACKING_OFF:
		state = enumspb.CALLBACK_STATE_BACKING_OFF
	case callbackspb.CALLBACK_STATUS_FAILED:
		state = enumspb.CALLBACK_STATE_FAILED
	case callbackspb.CALLBACK_STATUS_SUCCEEDED:
		state = enumspb.CALLBACK_STATE_SUCCEEDED
	default:
		return nil, serviceerror.NewInternalf("unknown callback state: %v", cb.GetStatus())
	}

	blockedReason := ""
	if state == enumspb.CALLBACK_STATE_SCHEDULED {
		if circuitBreakerState(cbSpec.GetNexus().GetUrl()) {
			state = enumspb.CALLBACK_STATE_BLOCKED
			blockedReason = "The circuit breaker is open."
		}
	}

	trigger := workflowpb.CallbackInfo_Trigger_builder{
		WorkflowClosed: &workflowpb.CallbackInfo_WorkflowClosed{},
	}.Build()

	return workflowpb.CallbackInfo_builder{
		Callback:                cbSpec,
		Trigger:                 trigger,
		RegistrationTime:        cb.GetRegistrationTime(),
		State:                   state,
		Attempt:                 cb.GetAttempt(),
		LastAttemptCompleteTime: cb.GetLastAttemptCompleteTime(),
		LastAttemptFailure:      cb.GetLastAttemptFailure(),
		NextAttemptScheduleTime: cb.GetNextAttemptScheduleTime(),
		BlockedReason:           blockedReason,
	}.Build(), nil
}

func buildPendingNexusOperationInfo(
	namespaceID namespace.ID,
	node *hsm.Node,
	op nexusoperations.Operation,
	outboundQueueCBPool *circuitbreakerpool.OutboundQueueCircuitBreakerPool,
) (*workflowpb.PendingNexusOperationInfo, error) {
	var state enumspb.PendingNexusOperationState
	switch op.State() {
	case enumsspb.NEXUS_OPERATION_STATE_UNSPECIFIED:
		return nil, errNexusOperationStateUnspecified
	case enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF:
		state = enumspb.PENDING_NEXUS_OPERATION_STATE_BACKING_OFF
	case enumsspb.NEXUS_OPERATION_STATE_SCHEDULED:
		state = enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED
	case enumsspb.NEXUS_OPERATION_STATE_STARTED:
		state = enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED
	case enumsspb.NEXUS_OPERATION_STATE_CANCELED,
		enumsspb.NEXUS_OPERATION_STATE_FAILED,
		enumsspb.NEXUS_OPERATION_STATE_SUCCEEDED,
		enumsspb.NEXUS_OPERATION_STATE_TIMED_OUT:
		// Operation is not pending
		return nil, nil
	}

	blockedReason := ""
	if state == enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED {
		cb := outboundQueueCBPool.Get(tasks.TaskGroupNamespaceIDAndDestination{
			TaskGroup:   nexusoperations.TaskTypeInvocation,
			NamespaceID: namespaceID.String(),
			Destination: op.GetEndpoint(),
		})
		if cb.State() != gobreaker.StateClosed {
			state = enumspb.PENDING_NEXUS_OPERATION_STATE_BLOCKED
			blockedReason = "The circuit breaker is open."
		}
	}

	cancellationInfo, err := buildNexusOperationCancellationInfo(namespaceID, node, op, outboundQueueCBPool)
	if err != nil {
		return nil, err
	}

	// We store nexus operations in the tree by their string formatted scheduled event ID.
	scheduledEventID, err := strconv.ParseInt(node.Key.ID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to determine Nexus operation scheduled event ID: %w", err)
	}

	return workflowpb.PendingNexusOperationInfo_builder{
		Endpoint:  op.GetEndpoint(),
		Service:   op.GetService(),
		Operation: op.GetOperation(),
		// TODO(bergundy): Remove this fallback after the 1.27 release.
		OperationId:             op.GetOperationToken(),
		OperationToken:          op.GetOperationToken(),
		ScheduledEventId:        scheduledEventID,
		ScheduleToCloseTimeout:  op.GetScheduleToCloseTimeout(),
		ScheduledTime:           op.GetScheduledTime(),
		State:                   state,
		Attempt:                 op.GetAttempt(),
		LastAttemptCompleteTime: op.GetLastAttemptCompleteTime(),
		LastAttemptFailure:      op.GetLastAttemptFailure(),
		NextAttemptScheduleTime: op.GetNextAttemptScheduleTime(),
		CancellationInfo:        cancellationInfo,
		BlockedReason:           blockedReason,
	}.Build(), nil
}

func buildNexusOperationCancellationInfo(
	namespaceID namespace.ID,
	node *hsm.Node,
	op nexusoperations.Operation,
	outboundQueueCBPool *circuitbreakerpool.OutboundQueueCircuitBreakerPool,
) (*workflowpb.NexusOperationCancellationInfo, error) {
	cancelation, err := op.Cancelation(node)
	if err != nil {
		return nil, fmt.Errorf("failed to load Nexus operation cancelation data: %w", err)
	}
	if cancelation == nil {
		return nil, nil
	}

	state := cancelation.State()
	blockedReason := ""
	if state == enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED {
		cb := outboundQueueCBPool.Get(tasks.TaskGroupNamespaceIDAndDestination{
			TaskGroup:   nexusoperations.TaskTypeCancelation,
			NamespaceID: namespaceID.String(),
			Destination: op.GetEndpoint(),
		})
		if cb.State() != gobreaker.StateClosed {
			state = enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BLOCKED
			blockedReason = "The circuit breaker is open."
		}
	}

	return workflowpb.NexusOperationCancellationInfo_builder{
		RequestedTime:           cancelation.GetRequestedTime(),
		State:                   state,
		Attempt:                 cancelation.GetAttempt(),
		LastAttemptCompleteTime: cancelation.GetLastAttemptCompleteTime(),
		LastAttemptFailure:      cancelation.GetLastAttemptFailure(),
		NextAttemptScheduleTime: cancelation.GetNextAttemptScheduleTime(),
		BlockedReason:           blockedReason,
	}.Build(), nil
}
