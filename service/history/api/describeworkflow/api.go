package describeworkflow

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

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
		target[k] = &commonpb.Payload{
			Metadata: metadata,
			Data:     v.GetData(),
		}
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
			req.NamespaceId,
			req.Request.Execution.WorkflowId,
			req.Request.Execution.RunId,
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
	result := &historyservice.DescribeWorkflowExecutionResponse{
		ExecutionConfig: &workflowpb.WorkflowExecutionConfig{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: executionInfo.TaskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			WorkflowExecutionTimeout:   executionInfo.WorkflowExecutionTimeout,
			WorkflowRunTimeout:         executionInfo.WorkflowRunTimeout,
			DefaultWorkflowTaskTimeout: executionInfo.DefaultWorkflowTaskTimeout,
			UserMetadata:               startEvent.UserMetadata,
		},
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: executionInfo.WorkflowId,
				RunId:      executionState.RunId,
			},
			Type:          &commonpb.WorkflowType{Name: executionInfo.WorkflowTypeName},
			StartTime:     executionState.StartTime,
			Status:        executionState.Status,
			HistoryLength: mutableState.GetNextEventID() - common.FirstEventID,
			ExecutionTime: executionInfo.ExecutionTime,
			// Memo and SearchAttributes are set below
			AutoResetPoints:      common.CloneProto(executionInfo.AutoResetPoints),
			TaskQueue:            executionInfo.TaskQueue,
			StateTransitionCount: executionInfo.StateTransitionCount,
			HistorySizeBytes:     executionInfo.GetExecutionStats().GetHistorySize(),
			RootExecution: &commonpb.WorkflowExecution{
				WorkflowId: executionInfo.RootWorkflowId,
				RunId:      executionInfo.RootRunId,
			},

			MostRecentWorkerVersionStamp: executionInfo.MostRecentWorkerVersionStamp,
			AssignedBuildId:              executionInfo.AssignedBuildId,
			InheritedBuildId:             executionInfo.InheritedBuildId,
			FirstRunId:                   executionInfo.FirstExecutionRunId,
			VersioningInfo:               common.CloneProto(executionInfo.VersioningInfo),
			WorkerDeploymentName:         executionInfo.WorkerDeploymentName,
			Priority:                     executionInfo.Priority,
		},
		WorkflowExtendedInfo: &workflowpb.WorkflowExecutionExtendedInfo{
			ExecutionExpirationTime: executionInfo.WorkflowExecutionExpirationTime,
			RunExpirationTime:       executionInfo.WorkflowRunExpirationTime,
			OriginalStartTime:       startEvent.EventTime,
			CancelRequested:         executionInfo.CancelRequested,
			ResetRunId:              executionInfo.ResetRunId,
			RequestIdInfos:          make(map[string]*workflowpb.RequestIdInfo),
		},
	}

	// copy pause info to the response if it exists
	if executionInfo.PauseInfo != nil {
		result.WorkflowExtendedInfo.PauseInfo = &workflowpb.WorkflowExecutionPauseInfo{
			PausedTime: executionInfo.PauseInfo.PauseTime,
			Identity:   executionInfo.PauseInfo.Identity,
			Reason:     executionInfo.PauseInfo.Reason,
		}
	}

	if mutableState.IsResetRun() {
		result.WorkflowExtendedInfo.LastResetTime = executionState.StartTime
	}

	if shard.GetConfig().ExternalPayloadsEnabled(namespaceName) {
		executionStats := executionInfo.GetExecutionStats()
		result.WorkflowExecutionInfo.ExternalPayloadSizeBytes = executionStats.GetExternalPayloadSize()
		result.WorkflowExecutionInfo.ExternalPayloadCount = executionStats.GetExternalPayloadCount()
	}

	for requestID, requestIDInfo := range mutableState.GetExecutionState().GetRequestIds() {
		info := &workflowpb.RequestIdInfo{
			EventType: requestIDInfo.EventType,
			Buffered:  requestIDInfo.EventId == common.BufferedEventID,
		}
		if !info.Buffered {
			info.EventId = requestIDInfo.EventId
		}
		result.WorkflowExtendedInfo.RequestIdInfos[requestID] = info
	}

	if executionInfo.ParentRunId != "" {
		result.WorkflowExecutionInfo.ParentExecution = &commonpb.WorkflowExecution{
			WorkflowId: executionInfo.ParentWorkflowId,
			RunId:      executionInfo.ParentRunId,
		}
		result.WorkflowExecutionInfo.ParentNamespaceId = executionInfo.ParentNamespaceId
	}
	if executionState.State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// for closed workflow
		result.WorkflowExecutionInfo.Status = executionState.Status
		closeTime, err := mutableState.GetWorkflowCloseTime(ctx)
		if err != nil {
			return nil, err
		}
		executionDuration, err := mutableState.GetWorkflowExecutionDuration(ctx)
		if err != nil {
			return nil, err
		}
		result.WorkflowExecutionInfo.CloseTime = timestamppb.New(closeTime)
		result.WorkflowExecutionInfo.ExecutionDuration = durationpb.New(executionDuration)
	}

	for _, ai := range mutableState.GetPendingActivityInfos() {
		p, err := workflow.GetPendingActivityInfo(ctx, shard, mutableState, ai)
		if err != nil {
			return nil, err
		}

		result.PendingActivities = append(result.PendingActivities, p)
	}

	for _, ch := range mutableState.GetPendingChildExecutionInfos() {
		p := &workflowpb.PendingChildExecutionInfo{
			WorkflowId:        ch.StartedWorkflowId,
			RunId:             ch.StartedRunId,
			WorkflowTypeName:  ch.WorkflowTypeName,
			InitiatedId:       ch.InitiatedEventId,
			ParentClosePolicy: ch.ParentClosePolicy,
		}
		result.PendingChildren = append(result.PendingChildren, p)
	}

	if pendingWorkflowTask := mutableState.GetPendingWorkflowTask(); pendingWorkflowTask != nil {
		result.PendingWorkflowTask = &workflowpb.PendingWorkflowTaskInfo{
			State:                 enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED,
			ScheduledTime:         timestamppb.New(pendingWorkflowTask.ScheduledTime),
			OriginalScheduledTime: timestamppb.New(pendingWorkflowTask.OriginalScheduledTime),
			Attempt:               pendingWorkflowTask.Attempt,
		}
		if pendingWorkflowTask.StartedEventID != common.EmptyEventID {
			result.PendingWorkflowTask.State = enumspb.PENDING_WORKFLOW_TASK_STATE_STARTED
			result.PendingWorkflowTask.StartedTime = timestamppb.New(pendingWorkflowTask.StartedTime)
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
			tag.WorkflowID(executionInfo.WorkflowId),
			tag.WorkflowRunID(executionState.RunId),
			tag.Error(err),
		)
		return nil, serviceerror.NewInternal("Failed to fetch memo and search attributes")
	}
	result.WorkflowExecutionInfo.Memo = &commonpb.Memo{
		Fields: clonePayloadMap(relocatableAttributes.Memo.GetFields()),
	}
	result.WorkflowExecutionInfo.SearchAttributes = &commonpb.SearchAttributes{
		IndexedFields: clonePayloadMap(relocatableAttributes.SearchAttributes.GetIndexedFields()),
	}

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
		result.Callbacks = append(result.Callbacks, chasmCallbackInfos...)
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
	result.Callbacks = append(result.Callbacks, hsmCallbackInfos...)

	opColl := nexusoperations.MachineCollection(mutableState.HSM())
	ops := opColl.List()
	result.PendingNexusOperations = make([]*workflowpb.PendingNexusOperationInfo, 0, len(ops))
	for _, node := range ops {
		op, err := opColl.Data(node.Key.ID)
		if err != nil {
			shard.GetLogger().Error(
				"failed to load Nexus operation data while building describe response",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.WorkflowId),
				tag.WorkflowRunID(executionState.RunId),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
		}

		operationInfo, err := buildPendingNexusOperationInfo(namespaceID, node, op, outboundQueueCBPool)
		if err != nil {
			shard.GetLogger().Error(
				"failed to build Nexus operation info while building describe response",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.WorkflowId),
				tag.WorkflowRunID(executionState.RunId),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
		}
		if operationInfo == nil {
			// Operation is not pending
			continue
		}
		result.PendingNexusOperations = append(result.PendingNexusOperations, operationInfo)
	}

	// Populate time-skipping info if enabled. Done after Nexus ops are
	// collected so we can derive hasNonAutoSkipWork without a second HSM walk.
	if executionInfo.GetTimeSkippingConfig() != nil {
		now := time.Now()
		virtualNow := now.Add(executionInfo.GetVirtualTimeOffset().AsDuration())
		result.UpcomingTimePoints = BuildUpcomingTimePoints(mutableState, executionInfo, virtualNow)
		hasNonAutoSkipWork := len(mutableState.GetPendingActivityInfos()) > 0 ||
			len(mutableState.GetPendingChildExecutionInfos()) > 0 ||
			len(result.PendingNexusOperations) > 0
		result.TimeSkippingInfo = BuildTimeSkippingInfo(executionInfo, result.UpcomingTimePoints, hasNonAutoSkipWork, mutableState.IsWorkflowExecutionRunning(), virtualNow)
	}

	return result, nil
}

// BuildTimeSkippingInfo constructs a TimeSkippingInfo proto from execution info.
// hasNonAutoSkipWork indicates whether there are pending activities, child
// workflows, or Nexus operations that should pause auto-skip.
// virtualNow is the current virtual time (wall clock + offset), used to
// compute VirtualTime and DeadlineRemaining.
func BuildTimeSkippingInfo(
	executionInfo *persistencespb.WorkflowExecutionInfo,
	upcomingTimePoints []*workflowpb.UpcomingTimePointInfo,
	hasNonAutoSkipWork bool,
	isRunning bool,
	virtualNow time.Time,
) *workflowpb.TimeSkippingInfo {
	info := &workflowpb.TimeSkippingInfo{
		Config:            executionInfo.GetTimeSkippingConfig(),
		VirtualTimeOffset: executionInfo.GetVirtualTimeOffset(),
		VirtualTime:       timestamppb.New(virtualNow),
	}

	autoSkip := executionInfo.GetTimeSkippingConfig().GetAutoSkip()
	if autoSkip != nil {
		maxFirings := api.EffectiveAutoSkipMaxFirings(autoSkip)
		firingsUsed := executionInfo.GetAutoSkipFiringsUsed()
		firingsRemaining := maxFirings - firingsUsed
		if firingsRemaining < 0 {
			firingsRemaining = 0
		}

		// Active when: within limits, no in-flight work, workflow is running,
		// and the next fire time is within the deadline (if set).
		active := firingsUsed < maxFirings &&
			!hasNonAutoSkipWork &&
			isRunning

		if active && len(upcomingTimePoints) > 0 {
			if dl := executionInfo.GetAutoSkipDeadline(); dl != nil && dl.IsValid() {
				if upcomingTimePoints[0].GetFireTime().AsTime().After(dl.AsTime()) {
					active = false
				}
			}
		}

		autoSkipInfo := &workflowpb.TimeSkippingInfo_AutoSkipInfo{
			Active:           active,
			Deadline:         executionInfo.GetAutoSkipDeadline(),
			FiringsUsed:      firingsUsed,
			FiringsRemaining: firingsRemaining,
		}

		if dl := executionInfo.GetAutoSkipDeadline(); dl != nil && dl.IsValid() {
			remaining := dl.AsTime().Sub(virtualNow)
			if remaining < 0 {
				remaining = 0
			}
			autoSkipInfo.DeadlineRemaining = durationpb.New(remaining)
		}

		info.AutoSkip = autoSkipInfo
	}

	return info
}

// BuildUpcomingTimePoints scans mutable state for all upcoming time points:
// user timers, activity timeouts (excluding heartbeat), and workflow timeouts.
// virtualNow is the current virtual time, used to compute FireTimeRemaining
// on each returned time point.
func BuildUpcomingTimePoints(
	mutableState historyi.MutableState,
	executionInfo *persistencespb.WorkflowExecutionInfo,
	virtualNow time.Time,
) []*workflowpb.UpcomingTimePointInfo {
	var result []*workflowpb.UpcomingTimePointInfo

	// User timers.
	for _, timerInfo := range mutableState.GetPendingTimerInfos() {
		if timerInfo.ExpiryTime == nil {
			continue
		}
		result = append(result, &workflowpb.UpcomingTimePointInfo{
			FireTime: timerInfo.ExpiryTime,
			Source: &workflowpb.UpcomingTimePointInfo_Timer{
				Timer: &workflowpb.UpcomingTimePointInfo_TimerTimePoint{
					TimerId:        timerInfo.TimerId,
					StartedEventId: timerInfo.StartedEventId,
				},
			},
		})
	}

	// Activity timeouts (excluding heartbeat).
	for _, ai := range mutableState.GetPendingActivityInfos() {
		if ai.Paused || ai.ScheduledEventId == common.EmptyEventID {
			continue
		}
		addActivityTimeoutPoints(&result, ai)
	}

	// Workflow run timeout.
	if executionInfo.WorkflowRunExpirationTime != nil && executionInfo.WorkflowRunExpirationTime.IsValid() && !executionInfo.WorkflowRunExpirationTime.AsTime().IsZero() {
		result = append(result, &workflowpb.UpcomingTimePointInfo{
			FireTime: executionInfo.WorkflowRunExpirationTime,
			Source: &workflowpb.UpcomingTimePointInfo_WorkflowTimeout{
				WorkflowTimeout: &workflowpb.UpcomingTimePointInfo_WorkflowTimeoutTimePoint{
					TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				},
			},
		})
	}

	// Workflow execution timeout.
	if executionInfo.WorkflowExecutionExpirationTime != nil && executionInfo.WorkflowExecutionExpirationTime.IsValid() && !executionInfo.WorkflowExecutionExpirationTime.AsTime().IsZero() {
		result = append(result, &workflowpb.UpcomingTimePointInfo{
			FireTime: executionInfo.WorkflowExecutionExpirationTime,
			Source: &workflowpb.UpcomingTimePointInfo_WorkflowTimeout{
				WorkflowTimeout: &workflowpb.UpcomingTimePointInfo_WorkflowTimeoutTimePoint{
					TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
				},
			},
		})
	}

	// Set FireTimeRemaining on each time point.
	for _, tp := range result {
		remaining := tp.GetFireTime().AsTime().Sub(virtualNow)
		if remaining < 0 {
			remaining = 0
		}
		tp.FireTimeRemaining = durationpb.New(remaining)
	}

	return result
}

func addActivityTimeoutPoints(result *[]*workflowpb.UpcomingTimePointInfo, ai *persistencespb.ActivityInfo) {
	add := func(t *timestamppb.Timestamp, timeoutType enumspb.TimeoutType) {
		if t == nil {
			return
		}
		*result = append(*result, &workflowpb.UpcomingTimePointInfo{
			FireTime: t,
			Source: &workflowpb.UpcomingTimePointInfo_ActivityTimeout{
				ActivityTimeout: &workflowpb.UpcomingTimePointInfo_ActivityTimeoutTimePoint{
					ActivityId:  ai.ActivityId,
					TimeoutType: timeoutType,
				},
			},
		})
	}

	// Schedule-to-start: only if not yet started.
	if ai.StartedEventId == common.EmptyEventID {
		if d := ai.ScheduleToStartTimeout.AsDuration(); d > 0 {
			t := timestamppb.New(ai.ScheduledTime.AsTime().Add(d))
			add(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START)
		}
	}

	// Schedule-to-close.
	if d := ai.ScheduleToCloseTimeout.AsDuration(); d > 0 {
		base := ai.FirstScheduledTime
		if base == nil {
			base = ai.ScheduledTime
		}
		t := timestamppb.New(base.AsTime().Add(d))
		add(t, enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE)
	}

	// Start-to-close: only if started.
	if ai.StartedEventId != common.EmptyEventID {
		if d := ai.StartToCloseTimeout.AsDuration(); d > 0 {
			t := timestamppb.New(ai.StartedTime.AsTime().Add(d))
			add(t, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
		}
	}
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

	cbSpec, err := workflow.PersistenceCallbackToAPICallback(callback.Callback)
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

	trigger := &workflowpb.CallbackInfo_Trigger{}
	switch callback.Trigger.Variant.(type) {
	case *persistencespb.CallbackInfo_Trigger_WorkflowClosed:
		trigger.Variant = &workflowpb.CallbackInfo_Trigger_WorkflowClosed{}
	}

	return &workflowpb.CallbackInfo{
		Callback:                cbSpec,
		Trigger:                 trigger,
		RegistrationTime:        callback.RegistrationTime,
		State:                   state,
		Attempt:                 callback.Attempt,
		LastAttemptCompleteTime: callback.LastAttemptCompleteTime,
		LastAttemptFailure:      callback.LastAttemptFailure,
		NextAttemptScheduleTime: callback.NextAttemptScheduleTime,
		BlockedReason:           blockedReason,
	}, nil
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
				tag.WorkflowID(executionInfo.WorkflowId),
				tag.WorkflowRunID(executionState.RunId),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
		}

		callbackInfo, err := buildCallbackInfoFromHSM(namespaceID, callback, outboundQueueCBPool)
		if err != nil {
			logger.Error(
				"failed to build callback info while building describe response",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.WorkflowId),
				tag.WorkflowRunID(executionState.RunId),
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
			tag.WorkflowID(executionInfo.WorkflowId),
			tag.WorkflowRunID(executionState.RunId),
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
				tag.WorkflowID(executionInfo.WorkflowId),
				tag.WorkflowRunID(executionState.RunId),
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
	switch cb.Status {
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
		return nil, serviceerror.NewInternalf("unknown callback state: %v", cb.Status)
	}

	blockedReason := ""
	if state == enumspb.CALLBACK_STATE_SCHEDULED {
		if circuitBreakerState(cbSpec.GetNexus().GetUrl()) {
			state = enumspb.CALLBACK_STATE_BLOCKED
			blockedReason = "The circuit breaker is open."
		}
	}

	trigger := &workflowpb.CallbackInfo_Trigger{
		Variant: &workflowpb.CallbackInfo_Trigger_WorkflowClosed{},
	}

	return &workflowpb.CallbackInfo{
		Callback:                cbSpec,
		Trigger:                 trigger,
		RegistrationTime:        cb.RegistrationTime,
		State:                   state,
		Attempt:                 cb.Attempt,
		LastAttemptCompleteTime: cb.LastAttemptCompleteTime,
		LastAttemptFailure:      cb.LastAttemptFailure,
		NextAttemptScheduleTime: cb.NextAttemptScheduleTime,
		BlockedReason:           blockedReason,
	}, nil
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
			Destination: op.Endpoint,
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

	return &workflowpb.PendingNexusOperationInfo{
		Endpoint:  op.Endpoint,
		Service:   op.Service,
		Operation: op.Operation,
		// TODO(bergundy): Remove this fallback after the 1.27 release.
		OperationId:             op.OperationToken,
		OperationToken:          op.OperationToken,
		ScheduledEventId:        scheduledEventID,
		ScheduleToCloseTimeout:  op.ScheduleToCloseTimeout,
		ScheduleToStartTimeout:  op.ScheduleToStartTimeout,
		StartToCloseTimeout:     op.StartToCloseTimeout,
		ScheduledTime:           op.ScheduledTime,
		State:                   state,
		Attempt:                 op.Attempt,
		LastAttemptCompleteTime: op.LastAttemptCompleteTime,
		LastAttemptFailure:      op.LastAttemptFailure,
		NextAttemptScheduleTime: op.NextAttemptScheduleTime,
		CancellationInfo:        cancellationInfo,
		BlockedReason:           blockedReason,
	}, nil
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
			Destination: op.Endpoint,
		})
		if cb.State() != gobreaker.StateClosed {
			state = enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BLOCKED
			blockedReason = "The circuit breaker is open."
		}
	}

	return &workflowpb.NexusOperationCancellationInfo{
		RequestedTime:           cancelation.RequestedTime,
		State:                   state,
		Attempt:                 cancelation.Attempt,
		LastAttemptCompleteTime: cancelation.LastAttemptCompleteTime,
		LastAttemptFailure:      cancelation.LastAttemptFailure,
		NextAttemptScheduleTime: cancelation.NextAttemptScheduleTime,
		BlockedReason:           blockedReason,
	}, nil
}
