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
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/circuitbreakerpool"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
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
	shard shard.Context,
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
			VersioningInfo:               executionInfo.VersioningInfo,
		},
		WorkflowExtendedInfo: &workflowpb.WorkflowExecutionExtendedInfo{
			ExecutionExpirationTime: executionInfo.WorkflowExecutionExpirationTime,
			RunExpirationTime:       executionInfo.WorkflowRunExpirationTime,
			OriginalStartTime:       startEvent.EventTime,
			CancelRequested:         executionInfo.CancelRequested,
		},
	}

	if mutableState.IsResetRun() {
		result.WorkflowExtendedInfo.LastResetTime = executionState.StartTime
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

	cbColl := callbacks.MachineCollection(mutableState.HSM())
	cbs := cbColl.List()
	result.Callbacks = make([]*workflowpb.CallbackInfo, 0, len(cbs))
	for _, node := range cbs {
		callback, err := cbColl.Data(node.Key.ID)
		if err != nil {
			shard.GetLogger().Error(
				"failed to load callback data while building describe response",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.WorkflowId),
				tag.WorkflowRunID(executionState.RunId),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
		}

		callbackInfo, err := buildCallbackInfo(namespaceID, callback, outboundQueueCBPool)
		if err != nil {
			shard.GetLogger().Error(
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
		result.Callbacks = append(result.Callbacks, callbackInfo)
	}

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

	return result, nil
}

func buildCallbackInfo(
	namespaceID namespace.ID,
	callback callbacks.Callback,
	outboundQueueCBPool *circuitbreakerpool.OutboundQueueCircuitBreakerPool,
) (*workflowpb.CallbackInfo, error) {
	destination := ""
	cbSpec := &commonpb.Callback{}
	switch variant := callback.Callback.Variant.(type) {
	case *persistencespb.Callback_Nexus_:
		cbSpec.Variant = &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url:    variant.Nexus.GetUrl(),
				Header: variant.Nexus.GetHeader(),
			},
		}
		destination = variant.Nexus.GetUrl()
	default:
		// Ignore non-nexus callbacks for now (there aren't any just yet).
		return nil, nil
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
	}

	blockedReason := ""
	if state == enumspb.CALLBACK_STATE_SCHEDULED {
		cb := outboundQueueCBPool.Get(tasks.TaskGroupNamespaceIDAndDestination{
			TaskGroup:   callbacks.TaskTypeInvocation,
			NamespaceID: namespaceID.String(),
			Destination: destination,
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
		Endpoint:                op.Endpoint,
		Service:                 op.Service,
		Operation:               op.Operation,
		OperationId:             op.OperationId,
		ScheduledEventId:        scheduledEventID,
		ScheduleToCloseTimeout:  op.ScheduleToCloseTimeout,
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
