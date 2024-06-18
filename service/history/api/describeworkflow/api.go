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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
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
) (_ *historyservice.DescribeWorkflowExecutionResponse, retError error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}

	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			req.NamespaceId,
			req.Request.Execution.WorkflowId,
			req.Request.Execution.RunId,
		),
		workflow.LockPriorityHigh,
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
			StartTime:     executionInfo.StartTime,
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
		},
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
		p := &workflowpb.PendingActivityInfo{
			ActivityId: ai.ActivityId,
		}
		if ai.GetUseWorkflowBuildIdInfo() != nil {
			p.AssignedBuildId = &workflowpb.PendingActivityInfo_UseWorkflowBuildId{UseWorkflowBuildId: &emptypb.Empty{}}
		} else if ai.GetLastIndependentlyAssignedBuildId() != "" {
			p.AssignedBuildId = &workflowpb.PendingActivityInfo_LastIndependentlyAssignedBuildId{
				LastIndependentlyAssignedBuildId: ai.GetLastIndependentlyAssignedBuildId(),
			}
		}
		if ai.CancelRequested {
			p.State = enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
		} else if ai.StartedEventId != common.EmptyEventID {
			p.State = enumspb.PENDING_ACTIVITY_STATE_STARTED
		} else {
			p.State = enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
		}
		if ai.LastHeartbeatUpdateTime != nil && !ai.LastHeartbeatUpdateTime.AsTime().IsZero() {
			p.LastHeartbeatTime = ai.LastHeartbeatUpdateTime
			p.HeartbeatDetails = ai.LastHeartbeatDetails
		}
		p.ActivityType, err = mutableState.GetActivityType(ctx, ai)
		if err != nil {
			return nil, err
		}
		if p.State == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			p.ScheduledTime = ai.ScheduledTime
		} else {
			p.LastStartedTime = ai.StartedTime
		}
		p.LastWorkerIdentity = ai.StartedIdentity
		if ai.HasRetryPolicy {
			p.Attempt = ai.Attempt
			p.ExpirationTime = ai.RetryExpirationTime
			if ai.RetryMaximumAttempts != 0 {
				p.MaximumAttempts = ai.RetryMaximumAttempts
			}
			if ai.RetryLastFailure != nil {
				p.LastFailure = ai.RetryLastFailure
			}
			if p.LastWorkerIdentity == "" && ai.RetryLastWorkerIdentity != "" {
				p.LastWorkerIdentity = ai.RetryLastWorkerIdentity
			}
		} else {
			p.Attempt = 1
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

	relocatableAttributes, err := workflow.RelocatableAttributesFetcherProvider(persistenceVisibilityMgr).Fetch(ctx, mutableState)
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
		var state enumspb.CallbackState
		switch callback.State() {
		case enumsspb.CALLBACK_STATE_UNSPECIFIED:
			shard.GetLogger().Error(
				"unexpected error: got an operation with an UNSPECIFIED state",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.WorkflowId),
				tag.WorkflowRunID(executionState.RunId),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
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
		cbSpec := &commonpb.Callback{}
		switch variant := callback.Callback.Variant.(type) {
		case *persistence.Callback_Nexus_:
			cbSpec.Variant = &commonpb.Callback_Nexus_{
				Nexus: &commonpb.Callback_Nexus{
					Url:    variant.Nexus.GetUrl(),
					Header: variant.Nexus.GetHeader(),
				},
			}
		default:
			// Ignore non-nexus callbacks for now (there aren't any just yet).
			continue
		}
		trigger := &workflowpb.CallbackInfo_Trigger{}
		switch callback.Trigger.Variant.(type) {
		case *persistence.CallbackInfo_Trigger_WorkflowClosed:
			trigger.Variant = &workflowpb.CallbackInfo_Trigger_WorkflowClosed{}
		}
		result.Callbacks = append(result.Callbacks, &workflowpb.CallbackInfo{
			Callback:                cbSpec,
			Trigger:                 trigger,
			RegistrationTime:        callback.RegistrationTime,
			State:                   state,
			Attempt:                 callback.Attempt,
			LastAttemptCompleteTime: callback.LastAttemptCompleteTime,
			LastAttemptFailure:      callback.LastAttemptFailure,
			NextAttemptScheduleTime: callback.NextAttemptScheduleTime,
		})
	}
	opColl := nexusoperations.MachineCollection(mutableState.HSM())
	ops := opColl.List()
	result.PendingNexusOperations = make([]*workflowpb.PendingNexusOperationInfo, 0, len(ops))
	for _, node := range ops {
		op, err := opColl.Data(node.Key.ID)
		if err != nil {
			shard.GetLogger().Error(
				"failed to load operation data while building describe response",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.WorkflowId),
				tag.WorkflowRunID(executionState.RunId),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
		}
		var state enumspb.PendingNexusOperationState
		switch op.State() {
		case enumsspb.NEXUS_OPERATION_STATE_UNSPECIFIED:
			shard.GetLogger().Error(
				"unexpected error: got an operation with an UNSPECIFIED state",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.WorkflowId),
				tag.WorkflowRunID(executionState.RunId),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
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
			continue
		}
		cancelation, err := op.Cancelation(node)
		if err != nil {
			shard.GetLogger().Error(
				"failed to load operation cancelation data while building describe response",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(executionInfo.WorkflowId),
				tag.WorkflowRunID(executionState.RunId),
				tag.Error(err),
			)
			return nil, serviceerror.NewInternal("failed to construct describe response")
		}
		var cancellationInfo *workflowpb.NexusOperationCancellationInfo
		if cancelation != nil {
			cancellationInfo = &workflowpb.NexusOperationCancellationInfo{
				RequestedTime:           cancelation.RequestedTime,
				State:                   cancelation.State(),
				Attempt:                 cancelation.Attempt,
				LastAttemptCompleteTime: cancelation.LastAttemptCompleteTime,
				LastAttemptFailure:      cancelation.LastAttemptFailure,
				NextAttemptScheduleTime: cancelation.NextAttemptScheduleTime,
			}
		}
		result.PendingNexusOperations = append(result.PendingNexusOperations, &workflowpb.PendingNexusOperationInfo{
			Endpoint:                op.Endpoint,
			Service:                 op.Service,
			Operation:               op.Operation,
			OperationId:             op.OperationId,
			ScheduleToCloseTimeout:  op.ScheduleToCloseTimeout,
			ScheduledTime:           op.ScheduledTime,
			State:                   state,
			Attempt:                 op.Attempt,
			LastAttemptCompleteTime: op.LastAttemptCompleteTime,
			LastAttemptFailure:      op.LastAttemptFailure,
			NextAttemptScheduleTime: op.NextAttemptScheduleTime,
			CancellationInfo:        cancellationInfo,
		})
	}

	return result, nil
}
