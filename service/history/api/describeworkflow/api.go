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

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

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

	weCtx, err := workflowConsistencyChecker.GetWorkflowContext(
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
	defer func() { weCtx.GetReleaseFn()(retError) }()

	mutableState := weCtx.GetMutableState()
	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()
	result := &historyservice.DescribeWorkflowExecutionResponse{
		ExecutionConfig: &workflowpb.WorkflowExecutionConfig{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: executionInfo.TaskQueue,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			},
			WorkflowExecutionTimeout:   executionInfo.WorkflowExecutionTimeout,
			WorkflowRunTimeout:         executionInfo.WorkflowRunTimeout,
			DefaultWorkflowTaskTimeout: executionInfo.DefaultWorkflowTaskTimeout,
		},
		WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: executionInfo.WorkflowId,
				RunId:      executionState.RunId,
			},
			Type:                 &commonpb.WorkflowType{Name: executionInfo.WorkflowTypeName},
			StartTime:            executionInfo.StartTime,
			Status:               executionState.Status,
			HistoryLength:        mutableState.GetNextEventID() - common.FirstEventID,
			ExecutionTime:        executionInfo.ExecutionTime,
			Memo:                 &commonpb.Memo{Fields: executionInfo.Memo},
			SearchAttributes:     &commonpb.SearchAttributes{IndexedFields: executionInfo.SearchAttributes},
			AutoResetPoints:      executionInfo.AutoResetPoints,
			TaskQueue:            executionInfo.TaskQueue,
			StateTransitionCount: executionInfo.StateTransitionCount,
			HistorySizeBytes:     executionInfo.GetExecutionStats().GetHistorySize(),

			MostRecentWorkerVersionStamp: executionInfo.WorkerVersionStamp,
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
		result.WorkflowExecutionInfo.CloseTime = closeTime
	}

	if len(mutableState.GetPendingActivityInfos()) > 0 {
		for _, ai := range mutableState.GetPendingActivityInfos() {
			p := &workflowpb.PendingActivityInfo{
				ActivityId: ai.ActivityId,
			}
			if ai.CancelRequested {
				p.State = enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
			} else if ai.StartedEventId != common.EmptyEventID {
				p.State = enumspb.PENDING_ACTIVITY_STATE_STARTED
			} else {
				p.State = enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
			}
			if !timestamp.TimeValue(ai.LastHeartbeatUpdateTime).IsZero() {
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
	}

	if len(mutableState.GetPendingChildExecutionInfos()) > 0 {
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
	}

	if pendingWorkflowTask := mutableState.GetPendingWorkflowTask(); pendingWorkflowTask != nil {
		result.PendingWorkflowTask = &workflowpb.PendingWorkflowTaskInfo{
			State:                 enumspb.PENDING_WORKFLOW_TASK_STATE_SCHEDULED,
			ScheduledTime:         pendingWorkflowTask.ScheduledTime,
			OriginalScheduledTime: pendingWorkflowTask.OriginalScheduledTime,
			Attempt:               pendingWorkflowTask.Attempt,
		}
		if pendingWorkflowTask.StartedEventID != common.EmptyEventID {
			result.PendingWorkflowTask.State = enumspb.PENDING_WORKFLOW_TASK_STATE_STARTED
			result.PendingWorkflowTask.StartedTime = pendingWorkflowTask.StartedTime
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
	result.WorkflowExecutionInfo.Memo = relocatableAttributes.Memo
	result.WorkflowExecutionInfo.SearchAttributes = relocatableAttributes.SearchAttributes

	return result, nil
}
