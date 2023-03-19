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

package updateworkflow

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.UpdateWorkflowExecutionRequest,
	shardCtx shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	matchingClient matchingservice.MatchingServiceClient,
) (_ *historyservice.UpdateWorkflowExecutionResponse, retErr error) {

	weCtx, err := workflowConsistencyChecker.GetWorkflowContext(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			req.NamespaceId,
			req.Request.WorkflowExecution.WorkflowId,
			req.Request.WorkflowExecution.RunId,
		),
	)
	if err != nil {
		return nil, err
	}
	defer func() { weCtx.GetReleaseFn()(retErr) }()

	ms := weCtx.GetMutableState()
	if !ms.IsWorkflowExecutionRunning() {
		return nil, consts.ErrWorkflowCompleted
	}

	if req.GetRequest().GetFirstExecutionRunId() != "" && ms.GetExecutionInfo().GetFirstExecutionRunId() != req.GetRequest().GetFirstExecutionRunId() {
		return nil, consts.ErrWorkflowExecutionNotFound
	}

	upd, duplicate, removeFn := weCtx.GetContext().UpdateRegistry().Add(req.GetRequest().GetRequest())
	if removeFn != nil {
		defer removeFn()
	}

	// If WT is scheduled, but not started, updates will be attached to it, when WT is started.
	// If WT has already started, new speculative WT will be created when started WT completes.
	// If update is duplicate, then WT for this update was already created.
	createNewWorkflowTask := !ms.HasPendingWorkflowTask() && duplicate == false

	if createNewWorkflowTask {
		// This will try not to add an event but will create speculative WT in mutable state.
		// Task generation will be skipped if WT is created as speculative.
		wt, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)
		if err != nil {
			return nil, err
		}
		if wt.Type != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
			// This should never happen because WT is created as normal (despite speculative is requested)
			// only if there were buffered events and because there were no pending WT, there can't be buffered events.
			return nil, consts.ErrWorkflowTaskStateInconsistent
		}

		// It is important to release workflow lock before calling matching.
		weCtx.GetReleaseFn()(nil)
		err = addWorkflowTaskToMatching(ctx, shardCtx, ms, matchingClient, wt, namespace.ID(req.GetNamespaceId()))
		if err != nil {
			return nil, err
		}
	} else {
		weCtx.GetReleaseFn()(nil)
	}

	updOutcome, err := upd.WaitOutcome(ctx)
	if err != nil {
		return nil, err
	}
	resp := &historyservice.UpdateWorkflowExecutionResponse{
		Response: &workflowservice.UpdateWorkflowExecutionResponse{
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: weCtx.GetWorkflowKey().WorkflowID,
					RunId:      weCtx.GetWorkflowKey().RunID,
				},
				UpdateId: req.GetRequest().GetRequest().GetMeta().GetUpdateId(),
			},
			Outcome: updOutcome,
		},
	}

	return resp, nil
}

// TODO (alex-update): Consider moving this func to a better place.
func addWorkflowTaskToMatching(
	ctx context.Context,
	shardCtx shard.Context,
	ms workflow.MutableState,
	matchingClient matchingservice.MatchingServiceClient,
	task *workflow.WorkflowTaskInfo,
	nsID namespace.ID,
) error {
	// TODO (alex-update): Timeout calculation is copied from somewhere else. Extract func instead?
	var taskScheduleToStartTimeout *time.Duration
	if ms.TaskQueue().GetName() != task.TaskQueue.GetName() {
		taskScheduleToStartTimeout = ms.GetExecutionInfo().StickyScheduleToStartTimeout
	} else {
		taskScheduleToStartTimeout = ms.GetExecutionInfo().WorkflowRunTimeout
	}

	wfKey := ms.GetWorkflowKey()
	clock, err := shardCtx.NewVectorClock()
	if err != nil {
		return err
	}

	_, err = matchingClient.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
		NamespaceId: nsID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wfKey.WorkflowID,
			RunId:      wfKey.RunID,
		},
		TaskQueue:              task.TaskQueue,
		ScheduledEventId:       task.ScheduledEventID,
		ScheduleToStartTimeout: taskScheduleToStartTimeout,
		Clock:                  clock,
	})
	if err != nil {
		return err
	}

	return nil
}
