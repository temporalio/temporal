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
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
)

func Invoke(
	ctx context.Context,
	req *historyservice.UpdateWorkflowExecutionRequest,
	shardCtx shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	matchingClient matchingservice.MatchingServiceClient,
) (*historyservice.UpdateWorkflowExecutionResponse, error) {

	var waitLifecycleStage func(ctx context.Context, u *update.Update) (*updatepb.Outcome, error)
	waitStage := req.GetRequest().GetWaitPolicy().GetLifecycleStage()
	switch waitStage {
	case enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED:
		waitLifecycleStage = func(
			ctx context.Context,
			u *update.Update,
		) (*updatepb.Outcome, error) {
			return u.WaitAccepted(ctx)
		}
	case enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED:
		waitLifecycleStage = func(
			ctx context.Context,
			u *update.Update,
		) (*updatepb.Outcome, error) {
			return u.WaitOutcome(ctx)
		}
	default:
		return nil, serviceerror.NewUnimplemented(
			fmt.Sprintf("%v is not implemented", waitStage))
	}

	wfKey := definition.NewWorkflowKey(
		req.NamespaceId,
		req.Request.WorkflowExecution.WorkflowId,
		req.Request.WorkflowExecution.RunId,
	)

	// Variables shared with wfCtxOperation. Using values instead of pointers to make sure
	// they are copied and don't have any pointers to workflow context or mutable state.
	var (
		upd                    *update.Update
		taskQueue              taskqueuepb.TaskQueue
		scheduledEventID       int64
		scheduleToStartTimeout time.Duration
	)

	// Wrapping workflow context related operation in separate func to prevent usage of its fields
	// (including any mutable state fields) outside of this func after workflow lock is released.
	// It is important to release workflow lock before calling matching.
	wfCtxOperation := func() (retErr error) {
		weCtx, err := workflowConsistencyChecker.GetWorkflowContext(
			ctx,
			nil,
			api.BypassMutableStateConsistencyPredicate,
			wfKey,
			workflow.LockPriorityHigh,
		)
		if err != nil {
			return err
		}
		defer func() { weCtx.GetReleaseFn()(retErr) }()

		ms := weCtx.GetMutableState()
		if !ms.IsWorkflowExecutionRunning() {
			return consts.ErrWorkflowCompleted
		}

		// wfKey built from request may have blank RunID so assign a fully
		// populated version
		wfKey = ms.GetWorkflowKey()

		if req.GetRequest().GetFirstExecutionRunId() != "" && ms.GetExecutionInfo().GetFirstExecutionRunId() != req.GetRequest().GetFirstExecutionRunId() {
			return consts.ErrWorkflowExecutionNotFound
		}

		updateID := req.GetRequest().GetRequest().GetMeta().GetUpdateId()
		updateReg := weCtx.GetUpdateRegistry(ctx)
		var alreadyExisted bool
		if upd, alreadyExisted, err = updateReg.FindOrCreate(ctx, updateID); err != nil {
			return err
		}
		if err = upd.OnMessage(ctx, req.GetRequest().GetRequest(), workflow.WithEffects(effect.Immediate(ctx), ms)); err != nil {
			return err
		}

		// If WT is scheduled, but not started, updates will be attached to it, when WT is started.
		// If WT has already started, new speculative WT will be created when started WT completes.
		// If update is duplicate, then WT for this update was already created.
		createNewWorkflowTask := !ms.HasPendingWorkflowTask() && !alreadyExisted

		if createNewWorkflowTask {
			// This will try not to add an event but will create speculative WT in mutable state.
			newWorkflowTask, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)
			if err != nil {
				return err
			}
			if newWorkflowTask.Type != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
				// This should never happen because WT is created as normal (despite speculative is requested)
				// only if there were buffered events and because there were no pending WT, there can't be buffered events.
				return consts.ErrWorkflowTaskStateInconsistent
			}

			scheduledEventID = newWorkflowTask.ScheduledEventID
			if _, scheduleToStartTimeoutPtr := ms.TaskQueueScheduleToStartTimeout(ms.CurrentTaskQueue().Name); scheduleToStartTimeoutPtr != nil {
				scheduleToStartTimeout = *scheduleToStartTimeoutPtr
			}
			taskQueue = taskqueuepb.TaskQueue{
				Name: newWorkflowTask.TaskQueue.Name,
				Kind: newWorkflowTask.TaskQueue.Kind,
			}
		}
		return nil
	}
	err := wfCtxOperation()
	if err != nil {
		return nil, err
	}

	// WT was created.
	if scheduledEventID != common.EmptyEventID {
		err = addWorkflowTaskToMatching(ctx, wfKey, &taskQueue, scheduledEventID, &scheduleToStartTimeout, namespace.ID(req.GetNamespaceId()), shardCtx, matchingClient)
		if err != nil {
			return nil, err
		}
	}

	updOutcome, err := waitLifecycleStage(ctx, upd)
	if err != nil {
		return nil, err
	}
	resp := &historyservice.UpdateWorkflowExecutionResponse{
		Response: &workflowservice.UpdateWorkflowExecutionResponse{
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: wfKey.WorkflowID,
					RunId:      wfKey.RunID,
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
	wfKey definition.WorkflowKey,
	tq *taskqueuepb.TaskQueue,
	scheduledEventID int64,
	wtScheduleToStartTimeout *time.Duration,
	nsID namespace.ID,
	shardCtx shard.Context,
	matchingClient matchingservice.MatchingServiceClient,
) error {
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
		TaskQueue:              tq,
		ScheduledEventId:       scheduledEventID,
		ScheduleToStartTimeout: wtScheduleToStartTimeout,
		Clock:                  clock,
	})
	if err != nil {
		return err
	}

	return nil
}
