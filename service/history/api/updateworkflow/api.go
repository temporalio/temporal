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
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/internal/effect"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
)

const (
	// Fail update fast if workflow task keeps failing (attempt >= 3).
	failUpdateWorkflowTaskAttemptCount = 3
)

type Updater struct {
	shardCtx                   shard.Context
	workflowConsistencyChecker api.WorkflowConsistencyChecker
	matchingClient             matchingservice.MatchingServiceClient
	req                        *historyservice.UpdateWorkflowExecutionRequest
	namespaceID                namespace.ID

	wfKey     definition.WorkflowKey
	upd       *update.Update
	directive *taskqueuespb.TaskVersionDirective

	// Variables referencing mutable state data.
	// WARNING: any references to mutable state data *have to* be copied
	// to avoid data races when used outside the workflow lease.
	taskQueue              *taskqueuepb.TaskQueue
	normalTaskQueueName    string
	scheduledEventID       int64
	scheduleToStartTimeout time.Duration
}

func NewUpdater(
	shardCtx shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	matchingClient matchingservice.MatchingServiceClient,
	request *historyservice.UpdateWorkflowExecutionRequest,
) *Updater {
	return &Updater{
		shardCtx:                   shardCtx,
		workflowConsistencyChecker: workflowConsistencyChecker,
		matchingClient:             matchingClient,
		req:                        request,
		namespaceID:                namespace.ID(request.GetNamespaceId()),
	}
}

func (u *Updater) Invoke(
	ctx context.Context,
) (*historyservice.UpdateWorkflowExecutionResponse, error) {
	wfKey := definition.NewWorkflowKey(
		u.req.NamespaceId,
		u.req.Request.WorkflowExecution.WorkflowId,
		u.req.Request.WorkflowExecution.RunId,
	)

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		wfKey,
		func(lease api.WorkflowLease) (*api.UpdateWorkflowAction, error) { return u.Apply(ctx, lease) },
		nil,
		u.shardCtx,
		u.workflowConsistencyChecker,
	)

	if err != nil {
		return u.OnError(err)
	}
	return u.OnSuccess(ctx)
}

func (u *Updater) Apply(
	ctx context.Context,
	workflowLease api.WorkflowLease,
) (*api.UpdateWorkflowAction, error) {
	ms := workflowLease.GetMutableState()
	if !ms.IsWorkflowExecutionRunning() {
		return nil, consts.ErrWorkflowCompleted
	}
	u.wfKey = ms.GetWorkflowKey()

	if u.req.GetRequest().GetFirstExecutionRunId() != "" && ms.GetExecutionInfo().GetFirstExecutionRunId() != u.req.GetRequest().GetFirstExecutionRunId() {
		return nil, consts.ErrWorkflowExecutionNotFound
	}

	if ms.GetExecutionInfo().WorkflowTaskAttempt >= failUpdateWorkflowTaskAttemptCount {
		// If workflow task is constantly failing, the update to that workflow will also fail.
		// Additionally, workflow update can't "fix" workflow state because updates (delivered with messages)
		// are applied after events.
		// Failing API call fast here to prevent wasting resources for an update that will fail.
		u.shardCtx.GetLogger().Info("Fail update fast due to WorkflowTask in failed state.",
			tag.WorkflowNamespace(u.req.Request.Namespace),
			tag.WorkflowNamespaceID(u.wfKey.NamespaceID),
			tag.WorkflowID(u.wfKey.WorkflowID),
			tag.WorkflowRunID(u.wfKey.RunID))
		return nil, serviceerror.NewWorkflowNotReady("Unable to perform workflow execution update due to Workflow Task in failed state.")
	}

	updateID := u.req.GetRequest().GetRequest().GetMeta().GetUpdateId()
	updateReg := workflowLease.GetUpdateRegistry(ctx)
	var (
		alreadyExisted bool
		err            error
	)
	if u.upd, alreadyExisted, err = updateReg.FindOrCreate(ctx, updateID); err != nil {
		return nil, err
	}
	if err = u.upd.Request(ctx, u.req.GetRequest().GetRequest(), workflow.WithEffects(effect.Immediate(ctx), ms)); err != nil {
		return nil, err
	}

	// If WT is scheduled, but not started, updates will be attached to it, when WT is started.
	// If WT has already started, new speculative WT will be created when started WT completes.
	// If update is duplicate, then WT for this update was already created.
	if alreadyExisted || ms.HasPendingWorkflowTask() {
		return &api.UpdateWorkflowAction{
			Noop:               true,
			CreateWorkflowTask: false,
		}, nil
	}

	// Speculative WT can be created only if there are no events which worker has not yet seen,
	// i.e. last event in the history is WTCompleted event.
	// It is guaranteed that WTStarted event is followed by WTCompleted event and history tail might look like:
	//   WTStarted
	//   WTCompleted
	//   --> NextEventID points here
	// In this case difference between NextEventID and LastWorkflowTaskStartedEventID is 2.
	// If there are other events after WTCompleted event, then difference is > 2 and speculative WT can't be created.
	canCreateSpeculativeWT := ms.GetNextEventID() == ms.GetLastWorkflowTaskStartedEventID()+2
	if !canCreateSpeculativeWT {
		return &api.UpdateWorkflowAction{
			Noop:               false,
			CreateWorkflowTask: true,
		}, nil
	}

	// This will try not to add an event but will create speculative WT in mutable state.
	newWorkflowTask, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)
	if err != nil {
		return nil, err
	}
	if newWorkflowTask.Type != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		// This should never happen because WT is created as normal (despite speculative is requested)
		// only if there were buffered events and because there were no pending WT, there can't be buffered events.
		return nil, consts.ErrWorkflowTaskStateInconsistent
	}

	u.scheduledEventID = newWorkflowTask.ScheduledEventID
	if _, scheduleToStartTimeoutPtr := ms.TaskQueueScheduleToStartTimeout(ms.CurrentTaskQueue().Name); scheduleToStartTimeoutPtr != nil {
		u.scheduleToStartTimeout = scheduleToStartTimeoutPtr.AsDuration()
	}

	u.taskQueue = common.CloneProto(newWorkflowTask.TaskQueue)
	u.normalTaskQueueName = ms.GetExecutionInfo().TaskQueue
	u.directive = worker_versioning.MakeDirectiveForWorkflowTask(
		ms.GetWorkerVersionStamp(),
		ms.GetLastWorkflowTaskStartedEventID(),
	)

	return &api.UpdateWorkflowAction{
		Noop:               true,
		CreateWorkflowTask: false,
	}, nil
}

func (u *Updater) OnError(
	err error,
) (*historyservice.UpdateWorkflowExecutionResponse, error) {
	// Special handling for consts.ErrWorkflowCompleted here is needed for consistency with the case when update is received while WFT is running and this WFT completes workflow. In this case update is rejected (see update.CancelIncomplete).
	if errors.Is(err, consts.ErrWorkflowCompleted) {
		rejectionResp := u.createResponse(
			u.wfKey,
			&updatepb.Outcome{
				Value: &updatepb.Outcome_Failure{Failure: update.CancelReasonWorkflowCompleted.RejectionFailure()},
			},
			enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED)
		return rejectionResp, err
	}
	return nil, err
}

func (u *Updater) OnSuccess(
	ctx context.Context,
) (*historyservice.UpdateWorkflowExecutionResponse, error) {
	// Speculative WT was created and needs to be added directly to matching w/o transfer task.
	// TODO (alex): This code is copied from transferQueueActiveTaskExecutor.processWorkflowTask.
	//   Helper function needs to be extracted to avoid code duplication.
	if u.scheduledEventID != common.EmptyEventID {
		err := u.addWorkflowTaskToMatching(ctx, u.wfKey, u.taskQueue, u.scheduledEventID, u.scheduleToStartTimeout, u.directive)

		if _, isStickyWorkerUnavailable := err.(*serviceerrors.StickyWorkerUnavailable); isStickyWorkerUnavailable {
			// If sticky worker is unavailable, switch to original normal task queue.
			u.taskQueue = &taskqueuepb.TaskQueue{
				Name: u.normalTaskQueueName,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			}
			err = u.addWorkflowTaskToMatching(ctx, u.wfKey, u.taskQueue, u.scheduledEventID, u.scheduleToStartTimeout, u.directive)
		}

		if err != nil {
			u.shardCtx.GetLogger().Warn("Unable to add WorkflowTask directly to matching.",
				tag.WorkflowNamespace(u.req.Request.Namespace),
				tag.WorkflowNamespaceID(u.wfKey.NamespaceID),
				tag.WorkflowID(u.wfKey.WorkflowID),
				tag.WorkflowRunID(u.wfKey.RunID),
				tag.Error(err))

			// Intentionally just log error here and don't return it to the client.
			// If adding speculative WT to matching failed with error,
			// this error can't be handled outside of WF lock and can't be returned to the client (because it is not a client error).
			// This speculative WT will be timed out in tasks.SpeculativeWorkflowTaskScheduleToStartTimeout (5) seconds,
			// and new normal WT will be scheduled.
			// If subsequent attempt succeeds within current context timeout, caller of this API will get a valid response.
			err = nil
		}
	}

	namespaceID := namespace.ID(u.req.GetNamespaceId())
	ns, err := u.shardCtx.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}
	serverTimeout := u.shardCtx.GetConfig().LongPollExpirationInterval(ns.Name().String())
	waitStage := u.req.GetRequest().GetWaitPolicy().GetLifecycleStage()
	// If the long-poll times out due to serverTimeout then return a non-error empty response.
	status, err := u.upd.WaitLifecycleStage(ctx, waitStage, serverTimeout)
	if err != nil {
		return nil, err
	}
	resp := u.createResponse(u.wfKey, status.Outcome, status.Stage)
	return resp, nil
}

// TODO (alex-update): Consider moving this func to a better place.
func (u *Updater) addWorkflowTaskToMatching(
	ctx context.Context,
	wfKey definition.WorkflowKey,
	tq *taskqueuepb.TaskQueue,
	scheduledEventID int64,
	wtScheduleToStartTimeout time.Duration,
	directive *taskqueuespb.TaskVersionDirective,
) error {
	clock, err := u.shardCtx.NewVectorClock()
	if err != nil {
		return err
	}

	_, err = u.matchingClient.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
		NamespaceId: u.namespaceID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: wfKey.WorkflowID,
			RunId:      wfKey.RunID,
		},
		TaskQueue:              tq,
		ScheduledEventId:       scheduledEventID,
		ScheduleToStartTimeout: durationpb.New(wtScheduleToStartTimeout),
		Clock:                  clock,
		VersionDirective:       directive,
	})
	if err != nil {
		return err
	}

	return nil
}

func (u *Updater) createResponse(
	wfKey definition.WorkflowKey,
	outcome *updatepb.Outcome,
	stage enumspb.UpdateWorkflowExecutionLifecycleStage,
) *historyservice.UpdateWorkflowExecutionResponse {
	return &historyservice.UpdateWorkflowExecutionResponse{
		Response: &workflowservice.UpdateWorkflowExecutionResponse{
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: wfKey.WorkflowID,
					RunId:      wfKey.RunID,
				},
				UpdateId: u.req.GetRequest().GetRequest().GetMeta().GetUpdateId(),
			},
			Outcome: outcome,
			Stage:   stage,
		},
	}
}
