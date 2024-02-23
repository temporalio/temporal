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
	"fmt"
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
	request                    *historyservice.UpdateWorkflowExecutionRequest
	namespaceID                namespace.ID
	wfKey                      definition.WorkflowKey

	// Variables shared with wfCtxOperation. Using values instead of pointers to make sure
	// they are copied and don't have any pointers to workflow context or mutable state.
	upd                    *update.Update
	taskQueue              *taskqueuepb.TaskQueue
	normalTaskQueueName    string
	scheduledEventID       int64
	scheduleToStartTimeout time.Duration
	directive              *taskqueuespb.TaskVersionDirective
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
		request:                    request,
		namespaceID:                namespace.ID(request.GetNamespaceId()),
	}
}

func (u *Updater) Invoke(
	ctx context.Context,
	workflowLease api.WorkflowLease,
) (*historyservice.UpdateWorkflowExecutionResponse, error) {
	fmt.Println("===== [API]", "updateworkflow.Invoke")

	u.wfKey = definition.NewWorkflowKey(
		u.request.NamespaceId,
		u.request.Request.WorkflowExecution.WorkflowId,
		u.request.Request.WorkflowExecution.RunId,
	)

	action := func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
		ms := workflowLease.GetMutableState()
		if !ms.IsWorkflowExecutionRunning() {
			return nil, consts.ErrWorkflowCompleted
		}

		// wfKey built from request may have blank RunID so assign a fully populated version
		u.wfKey = ms.GetWorkflowKey()

		if u.request.GetRequest().GetFirstExecutionRunId() != "" && ms.GetExecutionInfo().GetFirstExecutionRunId() != u.request.GetRequest().GetFirstExecutionRunId() {
			return nil, consts.ErrWorkflowExecutionNotFound
		}

		if ms.GetExecutionInfo().WorkflowTaskAttempt >= failUpdateWorkflowTaskAttemptCount {
			// If workflow task is constantly failing, the update to that workflow will also fail.
			// Additionally, workflow update can't "fix" workflow state because updates (delivered with messages)
			// are applied after events.
			// Failing API call fast here to prevent wasting resources for an update that will fail.
			u.shardCtx.GetLogger().Info("Fail update fast due to WorkflowTask in failed state.",
				tag.WorkflowNamespace(u.request.Request.Namespace),
				tag.WorkflowNamespaceID(u.wfKey.NamespaceID),
				tag.WorkflowID(u.wfKey.WorkflowID),
				tag.WorkflowRunID(u.wfKey.RunID))
			return nil, serviceerror.NewWorkflowNotReady("Unable to perform workflow execution update due to Workflow Task in failed state.")
		}

		updateID := u.request.GetRequest().GetRequest().GetMeta().GetUpdateId()
		updateReg := workflowLease.GetContext().UpdateRegistry(ctx)
		var (
			alreadyExisted bool
			err            error
		)
		if u.upd, alreadyExisted, err = updateReg.FindOrCreate(ctx, updateID); err != nil {
			return nil, err
		}
		if err = u.upd.Request(ctx, u.request.GetRequest().GetRequest(), workflow.WithEffects(effect.Immediate(ctx), ms)); err != nil {
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

	// TODO: abstract this into a wrapper
	var err error
	if workflowLease == nil {
		err = api.GetAndUpdateWorkflowWithNew(
			ctx,
			nil,
			api.BypassMutableStateConsistencyPredicate,
			u.wfKey,
			action,
			nil,
			u.shardCtx,
			u.workflowConsistencyChecker,
		)
	} else {
		err = api.UpdateWorkflowWithNew(
			u.shardCtx,
			ctx,
			workflowLease,
			action,
			nil,
		)
		workflowLease.GetReleaseFn()(nil) // TODO: move this
	}

	// Wrapping workflow context related operation in separate func to prevent usage of its fields
	// (including any mutable state fields) outside of this func after workflow lock is released.
	// It is important to release workflow lock before calling matching.
	if err != nil {
		// If update is received while WFT is running, it will be waiting for the next WFT.
		// And if that running WFT completes workflow, then update is rejected (see CancelIncomplete).
		// Special handling for consts.ErrWorkflowCompleted here is needed to keep parity with this.
		// I.e. if update is received and workflow was completed, or is about to be completed,
		// then update is consistently rejected (instead of returning error in some cases).
		if errors.Is(err, consts.ErrWorkflowCompleted) {
			rejectionResp := u.createResponse(
				u.request,
				&updatepb.Outcome{
					Value: &updatepb.Outcome_Failure{Failure: update.CancelReasonWorkflowCompleted.RejectionFailure()},
				},
				enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED)
			return rejectionResp, err
		}
		return nil, err
	}

	// Speculative WT was created and needs to be added directly to matching w/o transfer task.
	// TODO (alex): This code is copied from transferQueueActiveTaskExecutor.processWorkflowTask.
	//   Helper function needs to be extracted to avoid code duplication.
	if u.scheduledEventID != common.EmptyEventID {
		err = u.addWorkflowTaskToMatching(ctx)

		if _, isStickyWorkerUnavailable := err.(*serviceerrors.StickyWorkerUnavailable); isStickyWorkerUnavailable {
			// If sticky worker is unavailable, switch to original normal task queue.
			u.taskQueue = &taskqueuepb.TaskQueue{
				Name: u.normalTaskQueueName,
				Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			}
			err = u.addWorkflowTaskToMatching(ctx)
		}

		if err != nil {
			u.shardCtx.GetLogger().Warn("Unable to add WorkflowTask directly to matching.",
				tag.WorkflowNamespace(u.request.Request.Namespace),
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

	ns, err := u.shardCtx.GetNamespaceRegistry().GetNamespaceByID(u.namespaceID)
	if err != nil {
		return nil, err
	}
	serverTimeout := u.shardCtx.GetConfig().LongPollExpirationInterval(ns.Name().String())
	waitStage := u.request.GetRequest().GetWaitPolicy().GetLifecycleStage()
	// If the long-poll times out due to serverTimeout then return a non-error empty response.
	status, err := u.upd.WaitLifecycleStage(ctx, waitStage, serverTimeout)
	if err != nil {
		return nil, err
	}
	resp := u.createResponse(u.request, status.Outcome, status.Stage)
	return resp, nil
}

func (u *Updater) addWorkflowTaskToMatching(
	ctx context.Context,
) error {
	clock, err := u.shardCtx.NewVectorClock()
	if err != nil {
		return err
	}

	_, err = u.matchingClient.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
		NamespaceId: u.namespaceID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: u.wfKey.WorkflowID,
			RunId:      u.wfKey.RunID,
		},
		TaskQueue:              u.taskQueue,
		ScheduledEventId:       u.scheduledEventID,
		ScheduleToStartTimeout: durationpb.New(u.scheduleToStartTimeout),
		Clock:                  clock,
		VersionDirective:       u.directive,
	})
	if err != nil {
		return err
	}

	return nil
}

func (u *Updater) createResponse(
	req *historyservice.UpdateWorkflowExecutionRequest,
	outcome *updatepb.Outcome,
	stage enumspb.UpdateWorkflowExecutionLifecycleStage,
) *historyservice.UpdateWorkflowExecutionResponse {
	return &historyservice.UpdateWorkflowExecutionResponse{
		Response: &workflowservice.UpdateWorkflowExecutionResponse{
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: u.wfKey.WorkflowID,
					RunId:      u.wfKey.RunID,
				},
				UpdateId: req.GetRequest().GetRequest().GetMeta().GetUpdateId(),
			},
			Outcome: outcome,
			Stage:   stage,
		},
	}
}
