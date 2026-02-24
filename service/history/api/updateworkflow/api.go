package updateworkflow

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/effect"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	// Fail update fast if workflow task keeps failing (attempt >= 3).
	failUpdateWorkflowTaskAttemptCount = 3
)

type Updater struct {
	shardCtx                   historyi.ShardContext
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
	priority               *commonpb.Priority
	normalTaskQueueName    string
	scheduledEventID       int64
	scheduleToStartTimeout time.Duration
	workflowTaskStamp      int32
}

func NewUpdater(
	shardCtx historyi.ShardContext,
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
		wfKey,
		func(lease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			ms := lease.GetMutableState()
			updateReg := lease.GetContext().UpdateRegistry(ctx)
			return u.ApplyRequest(ctx, updateReg, ms)
		},
		nil,
		u.shardCtx,
		u.workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}

	return u.OnSuccess(ctx)
}

func (u *Updater) ApplyRequest(
	ctx context.Context,
	updateReg update.Registry,
	ms historyi.MutableState,
) (*api.UpdateWorkflowAction, error) {
	if u.req.GetRequest().GetFirstExecutionRunId() != "" &&
		ms.GetExecutionInfo().GetFirstExecutionRunId() != u.req.GetRequest().GetFirstExecutionRunId() {
		return nil, consts.ErrWorkflowExecutionNotFound
	}

	u.wfKey = ms.GetWorkflowKey()
	updateRequest := u.req.GetRequest().GetRequest()
	updateID := updateRequest.GetMeta().GetUpdateId()

	if !ms.IsWorkflowExecutionRunning() {
		// If the WF is not running anymore, use an existing Update, if it exists for the requested ID.
		// This ensures that repeated Update requests with the same ID see the same result.
		if u.upd = updateReg.Find(ctx, updateID); u.upd != nil {
			return &api.UpdateWorkflowAction{Noop: true}, nil
		}
		return nil, consts.ErrWorkflowCompleted
	}

	// We don't accept the request to update the workflow if the workflow is paused.
	if ms.IsWorkflowExecutionStatusPaused() {
		return nil, serviceerror.NewFailedPrecondition("Workflow is paused. Cannot update the workflow.")
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

	// If workflow attempted to close itself on previous WFT completion,
	// and has another WFT running, which most likely will try to complete workflow again,
	// then don't admit new updates.
	if ms.IsWorkflowCloseAttempted() && ms.HasStartedWorkflowTask() {
		u.shardCtx.GetLogger().Info("Fail update because workflow is closing.",
			tag.WorkflowNamespace(u.req.Request.Namespace),
			tag.WorkflowNamespaceID(u.wfKey.NamespaceID),
			tag.WorkflowID(u.wfKey.WorkflowID),
			tag.WorkflowRunID(u.wfKey.RunID))
		return nil, consts.ErrWorkflowClosing
	}

	var (
		alreadyExisted bool
		err            error
	)
	if u.upd, alreadyExisted, err = updateReg.FindOrCreate(ctx, updateID); err != nil {
		return nil, err
	}
	if err = u.upd.Admit(updateRequest, workflow.WithEffects(effect.Immediate(ctx), ms)); err != nil {
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

	// This will try not to add an event but will create speculative WT in mutable state.
	newWorkflowTask, err := ms.AddWorkflowTaskScheduledEvent(false, enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE)
	if err != nil {
		return nil, err
	}
	if newWorkflowTask.Type != enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		// This means that a normal WFT was created despite a speculative WFT having been requested. It implies that
		// there were buffered events. But because there was no pending WFT, there can't be buffered events. Therefore
		// this should never happen.
		return nil, consts.ErrWorkflowTaskStateInconsistent
	}

	u.scheduledEventID = newWorkflowTask.ScheduledEventID
	u.workflowTaskStamp = newWorkflowTask.Stamp
	if _, scheduleToStartTimeoutPtr := ms.TaskQueueScheduleToStartTimeout(ms.CurrentTaskQueue().Name); scheduleToStartTimeoutPtr != nil {
		u.scheduleToStartTimeout = scheduleToStartTimeoutPtr.AsDuration()
	}

	u.taskQueue = common.CloneProto(newWorkflowTask.TaskQueue)
	u.priority = common.CloneProto(ms.GetExecutionInfo().Priority)
	u.normalTaskQueueName = ms.GetExecutionInfo().TaskQueue
	u.directive = worker_versioning.MakeDirectiveForWorkflowTask(
		ms.GetInheritedBuildId(),
		ms.GetAssignedBuildId(),
		ms.GetMostRecentWorkerVersionStamp(),
		ms.HasCompletedAnyWorkflowTask(),
		ms.GetEffectiveVersioningBehavior(),
		ms.GetEffectiveDeployment(),
		ms.GetVersioningRevisionNumber(),
	)

	return &api.UpdateWorkflowAction{
		Noop:               true,
		CreateWorkflowTask: false,
	}, nil
}

func (u *Updater) OnSuccess(
	ctx context.Context,
) (*historyservice.UpdateWorkflowExecutionResponse, error) {
	usesSpeculativeWFT := u.scheduledEventID != common.EmptyEventID
	if usesSpeculativeWFT {
		// Speculative WFT was created and needs to be added directly to matching w/o transfer task.
		// TODO (alex): This code is copied from transferQueueActiveTaskExecutor.processWorkflowTask.
		//   Helper function needs to be extracted to avoid code duplication.
		err := u.addWorkflowTaskToMatching(ctx)

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
	resp := u.CreateResponse(u.wfKey, status.Outcome, status.Stage)
	return resp, nil
}

// TODO (alex-update): Consider moving this func to a better place.
func (u *Updater) addWorkflowTaskToMatching(ctx context.Context) error {
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
		Priority:               u.priority,
		Stamp:                  u.workflowTaskStamp,
	})
	if err != nil {
		return err
	}

	return nil
}

func (u *Updater) CreateResponse(
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
