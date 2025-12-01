package history

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow/cache"
)

func updateIndependentActivityBuildId(
	ctx context.Context,
	task tasks.Task,
	buildId string,
	shardContext historyi.ShardContext,
	transactionPolicy historyi.TransactionPolicy,
	workflowCache cache.Cache,
	metricsHandler metrics.Handler,
	logger log.Logger,
) (retErr error) {
	if buildId == "" {
		// the task is sync-matched, or versioning is not enabled for this Task Queue
		return nil
	}

	defer func() {
		if retErr != nil {
			// An independent task is scheduled but we could not persist the assigned build ID in mutable state.
			// This is only an issue for user visibility: until the task is started user would not see the assigned
			// build ID.
			// Since it does not affect WF progress, just logging warn and skipping the error.
			// TODO: let the error bubble up so the task is rescheduled and build ID is fully updated
			logger.Warn("failed to update activity's assigned build ID", append(tasks.Tags(task), tag.Error(retErr))...)
		}
		retErr = nil
	}()

	// We should reach to this point only when all the following conditions are true:
	// - assignment rules are added for the Task Queue (i.e. worker versioning is enabled)
	// - sync-match did not happen for this task
	// - the activity is on a different task queue (or otherwise asked to be independently assigned to a build ID)
	weContext, release, err := getWorkflowExecutionContextForTask(ctx, shardContext, workflowCache, task)
	if err != nil {
		return err
	}

	defer func() {
		release(retErr)
	}()

	var mutableState historyi.MutableState
	var scheduledEventId int64
	var taskVersion int64

	switch t := task.(type) {
	case *tasks.ActivityTask:
		scheduledEventId = t.ScheduledEventID
		taskVersion = t.Version
		mutableState, err = loadMutableStateForTransferTask(ctx, shardContext, weContext, t, metricsHandler, logger)
	case *tasks.ActivityRetryTimerTask:
		scheduledEventId = t.EventID
		taskVersion = t.Version
		mutableState, err = loadMutableStateForTimerTask(ctx, shardContext, weContext, t, metricsHandler, logger)
	default:
		return serviceerror.NewInvalidArgument("task type not supported")
	}

	if err != nil {
		return err
	}

	if mutableState == nil {
		return consts.ErrWorkflowExecutionNotFound
	}

	ai, activityRunning := mutableState.GetActivityInfo(scheduledEventId)
	if !activityRunning {
		// We did not find the activity task, it means that it's already completed.
		return nil
	}

	err = CheckTaskVersion(shardContext, logger, mutableState.GetNamespaceEntry(), ai.Version, taskVersion, task)
	if err != nil {
		return err
	}

	err = mutableState.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
		activityInfo.BuildIdInfo = &persistencespb.ActivityInfo_LastIndependentlyAssignedBuildId{LastIndependentlyAssignedBuildId: buildId}
		return nil
	})
	if err != nil {
		return err
	}

	return weContext.UpdateWorkflowExecutionWithNew(
		ctx,
		shardContext,
		persistence.UpdateWorkflowModeUpdateCurrent,
		nil,
		nil,
		transactionPolicy,
		nil,
	)
}

// initializeWorkflowAssignedBuildId sets the wf assigned build ID after scheduling the first workflow task, based on
// the build ID returned by matching.
func initializeWorkflowAssignedBuildId(
	ctx context.Context,
	transferTask *tasks.WorkflowTask,
	buildId string,
	shardContext historyi.ShardContext,
	transactionPolicy historyi.TransactionPolicy,
	workflowCache cache.Cache,
	metricsHandler metrics.Handler,
	logger log.Logger,
) (retErr error) {
	if buildId == "" {
		// the task is sync-matched, or versioning is not enabled for this Task Queue
		return nil
	}

	defer func() {
		if retErr != nil {
			// The first workflow task is scheduled but we could not persist the assigned build ID in mutable state.
			// This is only an issue for user visibility: until the task is started user would not see the assigned
			// build ID.
			// Since it does not affect WF progress, just logging warn and skipping the error.
			// TODO: let the error bubble up so the task is rescheduled and build ID is fully updated
			logger.Error("failed to update workflow's assigned build ID", append(tasks.Tags(transferTask), tag.Error(retErr))...)
		}
		retErr = nil
	}()

	// We should reach to this point only when all the following conditions are true:
	// - assignment rules are added for the Task Queue (i.e. worker versioning is enabled)
	// - sync-match did not happen for this task
	// - this is the first workflow task of the execution
	// - the workflow has not inherited a build ID (for child WF or CaN)
	weContext, release, err := getWorkflowExecutionContextForTask(ctx, shardContext, workflowCache, transferTask)
	if err != nil {
		return err
	}

	mutableState, err := loadMutableStateForTransferTask(ctx, shardContext, weContext, transferTask, metricsHandler, logger)
	if err != nil {
		release(err)
		return err
	}
	defer func() {
		release(retErr)
	}()

	if mutableState == nil {
		return consts.ErrWorkflowExecutionNotFound
	}

	workflowTask := mutableState.GetWorkflowTaskByID(transferTask.ScheduledEventID)
	if workflowTask == nil {
		return nil
	}
	err = CheckTaskVersion(shardContext, logger, mutableState.GetNamespaceEntry(), workflowTask.Version, transferTask.Version, transferTask)
	if err != nil {
		return err
	}

	if mutableState.HasCompletedAnyWorkflowTask() || workflowTask.StartedEventID != common.EmptyEventID {
		// workflow task is running or already completed. buildId is potentially stale and useless.
		// workflow's assigned build ID should be already updated via RecordWorkflowTaskStarted
		return nil
	}

	err = mutableState.UpdateBuildIdAssignment(buildId)
	if err != nil {
		return err
	}

	return weContext.UpdateWorkflowExecutionWithNew(
		ctx,
		shardContext,
		persistence.UpdateWorkflowModeUpdateCurrent,
		nil,
		nil,
		transactionPolicy,
		nil,
	)
}

func MakeDirectiveForWorkflowTask(ms historyi.MutableState) *taskqueuespb.TaskVersionDirective {
	return worker_versioning.MakeDirectiveForWorkflowTask(
		ms.GetInheritedBuildId(),
		ms.GetAssignedBuildId(),
		ms.GetMostRecentWorkerVersionStamp(),
		ms.HasCompletedAnyWorkflowTask(),
		ms.GetEffectiveVersioningBehavior(),
		ms.GetEffectiveDeployment(),
		ms.GetVersioningRevisionNumber(),
	)
}

func MakeDirectiveForActivityTask(mutableState historyi.MutableState, activityInfo *persistencespb.ActivityInfo) *taskqueuespb.TaskVersionDirective {
	if behavior := mutableState.GetEffectiveVersioningBehavior(); behavior != enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED {
		d := mutableState.GetEffectiveDeployment()
		return &taskqueuespb.TaskVersionDirective{Behavior: behavior,
			DeploymentVersion: worker_versioning.DeploymentVersionFromDeployment(d),
			RevisionNumber:    mutableState.GetVersioningRevisionNumber(),
		}
	}
	if !activityInfo.UseCompatibleVersion && activityInfo.GetUseWorkflowBuildIdInfo() == nil {
		return worker_versioning.MakeUseAssignmentRulesDirective()
	} else if id := mutableState.GetAssignedBuildId(); id != "" {
		return worker_versioning.MakeBuildIdDirective(id)
	} else if id := worker_versioning.BuildIdIfUsingVersioning(mutableState.GetMostRecentWorkerVersionStamp()); id != "" {
		// TODO: old versioning only [cleanup-old-wv]
		return worker_versioning.MakeBuildIdDirective(id)
	}
	// else: unversioned execution
	return nil
}
