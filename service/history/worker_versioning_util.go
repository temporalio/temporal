package history

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
)

func updateIndependentActivityBuildId(
	ctx context.Context,
	task tasks.Task,
	scheduledEventId int64,
	buildId string,
	shardContext shard.Context,
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
			logger.Warn("failed to update activity's assigned build ID", tag.Error(retErr))
		}
		retErr = nil
	}()

	weContext, release, err := getWorkflowExecutionContextForTask(ctx, shardContext, workflowCache, task)
	if err != nil {
		return err
	}

	mutableState, err := loadMutableStateForTransferTask(ctx, shardContext, weContext, task, metricsHandler, logger)
	if err != nil {
		release(err)
		return err
	}
	defer func() {
		// release(nil) so that the mutable state is not unloaded from cache
		release(nil)
	}()

	if mutableState == nil {
		return consts.ErrWorkflowExecutionNotFound
	}

	ai, activityRunning := mutableState.GetActivityInfo(scheduledEventId)
	if !activityRunning {
		// We did not find the activity task, it means that it's already completed.
		return nil
	}

	if ai.ScheduledEventId != scheduledEventId {
		// this scheduled event is stale now, so don't write build ID
		// The build ID should be already updated via RecordActivityTaskStarted
		return nil
	}

	ai.AssignedBuildId = &persistencespb.ActivityInfo_LastIndependentlyAssignedBuildId{LastIndependentlyAssignedBuildId: buildId}
	err = mutableState.UpdateActivity(ai)
	if err != nil {
		return err
	}

	return weContext.UpdateWorkflowExecutionAsActive(ctx, shardContext)
}

func updateWorkflowAssignedBuildId(
	ctx context.Context,
	transferTask *tasks.WorkflowTask,
	buildId string,
	shardContext shard.Context,
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
			logger.Error("failed to update workflow's assigned build ID", tag.Error(retErr))
		}
		retErr = nil
	}()

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
		// release(nil) so that the mutable state is not unloaded from cache
		release(nil)
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

	workflowTaskStarted := mutableState.GetLastWorkflowTaskStartedEventID() != common.EmptyEventID

	if workflowTaskStarted {
		// this scheduled event is stale now, so don't write build ID here.
		// The build ID should be already updated via RecordWorkflowTaskStarted
		return nil
	}

	err = mutableState.UpdateBuildIdAssignment(buildId)
	if err != nil {
		return err
	}
	return weContext.UpdateWorkflowExecutionAsActive(ctx, shardContext)
}

func MakeDirectiveForActivityTask(mutableState workflow.MutableState, activityInfo *persistencespb.ActivityInfo) *taskqueuespb.TaskVersionDirective {
	if !activityInfo.UseCompatibleVersion || activityInfo.GetUseWorkflowBuildId() == nil {
		return worker_versioning.MakeUseDefaultDirective()
	} else if id := mutableState.GetAssignedBuildId(); id != "" {
		return worker_versioning.MakeBuildIdDirective(id)
	} else if id := worker_versioning.StampIfUsingVersioning(mutableState.GetMostRecentWorkerVersionStamp()).GetBuildId(); id != "" {
		// TODO: old versioning only [cleanup-old-wv]
		return worker_versioning.MakeBuildIdDirective(id)
	}
	// else: unversioned execution
	return nil
}
