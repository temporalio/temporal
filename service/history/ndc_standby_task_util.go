package history

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/priorities"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

type (
	standbyActionFn     func(context.Context, historyi.WorkflowContext, historyi.MutableState) (interface{}, error)
	standbyPostActionFn func(context.Context, tasks.Task, interface{}, log.Logger) error

	standbyCurrentTimeFn func() time.Time
)

func standbyTaskPostActionNoOp(
	_ context.Context,
	_ tasks.Task,
	postActionInfo interface{},
	_ log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	if err, ok := postActionInfo.(error); ok {
		return err
	}

	// return error so task processing logic will retry
	return consts.ErrTaskRetry
}

func standbyTransferTaskPostActionTaskDiscarded(
	_ context.Context,
	taskInfo tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	logger.Warn("Discarding standby transfer task due to task being pending for too long.", tag.Task(taskInfo))
	return consts.ErrTaskDiscarded
}

func standbyTimerTaskPostActionTaskDiscarded(
	_ context.Context,
	taskInfo tasks.Task,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	logger.Warn("Discarding standby timer task due to task being pending for too long.", tag.Task(taskInfo))
	return consts.ErrTaskDiscarded
}

func executionExistsOnSource(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	archetypeID chasm.ArchetypeID,
	logger log.Logger,
	currentCluster string,
	clientBean client.Bean,
	registry namespace.Registry,
	chasmRegistry *chasm.Registry,
) bool {
	namespaceEntry, err := registry.GetNamespaceByID(namespace.ID(workflowKey.NamespaceID))
	if err != nil {
		return true
	}

	archetype, ok := chasmRegistry.ComponentFqnByID(archetypeID)
	if !ok {
		logger.Error("Unknown archetype ID.",
			tag.ArchetypeID(archetypeID),
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID),
		)
		return true
	}

	remoteClusterName, err := getSourceClusterName(
		currentCluster,
		registry,
		workflowKey.GetNamespaceID(),
		workflowKey.GetWorkflowID(),
	)
	if err != nil {
		return true
	}
	remoteAdminClient, err := clientBean.GetRemoteAdminClient(remoteClusterName)
	if err != nil {
		return true
	}
	_, err = remoteAdminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: namespaceEntry.Name().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowKey.GetWorkflowID(),
			RunId:      workflowKey.GetRunID(),
		},
		Archetype:       archetype,
		SkipForceReload: true,
	})
	if err != nil {
		if common.IsNotFoundError(err) {
			return false
		}
		logger.Error("Error describe mutable state from remote.",
			tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
			tag.WorkflowID(workflowKey.GetWorkflowID()),
			tag.WorkflowRunID(workflowKey.GetRunID()),
			tag.ClusterName(remoteClusterName),
			tag.Error(err))
	}
	return true
}

type (
	executionTimerPostActionInfo struct {
		currentRunID string
	}

	activityTaskPostActionInfo struct {
		taskQueue                          string
		activityTaskScheduleToStartTimeout time.Duration
		versionDirective                   *taskqueuespb.TaskVersionDirective
		priority                           *commonpb.Priority
	}

	verifyCompletionRecordedPostActionInfo struct {
		parentWorkflowKey *definition.WorkflowKey
	}

	workflowTaskPostActionInfo struct {
		workflowTaskScheduleToStartTimeout time.Duration
		taskqueue                          *taskqueuepb.TaskQueue
		versionDirective                   *taskqueuespb.TaskVersionDirective
		priority                           *commonpb.Priority
	}
)

func newExecutionTimerPostActionInfo(
	mutableState historyi.MutableState,
) (*executionTimerPostActionInfo, error) {
	return &executionTimerPostActionInfo{
		currentRunID: mutableState.GetExecutionState().RunId,
	}, nil
}

func newActivityTaskPostActionInfo(
	mutableState historyi.MutableState,
	activityInfo *persistencespb.ActivityInfo,
) (*activityTaskPostActionInfo, error) {
	directive := MakeDirectiveForActivityTask(mutableState, activityInfo)
	priority := priorities.Merge(mutableState.GetExecutionInfo().Priority, activityInfo.Priority)

	return &activityTaskPostActionInfo{
		activityTaskScheduleToStartTimeout: activityInfo.ScheduleToStartTimeout.AsDuration(),
		versionDirective:                   directive,
		priority:                           priority,
	}, nil
}

func newActivityRetryTimePostActionInfo(
	mutableState historyi.MutableState,
	taskQueue string,
	activityScheduleToStartTimeout time.Duration,
	activityInfo *persistencespb.ActivityInfo,
) (*activityTaskPostActionInfo, error) {
	directive := MakeDirectiveForActivityTask(mutableState, activityInfo)
	priority := priorities.Merge(mutableState.GetExecutionInfo().Priority, activityInfo.Priority)

	return &activityTaskPostActionInfo{
		taskQueue:                          taskQueue,
		activityTaskScheduleToStartTimeout: activityScheduleToStartTimeout,
		versionDirective:                   directive,
		priority:                           priority,
	}, nil
}

func newWorkflowTaskPostActionInfo(
	mutableState historyi.MutableState,
	workflowTaskScheduleToStartTimeout time.Duration,
	taskqueue *taskqueuepb.TaskQueue,
) (*workflowTaskPostActionInfo, error) {
	directive := MakeDirectiveForWorkflowTask(mutableState)
	priority := mutableState.GetExecutionInfo().Priority

	return &workflowTaskPostActionInfo{
		workflowTaskScheduleToStartTimeout: workflowTaskScheduleToStartTimeout,
		taskqueue:                          taskqueue,
		versionDirective:                   directive,
		priority:                           priority,
	}, nil
}

func getStandbyPostActionFn(
	taskInfo tasks.Task,
	standbyNow standbyCurrentTimeFn,
	standbyTaskMissingEventsDiscardDelay time.Duration,
	discardTaskStandbyPostActionFn standbyPostActionFn,
) standbyPostActionFn {

	// this is for task retry, use machine time
	now := standbyNow()
	taskTime := taskInfo.GetVisibilityTime()
	discardTime := taskTime.Add(standbyTaskMissingEventsDiscardDelay)

	// now < task start time + StandbyTaskMissingEventsResendDelay
	if now.Before(discardTime) {
		return standbyTaskPostActionNoOp
	}

	// task start time + StandbyTaskMissingEventsResendDelay <= now
	return discardTaskStandbyPostActionFn
}

func getSourceClusterName(
	currentCluster string,
	registry namespace.Registry,
	namespaceID string,
	workflowID string,
) (string, error) {
	namespaceEntry, err := registry.GetNamespaceByID(namespace.ID(namespaceID))
	if err != nil {
		return "", err
	}

	remoteClusterName := namespaceEntry.ActiveClusterName(workflowID)
	if remoteClusterName == currentCluster {
		// namespace has turned active, retry the task
		return "", errors.New("namespace becomes active when processing task as standby")
	}
	return remoteClusterName, nil
}
