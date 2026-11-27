package history

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api/workflowresend"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

type externalTaskSyncPosition struct {
	transition *persistencespb.VersionedTransition
	histories  *historyspb.VersionHistories
}

func (t *transferQueueStandbyTaskExecutor) processExternalTransferTask(
	ctx context.Context,
	task tasks.Task,
	attempt int,
	verify standbyVerificationFn,
) error {
	taskType := task.GetType()
	age := t.getCurrentTime().Sub(task.GetVisibilityTime())
	if age >= t.config.StandbyTaskMissingEventsDiscardDelay(taskType) {
		return t.processTransfer(ctx, false, task, verify,
			func(ctx context.Context, task tasks.Task, info any, logger log.Logger) error {
				if info != nil {
					entry, err := t.shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(task.GetNamespaceID()))
					if err != nil || entry.ActiveClusterName(namespace.RoutingKey{ID: task.GetWorkflowID()}) == t.clusterName {
						return consts.ErrTaskRetry
					}
				}
				return standbyTransferTaskPostActionTaskDiscarded(ctx, task, info, logger, nil)
			})
	}

	interval := t.config.StandbyTaskRemoteVerificationAttemptInterval(taskType)
	if age < t.config.StandbyTaskMissingEventsResendDelay(taskType) || interval <= 0 || attempt%interval != 0 {
		return t.processTransfer(ctx, false, task, verify, standbyTaskPostActionNoOp)
	}

	return t.processTransfer(ctx, false, task,
		func(ctx context.Context, task tasks.Task, state historyi.MutableState) (any, error) {
			pending, err := verify(ctx, task, state)
			if err != nil || pending == nil {
				return pending, err
			}
			// The sync runs after releasing the workflow lock.
			return &externalTaskSyncPosition{
				transition: common.CloneProto(state.CurrentVersionedTransition()),
				histories:  common.CloneProto(state.GetExecutionInfo().GetVersionHistories()),
			}, nil
		},
		func(ctx context.Context, task tasks.Task, info any, logger log.Logger) error {
			if info == nil {
				return nil
			}
			return t.verifyAndSyncExternalTransferTask(ctx, task, info.(*externalTaskSyncPosition), verify, logger)
		})
}

func (t *transferQueueStandbyTaskExecutor) verifyAndSyncExternalTransferTask(
	ctx context.Context,
	task tasks.Task,
	position *externalTaskSyncPosition,
	verify standbyVerificationFn,
	logger log.Logger,
) error {
	registry := t.shardContext.GetNamespaceRegistry()
	namespaceID := namespace.ID(task.GetNamespaceID())
	entry, err := registry.GetNamespaceByID(namespaceID)
	if err != nil {
		return consts.ErrTaskRetry
	}
	routingKey := namespace.RoutingKey{ID: task.GetWorkflowID()}
	source := entry.ActiveClusterName(routingKey)
	if source == t.clusterName {
		return consts.ErrTaskRetry
	}
	sourceUnchanged := func() bool {
		current, err := registry.GetNamespaceByID(namespaceID)
		return err == nil && current.IsOnCluster(t.clusterName) && current.ActiveClusterName(routingKey) == source &&
			current.FailoverVersion(routingKey.ID) == entry.FailoverVersion(routingKey.ID)
	}

	remote, err := t.clientBean.GetRemoteAdminClient(source)
	if err != nil {
		return consts.ErrTaskRetry
	}
	execution := &commonpb.WorkflowExecution{WorkflowId: task.GetWorkflowID(), RunId: task.GetRunID()}
	response, err := remote.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace:       entry.Name().String(),
		Execution:       execution,
		Archetype:       chasm.WorkflowArchetype,
		SkipForceReload: true,
	})
	if err != nil {
		return consts.ErrTaskRetry
	}
	if !externalTransferTaskResolved(task, response.GetDatabaseMutableState()) || !sourceUnchanged() {
		return consts.ErrTaskRetry
	}

	result, err := workflowresend.SyncWorkflowStateFromSource(
		ctx, t.shardContext, namespaceID, execution, position.transition, position.histories, nil,
	)
	if !sourceUnchanged() {
		return consts.ErrTaskRetry
	}
	if err != nil || result != workflowresend.SyncWorkflowStateResultApplied {
		logger.Error("Discarding standby transfer task after workflow state sync failed",
			tag.Task(task), tag.Error(err), tag.NewInt("sync-result", int(result)))
		return consts.ErrTaskDiscarded
	}

	err = t.processTransfer(ctx, false, task, verify, standbyTaskPostActionNoOp)
	if !sourceUnchanged() {
		return consts.ErrTaskRetry
	}
	if err != nil {
		logger.Error("Discarding standby transfer task after local verification failed following workflow state sync",
			tag.Task(task), tag.Error(err))
		return consts.ErrTaskDiscarded
	}
	return nil
}

// externalTransferTaskResolved requires the active branch to contain the exact
// initiation event. Missing or divergent state cannot authorize an early discard.
func externalTransferTaskResolved(task tasks.Task, state *persistencespb.WorkflowMutableState) bool {
	var initiatedEventID, version int64
	var pending bool
	switch task := task.(type) {
	case *tasks.SignalExecutionTask:
		initiatedEventID, version = task.InitiatedEventID, task.Version
		_, pending = state.GetSignalInfos()[initiatedEventID]
	case *tasks.CancelExecutionTask:
		initiatedEventID, version = task.InitiatedEventID, task.Version
		_, pending = state.GetRequestCancelInfos()[initiatedEventID]
	default:
		return false
	}
	if state.GetNextEventId() <= initiatedEventID {
		return false
	}
	histories := state.GetExecutionInfo().GetVersionHistories()
	if histories == nil {
		return false
	}
	history, err := versionhistory.GetCurrentVersionHistory(histories)
	if err != nil || history == nil || !versionhistory.ContainsVersionHistoryItem(history, versionhistory.NewVersionHistoryItem(initiatedEventID, version)) {
		return false
	}
	switch state.GetExecutionState().GetState() {
	case enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED:
		return true
	case enumsspb.WORKFLOW_EXECUTION_STATE_CREATED, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING:
		return !pending
	default:
		return false
	}
}
