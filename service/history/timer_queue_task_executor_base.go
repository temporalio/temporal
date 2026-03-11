package history

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	errNoTimerFired        = serviceerror.NewNotFound("no expired timer to fire found")
	errNoChasmTree         = serviceerror.NewInternal("mutable state associated with CHASM task has no CHASM tree")
	errNoChasmMutableState = serviceerror.NewInternal("mutable state couldn't be loaded for a CHASM task")
)

type (
	timerQueueTaskExecutorBase struct {
		stateMachineEnvironment
		chasmEngine        chasm.Engine
		currentClusterName string
		registry           namespace.Registry
		deleteManager      deletemanager.DeleteManager
		matchingRawClient  resource.MatchingRawClient
		config             *configs.Config
		isActive           bool
	}
)

func newTimerQueueTaskExecutorBase(
	shardContext historyi.ShardContext,
	workflowCache wcache.Cache,
	deleteManager deletemanager.DeleteManager,
	matchingRawClient resource.MatchingRawClient,
	chasmEngine chasm.Engine,
	logger log.Logger,
	metricsHandler metrics.Handler,
	config *configs.Config,
	isActive bool,
) *timerQueueTaskExecutorBase {
	return &timerQueueTaskExecutorBase{
		stateMachineEnvironment: stateMachineEnvironment{
			shardContext:   shardContext,
			cache:          workflowCache,
			logger:         logger,
			metricsHandler: metricsHandler,
		},
		currentClusterName: shardContext.GetClusterMetadata().GetCurrentClusterName(),
		registry:           shardContext.GetNamespaceRegistry(),
		chasmEngine:        chasmEngine,
		deleteManager:      deleteManager,
		matchingRawClient:  matchingRawClient,
		config:             config,
		isActive:           isActive,
	}
}

func (t *timerQueueTaskExecutorBase) executeDeleteHistoryEventTask(
	ctx context.Context,
	task *tasks.DeleteHistoryEventTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}

	if task.ArchetypeID == chasm.UnspecifiedArchetypeID {
		task.ArchetypeID = chasm.WorkflowArchetypeID
	}

	weContext, release, err := t.cache.GetOrCreateChasmExecution(
		ctx,
		t.shardContext,
		namespace.ID(task.GetNamespaceID()),
		workflowExecution,
		task.ArchetypeID,
		locks.PriorityLow,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, t.shardContext, weContext, task, t.metricsHandler, t.logger)
	switch err.(type) {
	case nil:
		if mutableState == nil {
			return nil
		}
	case *serviceerror.NotFound:
		// the mutable state is deleted and delete history branch operation failed.
		// use task branch token to delete the leftover history branch
		return t.deleteHistoryBranch(ctx, task.BranchToken)
	default:
		return err
	}

	if mutableState.GetExecutionState().GetState() != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// If workflow is running then just ignore DeleteHistoryEventTask timer task.
		// This should almost never happen because DeleteHistoryEventTask is created only for closed workflows.
		// But cross DC replication can resurrect workflow and therefore DeleteHistoryEventTask should be ignored.
		return nil
	}

	closeVersion, err := mutableState.GetCloseVersion()
	if err != nil {
		return err
	}
	if err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), closeVersion, task.Version, task); err != nil {
		return err
	}

	return t.deleteManager.DeleteWorkflowExecutionByRetention(
		ctx,
		namespace.ID(task.GetNamespaceID()),
		workflowExecution,
		weContext,
		mutableState,
		&task.ProcessStage,
	)
}

func (t *timerQueueTaskExecutorBase) deleteHistoryBranch(
	ctx context.Context,
	branchToken []byte,
) error {
	if len(branchToken) > 0 {
		return t.shardContext.GetExecutionManager().DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
			ShardID:     t.shardContext.GetShardID(),
			BranchToken: branchToken,
		})
	}
	return nil
}

func (t *timerQueueTaskExecutorBase) isValidExpirationTime(
	mutableState historyi.MutableState,
	task tasks.Task,
	expirationTime *timestamppb.Timestamp,
) bool {
	if !mutableState.IsWorkflowExecutionRunning() {
		return false
	}

	now := t.Now()
	taskShouldTriggerAt := expirationTime.AsTime()
	expired := queues.IsTimeExpired(task, now, taskShouldTriggerAt)
	return expired
}

func (t *timerQueueTaskExecutorBase) isValidWorkflowRunTimeoutTask(
	mutableState historyi.MutableState,
	task *tasks.WorkflowRunTimeoutTask,
) bool {
	executionInfo := mutableState.GetExecutionInfo()

	// Check if workflow execution timeout is not expired
	// This can happen if the workflow is reset but old timer task is still fired.
	return t.isValidExpirationTime(mutableState, task, executionInfo.WorkflowRunExpirationTime)
}

func (t *timerQueueTaskExecutorBase) isValidWorkflowExecutionTimeoutTask(
	mutableState historyi.MutableState,
	task *tasks.WorkflowExecutionTimeoutTask,
) bool {

	executionInfo := mutableState.GetExecutionInfo()
	if executionInfo.FirstExecutionRunId != task.FirstRunID {
		// current run does not belong to workflow chain the task is generated for
		return false
	}

	// Check if workflow execution timeout is not expired
	// This can happen if the workflow is reset since reset re-calculates
	// the execution timeout but shares the same firstRunID as the base run
	return t.isValidExpirationTime(mutableState, task, executionInfo.WorkflowExecutionExpirationTime)

	// NOTE: we don't need to do version check here because if we were to do it, we need to compare the task version
	// and the start version in the first run. However, failover & conflict resolution will never change
	// the first event of a workflowID (the history tree model we are using always share at least one node),
	// meaning start version check will always pass.
	// Also, there's no way we can perform version check before first run may already be deleted due to retention
}

func (t *timerQueueTaskExecutorBase) executeSingleStateMachineTimer(
	ctx context.Context,
	workflowContext historyi.WorkflowContext,
	ms historyi.MutableState,
	deadline time.Time,
	timer *persistencespb.StateMachineTaskInfo,
	execute func(node *hsm.Node, task hsm.Task) error,
) error {
	def, ok := t.shardContext.StateMachineRegistry().TaskSerializer(timer.Type)
	if !ok {
		return queueserrors.NewUnprocessableTaskError(fmt.Sprintf("deserializer not registered for task type %v", timer.Type))
	}
	smt, err := def.Deserialize(timer.Data, hsm.TaskAttributes{Deadline: deadline})
	if err != nil {
		return fmt.Errorf(
			"%w: %w",
			queueserrors.NewUnprocessableTaskError(fmt.Sprintf("cannot deserialize task %v", timer.Type)),
			err,
		)
	}
	ref := hsm.Ref{
		WorkflowKey:     ms.GetWorkflowKey(),
		StateMachineRef: timer.Ref,
		Validate:        smt.Validate,
	}
	// TODO(bergundy): Duplicated this logic from the Access method. We specify write access here because
	// validateNotZombieWorkflow only blocks write access to zombie workflows.
	if err := t.validateNotZombieWorkflow(ms, hsm.AccessWrite); err != nil {
		return err
	}
	if err := t.validateStateMachineRef(ctx, workflowContext, ms, ref, false); err != nil {
		return err
	}
	node, err := ms.HSM().Child(ref.StateMachinePath())
	if err != nil {
		return err
	}

	if err := execute(node, smt); err != nil {
		return fmt.Errorf("failed to execute task: %w", err)
	}
	return nil
}

// executeChasmPureTimers walks a CHASM tree for expired pure task timers,
// executes them, and returns a count of timers processed.
func (t *timerQueueTaskExecutorBase) executeChasmPureTimers(
	ms historyi.MutableState,
	task *tasks.ChasmTaskPure,
	callback func(node chasm.NodePureTask, taskAttributes chasm.TaskAttributes, task any) (bool, error),
) error {
	// Because CHASM timers can target closed workflows, we need to specifically
	// exclude zombie workflows, instead of merely checking that the workflow is
	// running.
	if ms.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		return consts.ErrWorkflowZombie
	}

	tree := ms.ChasmTree()
	if tree == nil {
		return errNoChasmTree
	}

	// Because the persistence layer can lose precision on the task compared to the
	// physical task stored in the queue, we take the max of both here. Time is also
	// truncated to a common (millisecond) precision later on.
	//
	// See also queues.IsTimeExpired.
	referenceTime := util.MaxTime(t.Now(), task.GetKey().FireTime)

	return tree.EachPureTask(referenceTime, callback)
}

// executeStateMachineTimers gets the state machine timers, processes the expired timers,
// and returns a count of timers processed.
func (t *timerQueueTaskExecutorBase) executeStateMachineTimers(
	ctx context.Context,
	workflowContext historyi.WorkflowContext,
	ms historyi.MutableState,
	task *tasks.StateMachineTimerTask,
	execute func(node *hsm.Node, task hsm.Task) error,
) (int, error) {

	// need to specifically check for zombie workflows here instead of workflow running
	// or not since zombie workflows are considered as not running but state machine timers
	// can target closed workflows.
	if ms.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		return 0, consts.ErrWorkflowZombie
	}

	timers := ms.GetExecutionInfo().StateMachineTimers
	processedTimers := 0

	// StateMachineTimers are sorted by Deadline, iterate through them as long as the deadline is expired.
	for len(timers) > 0 {
		group := timers[0]
		if !queues.IsTimeExpired(task, t.Now(), group.Deadline.AsTime()) {
			break
		}

		for _, timer := range group.Infos {
			err := t.executeSingleStateMachineTimer(ctx, workflowContext, ms, group.Deadline.AsTime(), timer, execute)
			if err != nil {
				// This includes errors such as ErrStaleReference and ErrWorkflowCompleted.
				if !errors.As(err, new(*serviceerror.NotFound)) {
					metrics.StateMachineTimerProcessingFailuresCounter.With(t.metricsHandler).Record(
						1,
						metrics.OperationTag(queues.GetTimerStateMachineTaskTypeTagValue(timer.GetType(), t.isActive)),
						metrics.ServiceErrorTypeTag(err),
					)
					// Return on first error as we don't want to duplicate the Executable's error handling logic.
					// This implies that a single bad task in the mutable state timer sequence will cause all other
					// tasks to be stuck. We'll accept this limitation for now.
					return 0, err
				}
				metrics.StateMachineTimerSkipsCounter.With(t.metricsHandler).Record(
					1,
					metrics.OperationTag(queues.GetTimerStateMachineTaskTypeTagValue(timer.GetType(), t.isActive)),
				)
				t.logger.Info("Skipped state machine timer", tag.Error(err))
			}
		}
		// Remove the processed timer group.
		timers = timers[1:]
		processedTimers++
	}

	if processedTimers > 0 {
		// Update processed timers.
		ms.GetExecutionInfo().StateMachineTimers = timers
	}
	return processedTimers, nil
}
