//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination task_refresher_mock.go

package workflow

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
)

type (
	TaskRefresher interface {
		// Refresh refreshes all tasks needed for the state machine to make progress or
		// those have side effects.
		Refresh(
			ctx context.Context,
			mutableState historyi.MutableState,
			shouldSkipGeneratingCloseTransferTask bool,
		) error
		// PartialRefresh refresh tasks for all sub state machines that have been updated
		// since the given minVersionedTransition (inclusive).
		// If a sub state machine's lastUpdateVersionedTransition is not available,
		// it will be treated the same as lastUpdateVersionedTransition equals to EmptyVersionedTransition.
		// The provided minVersionedTransition should NOT be nil, and if equals to EmptyVersionedTransition,
		// the behavior is equivalent to Refresh().
		//
		// PartialRefresh does not refresh tasks for CHASM components as they are smart enough to figure out
		// what tasks need to be generated when seeing a new version of the component.
		PartialRefresh(
			ctx context.Context,
			mutableState historyi.MutableState,
			minVersionedTransition *persistencespb.VersionedTransition,
			previousPendingChildIds map[int64]struct{},
			shouldSkipGeneratingCloseTransferTask bool,
		) error
	}

	TaskRefresherImpl struct {
		shard historyi.ShardContext

		// this defaults to the global taskGeneratorProvider
		// for testing purposes, it can be overridden to use a mock task generator
		taskGeneratorProvider TaskGeneratorProvider
	}
)

func NewTaskRefresher(
	shard historyi.ShardContext,
) *TaskRefresherImpl {

	return &TaskRefresherImpl{
		shard: shard,

		taskGeneratorProvider: taskGeneratorProvider,
	}
}

func (r *TaskRefresherImpl) Refresh(
	ctx context.Context,
	mutableState historyi.MutableState,
	shouldSkipGeneratingCloseTransferTask bool,
) error {
	if r.shard.GetConfig().EnableNexus() {
		// Invalidate all tasks generated for this mutable state before the refresh.
		mutableState.GetExecutionInfo().TaskGenerationShardClockTimestamp = r.shard.CurrentVectorClock().GetClock()
	}

	if err := r.PartialRefresh(ctx, mutableState, EmptyVersionedTransition, nil, shouldSkipGeneratingCloseTransferTask); err != nil {
		return err
	}

	if err := mutableState.ChasmTree().RefreshTasks(); err != nil {
		return err
	}

	if !mutableState.IsWorkflow() && mutableState.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		closeTime, err := mutableState.GetWorkflowCloseTime(ctx)
		if err != nil {
			return err
		}
		taskGenerator := r.taskGeneratorProvider.NewTaskGenerator(
			r.shard,
			mutableState,
		)
		return taskGenerator.GenerateDeleteHistoryEventTask(closeTime)
	}

	return nil
}

func (r *TaskRefresherImpl) PartialRefresh(
	ctx context.Context,
	mutableState historyi.MutableState,
	minVersionedTransition *persistencespb.VersionedTransition,
	previousPendingChildIds map[int64]struct{},
	shouldSkipGeneratingCloseTransferTask bool,
) error {
	// CHASM tasks will be replicated as part of ApplyMutation/ApplySnapshot.
	// Physical tasks will also be automatically generated upon CloseTransaction.
	// So there's no need to do partial refresh for CHASM components.
	if !mutableState.IsWorkflow() {
		return nil
	}

	taskGenerator := r.taskGeneratorProvider.NewTaskGenerator(
		r.shard,
		mutableState,
	)

	if err := RefreshTasksForWorkflowStart(
		ctx,
		mutableState,
		taskGenerator,
		minVersionedTransition,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForWorkflowClose(
		ctx,
		mutableState,
		taskGenerator,
		minVersionedTransition,
		shouldSkipGeneratingCloseTransferTask,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForRecordWorkflowStarted(
		ctx,
		mutableState,
		taskGenerator,
		minVersionedTransition,
	); err != nil {
		return err
	}

	if err := r.refreshWorkflowTaskTasks(
		mutableState,
		taskGenerator,
		minVersionedTransition,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForActivity(
		ctx,
		mutableState,
		taskGenerator,
		minVersionedTransition,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForTimer(
		mutableState,
		minVersionedTransition,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForChildWorkflow(
		mutableState,
		taskGenerator,
		minVersionedTransition,
		previousPendingChildIds,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForRequestCancelExternalWorkflow(
		ctx,
		mutableState,
		taskGenerator,
		minVersionedTransition,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForSignalExternalWorkflow(
		ctx,
		mutableState,
		taskGenerator,
		minVersionedTransition,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForWorkflowSearchAttr(
		mutableState,
		taskGenerator,
		minVersionedTransition,
	); err != nil {
		return err
	}

	return r.refreshTasksForSubStateMachines(
		mutableState,
		minVersionedTransition,
	)
}

func RefreshTasksForWorkflowStart(
	ctx context.Context,
	mutableState historyi.MutableState,
	taskGenerator TaskGenerator,
	minVersionedTransition *persistencespb.VersionedTransition,
) error {

	executionState := mutableState.GetExecutionState()
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	// Skip task generation if workflow state has not been updated since minVersionedTransition.
	if transitionhistory.Compare(
		executionState.LastUpdateVersionedTransition,
		minVersionedTransition,
	) < 0 {
		return nil
	}

	startEvent, err := mutableState.GetStartEvent(ctx)
	if err != nil {
		return err
	}

	// first clear execution timeout timer task status
	executionInfo := mutableState.GetExecutionInfo()
	executionInfo.WorkflowExecutionTimerTaskStatus = TimerTaskStatusNone

	executionInfo.WorkflowExecutionTimerTaskStatus, err = taskGenerator.GenerateWorkflowStartTasks(
		startEvent,
	)
	if err != nil {
		return err
	}

	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	if !mutableState.HadOrHasWorkflowTask() && timestamp.DurationValue(startAttr.GetFirstWorkflowTaskBackoff()) > 0 {
		if err := taskGenerator.GenerateDelayedWorkflowTasks(
			startEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForWorkflowClose(
	ctx context.Context,
	mutableState historyi.MutableState,
	taskGenerator TaskGenerator,
	minVersionedTransition *persistencespb.VersionedTransition,
	skipCloseTransferTask bool,
) error {

	executionState := mutableState.GetExecutionState()
	// Workflow close tasks don't apply when the workflow is in running or paused status.
	if executionState.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING || executionState.Status == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
		return nil
	}

	// Skip task generation if workflow state has not been updated since minVersionedTransition.
	if transitionhistory.Compare(
		executionState.LastUpdateVersionedTransition,
		minVersionedTransition,
	) < 0 {
		return nil
	}

	closeEventTime, err := mutableState.GetWorkflowCloseTime(ctx)
	if err != nil {
		return err
	}

	return taskGenerator.GenerateWorkflowCloseTasks(
		closeEventTime,
		false,
		skipCloseTransferTask,
	)
}

func (r *TaskRefresherImpl) refreshTasksForRecordWorkflowStarted(
	ctx context.Context,
	mutableState historyi.MutableState,
	taskGenerator TaskGenerator,
	minVersionedTransition *persistencespb.VersionedTransition,
) error {

	executionState := mutableState.GetExecutionState()
	// skip task generation if workflow is not running or paused.
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
		return nil
	}

	// Skip task generation if no transition since minVersionedTransition requires
	// an update in the visibility record.
	if transitionhistory.Compare(
		mutableState.GetExecutionInfo().VisibilityLastUpdateVersionedTransition,
		minVersionedTransition,
	) < 0 {
		return nil
	}

	startEvent, err := mutableState.GetStartEvent(ctx)
	if err != nil {
		return err
	}

	return taskGenerator.GenerateRecordWorkflowStartedTasks(
		startEvent,
	)
}

func (r *TaskRefresherImpl) refreshWorkflowTaskTasks(
	mutableState historyi.MutableState,
	taskGenerator TaskGenerator,
	minVersionedTransition *persistencespb.VersionedTransition,
) error {

	executionState := mutableState.GetExecutionState()

	// skip task generation if the workflow is not running.
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	if !mutableState.HasPendingWorkflowTask() {
		// no workflow task at all
		return nil
	}

	// Skip task generation if workflow task has not been updated since minVersionedTransition.
	if transitionhistory.Compare(
		mutableState.GetExecutionInfo().WorkflowTaskLastUpdateVersionedTransition,
		minVersionedTransition,
	) < 0 {
		return nil
	}

	workflowTask := mutableState.GetPendingWorkflowTask()
	if workflowTask == nil {
		return serviceerror.NewInternal("it could be a bug, cannot get pending workflow task")
	}

	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
		// Do not generate tasks because speculative WT doesn't have any timer or transfer tasks associated with it.
		return nil
	}

	// workflowTask already started
	if workflowTask.StartedEventID != common.EmptyEventID {
		return taskGenerator.GenerateStartWorkflowTaskTasks(
			workflowTask.ScheduledEventID,
		)
	}

	// workflowTask only scheduled
	return taskGenerator.GenerateScheduleWorkflowTaskTasks(
		workflowTask.ScheduledEventID,
	)
}

func (r *TaskRefresherImpl) refreshTasksForActivity(
	ctx context.Context,
	mutableState historyi.MutableState,
	taskGenerator TaskGenerator,
	minVersionedTransition *persistencespb.VersionedTransition,
) error {

	executionState := mutableState.GetExecutionState()
	// skip task generation if workflow is not running since activities are only scheduled when the workflow is in running status
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		return nil
	}

	pendingActivityInfos := mutableState.GetPendingActivityInfos()

	for _, activityInfo := range pendingActivityInfos {

		// Skip task generation if this activity has not been updated since minVersionedTransition.
		if transitionhistory.Compare(
			activityInfo.LastUpdateVersionedTransition,
			minVersionedTransition,
		) < 0 {
			continue
		}

		if transitionhistory.Compare(minVersionedTransition, EmptyVersionedTransition) == 0 { // Full refresh
			activityInfo.TimerTaskStatus = TimerTaskStatusNone // clear activity timer task mask for later activity timer task re-generation
			if err := mutableState.UpdateActivityTaskStatusWithTimerHeartbeat(
				activityInfo.ScheduledEventId,
				activityInfo.TimerTaskStatus,
				nil,
			); err != nil {
				return err
			}
		}

		if activityInfo.StartedEventId != common.EmptyEventID {
			continue
		}

		if activityInfo.Paused {
			continue
		}

		if activityInfo.Attempt > 1 {
			if err := taskGenerator.GenerateActivityRetryTasks(activityInfo); err != nil {
				return err
			}
		} else {
			if err := taskGenerator.GenerateActivityTasks(
				activityInfo.ScheduledEventId,
			); err != nil {
				return err
			}
		}
	}

	_, err := NewTimerSequence(mutableState).CreateNextActivityTimer()
	return err
}

func (r *TaskRefresherImpl) refreshTasksForTimer(
	mutableState historyi.MutableState,
	minVersionedTransition *persistencespb.VersionedTransition,
) error {

	executionState := mutableState.GetExecutionState()
	// skip task generation if workflow is not running or paused. For now timers continue to progress when the workflow is paused.
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
		return nil
	}

	pendingTimerInfos := mutableState.GetPendingTimerInfos()
	for _, timerInfo := range pendingTimerInfos {

		// Skip task generation if this user timer has not been updated since minVersionedTransition.
		if transitionhistory.Compare(
			timerInfo.LastUpdateVersionedTransition,
			minVersionedTransition,
		) < 0 {
			continue
		}

		// need to update user timer task mask for which task is generated
		if err := mutableState.UpdateUserTimerTaskStatus(
			timerInfo.TimerId,
			TimerTaskStatusNone, // clear timer task mask for later timer task re-generation
		); err != nil {
			return err
		}
	}

	_, err := NewTimerSequence(mutableState).CreateNextUserTimer()
	return err
}

func (r *TaskRefresherImpl) refreshTasksForChildWorkflow(
	mutableState historyi.MutableState,
	taskGenerator TaskGenerator,
	minVersionedTransition *persistencespb.VersionedTransition,
	previousPendingChildIds map[int64]struct{},
) error {

	pendingChildWorkflowInfos := mutableState.GetPendingChildExecutionInfos()

	for _, childWorkflowInfo := range pendingChildWorkflowInfos {
		// Skip task generation if this child workflow has already been started.
		// This is an optimization to avoid generating duplicate tasks.
		// However, if this child workflow was not in the previous pending child IDs,
		// we still need to generate tasks even if it's started, because this means
		// the child workflow was just added to the mutable state.
		if _, ok := previousPendingChildIds[childWorkflowInfo.InitiatedEventId]; ok {
			if childWorkflowInfo.StartedEventId != common.EmptyEventID {
				continue
			}
		}

		// Skip task generation if this child workflow has not been updated since minVersionedTransition.
		if transitionhistory.Compare(
			childWorkflowInfo.LastUpdateVersionedTransition,
			minVersionedTransition,
		) < 0 {
			continue
		}

		if err := taskGenerator.GenerateChildWorkflowTasks(
			childWorkflowInfo.InitiatedEventId,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForRequestCancelExternalWorkflow(
	ctx context.Context,
	mutableState historyi.MutableState,
	taskGenerator TaskGenerator,
	minVersionedTransition *persistencespb.VersionedTransition,
) error {

	executionState := mutableState.GetExecutionState()
	// skip task generation if workflow is not running or paused.
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
		return nil
	}

	pendingRequestCancelInfos := mutableState.GetPendingRequestCancelExternalInfos()

	for _, requestCancelInfo := range pendingRequestCancelInfos {

		// Skip task generation if this cancel external request has not been updated since minVersionedTransition.
		if transitionhistory.Compare(
			requestCancelInfo.LastUpdateVersionedTransition,
			minVersionedTransition,
		) < 0 {
			continue
		}

		initiateEvent, err := mutableState.GetRequesteCancelExternalInitiatedEvent(ctx, requestCancelInfo.GetInitiatedEventId())
		if err != nil {
			return err
		}

		if err := taskGenerator.GenerateRequestCancelExternalTasks(
			initiateEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForSignalExternalWorkflow(
	ctx context.Context,
	mutableState historyi.MutableState,
	taskGenerator TaskGenerator,
	minVersionedTransition *persistencespb.VersionedTransition,
) error {

	executionState := mutableState.GetExecutionState()
	// skip task generation if workflow is not running or paused.
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
		return nil
	}

	pendingSignalInfos := mutableState.GetPendingSignalExternalInfos()

	for _, signalInfo := range pendingSignalInfos {

		// Skip task generation if this signal external request has not been updated since minVersionedTransition.
		if transitionhistory.Compare(
			signalInfo.LastUpdateVersionedTransition,
			minVersionedTransition,
		) < 0 {
			continue
		}

		initiateEvent, err := mutableState.GetSignalExternalInitiatedEvent(ctx, signalInfo.GetInitiatedEventId())
		if err != nil {
			return err
		}

		if err := taskGenerator.GenerateSignalExternalTasks(
			initiateEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *TaskRefresherImpl) refreshTasksForWorkflowSearchAttr(
	mutableState historyi.MutableState,
	taskGenerator TaskGenerator,
	minVersionedTransition *persistencespb.VersionedTransition,
) error {
	executionState := mutableState.GetExecutionState()
	// skip task generation if workflow is not running or paused. Search attributes should continue to be updated when the workflow is paused.
	if executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING && executionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
		return nil
	}

	// Skip task generation if no transition since minVersionedTransition requires
	// an update in the visibility record.
	if transitionhistory.Compare(
		mutableState.GetExecutionInfo().VisibilityLastUpdateVersionedTransition,
		minVersionedTransition,
	) < 0 {
		return nil
	}
	return taskGenerator.GenerateUpsertVisibilityTask()
}

func (r *TaskRefresherImpl) refreshTasksForSubStateMachines(
	mutableState historyi.MutableState,
	minVersionedTransition *persistencespb.VersionedTransition,
) error {

	// NOTE: Not all callers of TaskRefresher goes through the closeTransaction process.
	// If we were to regenerate tasks here by doing a state machine transition and return
	// a TransitionOutput, then, we need to
	//   1. Call taskGenerator.GenerateDirtySubStateMachineTasks explictly to make sure
	//      tasks are added to mutable state.
	//   2. Make GenerateDirtySubStateMachineTasks idempotent, so that if the logic
	//      does go through closeTransaction, no duplicate tasks are generated.
	//   3. Explicitly clear transition state for sub state machines when not going through
	//      closeTransaction path.
	//   4. Sub state machine will be marked as dirty, causing task refresh being counted
	//      as a state transition. (if the logic does go through closeTransaction path,
	//      though we don't actually have such cases today)
	// With the implementation below, we avoid the above complexities by directly generating
	// tasks and adding them to mutable state immediately.

	// Task refresher is only meant for regenerating tasks that has been generated before,
	// in previous state transitions, so we can just call generateSubStateMachineTask
	// which uses the last versioned transition.
	// In replication case, task refresher should be called after applying the state from source
	// cluster, which updated the transition history.

	var nodesToRefresh []*hsm.Node
	if err := mutableState.HSM().Walk(func(node *hsm.Node) error {
		if node.Parent == nil {
			// root node is mutable state and can't be refreshed
			return nil
		}

		// Skip task generation if this state machine node has not been updated since minVersionedTransition.
		if transitionhistory.Compare(
			node.InternalRepr().LastUpdateVersionedTransition,
			minVersionedTransition,
		) < 0 {
			return nil
		}

		nodesToRefresh = append(nodesToRefresh, node)
		return nil
	}); err != nil {
		return err
	}

	if len(nodesToRefresh) != 0 || mutableState.IsSubStateMachineDeleted() {
		if err := TrimStateMachineTimers(mutableState, minVersionedTransition); err != nil {
			return err
		}
	}

	for _, node := range nodesToRefresh {
		taskRegenerator, err := hsm.MachineData[hsm.TaskRegenerator](node)
		if err != nil {
			return err
		}

		tasks, err := taskRegenerator.RegenerateTasks(node)
		if err != nil {
			return err
		}

		for _, task := range tasks {
			if err := generateSubStateMachineTask(
				mutableState,
				r.shard.StateMachineRegistry(),
				node,
				node.Path(),
				node.InternalRepr().GetTransitionCount(),
				task,
			); err != nil {
				return err
			}
		}
	}

	AddNextStateMachineTimerTask(mutableState)

	return nil
}
