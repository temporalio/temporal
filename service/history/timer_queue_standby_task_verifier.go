package history

import (
	"context"
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
)

func (t *timerQueueStandbyTaskExecutor) verifyChasmPureTimerTask(
	ctx context.Context,
	task *tasks.ChasmTaskPure,
	mutableState historyi.MutableState,
) (any, error) {
	err := t.executeChasmPureTimers(
		mutableState,
		task,
		func(_ chasm.NodePureTask, _ chasm.TaskAttributes, _ any) (bool, error) {
			// Any task present means replication has not yet removed it — retry.
			return false, consts.ErrTaskRetry
		},
	)
	if err != nil && errors.Is(err, consts.ErrTaskRetry) {
		return &struct{}{}, nil
	}

	return nil, err
}

func (t *timerQueueStandbyTaskExecutor) verifyChasmSideEffectTimerTask(
	ctx context.Context,
	task *tasks.ChasmTask,
	ms historyi.MutableState,
) (any, error) {
	isTaskInTree, isValid, err := validateChasmSideEffectTask(ctx, ms, task)
	if err != nil {
		return nil, err
	}
	if !isTaskInTree || !isValid {
		// Replication has removed the logical task, or the component reports it
		// invalid — drop the physical task.
		return nil, nil
	}

	// Task still exists in the tree; retry until the active cluster executes
	// and replicates the resulting state change.
	return ms.ChasmTree(), nil
}

func (t *timerQueueStandbyTaskExecutor) verifyTimeSkippingTimerTask(
	ctx context.Context,
	timerTask *tasks.TimeSkippingFastForwardTimerTask,
	mutableState historyi.MutableState,
) (any, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, nil
	}
	tsi := mutableState.GetExecutionInfo().GetTimeSkippingInfo()
	ffi := tsi.GetFastForwardInfo()

	// the fast-forward this timer task is associated with is still valid and has not been reached so keep waiting
	if ffi != nil &&
		transitionhistory.Compare(tsi.GetFastForwardInfoLastUpdateVersionedTransition(), timerTask.VersionedTransition) == 0 &&
		!ffi.GetHasReached() {
		return &struct{}{}, nil
	}
	return nil, nil
}

func (t *timerQueueStandbyTaskExecutor) verifyUserTimerTimeoutTask(
	ctx context.Context,
	timerTask *tasks.UserTimerTask,
	mutableState historyi.MutableState,
) (any, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		// workflow already finished, no need to process the timer
		return nil, nil
	}

	referenceTime := t.getCurrentTime()

	timerSequence := t.getTimerSequence(mutableState)
	timerSequenceIDs := timerSequence.LoadAndSortUserTimers()
	if len(timerSequenceIDs) > 0 {
		timerSequenceID := timerSequenceIDs[0]
		_, ok := mutableState.GetUserTimerInfoByEventID(timerSequenceID.EventID)
		if !ok {
			errString := fmt.Sprintf("failed to find in user timer event ID: %v", timerSequenceID.EventID)
			t.logger.Error(errString)
			return nil, serviceerror.NewInternal(errString)
		}

		// Timer task visibility and the converted deadline use wall-clock time.
		if queues.IsTimeExpired(
			timerTask,
			referenceTime,
			mutableState.ToPhysicalTimeWithCurrentTimeSkippingOffset(timerSequenceID.Timestamp),
		) {
			return &struct{}{}, nil
		}
		// Since the user timers are already sorted, then if there is one timer which is not expired,
		// all user timers after that timer are not expired.
	}
	// If there is no user timer expired, then we are good.
	return nil, nil
}

func (t *timerQueueStandbyTaskExecutor) verifyActivityRetryTimerTask(
	ctx context.Context,
	task *tasks.ActivityRetryTimerTask,
	mutableState historyi.MutableState,
) (any, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		// workflow already finished, no need to process the timer
		return nil, nil
	}

	activityInfo, ok := mutableState.GetActivityInfo(task.EventID) // activity schedule ID
	if !ok {
		return nil, nil
	}

	err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), activityInfo.Version, task.Version, task)
	if err != nil {
		return nil, err
	}

	// we ignore retry timer task if:
	// * this retry task is from old Stamp.
	// * attempts is not the same as recorded in activity info.
	// * activity is already started.
	if activityInfo.Attempt > task.Attempt ||
		activityInfo.Stamp != task.Stamp ||
		activityInfo.StartedEventId != common.EmptyEventID ||
		activityInfo.Paused {
		return nil, nil
	}

	return newActivityRetryTimePostActionInfo(mutableState, activityInfo.TaskQueue, activityInfo.ScheduleToStartTimeout.AsDuration(), activityInfo)
}

func (t *timerQueueStandbyTaskExecutor) verifyWorkflowTaskTimeoutTask(
	ctx context.Context,
	timerTask *tasks.WorkflowTaskTimeoutTask,
	mutableState historyi.MutableState,
) (any, error) {
	if timerTask.TimeoutType == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		return nil, nil
	}

	if !mutableState.IsWorkflowExecutionRunning() {
		// workflow already finished, no need to process the timer
		return nil, nil
	}

	workflowTask := mutableState.GetWorkflowTaskByID(timerTask.EventID)
	if workflowTask == nil {
		return nil, nil
	}
	if timerTask.Stamp != workflowTask.Stamp {
		return nil, consts.ErrStaleReference
	}

	err := CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), workflowTask.Version, timerTask.Version, timerTask)
	if err != nil {
		return nil, err
	}

	if workflowTask.Attempt != timerTask.ScheduleAttempt {
		return nil, nil
	}

	// We could check if workflow task is started state (since the timeout type here is START_TO_CLOSE)
	// but that's unnecessary.
	//
	// Ifthe  workflow task is in scheduled state, it must have a higher attempt
	// count and will be captured by the attempt check above.

	return &struct{}{}, nil
}

func (t *timerQueueStandbyTaskExecutor) verifyWorkflowBackoffTimerTask(
	ctx context.Context,
	timerTask *tasks.WorkflowBackoffTimerTask,
	mutableState historyi.MutableState,
) (any, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		// workflow already finished, no need to process the timer
		return nil, nil
	}

	if mutableState.HadOrHasWorkflowTask() {
		// if there is one workflow task already been processed
		// or has pending workflow task, meaning workflow has already running
		return nil, nil
	}

	// Note: do not need to verify task version here
	// logic can only go here if mutable state build's next event ID is 2
	// meaning history only contains workflow started event.
	// we can do the checking of task version vs workflow started version
	// however, workflow started version is immutable

	// active cluster will add first workflow task after backoff timeout.
	// standby cluster should just call ack manager to retry this task
	// since we are stilling waiting for the first WorkflowTaskScheduledEvent to be replicated from active side.

	return &struct{}{}, nil
}

func (t *timerQueueStandbyTaskExecutor) verifyWorkflowRunTimeoutTask(
	ctx context.Context,
	timerTask *tasks.WorkflowRunTimeoutTask,
	mutableState historyi.MutableState,
) (any, error) {
	if !t.isValidWorkflowRunTimeoutTask(mutableState, timerTask) {
		return nil, nil
	}

	startVersion, err := mutableState.GetStartVersion()
	if err != nil {
		return nil, err
	}
	err = CheckTaskVersion(t.shardContext, t.logger, mutableState.GetNamespaceEntry(), startVersion, timerTask.Version, timerTask)
	if err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (t *timerQueueStandbyTaskExecutor) verifyWorkflowExecutionTimeoutTask(
	ctx context.Context,
	timerTask *tasks.WorkflowExecutionTimeoutTask,
	mutableState historyi.MutableState,
) (any, error) {
	if !t.isValidWorkflowExecutionTimeoutTask(mutableState, timerTask) {
		return nil, nil
	}

	// If the task is valid, it means the workflow should be timed out but it is still running.
	// The standby logic should continue to wait for the workflow timeout event to be replicated from the active side.
	//
	// Return non-nil post action info to indicate that verification is not done yet.
	// The returned post action info can be used to resend history fron active side.

	return newExecutionTimerPostActionInfo(mutableState)
}

func (t *timerQueueStandbyTaskExecutor) verifyActivityTimeoutTask(
	ctx context.Context,
	timerTask *tasks.ActivityTimeoutTask,
	mutableState historyi.MutableState,
) (any, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		// workflow already finished, no need to process the timer
		return nil, nil
	}

	referenceTime := t.getCurrentTime()

	timerSequence := t.getTimerSequence(mutableState)
	timerSequenceIDs := timerSequence.LoadAndSortActivityTimers()
	if len(timerSequenceIDs) > 0 {
		timerSequenceID := timerSequenceIDs[0]
		_, ok := mutableState.GetActivityInfo(timerSequenceID.EventID)
		if !ok {
			errString := fmt.Sprintf("failed to find in memory activity timer: %v", timerSequenceID.EventID)
			t.logger.Error(errString)
			return nil, serviceerror.NewInternal(errString)
		}

		// Timer task visibility and the converted deadline use wall-clock time.
		if queues.IsTimeExpired(
			timerTask,
			referenceTime,
			mutableState.ToPhysicalTimeWithCurrentTimeSkippingOffset(timerSequenceID.Timestamp),
		) {
			return &struct{}{}, nil
		}
		// Since the activity timers are already sorted, then if there is one timer which is not expired,
		// all activity timers after that timer are not expired.
	}

	return activityTimerMaintenance{task: timerTask}, nil
}

func (t *timerQueueStandbyTaskExecutor) verifyStateMachineTimerTask(
	ctx context.Context,
	timerTask *tasks.StateMachineTimerTask,
	mutableState historyi.MutableState,
) (any, error) {
	processedTimers, remaining, err := t.visitStateMachineTimers(
		ctx,
		mutableState,
		timerTask,
		func(node *hsm.Node, task hsm.Task) error {
			// If this line of code is reached, the task's Validate() function returned no error, which indicates
			// that it is still expected to run. Return ErrTaskRetry to wait the machine to transition on the active
			// cluster.
			return consts.ErrTaskRetry
		},
	)
	if err != nil {
		if errors.Is(err, consts.ErrTaskRetry) {
			// This handles the ErrTaskRetry error returned by executeStateMachineTimers.
			return &struct{}{}, nil
		}
		return nil, err
	}

	// We haven't done any work, return without committing.
	if processedTimers == 0 {
		return nil, nil
	}

	return stateMachineTimerMaintenance{remainingTimers: remaining}, nil
}
