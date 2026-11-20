package history

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type activityTimerMaintenance struct{ task *tasks.ActivityTimeoutTask }
type stateMachineTimerMaintenance struct {
	remainingTimers []*persistencespb.StateMachineTimerGroup
}

func (t *timerQueueStandbyTaskExecutor) applyTimerMaintenance(
	ctx context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState, info any,
) (any, error) {
	switch info := info.(type) {
	case activityTimerMaintenance:
		return nil, t.refreshActivityTimer(ctx, wfContext, mutableState, info.task)
	case stateMachineTimerMaintenance:
		executionInfo := mutableState.GetExecutionInfo()
		executionInfo.StateMachineTimers = info.remainingTimers
		if t.config.EnableUpdateWorkflowModeIgnoreCurrent() {
			return nil, wfContext.UpdateWorkflowExecutionAsPassive(ctx, t.shardContext)
		}
		if mutableState.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
			return nil, wfContext.SubmitClosedWorkflowSnapshot(ctx, t.shardContext, historyi.TransactionPolicyPassive)
		}
		return nil, wfContext.UpdateWorkflowExecutionAsPassive(ctx, t.shardContext)
	default:
		return info, nil
	}
}

func (t *timerQueueStandbyTaskExecutor) refreshActivityTimer(
	ctx context.Context, wfContext historyi.WorkflowContext, mutableState historyi.MutableState, timerTask *tasks.ActivityTimeoutTask,
) error {
	timerSequence := t.getTimerSequence(mutableState)
	updateMutableState := false

	// Heartbeat timers have no history event to regenerate them on standby.
	// Persist a replacement timer before acknowledging the old task.

	// need to clear the activity heartbeat timer task marks
	// NOTE: LastHeartbeatTimeoutVisibilityInSeconds is for deduping heartbeat timer creation as it's possible
	// one heartbeat task was persisted multiple times with different taskIDs due to the retry logic
	// for updating workflow execution. In that case, only one new heartbeat timeout task should be
	// created.
	isHeartBeatTask := timerTask.TimeoutType == enumspb.TIMEOUT_TYPE_HEARTBEAT
	ai, heartbeatTimeoutVis, ok := mutableState.GetActivityInfoWithTimerHeartbeat(timerTask.EventID)

	if isHeartBeatTask && ok && queues.IsTimeExpired(timerTask, timerTask.GetVisibilityTime(), mutableState.ToPhysicalTimeWithCurrentTimeSkippingOffset(heartbeatTimeoutVis)) {
		if err := mutableState.UpdateActivityTaskStatusWithTimerHeartbeat(ai.ScheduledEventId, ai.TimerTaskStatus&^workflow.TimerTaskStatusCreatedHeartbeat, nil); err != nil {
			return err
		}
		updateMutableState = true
	}

	// passive logic need to explicitly call create timer
	modified, err := timerSequence.CreateNextActivityTimer()
	if err != nil {
		return err
	}
	updateMutableState = updateMutableState || modified

	if !updateMutableState {
		return nil
	}

	// TODO: why do we need to update the current version here?
	// Neither UpdateActivity nor CreateNextActivityTimer uses the current version.
	// Current version also not used when closing the transaction as passive
	//
	// we need to handcraft some of the variables
	// since the job being done here is update the activity and possibly write a timer task to DB
	// also need to reset the current version.
	lastWriteVersion, err := mutableState.GetLastWriteVersion()
	if err != nil {
		return err
	}
	if err := mutableState.UpdateCurrentVersion(lastWriteVersion, true); err != nil {
		return err
	}

	return wfContext.UpdateWorkflowExecutionAsPassive(ctx, t.shardContext)
}
