package history

import (
	"time"

	"github.com/gogo/protobuf/types"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
)

type (
	standbyActionFn     func(workflowExecutionContext, mutableState) (interface{}, error)
	standbyPostActionFn func(queueTaskInfo, interface{}, log.Logger) error

	standbyCurrentTimeFn func() time.Time
)

func standbyTaskPostActionNoOp(
	taskInfo queueTaskInfo,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	// return error so task processing logic will retry
	return ErrTaskRetry
}

func standbyTransferTaskPostActionTaskDiscarded(
	taskInfo queueTaskInfo,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	transferTask := taskInfo.(*persistenceblobs.TransferTaskInfo)
	logger.Error("Discarding standby transfer task due to task being pending for too long.",
		tag.WorkflowID(transferTask.GetWorkflowId()),
		tag.WorkflowRunIDBytes(transferTask.GetRunId()),
		tag.WorkflowNamespaceIDBytes(transferTask.GetNamespaceId()),
		tag.TaskID(transferTask.GetTaskId()),
		tag.TaskType(transferTask.TaskType),
		tag.FailoverVersion(transferTask.GetVersion()),
		tag.TimestampProto(transferTask.VisibilityTimestamp),
		tag.WorkflowEventID(transferTask.GetScheduleId()))
	return ErrTaskDiscarded
}

func standbyTimerTaskPostActionTaskDiscarded(
	taskInfo queueTaskInfo,
	postActionInfo interface{},
	logger log.Logger,
) error {

	if postActionInfo == nil {
		return nil
	}

	timerTask := taskInfo.(*persistenceblobs.TimerTaskInfo)
	logger.Error("Discarding standby timer task due to task being pending for too long.",
		tag.WorkflowID(timerTask.GetWorkflowId()),
		tag.WorkflowRunIDBytes(timerTask.GetRunId()),
		tag.WorkflowNamespaceIDBytes(timerTask.GetNamespaceId()),
		tag.TaskID(timerTask.GetTaskId()),
		tag.TaskType(timerTask.TaskType),
		tag.WorkflowTimeoutType(int64(timerTask.TimeoutType)),
		tag.FailoverVersion(timerTask.GetVersion()),
		tag.TimestampProto(timerTask.VisibilityTimestamp),
		tag.WorkflowEventID(timerTask.GetEventId()))
	return ErrTaskDiscarded
}

type (
	historyResendInfo struct {
		// used by 2DC, since 2DC only has one branch
		// TODO deprecate this nextEventID
		nextEventID *int64

		// used by NDC
		lastEventID      int64
		lastEventVersion int64
	}

	pushActivityToMatchingInfo struct {
		activityScheduleToStartTimeout int32
	}

	pushDecisionToMatchingInfo struct {
		decisionScheduleToStartTimeout int32
		tasklist                       tasklistpb.TaskList
	}
)

func newHistoryResendInfo(
	lastEventID int64,
	lastEventVersion int64,
) *historyResendInfo {
	return &historyResendInfo{
		lastEventID:      lastEventID,
		lastEventVersion: lastEventVersion,
	}
}

// TODO this logic is for 2DC, to be deprecated
func newHistoryResendInfoFor2DC(
	nextEventID int64,
) *historyResendInfo {
	return &historyResendInfo{
		nextEventID:      common.Int64Ptr(nextEventID),
		lastEventID:      common.EmptyEventID,
		lastEventVersion: common.EmptyVersion,
	}
}

func newPushActivityToMatchingInfo(
	activityScheduleToStartTimeout int32,
) *pushActivityToMatchingInfo {

	return &pushActivityToMatchingInfo{
		activityScheduleToStartTimeout: activityScheduleToStartTimeout,
	}
}

func newPushDecisionToMatchingInfo(
	decisionScheduleToStartTimeout int32,
	tasklist tasklistpb.TaskList,
) *pushDecisionToMatchingInfo {

	return &pushDecisionToMatchingInfo{
		decisionScheduleToStartTimeout: decisionScheduleToStartTimeout,
		tasklist:                       tasklist,
	}
}

func getHistoryResendInfo(
	mutableState mutableState,
) (*historyResendInfo, error) {

	// TODO this logic is for 2DC, to be deprecated
	if mutableState.GetVersionHistories() == nil {
		return newHistoryResendInfoFor2DC(mutableState.GetNextEventID()), nil
	}

	currentBranch, err := mutableState.GetVersionHistories().GetCurrentVersionHistory()
	if err != nil {
		return nil, err
	}
	lastItem, err := currentBranch.GetLastItem()
	if err != nil {
		return nil, err
	}
	return newHistoryResendInfo(lastItem.GetEventID(), lastItem.GetVersion()), nil
}

func getStandbyPostActionFn(
	taskInfo queueTaskInfo,
	standbyNow standbyCurrentTimeFn,
	standbyTaskMissingEventsResendDelay time.Duration,
	standbyTaskMissingEventsDiscardDelay time.Duration,
	fetchHistoryStandbyPostActionFn standbyPostActionFn,
	discardTaskStandbyPostActionFn standbyPostActionFn,
) standbyPostActionFn {

	// this is for task retry, use machine time
	now := standbyNow()
	taskTime, _ := types.TimestampFromProto(taskInfo.GetVisibilityTimestamp())
	resendTime := taskTime.Add(standbyTaskMissingEventsResendDelay)
	discardTime := taskTime.Add(standbyTaskMissingEventsDiscardDelay)

	// now < task start time + StandbyTaskMissingEventsResendDelay
	if now.Before(resendTime) {
		return standbyTaskPostActionNoOp
	}

	// task start time + StandbyTaskMissingEventsResendDelay <= now < task start time + StandbyTaskMissingEventsResendDelay
	if now.Before(discardTime) {
		return fetchHistoryStandbyPostActionFn
	}

	// task start time + StandbyTaskMissingEventsResendDelay <= now
	return discardTaskStandbyPostActionFn
}
