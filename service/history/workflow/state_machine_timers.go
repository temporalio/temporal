package workflow

import (
	"errors"
	"slices"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AddNextStateMachineTimerTask generates a state machine timer task if the first deadline doesn't have a task scheduled
// yet.
func AddNextStateMachineTimerTask(ms historyi.MutableState) {
	// filter out empty timer groups
	timers := ms.GetExecutionInfo().GetStateMachineTimers()
	timers = slices.DeleteFunc(timers, func(timerGroup *persistencespb.StateMachineTimerGroup) bool {
		return len(timerGroup.GetInfos()) == 0
	})
	ms.GetExecutionInfo().SetStateMachineTimers(timers)

	if len(timers) == 0 {
		return
	}

	timerGroup := timers[0]
	// We already have a timer for this deadline.
	if timerGroup.GetScheduled() {
		return
	}
	ms.AddTasks(&tasks.StateMachineTimerTask{
		WorkflowKey:         ms.GetWorkflowKey(),
		VisibilityTimestamp: timerGroup.GetDeadline().AsTime(),
		Version:             ms.GetCurrentVersion(),
	})
	timerGroup.SetScheduled(true)
}

// TrackStateMachineTimer tracks a timer task in the mutable state's StateMachineTimers slice sorted and grouped by
// deadline.
// Only a single task for a given type can be tracked for a given machine. If a task of the same type is already
// tracked, it will be overridden.
func TrackStateMachineTimer(ms historyi.MutableState, deadline time.Time, taskInfo *persistencespb.StateMachineTaskInfo) {
	execInfo := ms.GetExecutionInfo()
	group := persistencespb.StateMachineTimerGroup_builder{
		Deadline: timestamppb.New(deadline),
		Infos:    []*persistencespb.StateMachineTaskInfo{taskInfo},
	}.Build()
	idx, groupFound := slices.BinarySearchFunc(execInfo.GetStateMachineTimers(), group, func(a, b *persistencespb.StateMachineTimerGroup) int {
		return a.GetDeadline().AsTime().Compare(b.GetDeadline().AsTime())
	})
	if groupFound {
		groupIdx := slices.IndexFunc(execInfo.GetStateMachineTimers()[idx].GetInfos(), func(info *persistencespb.StateMachineTaskInfo) bool {
			return info.GetType() == taskInfo.GetType() && slices.EqualFunc(info.GetRef().GetPath(), taskInfo.GetRef().GetPath(), func(a, b *persistencespb.StateMachineKey) bool {
				return a.GetType() == b.GetType() && a.GetId() == b.GetId()
			})
		})
		if groupIdx == -1 {
			execInfo.GetStateMachineTimers()[idx].SetInfos(append(execInfo.GetStateMachineTimers()[idx].GetInfos(), taskInfo))
		} else {
			execInfo.GetStateMachineTimers()[idx].GetInfos()[groupIdx] = taskInfo
		}
	} else {
		execInfo.SetStateMachineTimers(slices.Insert(execInfo.GetStateMachineTimers(), idx, group))
	}
}

// TrimStateMachineTimers returns of copy of trimmed the StateMachineTimers slice by removing any timer tasks that are
// associated with an HSM node that has been deleted or updated on or after the provided minVersionedTransition.
func TrimStateMachineTimers(
	mutableState historyi.MutableState,
	minVersionedTransition *persistencespb.VersionedTransition,
) error {
	if transitionhistory.Compare(minVersionedTransition, EmptyVersionedTransition) == 0 {
		// Reset all the state machine timers, we'll recreate them all.
		mutableState.GetExecutionInfo().SetStateMachineTimers(nil)
		return nil
	}

	hsmRoot := mutableState.HSM()
	trimmedStateMachineTimers := make([]*persistencespb.StateMachineTimerGroup, 0, len(mutableState.GetExecutionInfo().GetStateMachineTimers()))
	for _, timerGroup := range mutableState.GetExecutionInfo().GetStateMachineTimers() {
		trimmedTaskInfos := make([]*persistencespb.StateMachineTaskInfo, 0, len(timerGroup.GetInfos()))
		for _, taskInfo := range timerGroup.GetInfos() {

			node, err := hsmRoot.Child(hsm.Ref{
				StateMachineRef: taskInfo.GetRef(),
			}.StateMachinePath())
			if err != nil {
				if errors.Is(err, hsm.ErrStateMachineNotFound) {
					// node deleted, trim the task
					continue
				}
				return err
			}

			if transitionhistory.Compare(
				node.InternalRepr().GetLastUpdateVersionedTransition(),
				minVersionedTransition,
			) >= 0 {
				// node recently updated, trim the task.
				// A complementary step is then required to regenerate timer tasks for it.
				continue
			}

			trimmedTaskInfos = append(trimmedTaskInfos, taskInfo)
		}
		if len(trimmedTaskInfos) > 0 || timerGroup.GetScheduled() {
			// We still want to keep the timer group if it has been scheduled even if it has no task info.
			// This will prevent us from scheduling a new timer task for the same group.
			trimmedStateMachineTimers = append(trimmedStateMachineTimers, persistencespb.StateMachineTimerGroup_builder{
				Infos:     trimmedTaskInfos,
				Deadline:  timerGroup.GetDeadline(),
				Scheduled: timerGroup.GetScheduled(),
			}.Build())
		}
	}
	mutableState.GetExecutionInfo().SetStateMachineTimers(trimmedStateMachineTimers)
	return nil
}
