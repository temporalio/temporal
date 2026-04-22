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
	timers := ms.GetExecutionInfo().StateMachineTimers
	timers = slices.DeleteFunc(timers, func(timerGroup *persistencespb.StateMachineTimerGroup) bool {
		return len(timerGroup.Infos) == 0
	})
	ms.GetExecutionInfo().StateMachineTimers = timers

	if len(timers) == 0 {
		return
	}

	timerGroup := timers[0]
	// We already have a timer for this deadline.
	if timerGroup.Scheduled {
		return
	}
	ms.AddTasks(&tasks.StateMachineTimerTask{
		WorkflowKey:         ms.GetWorkflowKey(),
		VisibilityTimestamp: timerGroup.Deadline.AsTime(),
		Version:             ms.GetCurrentVersion(),
	})
	timerGroup.Scheduled = true
}

// TrackStateMachineTimer tracks a timer task in the mutable state's StateMachineTimers slice sorted and grouped by
// deadline.
// Only a single task for a given type can be tracked for a given machine. If a task of the same type is already
// tracked, it will be overridden.
func TrackStateMachineTimer(ms historyi.MutableState, deadline time.Time, taskInfo *persistencespb.StateMachineTaskInfo) {
	execInfo := ms.GetExecutionInfo()
	group := &persistencespb.StateMachineTimerGroup{
		Deadline: timestamppb.New(deadline),
		Infos:    []*persistencespb.StateMachineTaskInfo{taskInfo},
	}
	idx, groupFound := slices.BinarySearchFunc(execInfo.StateMachineTimers, group, func(a, b *persistencespb.StateMachineTimerGroup) int {
		return a.Deadline.AsTime().Compare(b.Deadline.AsTime())
	})
	if groupFound {
		groupIdx := slices.IndexFunc(execInfo.StateMachineTimers[idx].Infos, func(info *persistencespb.StateMachineTaskInfo) bool {
			return info.GetType() == taskInfo.GetType() && slices.EqualFunc(info.GetRef().GetPath(), taskInfo.GetRef().GetPath(), func(a, b *persistencespb.StateMachineKey) bool {
				return a.GetType() == b.GetType() && a.GetId() == b.GetId()
			})
		})
		if groupIdx == -1 {
			execInfo.StateMachineTimers[idx].Infos = append(execInfo.StateMachineTimers[idx].Infos, taskInfo)
		} else {
			execInfo.StateMachineTimers[idx].Infos[groupIdx] = taskInfo
		}
	} else {
		execInfo.StateMachineTimers = slices.Insert(execInfo.StateMachineTimers, idx, group)
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
		mutableState.GetExecutionInfo().StateMachineTimers = nil
		return nil
	}

	hsmRoot := mutableState.HSM()
	trimmedStateMachineTimers := make([]*persistencespb.StateMachineTimerGroup, 0, len(mutableState.GetExecutionInfo().StateMachineTimers))
	for _, timerGroup := range mutableState.GetExecutionInfo().StateMachineTimers {
		trimmedTaskInfos := make([]*persistencespb.StateMachineTaskInfo, 0, len(timerGroup.Infos))
		for _, taskInfo := range timerGroup.GetInfos() {

			node, err := hsmRoot.Child(hsm.Ref{
				StateMachineRef: taskInfo.Ref,
			}.StateMachinePath())
			if err != nil {
				if errors.Is(err, hsm.ErrStateMachineNotFound) {
					// node deleted, trim the task
					continue
				}
				return err
			}

			if transitionhistory.Compare(
				node.InternalRepr().LastUpdateVersionedTransition,
				minVersionedTransition,
			) >= 0 {
				// node recently updated, trim the task.
				// A complementary step is then required to regenerate timer tasks for it.
				continue
			}

			trimmedTaskInfos = append(trimmedTaskInfos, taskInfo)
		}
		if len(trimmedTaskInfos) > 0 || timerGroup.Scheduled {
			// We still want to keep the timer group if it has been scheduled even if it has no task info.
			// This will prevent us from scheduling a new timer task for the same group.
			trimmedStateMachineTimers = append(trimmedStateMachineTimers, &persistencespb.StateMachineTimerGroup{
				Infos:     trimmedTaskInfos,
				Deadline:  timerGroup.Deadline,
				Scheduled: timerGroup.Scheduled,
			})
		}
	}
	mutableState.GetExecutionInfo().StateMachineTimers = trimmedStateMachineTimers
	return nil
}
