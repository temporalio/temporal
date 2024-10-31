// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package workflow

import (
	"errors"
	"slices"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AddNextStateMachineTimerTask generates a state machine timer task if the first deadline doesn't have a task scheduled
// yet.
func AddNextStateMachineTimerTask(ms MutableState) {
	timers := ms.GetExecutionInfo().StateMachineTimers
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
func TrackStateMachineTimer(ms MutableState, deadline time.Time, taskInfo *persistencespb.StateMachineTaskInfo) {
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
	mutableState MutableState,
	minVersionedTransition *persistencespb.VersionedTransition,
) error {
	if CompareVersionedTransition(minVersionedTransition, EmptyVersionedTransition) == 0 {
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

			if CompareVersionedTransition(
				node.InternalRepr().LastUpdateVersionedTransition,
				minVersionedTransition,
			) >= 0 {
				// node recently updated, trim the task.
				// A complementary step is then required to regenerate timer tasks for it.
				continue
			}

			trimmedTaskInfos = append(trimmedTaskInfos, taskInfo)
		}
		if len(trimmedTaskInfos) > 0 {
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
