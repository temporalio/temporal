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
	"slices"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
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
func TrackStateMachineTimer(ms MutableState, deadline time.Time, taskInfo *persistencespb.StateMachineTaskInfo) {
	execInfo := ms.GetExecutionInfo()
	group := &persistencespb.StateMachineTimerGroup{
		Deadline: timestamppb.New(deadline),
		Infos:    []*persistencespb.StateMachineTaskInfo{taskInfo},
	}
	idx, found := slices.BinarySearchFunc(execInfo.StateMachineTimers, group, func(a, b *persistencespb.StateMachineTimerGroup) int {
		return a.Deadline.AsTime().Compare(b.Deadline.AsTime())
	})
	if found {
		execInfo.StateMachineTimers[idx].Infos = append(execInfo.StateMachineTimers[idx].Infos, taskInfo)
	} else {
		execInfo.StateMachineTimers = slices.Insert(execInfo.StateMachineTimers, idx, group)
	}
}
