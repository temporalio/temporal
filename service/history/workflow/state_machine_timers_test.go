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

package workflow_test

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

func TestTrackStateMachineTimer_MaintainsSortedSlice(t *testing.T) {
	ctrl := gomock.NewController(t)
	ms := workflow.NewMockMutableState(ctrl)

	now := time.Now()
	execInfo := &persistencespb.WorkflowExecutionInfo{}
	ms.EXPECT().GetExecutionInfo().Return(execInfo).AnyTimes()
	ms.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(3)).AnyTimes()

	workflow.TrackStateMachineTimer(ms, now, &persistencespb.StateMachineTaskInfo{Type: "0"})
	workflow.TrackStateMachineTimer(ms, now.Add(time.Hour), &persistencespb.StateMachineTaskInfo{Type: "1"})
	workflow.TrackStateMachineTimer(ms, now.Add(time.Hour), &persistencespb.StateMachineTaskInfo{Type: "2"})
	workflow.TrackStateMachineTimer(ms, now.Add(-time.Hour), &persistencespb.StateMachineTaskInfo{Type: "3"})

	require.Equal(t, 3, len(execInfo.StateMachineTimers))
	require.Equal(t, "3", execInfo.StateMachineTimers[0].Infos[0].Type)
	require.Equal(t, "0", execInfo.StateMachineTimers[1].Infos[0].Type)
	require.Equal(t, "1", execInfo.StateMachineTimers[2].Infos[0].Type)
	require.Equal(t, "2", execInfo.StateMachineTimers[2].Infos[1].Type)
}

func TestAddNextStateMachineTimerTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	ms := workflow.NewMockMutableState(ctrl)

	now := time.Now().UTC()
	var scheduledTasks []tasks.Task

	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()
	ms.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey("ns-id", "wf-id", "run-id")).AnyTimes()
	ms.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(3)).AnyTimes()
	ms.EXPECT().AddTasks(gomock.Any()).DoAndReturn(func(task tasks.Task) {
		scheduledTasks = append(scheduledTasks, task)
	})

	workflow.TrackStateMachineTimer(ms, now, &persistencespb.StateMachineTaskInfo{Type: "0"})
	workflow.TrackStateMachineTimer(ms, now.Add(time.Hour), &persistencespb.StateMachineTaskInfo{Type: "1"})
	workflow.TrackStateMachineTimer(ms, now.Add(time.Hour), &persistencespb.StateMachineTaskInfo{Type: "2"})
	workflow.TrackStateMachineTimer(ms, now.Add(-time.Hour), &persistencespb.StateMachineTaskInfo{Type: "3"})

	workflow.AddNextStateMachineTimerTask(ms)

	require.Equal(t, 1, len(scheduledTasks))
	task, ok := scheduledTasks[0].(*tasks.StateMachineTimerTask)
	require.True(t, ok)
	require.Equal(t, "ns-id", task.GetNamespaceID())
	require.Equal(t, "wf-id", task.GetWorkflowID())
	require.Equal(t, "run-id", task.GetRunID())
	require.Equal(t, int64(1), task.Version)
	require.Equal(t, now.Add(-time.Hour), task.VisibilityTimestamp)

	// First timer already scheduled should not generate any tasks.
	workflow.AddNextStateMachineTimerTask(ms)
	require.Equal(t, 1, len(scheduledTasks))
}
