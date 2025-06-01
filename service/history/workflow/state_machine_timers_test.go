package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/testing/protorequire"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

func TestTrackStateMachineTimer_MaintainsSortedSlice(t *testing.T) {
	ctrl := gomock.NewController(t)
	ms := historyi.NewMockMutableState(ctrl)

	now := time.Now()
	execInfo := &persistencespb.WorkflowExecutionInfo{}
	ms.EXPECT().GetExecutionInfo().Return(execInfo).AnyTimes()
	ms.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	ms.EXPECT().NextTransitionCount().Return(int64(3)).AnyTimes()

	workflow.TrackStateMachineTimer(ms, now, &persistencespb.StateMachineTaskInfo{Type: "0", Ref: &persistencespb.StateMachineRef{
		Path: []*persistencespb.StateMachineKey{{Type: "t", Id: "a"}},
	}})
	// This should be deduped.
	workflow.TrackStateMachineTimer(ms, now, &persistencespb.StateMachineTaskInfo{Type: "0", Ref: &persistencespb.StateMachineRef{
		Path: []*persistencespb.StateMachineKey{{Type: "t", Id: "a"}},
	}})
	// This should be deduped.
	workflow.TrackStateMachineTimer(ms, now, &persistencespb.StateMachineTaskInfo{Type: "0", Ref: &persistencespb.StateMachineRef{
		Path: []*persistencespb.StateMachineKey{{Type: "t", Id: "b"}},
	}})
	workflow.TrackStateMachineTimer(ms, now.Add(time.Hour), &persistencespb.StateMachineTaskInfo{Type: "1"})
	workflow.TrackStateMachineTimer(ms, now.Add(time.Hour), &persistencespb.StateMachineTaskInfo{Type: "2"})
	workflow.TrackStateMachineTimer(ms, now.Add(-time.Hour), &persistencespb.StateMachineTaskInfo{Type: "3"})

	require.Equal(t, 3, len(execInfo.StateMachineTimers))

	require.Equal(t, 1, len(execInfo.StateMachineTimers[0].Infos))
	require.Equal(t, "3", execInfo.StateMachineTimers[0].Infos[0].Type)

	require.Equal(t, 2, len(execInfo.StateMachineTimers[1].Infos))
	require.Equal(t, "0", execInfo.StateMachineTimers[1].Infos[0].Type)
	protorequire.ProtoSliceEqual(t, []*persistencespb.StateMachineKey{{Type: "t", Id: "a"}}, execInfo.StateMachineTimers[1].Infos[0].Ref.Path)
	require.Equal(t, "0", execInfo.StateMachineTimers[1].Infos[1].Type)
	protorequire.ProtoSliceEqual(t, []*persistencespb.StateMachineKey{{Type: "t", Id: "b"}}, execInfo.StateMachineTimers[1].Infos[1].Ref.Path)

	require.Equal(t, 2, len(execInfo.StateMachineTimers[2].Infos))
	require.Equal(t, "1", execInfo.StateMachineTimers[2].Infos[0].Type)
	require.Equal(t, "2", execInfo.StateMachineTimers[2].Infos[1].Type)
}

func TestAddNextStateMachineTimerTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	ms := historyi.NewMockMutableState(ctrl)

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
