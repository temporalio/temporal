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

	workflow.TrackStateMachineTimer(ms, now, persistencespb.StateMachineTaskInfo_builder{Type: "0", Ref: persistencespb.StateMachineRef_builder{
		Path: []*persistencespb.StateMachineKey{persistencespb.StateMachineKey_builder{Type: "t", Id: "a"}.Build()},
	}.Build()}.Build())
	// This should be deduped.
	workflow.TrackStateMachineTimer(ms, now, persistencespb.StateMachineTaskInfo_builder{Type: "0", Ref: persistencespb.StateMachineRef_builder{
		Path: []*persistencespb.StateMachineKey{persistencespb.StateMachineKey_builder{Type: "t", Id: "a"}.Build()},
	}.Build()}.Build())
	// This should be deduped.
	workflow.TrackStateMachineTimer(ms, now, persistencespb.StateMachineTaskInfo_builder{Type: "0", Ref: persistencespb.StateMachineRef_builder{
		Path: []*persistencespb.StateMachineKey{persistencespb.StateMachineKey_builder{Type: "t", Id: "b"}.Build()},
	}.Build()}.Build())
	workflow.TrackStateMachineTimer(ms, now.Add(time.Hour), persistencespb.StateMachineTaskInfo_builder{Type: "1"}.Build())
	workflow.TrackStateMachineTimer(ms, now.Add(time.Hour), persistencespb.StateMachineTaskInfo_builder{Type: "2"}.Build())
	workflow.TrackStateMachineTimer(ms, now.Add(-time.Hour), persistencespb.StateMachineTaskInfo_builder{Type: "3"}.Build())

	require.Equal(t, 3, len(execInfo.GetStateMachineTimers()))

	require.Equal(t, 1, len(execInfo.GetStateMachineTimers()[0].GetInfos()))
	require.Equal(t, "3", execInfo.GetStateMachineTimers()[0].GetInfos()[0].GetType())

	require.Equal(t, 2, len(execInfo.GetStateMachineTimers()[1].GetInfos()))
	require.Equal(t, "0", execInfo.GetStateMachineTimers()[1].GetInfos()[0].GetType())
	protorequire.ProtoSliceEqual(t, []*persistencespb.StateMachineKey{persistencespb.StateMachineKey_builder{Type: "t", Id: "a"}.Build()}, execInfo.GetStateMachineTimers()[1].GetInfos()[0].GetRef().GetPath())
	require.Equal(t, "0", execInfo.GetStateMachineTimers()[1].GetInfos()[1].GetType())
	protorequire.ProtoSliceEqual(t, []*persistencespb.StateMachineKey{persistencespb.StateMachineKey_builder{Type: "t", Id: "b"}.Build()}, execInfo.GetStateMachineTimers()[1].GetInfos()[1].GetRef().GetPath())

	require.Equal(t, 2, len(execInfo.GetStateMachineTimers()[2].GetInfos()))
	require.Equal(t, "1", execInfo.GetStateMachineTimers()[2].GetInfos()[0].GetType())
	require.Equal(t, "2", execInfo.GetStateMachineTimers()[2].GetInfos()[1].GetType())
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

	workflow.TrackStateMachineTimer(ms, now, persistencespb.StateMachineTaskInfo_builder{Type: "0"}.Build())
	workflow.TrackStateMachineTimer(ms, now.Add(time.Hour), persistencespb.StateMachineTaskInfo_builder{Type: "1"}.Build())
	workflow.TrackStateMachineTimer(ms, now.Add(time.Hour), persistencespb.StateMachineTaskInfo_builder{Type: "2"}.Build())
	workflow.TrackStateMachineTimer(ms, now.Add(-time.Hour), persistencespb.StateMachineTaskInfo_builder{Type: "3"}.Build())

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
