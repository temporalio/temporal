package persistence

import (
	"testing"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/tasks"
)

func TestWorkflowMutationMerge(t *testing.T) {
	t.Parallel()
	for _, clear := range []bool{false, true} {
		t.Run(map[bool]string{false: "append", true: "clear"}[clear], func(t *testing.T) {
			t.Parallel()
			first := &WorkflowMutation{
				ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{NamespaceId: "namespace", WorkflowId: "workflow"},
				ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "run"}, Condition: 10, NextEventID: 11, DBRecordVersion: 42,
				UpsertTimerInfos:         map[string]*persistencespb.TimerInfo{"replace": {TimerId: "old"}, "remove": {TimerId: "remove"}},
				DeleteTimerInfos:         map[string]struct{}{"restore": {}},
				UpsertSignalRequestedIDs: map[string]struct{}{"old": {}},
				NewBufferedEvents:        []*historypb.HistoryEvent{{EventId: 1}},
				Tasks:                    map[tasks.Category][]tasks.Task{tasks.CategoryTransfer: {&tasks.WorkflowTask{ScheduledEventID: 10}}},
				BestEffortDeleteTasks:    map[tasks.Category][]tasks.Key{tasks.CategoryTransfer: {tasks.NewImmediateKey(1)}},
			}
			last := &WorkflowMutation{
				ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{NamespaceId: "namespace", WorkflowId: "workflow", SignalCount: 2},
				ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "run"}, Condition: 11, NextEventID: 12, DBRecordVersion: 42,
				UpsertTimerInfos: map[string]*persistencespb.TimerInfo{"replace": {TimerId: "new"}, "restore": {TimerId: "restore"}},
				DeleteTimerInfos: map[string]struct{}{"remove": {}}, DeleteSignalRequestedIDs: map[string]struct{}{"old": {}},
				NewBufferedEvents: []*historypb.HistoryEvent{{EventId: 2}}, ClearBufferedEvents: clear,
				Tasks:                 map[tasks.Category][]tasks.Task{tasks.CategoryTransfer: {&tasks.WorkflowTask{ScheduledEventID: 11}}},
				BestEffortDeleteTasks: map[tasks.Category][]tasks.Key{tasks.CategoryTransfer: {tasks.NewImmediateKey(2)}},
			}
			merged, err := first.Merge(last)
			require.NoError(t, err)
			require.Equal(t, int64(10), merged.Condition)
			require.Equal(t, int64(12), merged.NextEventID)
			protorequire.ProtoEqual(t, last.ExecutionInfo, merged.ExecutionInfo)
			require.Len(t, merged.UpsertTimerInfos, 2)
			protorequire.ProtoEqual(t, &persistencespb.TimerInfo{TimerId: "new"}, merged.UpsertTimerInfos["replace"])
			protorequire.ProtoEqual(t, &persistencespb.TimerInfo{TimerId: "restore"}, merged.UpsertTimerInfos["restore"])
			require.Equal(t, map[string]struct{}{"remove": {}}, merged.DeleteTimerInfos)
			require.Empty(t, merged.UpsertSignalRequestedIDs)
			require.Equal(t, map[string]struct{}{"old": {}}, merged.DeleteSignalRequestedIDs)
			wantEvents := []*historypb.HistoryEvent{{EventId: 1}, {EventId: 2}}
			if clear {
				wantEvents = wantEvents[1:]
			}
			protorequire.ProtoSliceEqual(t, wantEvents, merged.NewBufferedEvents)
			require.Equal(t, clear, merged.ClearBufferedEvents)
			require.Len(t, merged.Tasks[tasks.CategoryTransfer], 2)
			require.Equal(t, []tasks.Key{tasks.NewImmediateKey(1), tasks.NewImmediateKey(2)}, merged.BestEffortDeleteTasks[tasks.CategoryTransfer])
			merged.ExecutionInfo.SignalCount = 99
			merged.UpsertTimerInfos["replace"].TimerId = "mutated"
			merged.NewBufferedEvents[0].EventId = 99
			merged.Tasks[tasks.CategoryTransfer][0].SetTaskID(99)
			require.Equal(t, int64(2), last.ExecutionInfo.SignalCount)
			require.Equal(t, "new", last.UpsertTimerInfos["replace"].TimerId)
			require.Equal(t, int64(1), first.NewBufferedEvents[0].EventId)
			require.Equal(t, int64(0), first.Tasks[tasks.CategoryTransfer][0].GetTaskID())
		})
	}
}

func TestWorkflowMutationMergeIncompatible(t *testing.T) {
	t.Parallel()
	first := &WorkflowMutation{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{WorkflowId: "workflow"}, ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "run"}, NextEventID: 11}
	for _, next := range []*WorkflowMutation{
		{ExecutionInfo: first.ExecutionInfo, ExecutionState: first.ExecutionState, Condition: 12},
		{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{WorkflowId: "another"}, ExecutionState: first.ExecutionState, Condition: 11},
	} {
		_, err := first.Merge(next)
		require.ErrorIs(t, err, ErrIncompatibleWorkflowMutations)
	}
}
