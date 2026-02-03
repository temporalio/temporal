package requestcancelworkflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"github.com/golang/mock/gomock"
)

func TestCreateActivityCancelControlTasks(t *testing.T) {
	tests := []struct {
		name               string
		activities         map[int64]*persistencespb.ActivityInfo
		expectedTaskCount  int
		expectedWorkerKeys []string
	}{
		{
			name:              "no activities",
			activities:        map[int64]*persistencespb.ActivityInfo{},
			expectedTaskCount: 0,
		},
		{
			name: "only unstarted activities",
			activities: map[int64]*persistencespb.ActivityInfo{
				1: {
					ScheduledEventId:  1,
					StartedEventId:    common.EmptyEventID,
					WorkerInstanceKey: "worker-1",
				},
			},
			expectedTaskCount: 0,
		},
		{
			name: "only activities without worker key",
			activities: map[int64]*persistencespb.ActivityInfo{
				1: {
					ScheduledEventId:  1,
					StartedEventId:    10,
					WorkerInstanceKey: "",
				},
			},
			expectedTaskCount: 0,
		},
		{
			name: "single started activity with worker key",
			activities: map[int64]*persistencespb.ActivityInfo{
				1: {
					ScheduledEventId:  1,
					StartedEventId:    10,
					WorkerInstanceKey: "worker-1",
				},
			},
			expectedTaskCount:  1,
			expectedWorkerKeys: []string{"worker-1"},
		},
		{
			name: "multiple activities on same worker",
			activities: map[int64]*persistencespb.ActivityInfo{
				1: {
					ScheduledEventId:  1,
					StartedEventId:    10,
					WorkerInstanceKey: "worker-1",
				},
				2: {
					ScheduledEventId:  2,
					StartedEventId:    20,
					WorkerInstanceKey: "worker-1",
				},
			},
			expectedTaskCount:  1,
			expectedWorkerKeys: []string{"worker-1"},
		},
		{
			name: "activities on different workers",
			activities: map[int64]*persistencespb.ActivityInfo{
				1: {
					ScheduledEventId:  1,
					StartedEventId:    10,
					WorkerInstanceKey: "worker-1",
				},
				2: {
					ScheduledEventId:  2,
					StartedEventId:    20,
					WorkerInstanceKey: "worker-2",
				},
			},
			expectedTaskCount:  2,
			expectedWorkerKeys: []string{"worker-1", "worker-2"},
		},
		{
			name: "mixed: started with key, started without key, unstarted",
			activities: map[int64]*persistencespb.ActivityInfo{
				1: {
					ScheduledEventId:  1,
					StartedEventId:    10,
					WorkerInstanceKey: "worker-1",
				},
				2: {
					ScheduledEventId:  2,
					StartedEventId:    20,
					WorkerInstanceKey: "", // no worker key
				},
				3: {
					ScheduledEventId:  3,
					StartedEventId:    common.EmptyEventID, // not started
					WorkerInstanceKey: "worker-1",
				},
			},
			expectedTaskCount:  1,
			expectedWorkerKeys: []string{"worker-1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			ms := historyi.NewMockMutableState(ctrl)
			ms.EXPECT().GetPendingActivityInfos().Return(tc.activities)
			ms.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(
				"namespace-id",
				"workflow-id",
				"run-id",
			)).AnyTimes()

			var addedTasks []tasks.Task
			ms.EXPECT().AddTasks(gomock.Any()).DoAndReturn(func(ts ...tasks.Task) {
				addedTasks = append(addedTasks, ts...)
			}).AnyTimes()

			createActivityCancelControlTasks(ms)

			require.Len(t, addedTasks, tc.expectedTaskCount)

			if tc.expectedTaskCount > 0 {
				workerKeysFound := make(map[string]bool)
				for _, task := range addedTasks {
					cancelTask, ok := task.(*tasks.ActivityCancelControlTask)
					require.True(t, ok, "expected ActivityCancelControlTask")
					workerKeysFound[cancelTask.WorkerInstanceKey] = true
				}
				for _, expectedKey := range tc.expectedWorkerKeys {
					require.True(t, workerKeysFound[expectedKey], "expected worker key %s not found", expectedKey)
				}
			}
		})
	}
}

