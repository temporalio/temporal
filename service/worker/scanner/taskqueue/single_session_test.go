package taskqueue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The existing scanner suite mutates package-level job limits.
//
//parallelize:ignore
func TestSingleSessionMetadataCleanup(t *testing.T) {
	for _, tc := range []struct {
		name     string
		assigned bool
		kind     enumspb.TaskQueueKind
		taskType enumspb.TaskQueueType
		retained bool
	}{
		{name: "assigned workflow sticky", assigned: true, kind: enumspb.TASK_QUEUE_KIND_STICKY, taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, retained: true},
		{name: "unassigned workflow sticky", kind: enumspb.TASK_QUEUE_KIND_STICKY, taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
		{name: "activity sticky", assigned: true, kind: enumspb.TASK_QUEUE_KIND_STICKY, taskType: enumspb.TASK_QUEUE_TYPE_ACTIVITY},
		{name: "normal workflow", assigned: true, kind: enumspb.TASK_QUEUE_KIND_NORMAL, taskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, expiredTask := range []bool{false, true} {
				name := "empty queue"
				if expiredTask {
					name = "expired task"
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					tv := testvars.New(t)
					manager := p.NewMockTaskManager(gomock.NewController(t))
					scavenger := NewScavenger(manager, metrics.NoopMetricsHandler, log.NewTestLogger())
					t.Cleanup(scavenger.lifecycleCancel)
					info := &p.PersistedTaskQueueInfo{
						RangeID: 7,
						Data: &persistencespb.TaskQueueInfo{
							NamespaceId: tv.NamespaceID().String(), Name: tv.StickyTaskQueue().GetName(),
							TaskType: tc.taskType, Kind: tc.kind, SingleSessionAssigned: tc.assigned,
							LastUpdateTime: timestamppb.New(time.Now().Add(-72 * time.Hour)),
						},
					}
					response := &p.GetTasksResponse{}
					if expiredTask {
						response.Tasks = []*persistencespb.AllocatedTaskInfo{{TaskId: 1, Data: &persistencespb.TaskInfo{ExpiryTime: timestamppb.New(time.Now().Add(-time.Hour))}}}
						manager.EXPECT().CompleteTasksLessThan(gomock.Any(), &p.CompleteTasksLessThanRequest{
							NamespaceID: info.Data.NamespaceId, TaskQueueName: info.Data.Name, TaskType: tc.taskType,
							ExclusiveMaxTaskID: 2, Limit: 1,
						}).Return(1, nil)
					}
					manager.EXPECT().GetTasks(gomock.Any(), gomock.Any()).Return(response, nil)
					if !tc.retained {
						manager.EXPECT().DeleteTaskQueue(gomock.Any(), &p.DeleteTaskQueueRequest{
							TaskQueue: &p.TaskQueueKey{NamespaceID: info.Data.NamespaceId, TaskQueueName: info.Data.Name, TaskQueueType: tc.taskType},
							RangeID:   7,
						}).Return(nil)
					}
					require.Equal(t, handlerStatusDone, scavenger.newTask(info).Run())
					var expectedDeleted int64
					if !tc.retained {
						expectedDeleted = 1
					}
					require.Equal(t, expectedDeleted, scavenger.stats.taskqueue.nDeleted)
					require.Equal(t, int64(len(response.Tasks)), scavenger.stats.task.nDeleted)
				})
			}
		})
	}
}
