package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCassandraTaskWriteFence(t *testing.T) {
	testData, tearDown := setUpCassandraTest(t)
	defer tearDown()
	store, err := testData.Factory.NewTaskStore()
	require.NoError(t, err)
	manager := persistence.NewTaskManager(store, serialization.NewSerializer())

	for _, updateMetadata := range []bool{true, false} {
		for _, commitBeforeFence := range []bool{true, false} {
			name := "commit_after_fence"
			if commitBeforeFence {
				name = "commit_before_fence"
			}
			if updateMetadata {
				name += "_with_metadata"
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
				defer cancel()
				info := &persistencespb.TaskQueueInfo{
					NamespaceId: uuid.NewString(), Name: uuid.NewString(),
					TaskType: enumspb.TASK_QUEUE_TYPE_WORKFLOW, Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
					LastUpdateTime: timestamppb.Now(),
				}
				_, err := manager.CreateTaskQueue(ctx, &persistence.CreateTaskQueueRequest{RangeID: 1, TaskQueueInfo: info})
				require.NoError(t, err)

				// Delay a write carrying the old range ID until before or after the
				// lease update. Both outcomes must be safe for subsequent task reads.
				release := make(chan struct{})
				releaseWrite := sync.OnceFunc(func() { close(release) })
				result := make(chan error, 1)
				done := make(chan struct{})
				t.Cleanup(func() {
					releaseWrite()
					<-done
				})
				go func() {
					defer close(done)
					<-release
					_, err := manager.CreateTasks(ctx, &persistence.CreateTasksRequest{
						TaskQueueInfo:  &persistence.PersistedTaskQueueInfo{RangeID: 1, Data: info},
						Tasks:          []*persistencespb.AllocatedTaskInfo{{TaskId: 1, Data: &persistencespb.TaskInfo{CreateTime: timestamppb.Now()}}},
						UpdateMetadata: updateMetadata,
					})
					result <- err
				}()

				read := func() []*persistencespb.AllocatedTaskInfo {
					response, err := manager.GetTasks(ctx, &persistence.GetTasksRequest{
						NamespaceID: info.NamespaceId, TaskQueue: info.Name, TaskType: info.TaskType,
						InclusiveMinTaskID: 1, ExclusiveMaxTaskID: 2, PageSize: 10,
					})
					require.NoError(t, err)
					return response.Tasks
				}
				require.Empty(t, read())
				if commitBeforeFence {
					releaseWrite()
					require.NoError(t, <-result)
				}
				_, err = manager.UpdateTaskQueue(ctx, &persistence.UpdateTaskQueueRequest{
					PrevRangeID: 1, RangeID: 2, TaskQueueInfo: info,
				})
				require.NoError(t, err)
				if commitBeforeFence {
					require.Len(t, read(), 1)
				} else {
					releaseWrite()
					require.ErrorAs(t, <-result, new(*persistence.ConditionFailedError))
					require.Empty(t, read())
				}
			})
		}
	}
}
