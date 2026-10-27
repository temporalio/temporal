package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testvars"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTaskQueueSingleSessionMetadata(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		assigned bool
	}{
		{name: "unassigned"},
		{name: "assigned", assigned: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tv := testvars.New(t)
			logger := log.NewTestLogger()
			store := newTestTaskManager(logger)
			partition := newTestTaskQueue(tv.NamespaceID().String(), tv.TaskQueue().GetName(), enumspb.TASK_QUEUE_TYPE_WORKFLOW).
				StickyPartition(tv.StickyTaskQueue().GetName())
			queue := UnversionedQueueKey(partition)
			cfg := newTaskQueueConfig(partition.TaskQueue(), NewConfig(dynamicconfig.NewCollection(dynamicconfig.NewMemoryClient(), logger)), tv.NamespaceName())
			_, err := store.CreateTaskQueue(t.Context(), &persistence.CreateTaskQueueRequest{
				RangeID: 1,
				TaskQueueInfo: &persistencespb.TaskQueueInfo{
					NamespaceId:           queue.NamespaceId(),
					Name:                  queue.PersistenceName(),
					TaskType:              queue.TaskType(),
					Kind:                  enumspb.TASK_QUEUE_KIND_STICKY,
					LastUpdateTime:        timestamppb.Now(),
					ExpiryTime:            timestamppb.New(time.Now().Add(time.Hour)),
					SingleSessionAssigned: tc.assigned,
				},
			})
			require.NoError(t, err)

			for iteration := range 3 {
				db := newTaskQueueDB(cfg, store, queue, logger, metrics.NoopMetricsHandler, false)
				_, err = db.RenewLease(t.Context())
				require.NoError(t, err)
				db.updateAckLevelAndBacklogStats(subqueueZero, int64(iteration+1), 0, time.Time{})
				require.NoError(t, db.SyncState(t.Context()))

				persisted, err := store.GetTaskQueue(t.Context(), &persistence.GetTaskQueueRequest{
					NamespaceID: queue.NamespaceId(), TaskQueue: queue.PersistenceName(), TaskType: queue.TaskType(),
				})
				require.NoError(t, err)
				require.Equal(t, tc.assigned, persisted.TaskQueueInfo.GetSingleSessionAssigned())
				require.Equal(t, int64(iteration+1), persisted.TaskQueueInfo.GetAckLevel())
				if tc.assigned {
					require.Nil(t, persisted.TaskQueueInfo.GetExpiryTime())
				} else {
					require.NotNil(t, persisted.TaskQueueInfo.GetExpiryTime())
					require.True(t, persisted.TaskQueueInfo.GetExpiryTime().AsTime().After(time.Now()))
				}
			}
		})
	}
}
