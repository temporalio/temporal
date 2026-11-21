package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
)

func TestAppendTasksDeadline(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		earlierDeadline bool
	}{
		{name: "background context"},
		{name: "earlier caller deadline", earlierDeadline: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tc.earlierDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				defer cancel()
			}
			store := persistence.NewMockTaskManager(gomock.NewController(t))
			family, err := tqid.NewTaskQueueFamily("namespace", "queue")
			require.NoError(t, err)
			partition := family.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(0)
			config := newTaskQueueConfig(partition.TaskQueue(), NewConfig(dynamicconfig.NewNoopCollection()), "namespace")
			db := newTaskQueueDB(config, store, UnversionedQueueKey(partition), log.NewNoopLogger(), metrics.NoopMetricsHandler, false)
			db.subqueues = []*dbSubqueue{{}}
			writer := &priTaskWriter{
				backlogMgr: &priBacklogManagerImpl{tqCtx: ctx},
				db:         db,
			}
			var callCtx context.Context
			store.EXPECT().CreateTasks(gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctxFromStore context.Context, _ *persistence.CreateTasksRequest) (*persistence.CreateTasksResponse, error) {
					callCtx = ctxFromStore
					deadline, ok := callCtx.Deadline()
					require.True(t, ok, "background task writes must have a deadline")
					require.False(t, deadline.After(time.Now().Add(ioTimeout)))
					if tc.earlierDeadline {
						parentDeadline, _ := ctx.Deadline()
						require.Equal(t, parentDeadline, deadline)
					}
					return nil, context.Canceled
				},
			)
			err = writer.appendTasks([]*writeTaskRequest{{taskInfo: &persistencespb.TaskInfo{}}})
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorIs(t, callCtx.Err(), context.Canceled)
		})
	}
}
