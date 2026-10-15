package matching

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/matching/counter"
	"go.uber.org/mock/gomock"
)

func TestTaskWriterRejectsBatchAfterShutdown_Pri(t *testing.T) {
	t.Parallel()
	testTaskWriterRejectsBatchAfterShutdown(t, false)
}

func TestTaskWriterRejectsBatchAfterShutdown_Fair(t *testing.T) {
	t.Parallel()
	testTaskWriterRejectsBatchAfterShutdown(t, true)
}

func testTaskWriterRejectsBatchAfterShutdown(t *testing.T, fairness bool) {
	t.Helper()

	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	queue := newUnversionedRootQueueKey(namespaceID, "test-queue", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	pqMgr := NewMockphysicalTaskQueueManager(gomock.NewController(t))
	pqMgr.EXPECT().QueueKey().Return(queue).AnyTimes()
	pqMgr.EXPECT().GetFairnessWeightOverrides().Return(fairnessWeightOverrides{}).AnyTimes()
	pqMgr.EXPECT().AddSpooledTask(gomock.Any()).Return(nil).AnyTimes()
	taskMgr := newTestTaskManager(logger)
	if fairness {
		taskMgr = newTestFairTaskManager(logger)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := taskMgr.CreateTaskQueue(ctx, &persistence.CreateTaskQueueRequest{
		RangeID: 1,
		TaskQueueInfo: &persistencespb.TaskQueueInfo{
			NamespaceId: queue.NamespaceId(),
			Name:        queue.PersistenceName(),
			TaskType:    queue.TaskType(),
		},
	})
	require.NoError(t, err)
	config := newTaskQueueConfig(queue.Partition().TaskQueue(), defaultTestConfig(), matchingTestNamespace)
	var writeBatch func([]*writeTaskRequest) error
	if fairness {
		blm := newFairBacklogManager(
			ctx, pqMgr, config, taskMgr, logger, logger, nil, metrics.NoopMetricsHandler,
			func() counter.Counter { return counter.NewMapCounter(1000) }, false,
		)
		blm.db.rangeID = 1
		blm.db.subqueues = []*dbSubqueue{{}}
		reader := newFairTaskReader(blm, subqueueZero, fairLevel{})
		reader.atEnd = true
		blm.subqueues = []*fairTaskReader{reader}
		writeBatch = blm.taskWriter.writeBatch
	} else {
		blm := newPriBacklogManager(ctx, pqMgr, config, taskMgr, logger, logger, nil, metrics.NoopMetricsHandler, false)
		blm.db.rangeID = 1
		blm.db.subqueues = []*dbSubqueue{{}}
		blm.subqueues = []*priTaskReader{newPriTaskReader(blm, subqueueZero, 0)}
		writeBatch = blm.taskWriter.appendTasks
	}

	// A canceled queue can still have buffered appends. Reject the next batch before
	// persistence even if the store would accept it with the old range ID.
	cancel()
	err = writeBatch([]*writeTaskRequest{{
		fairLevel: fairLevel{id: 1},
		taskInfo:  &persistencespb.TaskInfo{},
	}})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, taskMgr.getTaskCount(queue))
}
