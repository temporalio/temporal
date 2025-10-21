package queues_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/queues/queuestest"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"
)

type (
	logRecord struct {
		msg  string
		tags []tag.Tag
	}
	logRecorder struct {
		log.SnTaggedLogger
		mu      sync.Mutex
		records []logRecord
	}
)

func (l *logRecorder) Warn(msg string, tags ...tag.Tag) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, logRecord{msg: msg, tags: tags})
}

func TestDLQWriter_ErrGetNamespaceName(t *testing.T) {
	t.Parallel()

	queueWriter := &queuestest.FakeQueueWriter{}
	ctrl := gomock.NewController(t)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	errorMsg := "GetNamespaceByID failed"
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(nil, errors.New(errorMsg)).AnyTimes()
	logger := &logRecorder{SnTaggedLogger: log.NewTestLogger()}
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	writer := queues.NewDLQWriter(queueWriter, metricsHandler, logger, namespaceRegistry)
	task := &tasks.WorkflowTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: string(tests.NamespaceID),
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
		},
	}
	err := writer.WriteTaskToDLQ(
		context.Background(),
		"source-cluster",
		"target-cluster",
		tasks.GetShardIDForTask(task, 100),
		task,
		true,
	)
	require.NoError(t, err)
	require.Len(t, queueWriter.EnqueueTaskRequests, 1)
	request := queueWriter.EnqueueTaskRequests[0]
	expectedShardID := tasks.GetShardIDForTask(task, 100)
	assert.Equal(t, expectedShardID, request.SourceShardID)
	assert.NotEmpty(t, logger.records)
	assert.Contains(t, logger.records[0].msg, "Failed to get namespace name while trying to write a task to DLQ")
	assert.Equal(t, logger.records[0].tags[1].Value(), errorMsg)
	assert.Contains(t, logger.records[1].msg, "Task enqueued to DLQ")
	snapshot := capture.Snapshot()
	recordings := snapshot[metrics.DLQWrites.Name()]
	assert.Len(t, recordings, 1)
	counter, ok := recordings[0].Value.(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(1), counter)
	assert.Len(t, recordings[0].Tags, 2)
	assert.Equal(t, "transfer", recordings[0].Tags[metrics.TaskCategoryTagName])
	namespaceStateTag := metrics.NamespaceStateTag(metrics.ActiveNamespaceStateTagValue)
	assert.Equal(t, metrics.ActiveNamespaceStateTagValue, recordings[0].Tags[namespaceStateTag.Key])
}

func TestDLQWriter_Ok(t *testing.T) {
	t.Parallel()

	queueWriter := &queuestest.FakeQueueWriter{}
	ctrl := gomock.NewController(t)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
	logger := &logRecorder{SnTaggedLogger: log.NewTestLogger()}
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	writer := queues.NewDLQWriter(queueWriter, metricsHandler, logger, namespaceRegistry)
	task := &tasks.WorkflowTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: string(tests.NamespaceID),
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
		},
	}
	err := writer.WriteTaskToDLQ(
		context.Background(),
		"source-cluster",
		"target-cluster",
		tasks.GetShardIDForTask(task, 100),
		task,
		true,
	)
	require.NoError(t, err)
	require.Len(t, queueWriter.EnqueueTaskRequests, 1)
	request := queueWriter.EnqueueTaskRequests[0]
	expectedShardID := tasks.GetShardIDForTask(task, 100)
	assert.Equal(t, expectedShardID, request.SourceShardID)
	assert.NotEmpty(t, logger.records)
	assert.Contains(t, logger.records[0].msg, "Task enqueued to DLQ")
	assert.Contains(t, logger.records[0].tags, tag.DLQMessageID(0))
	snapshot := capture.Snapshot()
	recordings := snapshot[metrics.DLQWrites.Name()]
	assert.Len(t, recordings, 1)
	counter, ok := recordings[0].Value.(int64)
	assert.True(t, ok)
	assert.Equal(t, int64(1), counter)
	assert.Len(t, recordings[0].Tags, 2)
	assert.Equal(t, "transfer", recordings[0].Tags[metrics.TaskCategoryTagName])
	namespaceStateTag := metrics.NamespaceStateTag("active")
	assert.Equal(t, "active", recordings[0].Tags[namespaceStateTag.Key])
}

func TestDLQWriter_ConcurrentWrites(t *testing.T) {
	t.Parallel()

	// This test verifies that the DLQ writer serializes concurrent writes to the same queue
	// using a process-level lock, preventing CAS conflicts in the persistence layer.
	queueWriter := &queuestest.FakeQueueWriter{}
	ctrl := gomock.NewController(t)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
	logger := &logRecorder{SnTaggedLogger: log.NewTestLogger()}
	metricsHandler := metricstest.NewCaptureHandler()
	writer := queues.NewDLQWriter(queueWriter, metricsHandler, logger, namespaceRegistry)

	const numConcurrentWrites = 50
	var g errgroup.Group
	var concurrentAccessCount atomic.Int32
	var maxConcurrentAccess atomic.Int32

	// Create tasks that will write to the same DLQ (same category, source, target)
	testTasks := make([]*tasks.WorkflowTask, numConcurrentWrites)
	for i := 0; i < numConcurrentWrites; i++ {
		testTasks[i] = &tasks.WorkflowTask{
			WorkflowKey: definition.WorkflowKey{
				NamespaceID: string(tests.NamespaceID),
				WorkflowID:  tests.WorkflowID,
				RunID:       tests.RunID,
			},
		}
	}

	// Wrap the queue writer to track concurrent access
	queueWriter.EnqueueTaskFunc = func(ctx context.Context, request *persistence.EnqueueTaskRequest) (*persistence.EnqueueTaskResponse, error) {
		// Increment concurrent access counter
		current := concurrentAccessCount.Add(1)

		// Track max concurrent access
		for {
			maxWrites := maxConcurrentAccess.Load()
			if current <= maxWrites || maxConcurrentAccess.CompareAndSwap(maxWrites, current) {
				break
			}
		}

		// Simulate some work that could cause race conditions.
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo

		// Decrement counter
		concurrentAccessCount.Add(-1)

		return &persistence.EnqueueTaskResponse{Metadata: persistence.MessageMetadata{ID: 0}}, nil
	}

	// Launch concurrent writes to the same DLQ
	for i := 0; i < numConcurrentWrites; i++ {
		task := testTasks[i]
		g.Go(func() error {
			err := writer.WriteTaskToDLQ(
				context.Background(),
				"source-cluster",
				"target-cluster",
				1, // same shard ID
				task,
				true,
			)
			require.NoError(t, err)
			return nil
		})
	}

	require.NoError(t, g.Wait())

	// Verify all writes succeeded
	require.Len(t, queueWriter.EnqueueTaskRequests, numConcurrentWrites)

	// The key assertion: with the lock in place, we should never have more than 1 concurrent access
	assert.Equal(t, int32(1), maxConcurrentAccess.Load(),
		"Expected serialized access (max 1 concurrent), but got %d concurrent accesses. "+
			"This indicates the lock is not working properly.", maxConcurrentAccess.Load())
}

func TestDLQWriter_ConcurrentWritesDifferentQueues(t *testing.T) {
	t.Parallel()

	// This test verifies that concurrent writes to DIFFERENT queues can proceed in parallel
	queueWriter := &queuestest.FakeQueueWriter{}
	ctrl := gomock.NewController(t)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
	logger := &logRecorder{SnTaggedLogger: log.NewTestLogger()}
	metricsHandler := metricstest.NewCaptureHandler()
	writer := queues.NewDLQWriter(queueWriter, metricsHandler, logger, namespaceRegistry)

	const numConcurrentWrites = 50
	const numQueues = 5
	var g errgroup.Group
	var concurrentAccessCount atomic.Int32
	var maxConcurrentAccess atomic.Int32

	// Wrap the queue writer to track concurrent access
	queueWriter.EnqueueTaskFunc = func(ctx context.Context, request *persistence.EnqueueTaskRequest) (*persistence.EnqueueTaskResponse, error) {
		current := concurrentAccessCount.Add(1)

		for {
			maxValue := maxConcurrentAccess.Load()
			if current <= maxValue || maxConcurrentAccess.CompareAndSwap(maxValue, current) {
				break
			}
		}

		// Simulate some work that could cause race conditions.
		time.Sleep(10 * time.Millisecond) //nolint:forbidigo
		concurrentAccessCount.Add(-1)

		return &persistence.EnqueueTaskResponse{Metadata: persistence.MessageMetadata{ID: 0}}, nil
	}

	// Launch concurrent writes to DIFFERENT target clusters (different DLQs)
	for i := 0; i < numConcurrentWrites; i++ {
		index := i
		g.Go(func() error {
			task := &tasks.WorkflowTask{
				WorkflowKey: definition.WorkflowKey{
					NamespaceID: string(tests.NamespaceID),
					WorkflowID:  tests.WorkflowID,
					RunID:       tests.RunID,
				},
			}
			// Use different target clusters to create different queue keys
			targetCluster := "target-cluster-" + string(rune('A'+index%numQueues))
			err := writer.WriteTaskToDLQ(
				context.Background(),
				"source-cluster",
				targetCluster,
				1,
				task,
				true,
			)
			require.NoError(t, err)
			return nil
		})
	}

	require.NoError(t, g.Wait())

	// Verify all writes succeeded
	require.Len(t, queueWriter.EnqueueTaskRequests, numConcurrentWrites)

	// Since these are different queues, they should be able to execute concurrently
	// We expect to see more than 1 concurrent access, but less than or equal to numQueues.
	assert.Greater(t, maxConcurrentAccess.Load(), int32(1),
		"Expected concurrent access to different queues (> 1), but got %d.", maxConcurrentAccess.Load())
	assert.LessOrEqual(t, maxConcurrentAccess.Load(), int32(numQueues),
		"Expected less than %d concurrent accesses, but got %d.", numQueues, maxConcurrentAccess.Load())
}
