package queues_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/queues/queuestest"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	logRecord struct {
		msg  string
		tags []tag.Tag
	}
	logRecorder struct {
		log.SnTaggedLogger
		records []logRecord
	}
)

func (l *logRecorder) Warn(msg string, tags ...tag.Tag) {
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
	assert.Equal(t, metrics.ActiveNamespaceStateTagValue, recordings[0].Tags[namespaceStateTag.Key()])
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
	assert.Equal(t, "active", recordings[0].Tags[namespaceStateTag.Key()])
}
