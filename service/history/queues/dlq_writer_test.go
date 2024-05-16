// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package queues_test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/cluster"
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

func TestDLQWriter_ErrGetClusterMetadata(t *testing.T) {
	t.Parallel()

	queueWriter := &queuestest.FakeQueueWriter{}
	ctrl := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{})
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
	writer := queues.NewDLQWriter(queueWriter, clusterMetadata, metrics.NoopMetricsHandler, log.NewTestLogger(), namespaceRegistry)
	err := writer.WriteTaskToDLQ(
		context.Background(),
		"source-cluster",
		"target-cluster",
		&tasks.WorkflowTask{},
	)
	assert.ErrorIs(t, err, queues.ErrGetClusterMetadata)
	assert.Empty(t, queueWriter.EnqueueTaskRequests)
}

func TestDLQWriter_ErrGetNamespaceName(t *testing.T) {
	t.Parallel()

	queueWriter := &queuestest.FakeQueueWriter{}
	ctrl := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		"source-cluster": {
			ShardCount: 100,
		},
	})
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	errorMsg := "GetNamespaceByID failed"
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(nil, errors.New(errorMsg)).AnyTimes()
	logger := &logRecorder{SnTaggedLogger: log.NewTestLogger()}
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	writer := queues.NewDLQWriter(queueWriter, clusterMetadata, metricsHandler, logger, namespaceRegistry)
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
		task,
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
	assert.Len(t, recordings[0].Tags, 1)
	assert.Equal(t, "transfer", recordings[0].Tags[metrics.TaskCategoryTagName])
}

func TestDLQWriter_Ok(t *testing.T) {
	t.Parallel()

	queueWriter := &queuestest.FakeQueueWriter{}
	ctrl := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		"source-cluster": {
			ShardCount: 100,
		},
	})
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
	logger := &logRecorder{SnTaggedLogger: log.NewTestLogger()}
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	writer := queues.NewDLQWriter(queueWriter, clusterMetadata, metricsHandler, logger, namespaceRegistry)
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
		task,
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
	assert.Len(t, recordings[0].Tags, 1)
	assert.Equal(t, "transfer", recordings[0].Tags[metrics.TaskCategoryTagName])
}
