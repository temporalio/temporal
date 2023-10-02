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

package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client/history/historytest"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api/getdlqtasks/getdlqtaskstest"
	"go.temporal.io/server/service/history/queues/queuestest"
	"go.temporal.io/server/service/history/tasks"
)

// RunHistoryTaskQueueManagerTestSuite runs all tests for the history task queue manager against a given queue provided by a
// particular database. This test suite should be re-used to test all queue implementations.
func RunHistoryTaskQueueManagerTestSuite(t *testing.T, queue persistence.QueueV2) {
	historyTaskQueueManager := persistence.NewHistoryTaskQueueManager(queue, 1)
	t.Run("TestHistoryTaskQueueManagerHappyPath", func(t *testing.T) {
		t.Parallel()
		testHistoryTaskQueueManagerHappyPath(t, historyTaskQueueManager)
	})
	t.Run("TestHistoryTaskQueueManagerErrDeserializeTask", func(t *testing.T) {
		t.Parallel()
		testHistoryTaskQueueManagerErrDeserializeHistoryTask(t, queue, historyTaskQueueManager)
	})
	t.Run("GetDLQTasks", func(t *testing.T) {
		t.Parallel()
		getdlqtaskstest.TestGetDLQTasks(t, historyTaskQueueManager)
	})
	t.Run("ClientTest", func(t *testing.T) {
		t.Parallel()
		historytest.TestClientGetDLQTasks(t, historyTaskQueueManager)
	})
	t.Run("ExecutableTest", func(t *testing.T) {
		t.Parallel()
		queuestest.TestExecutable(t, historyTaskQueueManager)
	})
}

func testHistoryTaskQueueManagerHappyPath(t *testing.T, manager persistence.HistoryTaskQueueManager) {
	numHistoryShards := 5
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	t.Cleanup(cancel)

	namespaceID := "test-namespace"
	workflowID := "test-workflow-id"
	workflowKey := definition.NewWorkflowKey(namespaceID, workflowID, "test-run-id")
	shardID := 2
	assert.Equal(t, int32(shardID), common.WorkflowIDToHistoryShard(namespaceID, workflowID, int32(numHistoryShards)))

	category := tasks.CategoryTransfer
	queueKey := persistence.QueueKey{
		QueueType:     persistence.QueueTypeHistoryNormal,
		Category:      category,
		SourceCluster: "test-source-cluster-" + t.Name(),
	}

	for i := 0; i < 2; i++ {
		task := &tasks.WorkflowTask{
			WorkflowKey: workflowKey,
			TaskID:      int64(i + 1),
		}
		res, err := manager.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
			QueueType:     queueKey.QueueType,
			SourceCluster: queueKey.SourceCluster,
			Task:          task,
		})
		require.NoError(t, err)
		assert.Equal(t, int64(persistence.FirstQueueMessageID+i), res.Metadata.ID)
	}

	var nextPageToken []byte
	for i := 0; i < 3; i++ {
		readRes, err := manager.ReadTasks(ctx, &persistence.ReadTasksRequest{
			QueueKey:      queueKey,
			PageSize:      1,
			NextPageToken: nextPageToken,
		})
		require.NoError(t, err)

		if i < 2 {
			require.Len(t, readRes.Tasks, 1)
			assert.Equal(t, shardID, tasks.GetShardIDForTask(readRes.Tasks[0].Task, numHistoryShards))
			assert.Equal(t, int64(i+1), readRes.Tasks[0].Task.GetTaskID())
			nextPageToken = readRes.NextPageToken
		} else {
			assert.Empty(t, readRes.Tasks)
			assert.Empty(t, readRes.NextPageToken)
		}
	}
}

func testHistoryTaskQueueManagerErrDeserializeHistoryTask(
	t *testing.T,
	queue persistence.QueueV2,
	manager persistence.HistoryTaskQueueManager,
) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	t.Cleanup(cancel)

	t.Run("nil blob", func(t *testing.T) {
		t.Parallel()

		err := enqueueAndDeserializeBlob(ctx, t, queue, nil)
		assert.ErrorContains(t, err, persistence.ErrHistoryTaskBlobIsNil.Error())
	})
	t.Run("empty blob", func(t *testing.T) {
		t.Parallel()

		err := enqueueAndDeserializeBlob(ctx, t, queue, &commonpb.DataBlob{})
		assert.ErrorContains(t, err, persistence.ErrMsgDeserializeHistoryTask)
	})
}

func enqueueAndDeserializeBlob(ctx context.Context, t *testing.T, queue persistence.QueueV2, blob *commonpb.DataBlob) error {
	queueType := persistence.QueueTypeHistoryNormal
	queueKey := persistence.QueueKey{
		QueueType:     queueType,
		Category:      tasks.CategoryTransfer,
		SourceCluster: "test-source-cluster-" + t.Name(),
	}
	queueName := queueKey.GetQueueName()
	historyTask := persistencespb.HistoryTask{
		ShardId: 1,
		Blob:    blob,
	}
	historyTaskBytes, _ := historyTask.Marshal()
	_, err := queue.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
		QueueType: queueType,
		QueueName: queueName,
		Blob: commonpb.DataBlob{
			EncodingType: enums.ENCODING_TYPE_PROTO3,
			Data:         historyTaskBytes,
		},
	})
	require.NoError(t, err)

	manager := persistence.NewHistoryTaskQueueManager(queue, 1)
	_, err = manager.ReadTasks(ctx, &persistence.ReadTasksRequest{
		QueueKey: queueKey,
		PageSize: 1,
	})
	return err
}
