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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client/history/historytest"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/api/deletedlqtasks/deletedlqtaskstest"
	"go.temporal.io/server/service/history/api/getdlqtasks/getdlqtaskstest"
	"go.temporal.io/server/service/history/api/listqueues/listqueuestest"
	"go.temporal.io/server/service/history/tasks"
)

type (
	faultyQueue struct {
		base                   persistence.QueueV2
		enqueueErr             error
		readMessagesErr        error
		createQueueErr         error
		rangeDeleteMessagesErr error
	}
)

func (q faultyQueue) EnqueueMessage(
	ctx context.Context,
	req *persistence.InternalEnqueueMessageRequest,
) (*persistence.InternalEnqueueMessageResponse, error) {
	if q.enqueueErr != nil {
		return nil, q.enqueueErr
	}
	return q.base.EnqueueMessage(ctx, req)
}

func (q faultyQueue) ReadMessages(
	ctx context.Context,
	req *persistence.InternalReadMessagesRequest,
) (*persistence.InternalReadMessagesResponse, error) {
	if q.readMessagesErr != nil {
		return nil, q.readMessagesErr
	}
	return q.base.ReadMessages(ctx, req)
}

func (q faultyQueue) CreateQueue(
	ctx context.Context,
	req *persistence.InternalCreateQueueRequest,
) (*persistence.InternalCreateQueueResponse, error) {
	if q.createQueueErr != nil {
		return nil, q.createQueueErr
	}
	return q.base.CreateQueue(ctx, req)
}

func (q faultyQueue) RangeDeleteMessages(
	ctx context.Context,
	req *persistence.InternalRangeDeleteMessagesRequest,
) (*persistence.InternalRangeDeleteMessagesResponse, error) {
	if q.rangeDeleteMessagesErr != nil {
		return nil, q.rangeDeleteMessagesErr
	}
	return q.base.RangeDeleteMessages(ctx, req)
}

func (q faultyQueue) ListQueues(
	ctx context.Context,
	req *persistence.InternalListQueuesRequest,
) (*persistence.InternalListQueuesResponse, error) {
	if q.rangeDeleteMessagesErr != nil {
		return nil, q.rangeDeleteMessagesErr
	}
	return q.base.ListQueues(ctx, req)
}

// RunHistoryTaskQueueManagerTestSuite runs all tests for the history task queue manager against a given queue provided by a
// particular database. This test suite should be re-used to test all queue implementations.
func RunHistoryTaskQueueManagerTestSuite(t *testing.T, queue persistence.QueueV2) {
	historyTaskQueueManager := persistence.NewHistoryTaskQueueManager(queue, serialization.NewSerializer())
	t.Run("ListQueues", func(t *testing.T) {
		listqueuestest.TestInvoke(t, historyTaskQueueManager)
	})
	t.Run("TestHistoryTaskQueueManagerEnqueueTasks", func(t *testing.T) {
		t.Parallel()
		testHistoryTaskQueueManagerEnqueueTasks(t, historyTaskQueueManager)
	})
	t.Run("TestHistoryTaskQueueManagerEnqueueTasksErr", func(t *testing.T) {
		t.Parallel()
		testHistoryTaskQueueManagerEnqueueTasksErr(t, queue)
	})
	t.Run("TestHistoryTaskQueueManagerCreateQueueErr", func(t *testing.T) {
		t.Parallel()
		testHistoryTaskQueueManagerCreateQueueErr(t, queue)
	})
	t.Run("TestHistoryTQMErrDeserializeTask", func(t *testing.T) {
		t.Parallel()
		testHistoryTaskQueueManagerErrDeserializeHistoryTask(t, queue, historyTaskQueueManager)
	})
	t.Run("DeleteTasks", func(t *testing.T) {
		t.Parallel()
		testHistoryTaskQueueManagerDeleteTasks(t, historyTaskQueueManager)
	})
	t.Run("DeleteTasksErr", func(t *testing.T) {
		t.Parallel()
		testHistoryTaskQueueManagerDeleteTasksErr(t, queue)
	})
	t.Run("GetDLQTasks", func(t *testing.T) {
		t.Parallel()
		getdlqtaskstest.TestInvoke(t, historyTaskQueueManager)
	})
	t.Run("DeleteDLQTasks", func(t *testing.T) {
		t.Parallel()
		deletedlqtaskstest.TestInvoke(t, historyTaskQueueManager)
	})
	t.Run("ClientTest", func(t *testing.T) {
		t.Parallel()
		historytest.TestClient(t, historyTaskQueueManager)
	})
}

func testHistoryTaskQueueManagerCreateQueueErr(t *testing.T, queue persistence.QueueV2) {
	retErr := errors.New("test")
	manager := persistence.NewHistoryTaskQueueManager(faultyQueue{
		base:           queue,
		createQueueErr: retErr,
	}, serialization.NewSerializer())
	_, err := manager.CreateQueue(context.Background(), &persistence.CreateQueueRequest{
		QueueKey: persistencetest.GetQueueKey(t),
	})
	assert.ErrorIs(t, err, retErr)
}

func testHistoryTaskQueueManagerEnqueueTasks(t *testing.T, manager persistence.HistoryTaskQueueManager) {
	numHistoryShards := 5
	ctx := context.Background()

	namespaceID := "test-namespace"
	workflowID := "test-workflow-id"
	workflowKey := definition.NewWorkflowKey(namespaceID, workflowID, "test-run-id")
	shardID := 2
	assert.Equal(t, int32(shardID), common.WorkflowIDToHistoryShard(namespaceID, workflowID, int32(numHistoryShards)))

	queueKey := persistencetest.GetQueueKey(t)
	_, err := manager.CreateQueue(ctx, &persistence.CreateQueueRequest{
		QueueKey: queueKey,
	})
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		task := &tasks.WorkflowTask{
			WorkflowKey: workflowKey,
			TaskID:      int64(i + 1),
		}
		res, err := enqueueTask(ctx, manager, queueKey, task)
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

func testHistoryTaskQueueManagerEnqueueTasksErr(t *testing.T, queue persistence.QueueV2) {
	ctx := context.Background()

	retErr := errors.New("test")
	manager := persistence.NewHistoryTaskQueueManager(faultyQueue{
		base:       queue,
		enqueueErr: retErr,
	}, serialization.NewSerializer())
	queueKey := persistencetest.GetQueueKey(t)
	_, err := manager.CreateQueue(ctx, &persistence.CreateQueueRequest{
		QueueKey: queueKey,
	})
	require.NoError(t, err)
	_, err = enqueueTask(ctx, manager, queueKey, &tasks.WorkflowTask{
		TaskID: 1,
	})
	assert.ErrorIs(t, err, retErr)
}

func testHistoryTaskQueueManagerErrDeserializeHistoryTask(
	t *testing.T,
	queue persistence.QueueV2,
	manager persistence.HistoryTaskQueueManager,
) {
	ctx := context.Background()

	t.Run("nil blob", func(t *testing.T) {
		t.Parallel()

		err := enqueueAndDeserializeBlob(ctx, t, queue, manager, nil)
		assert.ErrorContains(t, err, persistence.ErrHistoryTaskBlobIsNil.Error())
	})
	t.Run("empty blob", func(t *testing.T) {
		t.Parallel()

		err := enqueueAndDeserializeBlob(ctx, t, queue, manager, &commonpb.DataBlob{})
		assert.ErrorContains(t, err, persistence.ErrMsgDeserializeHistoryTask)
	})
}

func testHistoryTaskQueueManagerDeleteTasks(t *testing.T, manager *persistence.HistoryTaskQueueManagerImpl) {
	ctx := context.Background()

	queueKey := persistencetest.GetQueueKey(t)
	_, err := manager.CreateQueue(ctx, &persistence.CreateQueueRequest{
		QueueKey: queueKey,
	})
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		_, err := enqueueTask(ctx, manager, queueKey, &tasks.WorkflowTask{
			TaskID: int64(i + 1),
		})
		require.NoError(t, err)
	}
	_, err = manager.DeleteTasks(ctx, &persistence.DeleteTasksRequest{
		QueueKey: queueKey,
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{
			ID: persistence.FirstQueueMessageID,
		},
	})
	require.NoError(t, err)
	res, err := manager.ReadTasks(ctx, &persistence.ReadTasksRequest{
		QueueKey: queueKey,
		PageSize: 10,
	})
	require.NoError(t, err)
	require.Len(t, res.Tasks, 1)
	assert.Equal(t, int64(2), res.Tasks[0].Task.GetTaskID())
}

func enqueueAndDeserializeBlob(
	ctx context.Context,
	t *testing.T,
	queue persistence.QueueV2,
	manager persistence.HistoryTaskQueueManager,
	blob *commonpb.DataBlob,
) error {
	t.Helper()

	queueType := persistence.QueueTypeHistoryNormal
	queueKey := persistencetest.GetQueueKey(t)
	queueName := queueKey.GetQueueName()

	_, err := queue.CreateQueue(ctx, &persistence.InternalCreateQueueRequest{
		QueueType: queueType,
		QueueName: queueKey.GetQueueName(),
	})
	require.NoError(t, err)
	historyTask := persistencespb.HistoryTask{
		ShardId: 1,
		Blob:    blob,
	}
	historyTaskBytes, _ := historyTask.Marshal()
	_, err = queue.EnqueueMessage(ctx, &persistence.InternalEnqueueMessageRequest{
		QueueType: queueType,
		QueueName: queueName,
		Blob: &commonpb.DataBlob{
			EncodingType: enums.ENCODING_TYPE_PROTO3,
			Data:         historyTaskBytes,
		},
	})
	require.NoError(t, err)

	_, err = manager.ReadTasks(ctx, &persistence.ReadTasksRequest{
		QueueKey: queueKey,
		PageSize: 1,
	})
	return err
}

func testHistoryTaskQueueManagerDeleteTasksErr(t *testing.T, queue persistence.QueueV2) {
	ctx := context.Background()

	retErr := errors.New("test")
	manager := persistence.NewHistoryTaskQueueManager(faultyQueue{
		base:                   queue,
		rangeDeleteMessagesErr: retErr,
	}, serialization.NewSerializer())
	queueKey := persistencetest.GetQueueKey(t)
	_, err := manager.CreateQueue(ctx, &persistence.CreateQueueRequest{
		QueueKey: queueKey,
	})
	require.NoError(t, err)
	_, err = enqueueTask(ctx, manager, queueKey, &tasks.WorkflowTask{
		TaskID: 1,
	})
	require.NoError(t, err)
	_, err = manager.DeleteTasks(ctx, &persistence.DeleteTasksRequest{
		QueueKey: queueKey,
		InclusiveMaxMessageMetadata: persistence.MessageMetadata{
			ID: persistence.FirstQueueMessageID,
		},
	})
	assert.ErrorIs(t, err, retErr)
}

func enqueueTask(
	ctx context.Context,
	manager persistence.HistoryTaskQueueManager,
	queueKey persistence.QueueKey,
	task *tasks.WorkflowTask,
) (*persistence.EnqueueTaskResponse, error) {
	return manager.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
		QueueType:     queueKey.QueueType,
		SourceCluster: queueKey.SourceCluster,
		TargetCluster: queueKey.TargetCluster,
		Task:          task,
		SourceShardID: 1,
	})
}
