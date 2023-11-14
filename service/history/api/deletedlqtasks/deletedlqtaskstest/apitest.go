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

package deletedlqtaskstest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc/codes"

	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/persistencetest"
	"go.temporal.io/server/service/history/api/deletedlqtasks"
	"go.temporal.io/server/service/history/tasks"
)

func TestInvoke(t *testing.T, manager persistence.HistoryTaskQueueManager) {
	ctx := context.Background()
	t.Run("HappyPath", func(t *testing.T) {
		t.Parallel()

		queueKey := persistencetest.GetQueueKey(t, persistencetest.WithQueueType(persistence.QueueTypeHistoryDLQ))
		_, err := manager.CreateQueue(ctx, &persistence.CreateQueueRequest{
			QueueKey: queueKey,
		})
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			_, err := manager.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
				QueueType:     queueKey.QueueType,
				SourceCluster: queueKey.SourceCluster,
				TargetCluster: queueKey.TargetCluster,
				Task:          &tasks.WorkflowTask{},
				SourceShardID: 1,
			})
			require.NoError(t, err)
		}
		_, err = deletedlqtasks.Invoke(ctx, manager, &historyservice.DeleteDLQTasksRequest{
			DlqKey: &commonspb.HistoryDLQKey{
				TaskCategory:  int32(queueKey.Category.ID()),
				SourceCluster: queueKey.SourceCluster,
				TargetCluster: queueKey.TargetCluster,
			},
			InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
				MessageId: persistence.FirstQueueMessageID + 1,
			},
		}, tasks.NewDefaultTaskCategoryRegistry())
		require.NoError(t, err)
		resp, err := manager.ReadRawTasks(ctx, &persistence.ReadRawTasksRequest{
			QueueKey: queueKey,
			PageSize: 10,
		})
		require.NoError(t, err)
		require.Len(t, resp.Tasks, 1)
		assert.Equal(t, int64(persistence.FirstQueueMessageID+2), resp.Tasks[0].MessageMetadata.ID)
	})
	t.Run("QueueDoesNotExist", func(t *testing.T) {
		t.Parallel()

		queueKey := persistencetest.GetQueueKey(t, persistencetest.WithQueueType(persistence.QueueTypeHistoryDLQ))
		_, err := deletedlqtasks.Invoke(ctx, manager, &historyservice.DeleteDLQTasksRequest{
			DlqKey: &commonspb.HistoryDLQKey{
				TaskCategory:  int32(queueKey.Category.ID()),
				SourceCluster: queueKey.SourceCluster,
				TargetCluster: queueKey.TargetCluster,
			},
			InclusiveMaxTaskMetadata: &commonspb.HistoryDLQTaskMetadata{
				MessageId: persistence.FirstQueueMessageID,
			},
		}, tasks.NewDefaultTaskCategoryRegistry())
		require.Error(t, err)
		assert.Equal(t, codes.NotFound, serviceerror.ToStatus(err).Code(), err.Error())
	})
}
