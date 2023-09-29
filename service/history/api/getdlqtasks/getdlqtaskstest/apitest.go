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

package getdlqtaskstest

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/api/getdlqtasks"
	"go.temporal.io/server/service/history/tasks"
)

// TestGetDLQTasks is a library test function intended to be invoked from a persistence test suite. It works by
// enqueueing a task into the DLQ and then calling [getdlqtasks.Invoke] to verify that the right task is returned.
func TestGetDLQTasks(t *testing.T, manager persistence.HistoryTaskQueueManager) {
	ctx := context.Background()
	inTask := &tasks.WorkflowTask{
		TaskID: 42,
	}
	sourceCluster := "test-source-cluster-" + t.Name()
	targetCluster := "test-target-cluster-" + t.Name()
	_, err := manager.EnqueueTask(ctx, &persistence.EnqueueTaskRequest{
		QueueType:     persistence.QueueTypeHistoryDLQ,
		SourceCluster: sourceCluster,
		TargetCluster: targetCluster,
		Task:          inTask,
	})
	require.NoError(t, err)
	res, err := getdlqtasks.Invoke(
		context.Background(),
		manager,
		&historyservice.GetDLQTasksRequest{
			DlqKey: &historyservice.HistoryDLQKey{
				Category:      enumsspb.TASK_CATEGORY_TRANSFER,
				SourceCluster: sourceCluster,
				TargetCluster: targetCluster,
			},
			PageSize: 1,
		},
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(res.DlqTasks))
	assert.Equal(t, int64(persistence.FirstQueueMessageID), res.DlqTasks[0].Metadata.MessageId)
	assert.Equal(t, 1, int(res.DlqTasks[0].Task.ShardId))
	serializer := serialization.NewTaskSerializer()
	outTask, err := serializer.DeserializeTask(tasks.CategoryTransfer, *res.DlqTasks[0].Task.Task)
	require.NoError(t, err)
	assert.Equal(t, inTask, outTask)
}
