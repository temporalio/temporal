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

package replication_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/queues/queuestest"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	fakeExecutionManager struct {
		requests []*persistence.PutReplicationTaskToDLQRequest
	}
)

func TestNewExecutionManagerDLQWriter(t *testing.T) {
	t.Parallel()

	executionManager := &fakeExecutionManager{}
	writer := replication.NewExecutionManagerDLQWriter(executionManager)
	replicationTaskInfo := &persistencespb.ReplicationTaskInfo{
		TaskId: 21,
	}
	err := writer.WriteTaskToDLQ(context.Background(), replication.DLQWriteRequest{
		SourceShardID:       13,
		TargetShardID:       26,
		SourceCluster:       "test-source-cluster",
		ReplicationTaskInfo: replicationTaskInfo,
	})
	require.NoError(t, err)
	require.Len(t, executionManager.requests, 1)
	request := executionManager.requests[0]
	assert.Equal(t, 26, int(request.ShardID))
	assert.Equal(t, "test-source-cluster", request.SourceClusterName)
	assert.Equal(t, replicationTaskInfo, request.TaskInfo)
}

func TestNewDLQWriterAdapter(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		taskType  enumsspb.TaskType
		expectErr bool
	}{
		{
			name:      "history replication task",
			taskType:  enumsspb.TASK_TYPE_REPLICATION_HISTORY,
			expectErr: false,
		},
		{
			name:      "unspecified task type",
			taskType:  enumsspb.TASK_TYPE_UNSPECIFIED,
			expectErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			queueWriter := &queuestest.FakeQueueWriter{}
			taskSerializer := serialization.NewTaskSerializer()
			namespaceRegistry := namespace.NewMockRegistry(controller)
			namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(&namespace.Namespace{}, nil).AnyTimes()
			writer := replication.NewDLQWriterAdapter(
				queues.NewDLQWriter(queueWriter, metrics.NoopMetricsHandler, log.NewTestLogger(), namespaceRegistry),
				taskSerializer,
				"test-current-cluster",
			)

			replicationTaskInfo := &persistencespb.ReplicationTaskInfo{
				NamespaceId: string(tests.NamespaceID),
				WorkflowId:  tests.WorkflowID,
				RunId:       tests.RunID,
				TaskType:    tc.taskType,
				TaskId:      21,
			}
			err := writer.WriteTaskToDLQ(context.Background(), replication.DLQWriteRequest{
				SourceShardID:       13,
				SourceCluster:       "test-source-cluster",
				ReplicationTaskInfo: replicationTaskInfo,
			})
			if tc.expectErr {
				require.Error(t, err)
				assert.Contains(t, strings.ToLower(err.Error()), "unknown replication task type")
				assert.Empty(t, queueWriter.EnqueueTaskRequests)
			} else {
				require.NoError(t, err)
				require.Len(t, queueWriter.EnqueueTaskRequests, 1)
				request := queueWriter.EnqueueTaskRequests[0]
				assert.Equal(t, string(tests.NamespaceID), request.Task.GetNamespaceID())
				assert.Equal(t, tests.WorkflowID, request.Task.GetWorkflowID())
				assert.Equal(t, tests.RunID, request.Task.GetRunID())
				assert.Equal(t, 21, int(request.Task.GetTaskID()))
				assert.Equal(t, "test-source-cluster", request.SourceCluster)
				assert.Equal(t, "test-current-cluster", request.TargetCluster)
			}
		})
	}
}

func (f *fakeExecutionManager) PutReplicationTaskToDLQ(
	_ context.Context,
	request *persistence.PutReplicationTaskToDLQRequest,
) error {
	f.requests = append(f.requests, request)
	return nil
}
