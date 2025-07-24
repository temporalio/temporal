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
	"go.temporal.io/server/common/metrics/metricstest"
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
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			writer := replication.NewDLQWriterAdapter(
				queues.NewDLQWriter(queueWriter, metricsHandler, log.NewTestLogger(), namespaceRegistry),
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
				snapshot := capture.Snapshot()
				recordings := snapshot[metrics.DLQWrites.Name()]
				assert.Len(t, recordings, 1)
				assert.Len(t, recordings[0].Tags, 2)
				assert.Equal(t, "replication", recordings[0].Tags[metrics.TaskCategoryTagName])
				namespaceStateTag := metrics.NamespaceStateTag(metrics.PassiveNamespaceStateTagValue)
				assert.Equal(t, metrics.PassiveNamespaceStateTagValue, recordings[0].Tags[namespaceStateTag.Key()])
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
