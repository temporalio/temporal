package tests

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func BenchmarkCassandraHistoryNodeAppendRead(b *testing.B) {
	testData, tearDown := setUpCassandraTest(b)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	require.NoError(b, err)

	serializer := serialization.NewSerializer()
	manager := p.NewExecutionManager(
		store,
		serializer,
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(4*1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)

	ctx := context.Background()
	history := newBenchmarkHistoryBlob(b, serializer, common.FirstEventID)

	b.Run("append", func(b *testing.B) {
		branchToken := newBenchmarkBranchToken(b, manager.GetHistoryBranchUtil())
		seedBenchmarkHistoryNodes(ctx, b, manager, serializer, branchToken, 1)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			nodeID := int64(i) + common.FirstEventID + 1
			_, err := manager.AppendRawHistoryNodes(ctx, &p.AppendRawHistoryNodesRequest{
				ShardID:           1,
				BranchToken:       branchToken,
				NodeID:            nodeID,
				TransactionID:     nodeID,
				PrevTransactionID: nodeID - 1,
				History:           history,
			})
			require.NoError(b, err)
		}
	})

	b.Run("read", func(b *testing.B) {
		branchToken := newBenchmarkBranchToken(b, manager.GetHistoryBranchUtil())
		seedBenchmarkHistoryNodes(ctx, b, manager, serializer, branchToken, 100)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := manager.ReadHistoryBranch(ctx, &p.ReadHistoryBranchRequest{
				ShardID:     1,
				BranchToken: branchToken,
				MinEventID:  common.FirstEventID,
				MaxEventID:  common.EndEventID,
				PageSize:    100,
			})
			require.NoError(b, err)
		}
	})
}

func BenchmarkCassandraHistoryNodeMultiBranchRead(b *testing.B) {
	testData, tearDown := setUpCassandraTest(b)
	defer tearDown()

	store, err := testData.Factory.NewExecutionStore()
	require.NoError(b, err)

	serializer := serialization.NewSerializer()
	manager := p.NewExecutionManager(
		store,
		serializer,
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(4*1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)

	ctx := context.Background()
	treeID := uuid.NewString()
	branchTokens := make([][]byte, 16)
	for i := range branchTokens {
		branchToken := newBenchmarkBranchTokenForTree(b, manager.GetHistoryBranchUtil(), treeID)
		seedBenchmarkHistoryNodes(ctx, b, manager, serializer, branchToken, 100)
		branchTokens[i] = branchToken
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := manager.ReadHistoryBranch(ctx, &p.ReadHistoryBranchRequest{
			ShardID:     1,
			BranchToken: branchTokens[i%len(branchTokens)],
			MinEventID:  common.FirstEventID,
			MaxEventID:  common.EndEventID,
			PageSize:    100,
		})
		require.NoError(b, err)
	}
}

func BenchmarkCassandraQueueV2EnqueueRead(b *testing.B) {
	testData, tearDown := setUpCassandraTest(b)
	defer tearDown()

	queue, err := testData.Factory.NewQueueV2()
	require.NoError(b, err)

	ctx := context.Background()
	blob := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("benchmark-queue-message"),
	}

	b.Run("enqueue", func(b *testing.B) {
		queueName := benchmarkQueueName(b, "enqueue")
		createBenchmarkQueue(ctx, b, queue, queueName)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := queue.EnqueueMessage(ctx, &p.InternalEnqueueMessageRequest{
				QueueType: p.QueueTypeHistoryNormal,
				QueueName: queueName,
				Blob:      blob,
			})
			require.NoError(b, err)
		}
	})

	b.Run("read", func(b *testing.B) {
		queueName := benchmarkQueueName(b, "read")
		createBenchmarkQueue(ctx, b, queue, queueName)
		seedBenchmarkQueueMessages(ctx, b, queue, queueName, blob, 100)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := queue.ReadMessages(ctx, &p.InternalReadMessagesRequest{
				QueueType: p.QueueTypeHistoryNormal,
				QueueName: queueName,
				PageSize:  100,
			})
			require.NoError(b, err)
		}
	})

	b.Run("range_delete", func(b *testing.B) {
		queueName := benchmarkQueueName(b, "range-delete")
		createBenchmarkQueue(ctx, b, queue, queueName)
		seedBenchmarkQueueMessages(ctx, b, queue, queueName, blob, b.N+1)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := queue.RangeDeleteMessages(ctx, &p.InternalRangeDeleteMessagesRequest{
				QueueType: p.QueueTypeHistoryNormal,
				QueueName: queueName,
				InclusiveMaxMessageMetadata: p.MessageMetadata{
					ID: int64(i),
				},
			})
			require.NoError(b, err)
		}
	})

	b.Run("list", func(b *testing.B) {
		for i := 0; i < 100; i++ {
			createBenchmarkQueue(ctx, b, queue, benchmarkQueueName(b, fmt.Sprintf("list-%03d", i)))
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := queue.ListQueues(ctx, &p.InternalListQueuesRequest{
				QueueType: p.QueueTypeHistoryNormal,
				PageSize:  100,
			})
			require.NoError(b, err)
		}
	})
}

func BenchmarkCassandraQueueEnqueueRead(b *testing.B) {
	testData, tearDown := setUpCassandraTest(b)
	defer tearDown()

	queue, err := testData.Factory.NewQueue(p.NamespaceReplicationQueueType)
	require.NoError(b, err)

	ctx := context.Background()
	blob := &commonpb.DataBlob{
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
		Data:         []byte("benchmark-queue-message"),
	}
	require.NoError(b, queue.Init(ctx, blob))

	b.Run("enqueue", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			err := queue.EnqueueMessage(ctx, blob)
			require.NoError(b, err)
		}
	})

	b.Run("read", func(b *testing.B) {
		seedBenchmarkLegacyQueueMessages(ctx, b, queue, blob, 100)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := queue.ReadMessages(ctx, p.EmptyQueueMessageID, 100)
			require.NoError(b, err)
		}
	})
}

func BenchmarkCassandraMatchingTaskQueue(b *testing.B) {
	testData, tearDown := setUpCassandraTest(b)
	defer tearDown()

	legacyStore, err := testData.Factory.NewTaskStore()
	require.NoError(b, err)
	fairStore, err := testData.Factory.NewFairTaskStore()
	require.NoError(b, err)

	for _, tc := range []struct {
		name string
		fair bool
		mgr  p.TaskManager
	}{
		{
			name: "legacy",
			mgr:  p.NewTaskManager(legacyStore, serialization.NewSerializer()),
		},
		{
			name: "fair",
			fair: true,
			mgr:  p.NewTaskManager(fairStore, serialization.NewSerializer()),
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			benchmarkCassandraMatchingTaskQueue(b, tc.mgr, tc.fair)
		})
	}
}

func BenchmarkCassandraTaskQueueUserDataBuildIDCount(b *testing.B) {
	testData, tearDown := setUpCassandraTest(b)
	defer tearDown()

	store, err := testData.Factory.NewTaskStore()
	require.NoError(b, err)
	manager := p.NewTaskManager(store, serialization.NewSerializer())

	ctx := context.Background()
	namespaceID := uuid.NewString()
	buildID := "benchmark-build-id-" + uuid.NewString()
	seedBenchmarkTaskQueueUserData(ctx, b, manager, namespaceID, buildID, 100)

	b.Run("exact_count", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			count, err := manager.CountTaskQueuesByBuildId(ctx, &p.CountTaskQueuesByBuildIdRequest{
				NamespaceID: namespaceID,
				BuildID:     buildID,
			})
			require.NoError(b, err)
			require.Equal(b, 100, count)
		}
	})

	b.Run("limited_count", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			count, err := manager.CountTaskQueuesByBuildId(ctx, &p.CountTaskQueuesByBuildIdRequest{
				NamespaceID: namespaceID,
				BuildID:     buildID,
				Limit:       20,
			})
			require.NoError(b, err)
			require.Equal(b, 20, count)
		}
	})
}

func benchmarkCassandraMatchingTaskQueue(
	b *testing.B,
	manager p.TaskManager,
	fair bool,
) {
	ctx := context.Background()
	const rangeID = int64(1)
	const batchSize = 16

	b.Run("create", func(b *testing.B) {
		taskQueueInfo := createBenchmarkTaskQueue(ctx, b, manager, "create", rangeID)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			startTaskID := int64(i*batchSize + 1)
			_, err := manager.CreateTasks(ctx, &p.CreateTasksRequest{
				TaskQueueInfo: &p.PersistedTaskQueueInfo{
					Data:    taskQueueInfo,
					RangeID: rangeID,
				},
				Tasks: benchmarkMatchingTasks(taskQueueInfo, startTaskID, batchSize, fair),
			})
			require.NoError(b, err)
		}
	})

	b.Run("read", func(b *testing.B) {
		taskQueueInfo := createBenchmarkTaskQueue(ctx, b, manager, "read", rangeID)
		seedBenchmarkMatchingTasks(ctx, b, manager, taskQueueInfo, rangeID, 1, 100, fair)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := manager.GetTasks(ctx, benchmarkGetTasksRequest(taskQueueInfo, fair, 100))
			require.NoError(b, err)
		}
	})

	b.Run("delete", func(b *testing.B) {
		taskQueueInfo := createBenchmarkTaskQueue(ctx, b, manager, "delete", rangeID)
		seedBenchmarkMatchingTasks(ctx, b, manager, taskQueueInfo, rangeID, 1, b.N+1, fair)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_, err := manager.CompleteTasksLessThan(ctx, benchmarkCompleteTasksRequest(taskQueueInfo, fair, int64(i+2)))
			require.NoError(b, err)
		}
	})
}

func seedBenchmarkHistoryNodes(
	ctx context.Context,
	b *testing.B,
	manager p.ExecutionManager,
	serializer serialization.Serializer,
	branchToken []byte,
	count int,
) {
	b.Helper()

	for i := 0; i < count; i++ {
		nodeID := int64(i) + common.FirstEventID
		history := newBenchmarkHistoryBlob(b, serializer, nodeID)
		_, err := manager.AppendRawHistoryNodes(ctx, &p.AppendRawHistoryNodesRequest{
			ShardID:           1,
			BranchToken:       branchToken,
			NodeID:            nodeID,
			TransactionID:     nodeID,
			PrevTransactionID: nodeID - 1,
			IsNewBranch:       i == 0,
			History:           history,
		})
		require.NoError(b, err)
	}
}

func newBenchmarkHistoryBlob(
	b *testing.B,
	serializer serialization.Serializer,
	eventID int64,
) *commonpb.DataBlob {
	b.Helper()

	history, err := serializer.SerializeEvents([]*historypb.HistoryEvent{
		{EventId: eventID},
	})
	require.NoError(b, err)
	return history
}

func createBenchmarkQueue(
	ctx context.Context,
	b *testing.B,
	queue p.QueueV2,
	queueName string,
) {
	b.Helper()

	_, err := queue.CreateQueue(ctx, &p.InternalCreateQueueRequest{
		QueueType: p.QueueTypeHistoryNormal,
		QueueName: queueName,
	})
	require.NoError(b, err)
}

func seedBenchmarkQueueMessages(
	ctx context.Context,
	b *testing.B,
	queue p.QueueV2,
	queueName string,
	blob *commonpb.DataBlob,
	count int,
) {
	b.Helper()

	for i := 0; i < count; i++ {
		_, err := queue.EnqueueMessage(ctx, &p.InternalEnqueueMessageRequest{
			QueueType: p.QueueTypeHistoryNormal,
			QueueName: queueName,
			Blob:      blob,
		})
		require.NoError(b, err)
	}
}

func seedBenchmarkLegacyQueueMessages(
	ctx context.Context,
	b *testing.B,
	queue p.Queue,
	blob *commonpb.DataBlob,
	count int,
) {
	b.Helper()

	for i := 0; i < count; i++ {
		err := queue.EnqueueMessage(ctx, blob)
		require.NoError(b, err)
	}
}

func benchmarkQueueName(b *testing.B, name string) string {
	b.Helper()
	return fmt.Sprintf("benchmark-%s-%s", name, uuid.NewString())
}

func seedBenchmarkTaskQueueUserData(
	ctx context.Context,
	b *testing.B,
	manager p.TaskManager,
	namespaceID string,
	buildID string,
	count int,
) {
	b.Helper()

	for i := 0; i < count; i++ {
		err := manager.UpdateTaskQueueUserData(ctx, &p.UpdateTaskQueueUserDataRequest{
			NamespaceID: namespaceID,
			Updates: map[string]*p.SingleTaskQueueUserDataUpdate{
				fmt.Sprintf("benchmark-task-queue-%03d", i): {
					UserData: &persistencespb.VersionedTaskQueueUserData{
						Data:    &persistencespb.TaskQueueUserData{},
						Version: 0,
					},
					BuildIdsAdded: []string{buildID},
				},
			},
		})
		require.NoError(b, err)
	}
}

func createBenchmarkTaskQueue(
	ctx context.Context,
	b *testing.B,
	manager p.TaskManager,
	name string,
	rangeID int64,
) *persistencespb.TaskQueueInfo {
	b.Helper()

	now := timestamppb.Now()
	taskQueueInfo := &persistencespb.TaskQueueInfo{
		NamespaceId:    uuid.NewString(),
		Name:           fmt.Sprintf("benchmark-%s-%s", name, uuid.NewString()),
		TaskType:       enumspb.TASK_QUEUE_TYPE_WORKFLOW,
		Kind:           enumspb.TASK_QUEUE_KIND_NORMAL,
		AckLevel:       0,
		LastUpdateTime: now,
	}
	_, err := manager.CreateTaskQueue(ctx, &p.CreateTaskQueueRequest{
		RangeID:       rangeID,
		TaskQueueInfo: taskQueueInfo,
	})
	require.NoError(b, err)
	return taskQueueInfo
}

func seedBenchmarkMatchingTasks(
	ctx context.Context,
	b *testing.B,
	manager p.TaskManager,
	taskQueueInfo *persistencespb.TaskQueueInfo,
	rangeID int64,
	startTaskID int64,
	count int,
	fair bool,
) {
	b.Helper()

	const batchSize = 16
	for remaining, nextTaskID := count, startTaskID; remaining > 0; {
		currentBatchSize := min(batchSize, remaining)
		_, err := manager.CreateTasks(ctx, &p.CreateTasksRequest{
			TaskQueueInfo: &p.PersistedTaskQueueInfo{
				Data:    taskQueueInfo,
				RangeID: rangeID,
			},
			Tasks: benchmarkMatchingTasks(taskQueueInfo, nextTaskID, currentBatchSize, fair),
		})
		require.NoError(b, err)
		nextTaskID += int64(currentBatchSize)
		remaining -= currentBatchSize
	}
}

func benchmarkMatchingTasks(
	taskQueueInfo *persistencespb.TaskQueueInfo,
	startTaskID int64,
	count int,
	fair bool,
) []*persistencespb.AllocatedTaskInfo {
	tasks := make([]*persistencespb.AllocatedTaskInfo, count)
	now := timestamppb.Now()
	for i := range tasks {
		taskID := startTaskID + int64(i)
		tasks[i] = &persistencespb.AllocatedTaskInfo{
			TaskId: taskID,
			Data: &persistencespb.TaskInfo{
				NamespaceId:      taskQueueInfo.GetNamespaceId(),
				WorkflowId:       uuid.NewString(),
				RunId:            uuid.NewString(),
				ScheduledEventId: taskID,
				CreateTime:       now,
				Clock: &clockspb.VectorClock{
					ClusterId: 1,
					ShardId:   1,
					Clock:     taskID,
				},
			},
		}
		if fair {
			tasks[i].TaskPass = taskID
		}
	}
	return tasks
}

func benchmarkGetTasksRequest(
	taskQueueInfo *persistencespb.TaskQueueInfo,
	fair bool,
	pageSize int,
) *p.GetTasksRequest {
	request := &p.GetTasksRequest{
		NamespaceID:        taskQueueInfo.GetNamespaceId(),
		TaskQueue:          taskQueueInfo.GetName(),
		TaskType:           taskQueueInfo.GetTaskType(),
		InclusiveMinTaskID: 1,
		ExclusiveMaxTaskID: math.MaxInt64,
		PageSize:           pageSize,
	}
	if fair {
		request.InclusiveMinPass = 1
		request.UseLimit = true
	}
	return request
}

func benchmarkCompleteTasksRequest(
	taskQueueInfo *persistencespb.TaskQueueInfo,
	fair bool,
	exclusiveMaxTaskID int64,
) *p.CompleteTasksLessThanRequest {
	request := &p.CompleteTasksLessThanRequest{
		NamespaceID:        taskQueueInfo.GetNamespaceId(),
		TaskQueueName:      taskQueueInfo.GetName(),
		TaskType:           taskQueueInfo.GetTaskType(),
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		Limit:              100,
	}
	if fair {
		request.ExclusiveMaxPass = exclusiveMaxTaskID
		request.ExclusiveMaxTaskID = 0
	}
	return request
}

func newBenchmarkBranchToken(b *testing.B, historyBranchUtil p.HistoryBranchUtil) []byte {
	b.Helper()

	return newBenchmarkBranchTokenForTree(b, historyBranchUtil, uuid.NewString())
}

func newBenchmarkBranchTokenForTree(
	b *testing.B,
	historyBranchUtil p.HistoryBranchUtil,
	treeID string,
) []byte {
	b.Helper()

	branchToken, err := historyBranchUtil.NewHistoryBranch(
		uuid.NewString(),
		"benchmark-workflow",
		uuid.NewString(),
		treeID,
		nil,
		nil,
		0,
		0,
		0,
	)
	require.NoError(b, err)
	return branchToken
}
