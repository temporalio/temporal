package replication

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func newMergeTestHandler(t *testing.T) (*dlqHandlerImpl, *persistence.MockExecutionManager, *adminservicemock.MockAdminServiceClient, *MockTaskExecutor) {
	t.Helper()
	ctrl := gomock.NewController(t)
	shard := historyi.NewMockShardContext(ctrl)
	manager := persistence.NewMockExecutionManager(ctrl)
	admin := adminservicemock.NewMockAdminServiceClient(ctrl)
	executor := NewMockTaskExecutor(ctrl)
	shard.EXPECT().GetReplicatorDLQAckLevel("source").Return(int64(-1)).AnyTimes()
	shard.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	shard.EXPECT().GetExecutionManager().Return(manager).AnyTimes()
	shard.EXPECT().GetRemoteAdminClient("source").Return(admin, nil).AnyTimes()
	return &dlqHandlerImpl{
		shard:         shard,
		taskExecutors: map[string]TaskExecutor{"source": executor},
		logger:        log.NewNoopLogger(),
	}, manager, admin, executor
}

func TestMergeMessagesPreservesUnreplayedEntries(t *testing.T) {
	t.Parallel()
	handler, manager, admin, executor := newMergeTestHandler(t)
	var mu sync.Mutex
	entries := map[int64]bool{10: true, 30: true, 50: true}
	manager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.GetReplicationTasksFromDLQRequest) (*persistence.GetHistoryTasksResponse, error) {
			mu.Lock()
			defer mu.Unlock()
			after := request.InclusiveMinTaskKey.TaskID - 1
			if len(request.NextPageToken) > 0 {
				var err error
				after, err = strconv.ParseInt(string(request.NextPageToken), 10, 64)
				require.NoError(t, err)
			}
			ids := make([]int64, 0, len(entries))
			for id := range entries {
				if id > after && id < request.ExclusiveMaxTaskKey.TaskID {
					ids = append(ids, id)
				}
			}
			slices.Sort(ids)
			response := &persistence.GetHistoryTasksResponse{}
			if len(ids) > request.BatchSize {
				ids = ids[:request.BatchSize]
				response.NextPageToken = []byte(strconv.FormatInt(ids[len(ids)-1], 10))
			}
			for _, id := range ids {
				response.Tasks = append(response.Tasks, &tasks.HistoryReplicationTask{TaskID: id})
			}
			return response, nil
		}).AnyTimes()
	admin.EXPECT().GetDLQReplicationMessages(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *adminservice.GetDLQReplicationMessagesRequest, _ ...any) (*adminservice.GetDLQReplicationMessagesResponse, error) {
			response := &adminservice.GetDLQReplicationMessagesResponse{}
			// Source shards can finish their fetches in a different order.
			for _, info := range slices.Backward(request.TaskInfos) {
				response.ReplicationTasks = append(response.ReplicationTasks, &replicationspb.ReplicationTask{SourceTaskId: info.TaskId})
			}
			return response, nil
		}).AnyTimes()

	appendEntry := make(chan struct{})
	appended := make(chan struct{})
	go func() {
		defer close(appended)
		select {
		case <-appendEntry:
			mu.Lock()
			entries[20] = true
			mu.Unlock()
		case <-t.Context().Done():
		}
	}()
	var once sync.Once
	var replayed []int64
	executor.EXPECT().Execute(gomock.Any(), gomock.Any(), true).DoAndReturn(
		func(ctx context.Context, task *replicationspb.ReplicationTask, _ bool) error {
			once.Do(func() { close(appendEntry) })
			select {
			case <-appended:
			case <-ctx.Done():
				return ctx.Err()
			}
			replayed = append(replayed, task.SourceTaskId)
			return nil
		}).AnyTimes()
	manager.EXPECT().DeleteReplicationTaskFromDLQ(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.DeleteReplicationTaskFromDLQRequest) error {
			require.Equal(t, int32(1), request.ShardID)
			require.Equal(t, "source", request.SourceClusterName)
			require.Equal(t, tasks.CategoryReplication, request.TaskCategory)
			require.Contains(t, replayed, request.TaskKey.TaskID)
			mu.Lock()
			defer mu.Unlock()
			delete(entries, request.TaskKey.TaskID)
			return nil
		}).AnyTimes()

	token, err := handler.MergeMessages(t.Context(), "source", 100, 2, nil)
	require.NoError(t, err)
	require.Equal(t, []byte("30"), token)
	require.Equal(t, map[int64]bool{20: true, 50: true}, entries)
	require.ElementsMatch(t, []int64{10, 30}, replayed)

	token, err = handler.MergeMessages(t.Context(), "source", 100, 2, token)
	require.NoError(t, err)
	require.Empty(t, token)
	require.Equal(t, map[int64]bool{20: true}, entries)

	// A late insertion below the previous page remains visible to a fresh traversal.
	token, err = handler.MergeMessages(t.Context(), "source", 100, 2, nil)
	require.NoError(t, err)
	require.Empty(t, token)
	require.Empty(t, entries)
	require.ElementsMatch(t, []int64{10, 30, 50, 20}, replayed)
}

func TestMergeMessagesDoesNotRetireOnFailure(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"read", "remote", "replay", "missing", "duplicate", "unknown", "nil"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			handler, manager, admin, executor := newMergeTestHandler(t)
			failure := errors.New("injected failure")
			response := &persistence.GetHistoryTasksResponse{Tasks: []tasks.Task{
				&tasks.HistoryReplicationTask{TaskID: 10},
				&tasks.HistoryReplicationTask{TaskID: 30},
			}}
			if scenario == "read" {
				manager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), gomock.Any()).Return(nil, failure)
			} else {
				manager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), gomock.Any()).Return(response, nil)
				remoteTasks := []*replicationspb.ReplicationTask{{SourceTaskId: 10}, {SourceTaskId: 30}}
				switch scenario {
				case "missing":
					remoteTasks = remoteTasks[:1]
				case "duplicate":
					remoteTasks[1].SourceTaskId = 10
				case "unknown":
					remoteTasks[1].SourceTaskId = 100
				case "nil":
					remoteTasks[1] = nil
				default:
				}
				if scenario == "remote" {
					admin.EXPECT().GetDLQReplicationMessages(gomock.Any(), gomock.Any()).Return(nil, failure)
				} else {
					admin.EXPECT().GetDLQReplicationMessages(gomock.Any(), gomock.Any()).Return(&adminservice.GetDLQReplicationMessagesResponse{ReplicationTasks: remoteTasks}, nil)
					if scenario == "replay" {
						gomock.InOrder(
							executor.EXPECT().Execute(gomock.Any(), remoteTasks[0], true).Return(nil),
							executor.EXPECT().Execute(gomock.Any(), remoteTasks[1], true).Return(failure),
						)
					}
				}
			}
			token, err := handler.MergeMessages(t.Context(), "source", 100, 2, nil)
			require.Error(t, err)
			require.Nil(t, token)
		})
	}
}

func TestMergeMessagesEmptyPage(t *testing.T) {
	t.Parallel()
	for _, token := range [][]byte{nil, []byte("next page")} {
		t.Run(string(token), func(t *testing.T) {
			t.Parallel()
			handler, manager, _, _ := newMergeTestHandler(t)
			manager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), gomock.Any()).Return(&persistence.GetHistoryTasksResponse{NextPageToken: token}, nil)
			actual, err := handler.MergeMessages(t.Context(), "source", 100, 2, nil)
			require.NoError(t, err)
			require.Equal(t, token, actual)
		})
	}
}

func TestMergeMessagesDeleteFailure(t *testing.T) {
	t.Parallel()
	handler, manager, admin, executor := newMergeTestHandler(t)
	failure := errors.New("delete failed")
	manager.EXPECT().GetReplicationTasksFromDLQ(gomock.Any(), gomock.Any()).Return(&persistence.GetHistoryTasksResponse{
		Tasks:         []tasks.Task{&tasks.HistoryReplicationTask{TaskID: 10}, &tasks.HistoryReplicationTask{TaskID: 30}},
		NextPageToken: []byte("next page"),
	}, nil)
	remoteTasks := []*replicationspb.ReplicationTask{{SourceTaskId: 10}, {SourceTaskId: 30}}
	admin.EXPECT().GetDLQReplicationMessages(gomock.Any(), gomock.Any()).Return(&adminservice.GetDLQReplicationMessagesResponse{ReplicationTasks: remoteTasks}, nil)
	gomock.InOrder(
		executor.EXPECT().Execute(gomock.Any(), remoteTasks[0], true).Return(nil),
		executor.EXPECT().Execute(gomock.Any(), remoteTasks[1], true).Return(nil),
		manager.EXPECT().DeleteReplicationTaskFromDLQ(gomock.Any(), &persistence.DeleteReplicationTaskFromDLQRequest{
			CompleteHistoryTaskRequest: persistence.CompleteHistoryTaskRequest{ShardID: 1, TaskCategory: tasks.CategoryReplication, TaskKey: tasks.NewImmediateKey(10)},
			SourceClusterName:          "source",
		}).Return(nil),
		manager.EXPECT().DeleteReplicationTaskFromDLQ(gomock.Any(), &persistence.DeleteReplicationTaskFromDLQRequest{
			CompleteHistoryTaskRequest: persistence.CompleteHistoryTaskRequest{ShardID: 1, TaskCategory: tasks.CategoryReplication, TaskKey: tasks.NewImmediateKey(30)},
			SourceClusterName:          "source",
		}).Return(failure),
	)
	token, err := handler.MergeMessages(t.Context(), "source", 100, 2, nil)
	require.ErrorIs(t, err, failure)
	require.Nil(t, token)
}
