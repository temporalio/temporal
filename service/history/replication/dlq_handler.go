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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination dlq_handler_mock.go

package replication

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type (
	// DLQHandler is the interface handles replication DLQ messages
	DLQHandler interface {
		GetMessages(
			ctx context.Context,
			sourceCluster string,
			lastMessageID int64,
			pageSize int,
			pageToken []byte,
		) ([]*replicationspb.ReplicationTask, []byte, error)
		PurgeMessages(
			ctx context.Context,
			sourceCluster string,
			lastMessageID int64,
		) error
		MergeMessages(
			ctx context.Context,
			sourceCluster string,
			lastMessageID int64,
			pageSize int,
			pageToken []byte,
		) ([]byte, error)
	}

	dlqHandlerImpl struct {
		taskExecutorsLock    sync.Mutex
		taskExecutors        map[string]TaskExecutor
		shard                shard.Context
		deleteManager        workflow.DeleteManager
		workflowCache        workflow.Cache
		resender             xdc.NDCHistoryResender
		taskExecutorProvider TaskExecutorProvider
		logger               log.Logger
	}
)

func NewLazyDLQHandler(
	shard shard.Context,
	deleteManager workflow.DeleteManager,
	workflowCache workflow.Cache,
	clientBean client.Bean,
	taskExecutorProvider TaskExecutorProvider,
) DLQHandler {
	return newDLQHandler(
		shard,
		deleteManager,
		workflowCache,
		clientBean,
		make(map[string]TaskExecutor),
		taskExecutorProvider,
	)
}

func newDLQHandler(
	shard shard.Context,
	deleteManager workflow.DeleteManager,
	workflowCache workflow.Cache,
	clientBean client.Bean,
	taskExecutors map[string]TaskExecutor,
	taskExecutorProvider TaskExecutorProvider,
) *dlqHandlerImpl {

	if taskExecutors == nil {
		panic("Failed to initialize replication DLQ handler due to nil task executors")
	}

	historyClient := shard.GetHistoryClient()
	historyRetryableClient := history.NewRetryableClient(
		historyClient,
		common.CreateReplicationServiceBusyRetryPolicy(),
		common.IsResourceExhausted,
	)

	return &dlqHandlerImpl{
		shard:         shard,
		deleteManager: deleteManager,
		workflowCache: workflowCache,
		resender: xdc.NewNDCHistoryResender(
			shard.GetNamespaceRegistry(),
			clientBean,
			func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
				_, err := historyRetryableClient.ReplicateEventsV2(ctx, request)
				return err
			},
			shard.GetPayloadSerializer(),
			shard.GetConfig().StandbyTaskReReplicationContextTimeout,
			shard.GetLogger(),
		),
		taskExecutors:        taskExecutors,
		taskExecutorProvider: taskExecutorProvider,
		logger:               shard.GetLogger(),
	}
}

func (r *dlqHandlerImpl) GetMessages(
	ctx context.Context,
	sourceCluster string,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*replicationspb.ReplicationTask, []byte, error) {

	tasks, _, token, err := r.readMessagesWithAckLevel(
		ctx,
		sourceCluster,
		lastMessageID,
		pageSize,
		pageToken,
	)
	return tasks, token, err
}

func (r *dlqHandlerImpl) PurgeMessages(
	ctx context.Context,
	sourceCluster string,
	lastMessageID int64,
) error {

	ackLevel := r.shard.GetReplicatorDLQAckLevel(sourceCluster)
	err := r.shard.GetExecutionManager().RangeDeleteReplicationTaskFromDLQ(
		ctx,
		&persistence.RangeDeleteReplicationTaskFromDLQRequest{
			RangeCompleteHistoryTasksRequest: persistence.RangeCompleteHistoryTasksRequest{
				ShardID:             r.shard.GetShardID(),
				TaskCategory:        tasks.CategoryReplication,
				InclusiveMinTaskKey: tasks.NewImmediateKey(ackLevel + 1),
				ExclusiveMaxTaskKey: tasks.NewImmediateKey(lastMessageID + 1),
			},
			SourceClusterName: sourceCluster,
		},
	)
	if err != nil {
		return err
	}

	if err = r.shard.UpdateReplicatorDLQAckLevel(
		sourceCluster,
		lastMessageID,
	); err != nil {
		r.logger.Error("Failed to purge history replication message", tag.Error(err))
		// The update ack level should not block the call. Ignore the error.
	}
	return nil
}

func (r *dlqHandlerImpl) MergeMessages(
	ctx context.Context,
	sourceCluster string,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]byte, error) {

	replicationTasks, ackLevel, token, err := r.readMessagesWithAckLevel(
		ctx,
		sourceCluster,
		lastMessageID,
		pageSize,
		pageToken,
	)
	if err != nil {
		return nil, err
	}

	taskExecutor, err := r.getOrCreateTaskExecutor(ctx, sourceCluster)
	if err != nil {
		return nil, err
	}

	for _, task := range replicationTasks {
		if _, err := taskExecutor.Execute(
			task,
			true,
		); err != nil {
			return nil, err
		}
	}

	err = r.shard.GetExecutionManager().RangeDeleteReplicationTaskFromDLQ(
		ctx,
		&persistence.RangeDeleteReplicationTaskFromDLQRequest{
			RangeCompleteHistoryTasksRequest: persistence.RangeCompleteHistoryTasksRequest{
				ShardID:             r.shard.GetShardID(),
				TaskCategory:        tasks.CategoryReplication,
				InclusiveMinTaskKey: tasks.NewImmediateKey(ackLevel + 1),
				ExclusiveMaxTaskKey: tasks.NewImmediateKey(lastMessageID + 1),
			},
			SourceClusterName: sourceCluster,
		},
	)
	if err != nil {
		return nil, err
	}

	if err = r.shard.UpdateReplicatorDLQAckLevel(
		sourceCluster,
		lastMessageID,
	); err != nil {
		r.logger.Error("Failed to purge history replication message", tag.Error(err))
		// The update ack level should not block the call. Ignore the error.
	}
	return token, nil
}

func (r *dlqHandlerImpl) readMessagesWithAckLevel(
	ctx context.Context,
	sourceCluster string,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*replicationspb.ReplicationTask, int64, []byte, error) {

	ackLevel := r.shard.GetReplicatorDLQAckLevel(sourceCluster)
	resp, err := r.shard.GetExecutionManager().GetReplicationTasksFromDLQ(ctx, &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:             r.shard.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(ackLevel + 1),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(lastMessageID + 1),
			BatchSize:           pageSize,
			NextPageToken:       pageToken,
		},
		SourceClusterName: sourceCluster,
	})
	if err != nil {
		return nil, ackLevel, nil, err
	}
	pageToken = resp.NextPageToken

	remoteAdminClient, err := r.shard.GetRemoteAdminClient(sourceCluster)
	if err != nil {
		return nil, ackLevel, nil, err
	}
	taskInfo := make([]*replicationspb.ReplicationTaskInfo, 0, len(resp.Tasks))
	for _, task := range resp.Tasks {
		switch task := task.(type) {
		case *tasks.SyncActivityTask:
			taskInfo = append(taskInfo, &replicationspb.ReplicationTaskInfo{
				NamespaceId:      task.NamespaceID,
				WorkflowId:       task.WorkflowID,
				RunId:            task.RunID,
				TaskType:         enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
				TaskId:           task.TaskID,
				Version:          task.GetVersion(),
				FirstEventId:     0,
				NextEventId:      0,
				ScheduledEventId: task.ScheduledEventID,
			})
		case *tasks.HistoryReplicationTask:
			taskInfo = append(taskInfo, &replicationspb.ReplicationTaskInfo{
				NamespaceId:      task.NamespaceID,
				WorkflowId:       task.WorkflowID,
				RunId:            task.RunID,
				TaskType:         enumsspb.TASK_TYPE_REPLICATION_HISTORY,
				TaskId:           task.TaskID,
				Version:          task.Version,
				FirstEventId:     task.FirstEventID,
				NextEventId:      task.NextEventID,
				ScheduledEventId: 0,
			})
		default:
			panic(fmt.Sprintf("Unknown repication task type: %v", task))
		}
	}

	if len(taskInfo) == 0 {
		return nil, ackLevel, pageToken, nil
	}

	dlqResponse, err := remoteAdminClient.GetDLQReplicationMessages(
		ctx,
		&adminservice.GetDLQReplicationMessagesRequest{
			TaskInfos: taskInfo,
		},
	)
	if err != nil {
		return nil, ackLevel, nil, err
	}

	return dlqResponse.ReplicationTasks, ackLevel, pageToken, nil
}

func (r *dlqHandlerImpl) getOrCreateTaskExecutor(ctx context.Context, clusterName string) (TaskExecutor, error) {
	r.taskExecutorsLock.Lock()
	defer r.taskExecutorsLock.Unlock()
	if executor, ok := r.taskExecutors[clusterName]; ok {
		return executor, nil
	}
	engine, err := r.shard.GetEngine(ctx)
	if err != nil {
		return nil, err
	}
	taskExecutor := r.taskExecutorProvider(TaskExecutorParams{
		RemoteCluster:   clusterName,
		Shard:           r.shard,
		HistoryResender: r.resender,
		HistoryEngine:   engine,
		DeleteManager:   r.deleteManager,
		WorkflowCache:   r.workflowCache,
	})
	r.taskExecutors[clusterName] = taskExecutor
	return taskExecutor, nil
}
