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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination replicationDLQHandler_mock.go -self_package go.temporal.io/server/service/history

package history

import (
	"context"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
)

var (
	errInvalidCluster = &serviceerror.InvalidArgument{Message: "Invalid target cluster name."}
)

type (
	// replicationDLQHandler is the interface handles replication DLQ messages
	replicationDLQHandler interface {
		getMessages(
			ctx context.Context,
			sourceCluster string,
			lastMessageID int64,
			pageSize int,
			pageToken []byte,
		) ([]*replicationspb.ReplicationTask, []byte, error)
		purgeMessages(
			sourceCluster string,
			lastMessageID int64,
		) error
		mergeMessages(
			ctx context.Context,
			sourceCluster string,
			lastMessageID int64,
			pageSize int,
			pageToken []byte,
		) ([]byte, error)
	}

	replicationDLQHandlerImpl struct {
		taskExecutors map[string]replicationTaskExecutor
		shard         shard.Context
		logger        log.Logger
	}
)

func newReplicationDLQHandler(
	shard shard.Context,
	taskExecutors map[string]replicationTaskExecutor,
) replicationDLQHandler {

	if taskExecutors == nil {
		panic("Failed to initialize replication DLQ handler due to nil task executors")
	}
	return &replicationDLQHandlerImpl{
		shard:         shard,
		taskExecutors: taskExecutors,
		logger:        shard.GetLogger(),
	}
}
func (r *replicationDLQHandlerImpl) getMessages(
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

func (r *replicationDLQHandlerImpl) readMessagesWithAckLevel(
	ctx context.Context,
	sourceCluster string,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]*replicationspb.ReplicationTask, int64, []byte, error) {

	ackLevel := r.shard.GetReplicatorDLQAckLevel(sourceCluster)
	resp, err := r.shard.GetExecutionManager().GetReplicationTasksFromDLQ(&persistence.GetReplicationTasksFromDLQRequest{
		SourceClusterName: sourceCluster,
		GetReplicationTasksRequest: persistence.GetReplicationTasksRequest{
			ReadLevel:     ackLevel,
			MaxReadLevel:  lastMessageID,
			BatchSize:     pageSize,
			NextPageToken: pageToken,
		},
	})
	if err != nil {
		return nil, ackLevel, nil, err
	}

	remoteAdminClient := r.shard.GetService().GetClientBean().GetRemoteAdminClient(sourceCluster)
	taskInfo := make([]*replicationspb.ReplicationTaskInfo, len(resp.Tasks))
	for _, task := range resp.Tasks {
		taskInfo = append(taskInfo, &replicationspb.ReplicationTaskInfo{
			NamespaceId:  task.GetNamespaceId(),
			WorkflowId:   task.GetWorkflowId(),
			RunId:        task.GetRunId(),
			TaskType:     task.GetTaskType(),
			TaskId:       task.GetTaskId(),
			Version:      task.GetVersion(),
			FirstEventId: task.GetFirstEventId(),
			NextEventId:  task.GetNextEventId(),
			ScheduledId:  task.GetScheduledId(),
		})
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
	return dlqResponse.ReplicationTasks, ackLevel, resp.NextPageToken, nil
}

func (r *replicationDLQHandlerImpl) purgeMessages(
	sourceCluster string,
	lastMessageID int64,
) error {

	ackLevel := r.shard.GetReplicatorDLQAckLevel(sourceCluster)
	err := r.shard.GetExecutionManager().RangeDeleteReplicationTaskFromDLQ(
		&persistence.RangeDeleteReplicationTaskFromDLQRequest{
			SourceClusterName:    sourceCluster,
			ExclusiveBeginTaskID: ackLevel,
			InclusiveEndTaskID:   lastMessageID,
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

func (r *replicationDLQHandlerImpl) mergeMessages(
	ctx context.Context,
	sourceCluster string,
	lastMessageID int64,
	pageSize int,
	pageToken []byte,
) ([]byte, error) {

	if _, ok := r.taskExecutors[sourceCluster]; !ok {
		return nil, errInvalidCluster
	}

	tasks, ackLevel, token, err := r.readMessagesWithAckLevel(
		ctx,
		sourceCluster,
		lastMessageID,
		pageSize,
		pageToken,
	)

	for _, task := range tasks {
		if _, err := r.taskExecutors[sourceCluster].execute(
			task,
			true,
		); err != nil {
			return nil, err
		}
	}

	err = r.shard.GetExecutionManager().RangeDeleteReplicationTaskFromDLQ(
		&persistence.RangeDeleteReplicationTaskFromDLQRequest{
			SourceClusterName:    sourceCluster,
			ExclusiveBeginTaskID: ackLevel,
			InclusiveEndTaskID:   lastMessageID,
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
