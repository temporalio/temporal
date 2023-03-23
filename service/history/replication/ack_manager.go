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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination ack_manager_mock.go

package replication

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	AckManager interface {
		NotifyNewTasks(tasks []tasks.Task)
		GetMaxTaskInfo() (int64, time.Time)
		GetTasks(ctx context.Context, pollingCluster string, queryMessageID int64) (*replicationspb.ReplicationMessages, error)
		GetTask(ctx context.Context, taskInfo *replicationspb.ReplicationTaskInfo) (*replicationspb.ReplicationTask, error)
		Close()
	}

	ackMgrImpl struct {
		currentClusterName string
		shard              shard.Context
		config             *configs.Config
		workflowCache      wcache.Cache
		executionMgr       persistence.ExecutionManager
		metricsHandler     metrics.Handler
		logger             log.Logger
		retryPolicy        backoff.RetryPolicy
		namespaceRegistry  namespace.Registry
		pageSize           dynamicconfig.IntPropertyFn
		maxSkipTaskCount   dynamicconfig.IntPropertyFn

		sync.Mutex
		// largest replication task ID generated
		maxTaskID                  *int64
		maxTaskVisibilityTimestamp *time.Time
		sanityCheckTime            time.Time
		registeredQueueReaders     map[int32]struct{}
	}
)

var (
	errUnknownReplicationTask = serviceerror.NewInternal("unknown replication task")
)

var (
	closeAckMangerTimeout = 10 * time.Second
)

func NewAckManager(
	shard shard.Context,
	workflowCache wcache.Cache,
	executionMgr persistence.ExecutionManager,
	logger log.Logger,
) AckManager {

	currentClusterName := shard.GetClusterMetadata().GetCurrentClusterName()
	config := shard.GetConfig()

	retryPolicy := backoff.NewExponentialRetryPolicy(200 * time.Millisecond).
		WithMaximumAttempts(5).
		WithBackoffCoefficient(1)

	return &ackMgrImpl{
		currentClusterName: currentClusterName,
		shard:              shard,
		config:             shard.GetConfig(),
		workflowCache:      workflowCache,
		executionMgr:       executionMgr,
		metricsHandler:     shard.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.ReplicatorQueueProcessorScope)),
		logger:             log.With(logger, tag.ComponentReplicatorQueue),
		retryPolicy:        retryPolicy,
		namespaceRegistry:  shard.GetNamespaceRegistry(),
		pageSize:           config.ReplicatorProcessorFetchTasksBatchSize,
		maxSkipTaskCount:   config.ReplicatorProcessorMaxSkipTaskCount,

		maxTaskID:              nil,
		sanityCheckTime:        time.Time{},
		registeredQueueReaders: make(map[int32]struct{}),
	}
}

func (p *ackMgrImpl) NotifyNewTasks(
	tasks []tasks.Task,
) {

	if len(tasks) == 0 {
		return
	}
	maxTaskID := tasks[0].GetTaskID()
	maxVisibilityTimestamp := tasks[0].GetVisibilityTime()
	for _, task := range tasks {
		if maxTaskID < task.GetTaskID() {
			maxTaskID = task.GetTaskID()
		}
		if maxVisibilityTimestamp.Before(task.GetVisibilityTime()) {
			maxVisibilityTimestamp = task.GetVisibilityTime()
		}
	}

	p.Lock()
	defer p.Unlock()
	if p.maxTaskID == nil || *p.maxTaskID < maxTaskID {
		p.maxTaskID = &maxTaskID
	}
	if p.maxTaskVisibilityTimestamp == nil || p.maxTaskVisibilityTimestamp.Before(maxVisibilityTimestamp) {
		p.maxTaskVisibilityTimestamp = timestamp.TimePtr(maxVisibilityTimestamp)
	}
}

func (p *ackMgrImpl) GetMaxTaskInfo() (int64, time.Time) {
	p.Lock()
	defer p.Unlock()

	maxTaskID := p.maxTaskID
	if maxTaskID == nil {
		// maxTaskID is nil before any replication task is written which happens right after shard reload. In that case,
		// use ImmediateTaskMaxReadLevel which is the max task id of any immediate task queues.
		// ImmediateTaskMaxReadLevel will be the lower bound of new range_id if shard reload. Remote cluster will quickly (in
		// a few seconds) ack to the latest ImmediateTaskMaxReadLevel if there is no replication tasks at all.
		taskID := p.shard.GetImmediateQueueExclusiveHighReadWatermark().Prev().TaskID
		maxTaskID = &taskID
	}
	maxVisibilityTimestamp := p.maxTaskVisibilityTimestamp
	if maxVisibilityTimestamp == nil {
		maxVisibilityTimestamp = timestamp.TimePtr(p.shard.GetTimeSource().Now())
	}

	return *maxTaskID, timestamp.TimeValue(maxVisibilityTimestamp)
}

func (p *ackMgrImpl) GetTask(
	ctx context.Context,
	taskInfo *replicationspb.ReplicationTaskInfo,
) (*replicationspb.ReplicationTask, error) {

	switch taskInfo.TaskType {
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
		return p.toReplicationTask(ctx, &tasks.SyncActivityTask{
			WorkflowKey: definition.NewWorkflowKey(
				taskInfo.GetNamespaceId(),
				taskInfo.GetWorkflowId(),
				taskInfo.GetRunId(),
			),
			VisibilityTimestamp: time.Unix(0, 0), // TODO add the missing attribute to proto definition
			TaskID:              taskInfo.TaskId,
			Version:             taskInfo.Version,
			ScheduledEventID:    taskInfo.ScheduledEventId,
		})
	case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
		return p.toReplicationTask(ctx, &tasks.HistoryReplicationTask{
			WorkflowKey: definition.NewWorkflowKey(
				taskInfo.GetNamespaceId(),
				taskInfo.GetWorkflowId(),
				taskInfo.GetRunId(),
			),
			VisibilityTimestamp: time.Unix(0, 0), // TODO add the missing attribute to proto definition
			TaskID:              taskInfo.TaskId,
			Version:             taskInfo.Version,
			FirstEventID:        taskInfo.FirstEventId,
			NextEventID:         taskInfo.NextEventId,
		})
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE:
		return p.toReplicationTask(ctx, &tasks.SyncWorkflowStateTask{
			WorkflowKey: definition.NewWorkflowKey(
				taskInfo.GetNamespaceId(),
				taskInfo.GetWorkflowId(),
				taskInfo.GetRunId(),
			),
			VisibilityTimestamp: time.Unix(0, 0),
			TaskID:              taskInfo.TaskId,
			Version:             taskInfo.Version,
		})
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown replication task type: %v", taskInfo.TaskType))
	}
}

func (p *ackMgrImpl) GetTasks(
	ctx context.Context,
	pollingCluster string,
	queryMessageID int64,
) (*replicationspb.ReplicationMessages, error) {

	minTaskID, maxTaskID := p.taskIDsRange(queryMessageID)
	replicationTasks, lastTaskID, err := p.getTasks(
		ctx,
		pollingCluster,
		minTaskID,
		maxTaskID,
	)
	if err != nil {
		return nil, err
	}

	// Note this is a very rough indicator of how much the remote DC is behind on this shard.
	p.metricsHandler.Histogram(metrics.ReplicationTasksLag.GetMetricName(), metrics.ReplicationTasksLag.GetMetricUnit()).Record(
		maxTaskID-lastTaskID,
		metrics.TargetClusterTag(pollingCluster))

	p.metricsHandler.Histogram(metrics.ReplicationTasksFetched.GetMetricName(), metrics.ReplicationTasksFetched.GetMetricUnit()).
		Record(int64(len(replicationTasks)))

	p.metricsHandler.Histogram(metrics.ReplicationTasksReturned.GetMetricName(), metrics.ReplicationTasksReturned.GetMetricUnit()).
		Record(int64(len(replicationTasks)))

	replicationEventTime := timestamp.TimePtr(p.shard.GetTimeSource().Now())
	if len(replicationTasks) > 0 {
		replicationEventTime = replicationTasks[len(replicationTasks)-1].GetVisibilityTime()
	}
	return &replicationspb.ReplicationMessages{
		ReplicationTasks:       replicationTasks,
		HasMore:                lastTaskID < maxTaskID,
		LastRetrievedMessageId: lastTaskID,
		SyncShardStatus: &replicationspb.SyncShardStatus{
			StatusTime: replicationEventTime,
		},
	}, nil
}

func (p *ackMgrImpl) Close() {
	p.Lock()
	defer p.Unlock()

	if p.registeredQueueReaders == nil {
		// Close called multiple times
		// should not happen
		return
	}

	closeCtx, cancel := context.WithTimeout(context.Background(), closeAckMangerTimeout)
	closeCtx = headers.SetCallerInfo(closeCtx, headers.SystemPreemptableCallerInfo)
	defer cancel()

	for readerID := range p.registeredQueueReaders {
		p.executionMgr.UnregisterHistoryTaskReader(closeCtx, &persistence.UnregisterHistoryTaskReaderRequest{
			ShardID:      p.shard.GetShardID(),
			ShardOwner:   p.shard.GetOwner(),
			TaskCategory: tasks.CategoryReplication,
			ReaderID:     readerID,
		})
	}

	p.registeredQueueReaders = nil
}

func (p *ackMgrImpl) getTasks(
	ctx context.Context,
	pollingCluster string,
	minTaskID int64,
	maxTaskID int64,
) ([]*replicationspb.ReplicationTask, int64, error) {
	if minTaskID > maxTaskID {
		return nil, 0, serviceerror.NewUnavailable("min task ID > max task ID, probably due to shard re-balancing")
	} else if minTaskID == maxTaskID {
		return nil, maxTaskID, nil
	}

	readerID, err := p.clusterToReaderID(ctx, pollingCluster)
	if err != nil {
		return nil, 0, err
	}

	replicationTasks := make([]*replicationspb.ReplicationTask, 0, p.pageSize())
	skippedTaskCount := 0
	lastTaskID := maxTaskID // If no tasks are returned, then it means there are no tasks bellow maxTaskID.
	iter := collection.NewPagingIterator(p.getReplicationTasksFn(ctx, readerID, minTaskID, maxTaskID, p.pageSize()))
	// iter.HasNext() should be the last check to avoid extra page read in case if replicationTasks is already full.
	for len(replicationTasks) < p.pageSize() && skippedTaskCount <= p.maxSkipTaskCount() && iter.HasNext() {
		task, err := iter.Next()
		if err != nil {
			return p.swallowPartialResultsError(replicationTasks, lastTaskID, err)
		}

		// If, for any reason, task is skipped:
		//  - lastTaskID needs to be updated because this task should not be read next time,
		//  - skippedTaskCount needs to be incremented to prevent timeout on caller side (too many tasks are skipped).
		// If error has occurred though, lastTaskID shouldn't be updated, and next time task needs to be read again.

		ns, err := p.namespaceRegistry.GetNamespaceByID(namespace.ID(task.GetNamespaceID()))
		if err != nil {
			if _, isNotFound := err.(*serviceerror.NamespaceNotFound); !isNotFound {
				return p.swallowPartialResultsError(replicationTasks, lastTaskID, err)
			}
			// Namespace doesn't exist on this cluster (i.e. deleted). It is safe to skip the task.
			lastTaskID = task.GetTaskID()
			skippedTaskCount++
			continue
		}
		// If namespace doesn't exist on polling cluster, there is no reason to send the task.
		if !ns.IsOnCluster(pollingCluster) {
			lastTaskID = task.GetTaskID()
			skippedTaskCount++
			continue
		}

		replicationTask, err := p.toReplicationTask(ctx, task)
		if err != nil {
			return p.swallowPartialResultsError(replicationTasks, lastTaskID, err)
		} else if replicationTask == nil {
			lastTaskID = task.GetTaskID()
			skippedTaskCount++
			continue
		}
		lastTaskID = task.GetTaskID()
		replicationTasks = append(replicationTasks, replicationTask)
	}

	return replicationTasks, lastTaskID, nil
}

func (p *ackMgrImpl) getReplicationTasksFn(
	ctx context.Context,
	readerID int32,
	minTaskID int64,
	maxTaskID int64,
	batchSize int,
) collection.PaginationFn[tasks.Task] {
	return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
		response, err := p.executionMgr.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
			ShardID:             p.shard.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
			ReaderID:            readerID,
			InclusiveMinTaskKey: tasks.NewImmediateKey(minTaskID + 1),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxTaskID + 1),
			BatchSize:           batchSize,
			NextPageToken:       paginationToken,
		})
		if err != nil {
			return nil, nil, err
		}
		return response.Tasks, response.NextPageToken, nil
	}
}

func (p *ackMgrImpl) swallowPartialResultsError(
	replicationTasks []*replicationspb.ReplicationTask,
	lastTaskID int64,
	err error,
) ([]*replicationspb.ReplicationTask, int64, error) {

	p.logger.Error("Replication tasks reader encountered error, return earlier.", tag.Error(err), tag.Value(len(replicationTasks)))
	if len(replicationTasks) == 0 {
		return nil, 0, err
	}
	return replicationTasks, lastTaskID, nil
}

func (p *ackMgrImpl) taskIDsRange(
	lastReadMessageID int64,
) (minTaskID int64, maxTaskID int64) {
	minTaskID = lastReadMessageID
	maxTaskID = p.shard.GetImmediateQueueExclusiveHighReadWatermark().Prev().TaskID

	p.Lock()
	defer p.Unlock()
	defer func() { p.maxTaskID = convert.Int64Ptr(maxTaskID) }()

	now := p.shard.GetTimeSource().Now()
	if p.sanityCheckTime.IsZero() || p.sanityCheckTime.Before(now) {
		p.sanityCheckTime = now.Add(backoff.Jitter(
			p.config.ReplicatorProcessorMaxPollInterval(),
			p.config.ReplicatorProcessorMaxPollIntervalJitterCoefficient(),
		))
		return minTaskID, maxTaskID
	}

	if p.maxTaskID != nil && *p.maxTaskID < maxTaskID {
		maxTaskID = *p.maxTaskID
	}

	return minTaskID, maxTaskID
}

func (p *ackMgrImpl) toReplicationTask(
	ctx context.Context,
	task tasks.Task,
) (*replicationspb.ReplicationTask, error) {

	switch task := task.(type) {
	case *tasks.SyncActivityTask:
		return p.generateSyncActivityTask(ctx, task)
	case *tasks.HistoryReplicationTask:
		return p.generateHistoryReplicationTask(ctx, task)
	case *tasks.SyncWorkflowStateTask:
		return p.generateSyncWorkflowStateTask(ctx, task)
	default:
		return nil, errUnknownReplicationTask
	}
}

func (p *ackMgrImpl) generateSyncActivityTask(
	ctx context.Context,
	taskInfo *tasks.SyncActivityTask,
) (*replicationspb.ReplicationTask, error) {
	return convertActivityStateReplicationTask(
		ctx,
		taskInfo,
		p.workflowCache,
	)
}

func (p *ackMgrImpl) generateHistoryReplicationTask(
	ctx context.Context,
	taskInfo *tasks.HistoryReplicationTask,
) (*replicationspb.ReplicationTask, error) {
	return convertHistoryReplicationTask(
		ctx,
		taskInfo,
		p.shard.GetShardID(),
		p.workflowCache,
		p.executionMgr,
		p.logger,
	)
}

func (p *ackMgrImpl) generateSyncWorkflowStateTask(
	ctx context.Context,
	taskInfo *tasks.SyncWorkflowStateTask,
) (*replicationspb.ReplicationTask, error) {
	return convertWorkflowStateReplicationTask(
		ctx,
		taskInfo,
		p.workflowCache,
	)
}

func (p *ackMgrImpl) clusterToReaderID(
	ctx context.Context,
	pollingCluster string,
) (int32, error) {
	// TODO: need different readerID for different remote clusters
	// e.g. use cluster's initial failover version
	readerID := int32(common.DefaultQueueReaderID)

	p.Lock()
	defer p.Unlock()

	if p.registeredQueueReaders == nil {
		return readerID, errors.New("replication ack manager already closed")
	}

	if _, ok := p.registeredQueueReaders[readerID]; !ok {
		err := p.executionMgr.RegisterHistoryTaskReader(ctx, &persistence.RegisterHistoryTaskReaderRequest{
			ShardID:      p.shard.GetShardID(),
			ShardOwner:   p.shard.GetOwner(),
			TaskCategory: tasks.CategoryReplication,
			ReaderID:     readerID,
		})
		if err != nil {
			return readerID, err
		}
		p.registeredQueueReaders[readerID] = struct{}{}
	}

	return readerID, nil
}
