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
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
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

		SubscribeNotification() (<-chan struct{}, string)
		UnsubscribeNotification(string)
		ConvertTask(
			ctx context.Context,
			task tasks.Task,
		) (*replicationspb.ReplicationTask, error)
		GetReplicationTasksIter(
			ctx context.Context,
			pollingCluster string,
			minInclusiveTaskID int64,
			maxExclusiveTaskID int64,
		) (collection.Iterator[tasks.Task], error)
	}

	ackMgrImpl struct {
		currentClusterName string
		shardContext       shard.Context
		config             *configs.Config
		workflowCache      wcache.Cache
		eventBlobCache     persistence.XDCCache
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
		maxTaskVisibilityTimestamp time.Time
		sanityCheckTime            time.Time

		subscriberLock sync.Mutex
		subscribers    map[string]chan struct{}
	}
)

var (
	errUnknownReplicationTask = serviceerror.NewInternal("unknown replication task")
)

func NewAckManager(
	shardContext shard.Context,
	workflowCache wcache.Cache,
	eventBlobCache persistence.XDCCache,
	executionMgr persistence.ExecutionManager,
	logger log.Logger,
) AckManager {

	currentClusterName := shardContext.GetClusterMetadata().GetCurrentClusterName()
	config := shardContext.GetConfig()

	retryPolicy := backoff.NewExponentialRetryPolicy(200 * time.Millisecond).
		WithMaximumAttempts(5).
		WithBackoffCoefficient(1)

	return &ackMgrImpl{
		currentClusterName: currentClusterName,
		shardContext:       shardContext,
		config:             shardContext.GetConfig(),
		workflowCache:      workflowCache,
		eventBlobCache:     eventBlobCache,
		executionMgr:       executionMgr,
		metricsHandler:     shardContext.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.ReplicatorQueueProcessorScope)),
		logger:             log.With(logger, tag.ComponentReplicatorQueue),
		retryPolicy:        retryPolicy,
		namespaceRegistry:  shardContext.GetNamespaceRegistry(),
		pageSize:           config.ReplicatorProcessorFetchTasksBatchSize,
		maxSkipTaskCount:   config.ReplicatorProcessorMaxSkipTaskCount,

		maxTaskID:       nil,
		sanityCheckTime: time.Time{},

		subscribers: make(map[string]chan struct{}),
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

	defer p.broadcast()

	p.Lock()
	defer p.Unlock()
	if p.maxTaskID == nil || *p.maxTaskID < maxTaskID {
		p.maxTaskID = &maxTaskID
	}
	if p.maxTaskVisibilityTimestamp.IsZero() || p.maxTaskVisibilityTimestamp.Before(maxVisibilityTimestamp) {
		p.maxTaskVisibilityTimestamp = maxVisibilityTimestamp
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
		taskID := p.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID
		maxTaskID = &taskID
	}
	maxVisibilityTimestamp := p.maxTaskVisibilityTimestamp
	if maxVisibilityTimestamp.IsZero() {
		maxVisibilityTimestamp = p.shardContext.GetTimeSource().Now()
	}

	return *maxTaskID, maxVisibilityTimestamp
}

// TODO: deprecate this method
// It's only used by the replication DLQ v1 logic.
func (p *ackMgrImpl) GetTask(
	ctx context.Context,
	taskInfo *replicationspb.ReplicationTaskInfo,
) (*replicationspb.ReplicationTask, error) {

	switch taskInfo.TaskType {
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
		return p.ConvertTask(ctx, &tasks.SyncActivityTask{
			WorkflowKey: definition.NewWorkflowKey(
				taskInfo.GetNamespaceId(),
				taskInfo.GetWorkflowId(),
				taskInfo.GetRunId(),
			),
			VisibilityTimestamp: time.Unix(0, 0),
			TaskID:              taskInfo.TaskId,
			Version:             taskInfo.Version,
			ScheduledEventID:    taskInfo.ScheduledEventId,
		})
	case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
		return p.ConvertTask(ctx, &tasks.HistoryReplicationTask{
			WorkflowKey: definition.NewWorkflowKey(
				taskInfo.GetNamespaceId(),
				taskInfo.GetWorkflowId(),
				taskInfo.GetRunId(),
			),
			VisibilityTimestamp: time.Unix(0, 0),
			TaskID:              taskInfo.TaskId,
			Version:             taskInfo.Version,
			FirstEventID:        taskInfo.FirstEventId,
			NextEventID:         taskInfo.NextEventId,
		})
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE:
		return p.ConvertTask(ctx, &tasks.SyncWorkflowStateTask{
			WorkflowKey: definition.NewWorkflowKey(
				taskInfo.GetNamespaceId(),
				taskInfo.GetWorkflowId(),
				taskInfo.GetRunId(),
			),
			VisibilityTimestamp: time.Unix(0, 0),
			TaskID:              taskInfo.TaskId,
			Version:             taskInfo.Version,
			Priority:            taskInfo.GetPriority(),
		})
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM:
		return p.ConvertTask(ctx, &tasks.SyncHSMTask{
			WorkflowKey: definition.NewWorkflowKey(
				taskInfo.GetNamespaceId(),
				taskInfo.GetWorkflowId(),
				taskInfo.GetRunId(),
			),
			VisibilityTimestamp: time.Unix(0, 0),
			TaskID:              taskInfo.TaskId,
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
	metrics.ReplicationTasksLag.With(p.metricsHandler).Record(
		maxTaskID-lastTaskID,
		metrics.TargetClusterTag(pollingCluster),
		metrics.OperationTag(metrics.ReplicationTaskFetcherScope),
	)

	metrics.ReplicationTasksFetched.With(p.metricsHandler).
		Record(int64(len(replicationTasks)))

	replicationEventTime := timestamppb.New(p.shardContext.GetTimeSource().Now())
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

	replicationTasks := make([]*replicationspb.ReplicationTask, 0, p.pageSize())
	skippedTaskCount := 0
	lastTaskID := maxTaskID // If no tasks are returned, then it means there are no tasks bellow maxTaskID.
	iter := collection.NewPagingIterator(p.getReplicationTasksFn(ctx, minTaskID, maxTaskID, p.pageSize()))
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

		replicationTask, err := p.ConvertTask(ctx, task)
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
	minTaskID int64,
	maxTaskID int64,
	batchSize int,
) collection.PaginationFn[tasks.Task] {
	return func(paginationToken []byte) ([]tasks.Task, []byte, error) {
		response, err := p.executionMgr.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
			ShardID:             p.shardContext.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
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
	maxTaskID = p.shardContext.GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Prev().TaskID

	p.Lock()
	defer p.Unlock()
	defer func() { p.maxTaskID = util.Ptr(maxTaskID) }()

	now := p.shardContext.GetTimeSource().Now()
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

// TODO split the ack manager into 2 components

func (p *ackMgrImpl) ConvertTask(
	ctx context.Context,
	task tasks.Task,
) (*replicationspb.ReplicationTask, error) {
	switch task := task.(type) {
	case *tasks.SyncActivityTask:
		return convertActivityStateReplicationTask(
			ctx,
			p.shardContext,
			task,
			p.workflowCache,
		)
	case *tasks.SyncWorkflowStateTask:
		return convertWorkflowStateReplicationTask(
			ctx,
			p.shardContext,
			task,
			p.workflowCache,
		)
	case *tasks.HistoryReplicationTask:
		return convertHistoryReplicationTask(
			ctx,
			p.shardContext,
			task,
			p.shardContext.GetShardID(),
			p.workflowCache,
			p.eventBlobCache,
			p.executionMgr,
			p.logger,
			p.config,
		)
	case *tasks.SyncHSMTask:
		return convertSyncHSMReplicationTask(
			ctx,
			p.shardContext,
			task,
			p.workflowCache,
		)
	default:
		return nil, errUnknownReplicationTask
	}
}

func (p *ackMgrImpl) SubscribeNotification() (<-chan struct{}, string) {
	subscriberID := uuid.New().String()

	p.subscriberLock.Lock()
	defer p.subscriberLock.Unlock()

	for {
		if _, ok := p.subscribers[subscriberID]; !ok {
			channel := make(chan struct{}, 1)
			p.subscribers[subscriberID] = channel
			return channel, subscriberID
		}
		subscriberID = uuid.New().String()
	}
}

func (p *ackMgrImpl) UnsubscribeNotification(subscriberID string) {
	p.subscriberLock.Lock()
	defer p.subscriberLock.Unlock()

	delete(p.subscribers, subscriberID)
}

func (p *ackMgrImpl) broadcast() {
	p.subscriberLock.Lock()
	defer p.subscriberLock.Unlock()

	for _, channel := range p.subscribers {
		select {
		case channel <- struct{}{}:
		default:
			// noop
		}
	}
}

func (p *ackMgrImpl) GetReplicationTasksIter(
	ctx context.Context,
	pollingCluster string,
	minInclusiveTaskID int64,
	maxExclusiveTaskID int64,
) (collection.Iterator[tasks.Task], error) {
	return collection.NewPagingIterator(func(paginationToken []byte) ([]tasks.Task, []byte, error) {
		ctx1, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		response, err := p.executionMgr.GetHistoryTasks(ctx1, &persistence.GetHistoryTasksRequest{
			ShardID:             p.shardContext.GetShardID(),
			TaskCategory:        tasks.CategoryReplication,
			InclusiveMinTaskKey: tasks.NewImmediateKey(minInclusiveTaskID),
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(maxExclusiveTaskID),
			BatchSize:           p.config.ReplicatorProcessorFetchTasksBatchSize(),
			NextPageToken:       paginationToken,
		})
		if err != nil {
			return nil, nil, err
		}
		return response.Tasks, response.NextPageToken, nil
	}), nil
}
