// Copyright (c) 2017 Uber Technologies, Inc.
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

package history

import (
	"errors"
	"sync"
	"time"

	"github.com/uber-common/bark"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	replicatorQueueProcessorImpl struct {
		currentClusterNamer string
		shard               ShardContext
		executionMgr        persistence.ExecutionManager
		historyMgr          persistence.HistoryManager
		hSerializerFactory  persistence.HistorySerializerFactory
		replicator          messaging.Producer
		metricsClient       metrics.Client
		options             *QueueProcessorOptions
		logger              bark.Logger
		*queueProcessorBase
		queueAckMgr

		sync.Mutex
		lastShardSyncTimestamp time.Time
	}
)

var (
	errUnknownReplicationTask = errors.New("Unknown replication task")
	defaultHistoryPageSize    = 1000
)

func newReplicatorQueueProcessor(shard ShardContext, replicator messaging.Producer,
	executionMgr persistence.ExecutionManager, historyMgr persistence.HistoryManager,
	hSerializerFactory persistence.HistorySerializerFactory, logger bark.Logger) queueProcessor {

	currentClusterNamer := shard.GetService().GetClusterMetadata().GetCurrentClusterName()

	config := shard.GetConfig()
	options := &QueueProcessorOptions{
		StartDelay:                         config.ReplicatorProcessorStartDelay,
		BatchSize:                          config.ReplicatorTaskBatchSize,
		WorkerCount:                        config.ReplicatorTaskWorkerCount,
		MaxPollRPS:                         config.ReplicatorProcessorMaxPollRPS,
		MaxPollInterval:                    config.ReplicatorProcessorMaxPollInterval,
		MaxPollIntervalJitterCoefficient:   config.ReplicatorProcessorMaxPollIntervalJitterCoefficient,
		UpdateAckInterval:                  config.ReplicatorProcessorUpdateAckInterval,
		UpdateAckIntervalJitterCoefficient: config.ReplicatorProcessorUpdateAckIntervalJitterCoefficient,
		MaxRetryCount:                      config.ReplicatorTaskMaxRetryCount,
		MetricScope:                        metrics.ReplicatorQueueProcessorScope,
	}

	logger = logger.WithFields(bark.Fields{
		logging.TagWorkflowComponent: logging.TagValueReplicatorQueueComponent,
	})

	processor := &replicatorQueueProcessorImpl{
		currentClusterNamer: currentClusterNamer,
		shard:               shard,
		executionMgr:        executionMgr,
		historyMgr:          historyMgr,
		hSerializerFactory:  hSerializerFactory,
		replicator:          replicator,
		metricsClient:       shard.GetMetricsClient(),
		options:             options,
		logger:              logger,
	}

	queueAckMgr := newQueueAckMgr(shard, options, processor, shard.GetReplicatorAckLevel(), logger)
	queueProcessorBase := newQueueProcessorBase(currentClusterNamer, shard, options, processor, queueAckMgr, logger)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func (p *replicatorQueueProcessorImpl) process(qTask queueTaskInfo) error {
	task, ok := qTask.(*persistence.ReplicationTaskInfo)
	if !ok {
		return errUnexpectedQueueTask
	}
	scope := metrics.ReplicatorQueueProcessorScope
	var err error
	switch task.TaskType {
	case persistence.ReplicationTaskTypeHistory:
		scope = metrics.ReplicatorTaskHistoryScope
		err = p.processHistoryReplicationTask(task)
	default:
		err = errUnknownReplicationTask
	}

	if err != nil {
		p.metricsClient.IncCounter(scope, metrics.TaskFailures)
	} else {
		err = p.queueAckMgr.completeQueueTask(task.TaskID)
	}
	return err
}

func (p *replicatorQueueProcessorImpl) queueShutdown() error {
	// there is no shutdown specific behavior for replication queue
	return nil
}

func (p *replicatorQueueProcessorImpl) processHistoryReplicationTask(task *persistence.ReplicationTaskInfo) error {
	p.metricsClient.IncCounter(metrics.ReplicatorTaskHistoryScope, metrics.TaskRequests)
	sw := p.metricsClient.StartTimer(metrics.ReplicatorTaskHistoryScope, metrics.TaskLatency)
	defer sw.Stop()

	domainEntry, err := p.shard.GetDomainCache().GetDomainByID(task.DomainID)
	if err != nil {
		return err
	}
	targetClusters := []string{}
	for _, cluster := range domainEntry.GetReplicationConfig().Clusters {
		targetClusters = append(targetClusters, cluster.ClusterName)
	}

	history, err := p.getHistory(task.DomainID, task.WorkflowID, task.RunID, task.FirstEventID, task.NextEventID)
	if err != nil {
		return err
	}

	// Check if this is replication task for ContinueAsNew event, then retrieve the history for new execution
	var newRunHistory *shared.History
	events := history.Events
	if len(events) > 0 {
		lastEvent := events[len(events)-1]
		if lastEvent.GetEventType() == shared.EventTypeWorkflowExecutionContinuedAsNew {
			newRunID := lastEvent.WorkflowExecutionContinuedAsNewEventAttributes.GetNewExecutionRunId()
			newRunHistory, err = p.getHistory(task.DomainID, task.WorkflowID, newRunID, common.FirstEventID, int64(3))
			if err != nil {
				return err
			}
		}
	}

	replicationTask := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeHistory),
		HistoryTaskAttributes: &replicator.HistoryTaskAttributes{
			TargetClusters:  targetClusters,
			DomainId:        common.StringPtr(task.DomainID),
			WorkflowId:      common.StringPtr(task.WorkflowID),
			RunId:           common.StringPtr(task.RunID),
			FirstEventId:    common.Int64Ptr(task.FirstEventID),
			NextEventId:     common.Int64Ptr(task.NextEventID),
			Version:         common.Int64Ptr(task.Version),
			ReplicationInfo: convertLastReplicationInfo(task.LastReplicationInfo),
			History:         history,
			NewRunHistory:   newRunHistory,
		},
	}

	err = p.replicator.Publish(replicationTask)
	if err == nil {
		p.Lock()
		p.lastShardSyncTimestamp = common.NewRealTimeSource().Now()
		p.Unlock()
	}
	return err
}

func (p *replicatorQueueProcessorImpl) readTasks(readLevel int64) ([]queueTaskInfo, bool, error) {
	response, err := p.executionMgr.GetReplicationTasks(&persistence.GetReplicationTasksRequest{
		ReadLevel:    readLevel,
		MaxReadLevel: p.shard.GetTransferMaxReadLevel(),
		BatchSize:    p.options.BatchSize(),
	})

	if err != nil {
		return nil, false, err
	}

	tasks := make([]queueTaskInfo, len(response.Tasks))
	for i := range response.Tasks {
		tasks[i] = response.Tasks[i]
	}

	return tasks, len(response.NextPageToken) != 0, nil
}

func (p *replicatorQueueProcessorImpl) completeTask(taskID int64) error {
	return p.executionMgr.CompleteReplicationTask(&persistence.CompleteReplicationTaskRequest{
		TaskID: taskID,
	})
}

func (p *replicatorQueueProcessorImpl) updateAckLevel(ackLevel int64) error {
	err := p.shard.UpdateReplicatorAckLevel(ackLevel)

	// this is a hack, since there is not dedicated ticker on the queue processor
	// to periodically send out sync shard message, put it here
	now := common.NewRealTimeSource().Now()
	sendSyncTask := false
	p.Lock()
	if p.lastShardSyncTimestamp.Add(p.shard.GetConfig().ShardSyncMinInterval()).Before(now) {
		p.lastShardSyncTimestamp = now
		sendSyncTask = true
	}
	p.Unlock()

	if sendSyncTask {
		syncStatusTask := &replicator.ReplicationTask{
			TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeSyncShardStatus),
			SyncShardStatusTaskAttributes: &replicator.SyncShardStatusTaskAttributes{
				SourceCluster: common.StringPtr(p.currentClusterNamer),
				ShardId:       common.Int64Ptr(int64(p.shard.GetShardID())),
				Timestamp:     common.Int64Ptr(now.UnixNano()),
			},
		}
		// ignore the error
		p.replicator.Publish(syncStatusTask)
	}

	return err
}

func (p *replicatorQueueProcessorImpl) getHistory(domainID, workflowID, runID string, firstEventID,
	nextEventID int64) (*shared.History, error) {

	var nextPageToken []byte
	historyEvents := []*shared.HistoryEvent{}
	for hasMore := true; hasMore; hasMore = len(nextPageToken) > 0 {
		response, err := p.historyMgr.GetWorkflowExecutionHistory(&persistence.GetWorkflowExecutionHistoryRequest{
			DomainID: domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			FirstEventID:  firstEventID,
			NextEventID:   nextEventID,
			PageSize:      defaultHistoryPageSize,
			NextPageToken: nextPageToken,
		})

		if err != nil {
			return nil, err
		}

		historyEvents = append(historyEvents, response.History.Events...)
		nextPageToken = response.NextPageToken
	}

	executionHistory := &shared.History{}
	executionHistory.Events = historyEvents
	return executionHistory, nil
}

func convertLastReplicationInfo(info map[string]*persistence.ReplicationInfo) map[string]*h.ReplicationInfo {
	replicationInfoMap := make(map[string]*h.ReplicationInfo)
	for k, v := range info {
		replicationInfoMap[k] = &h.ReplicationInfo{
			Version:     common.Int64Ptr(v.Version),
			LastEventId: common.Int64Ptr(v.LastEventID),
		}
	}

	return replicationInfoMap
}
