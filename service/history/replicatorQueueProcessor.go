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
	"time"

	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	replicatorQueueProcessorImpl struct {
		currentClusterNamer   string
		shard                 ShardContext
		historyCache          *historyCache
		replicationTaskFilter queueTaskFilter
		executionMgr          persistence.ExecutionManager
		historyMgr            persistence.HistoryManager
		historyV2Mgr          persistence.HistoryV2Manager
		replicator            messaging.Producer
		metricsClient         metrics.Client
		options               *QueueProcessorOptions
		logger                log.Logger
		*queueProcessorBase
		queueAckMgr

		lastShardSyncTimestamp time.Time
	}
)

var (
	errUnknownReplicationTask = errors.New("Unknown replication task")
	errHistoryNotFoundTask    = errors.New("History not found")
	defaultHistoryPageSize    = 1000
)

func newReplicatorQueueProcessor(shard ShardContext, historyCache *historyCache, replicator messaging.Producer,
	executionMgr persistence.ExecutionManager, historyMgr persistence.HistoryManager, historyV2Mgr persistence.HistoryV2Manager, logger log.Logger) queueProcessor {

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

	logger = logger.WithTags(tag.ComponentReplicatorQueue)

	replicationTaskFilter := func(qTask queueTaskInfo) (bool, error) {
		return true, nil
	}

	processor := &replicatorQueueProcessorImpl{
		currentClusterNamer:   currentClusterNamer,
		shard:                 shard,
		historyCache:          historyCache,
		replicationTaskFilter: replicationTaskFilter,
		executionMgr:          executionMgr,
		historyMgr:            historyMgr,
		historyV2Mgr:          historyV2Mgr,
		replicator:            replicator,
		metricsClient:         shard.GetMetricsClient(),
		options:               options,
		logger:                logger,
	}

	queueAckMgr := newQueueAckMgr(shard, options, processor, shard.GetReplicatorAckLevel(), logger)
	queueProcessorBase := newQueueProcessorBase(currentClusterNamer, shard, options, processor, queueAckMgr, logger)
	processor.queueAckMgr = queueAckMgr
	processor.queueProcessorBase = queueProcessorBase

	return processor
}

func (p *replicatorQueueProcessorImpl) getTaskFilter() queueTaskFilter {
	return p.replicationTaskFilter
}

func (p *replicatorQueueProcessorImpl) process(qTask queueTaskInfo, shouldProcessTask bool) (int, error) {
	task, ok := qTask.(*persistence.ReplicationTaskInfo)
	if !ok {
		return metrics.ReplicatorQueueProcessorScope, errUnexpectedQueueTask
	}
	// replication queue should always process all tasks
	// so should not do anything to shouldProcessTask variable

	switch task.TaskType {
	case persistence.ReplicationTaskTypeSyncActivity:
		err := p.processSyncActivityTask(task)
		if err == nil {
			err = p.executionMgr.CompleteReplicationTask(&persistence.CompleteReplicationTaskRequest{TaskID: task.GetTaskID()})
		}
		return metrics.ReplicatorTaskSyncActivityScope, err
	case persistence.ReplicationTaskTypeHistory:
		err := p.processHistoryReplicationTask(task)
		if _, ok := err.(*shared.EntityNotExistsError); ok {
			err = errHistoryNotFoundTask
		}
		if err == nil {
			err = p.executionMgr.CompleteReplicationTask(&persistence.CompleteReplicationTaskRequest{TaskID: task.GetTaskID()})
		}
		return metrics.ReplicatorTaskHistoryScope, err
	default:
		return metrics.ReplicatorQueueProcessorScope, errUnknownReplicationTask
	}
}

func (p *replicatorQueueProcessorImpl) queueShutdown() error {
	// there is no shutdown specific behavior for replication queue
	return nil
}

func (p *replicatorQueueProcessorImpl) processSyncActivityTask(task *persistence.ReplicationTaskInfo) (retError error) {
	domainID := task.DomainID
	execution := shared.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
	context, release, err := p.historyCache.getOrCreateWorkflowExecution(domainID, execution)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); ok {
			return nil
		}
		return err
	}
	if !msBuilder.IsWorkflowExecutionRunning() {
		// workflow already finished, no need to process the timer
		return nil
	}

	activityInfo, ok := msBuilder.GetActivityInfo(task.ScheduledID)
	if !ok {
		return nil
	}

	var startedTime *int64
	var heartbeatTime *int64
	scheduledTime := common.Int64Ptr(activityInfo.ScheduledTime.UnixNano())
	if activityInfo.StartedID != common.EmptyEventID {
		startedTime = common.Int64Ptr(activityInfo.StartedTime.UnixNano())
	}
	// LastHeartBeatUpdatedTime must be valid when getting the sync activity replication task
	heartbeatTime = common.Int64Ptr(activityInfo.LastHeartBeatUpdatedTime.UnixNano())

	replicationTask := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeSyncActivity),
		SyncActicvityTaskAttributes: &replicator.SyncActicvityTaskAttributes{
			DomainId:          common.StringPtr(task.DomainID),
			WorkflowId:        common.StringPtr(task.WorkflowID),
			RunId:             common.StringPtr(task.RunID),
			Version:           common.Int64Ptr(activityInfo.Version),
			ScheduledId:       common.Int64Ptr(activityInfo.ScheduleID),
			ScheduledTime:     scheduledTime,
			StartedId:         common.Int64Ptr(activityInfo.StartedID),
			StartedTime:       startedTime,
			LastHeartbeatTime: heartbeatTime,
			Details:           activityInfo.Details,
			Attempt:           common.Int32Ptr(activityInfo.Attempt),
		},
	}

	return p.replicator.Publish(replicationTask)
}

func (p *replicatorQueueProcessorImpl) processHistoryReplicationTask(task *persistence.ReplicationTaskInfo) error {

	domainEntry, err := p.shard.GetDomainCache().GetDomainByID(task.DomainID)
	if err != nil {
		return err
	}
	targetClusters := []string{}
	for _, cluster := range domainEntry.GetReplicationConfig().Clusters {
		targetClusters = append(targetClusters, cluster.ClusterName)
	}
	replicationTask, err := GenerateReplicationTask(targetClusters, task, p.historyMgr, p.historyV2Mgr, p.metricsClient, p.logger, nil, common.IntPtr(p.shard.GetShardID()))
	if err != nil || replicationTask == nil {
		return err
	}

	return p.replicator.Publish(replicationTask)
}

// GenerateReplicationTask generate replication task
func GenerateReplicationTask(targetClusters []string, task *persistence.ReplicationTaskInfo,
	historyMgr persistence.HistoryManager, historyV2Mgr persistence.HistoryV2Manager,
	metricsClient metrics.Client, logger log.Logger, history *shared.History, shardID *int,
) (*replicator.ReplicationTask, error) {
	var err error
	if history == nil {
		history, _, err = GetAllHistory(historyMgr, historyV2Mgr, metricsClient, logger, false,
			task.DomainID, task.WorkflowID, task.RunID, task.FirstEventID, task.NextEventID, task.EventStoreVersion, task.BranchToken, shardID)
		if err != nil {
			return nil, err
		}
		for _, event := range history.Events {
			if task.Version != event.GetVersion() {
				return nil, nil
			}
		}
	}

	var newRunHistory *shared.History
	events := history.Events
	if len(events) > 0 {
		lastEvent := events[len(events)-1]
		if lastEvent.GetEventType() == shared.EventTypeWorkflowExecutionContinuedAsNew {
			// Check if this is replication task for ContinueAsNew event, then retrieve the history for new execution
			newRunID := lastEvent.WorkflowExecutionContinuedAsNewEventAttributes.GetNewExecutionRunId()
			newRunHistory, _, err = GetAllHistory(historyMgr, historyV2Mgr, metricsClient, logger, false,
				task.DomainID, task.WorkflowID, newRunID, common.FirstEventID, int64(3), task.NewRunEventStoreVersion, task.NewRunBranchToken, shardID)
			if err != nil {
				return nil, err
			}
		}
	}

	ret := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeHistory),
		HistoryTaskAttributes: &replicator.HistoryTaskAttributes{
			TargetClusters:          targetClusters,
			DomainId:                common.StringPtr(task.DomainID),
			WorkflowId:              common.StringPtr(task.WorkflowID),
			RunId:                   common.StringPtr(task.RunID),
			FirstEventId:            common.Int64Ptr(task.FirstEventID),
			NextEventId:             common.Int64Ptr(task.NextEventID),
			Version:                 common.Int64Ptr(task.Version),
			ReplicationInfo:         convertLastReplicationInfo(task.LastReplicationInfo),
			History:                 history,
			NewRunHistory:           newRunHistory,
			EventStoreVersion:       common.Int32Ptr(task.EventStoreVersion),
			NewRunEventStoreVersion: common.Int32Ptr(task.NewRunEventStoreVersion),
			ResetWorkflow:           common.BoolPtr(task.ResetWorkflow),
		},
	}
	return ret, nil
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

func (p *replicatorQueueProcessorImpl) updateAckLevel(ackLevel int64) error {
	err := p.shard.UpdateReplicatorAckLevel(ackLevel)

	// this is a hack, since there is not dedicated ticker on the queue processor
	// to periodically send out sync shard message, put it here
	now := clock.NewRealTimeSource().Now()
	if p.lastShardSyncTimestamp.Add(p.shard.GetConfig().ShardSyncMinInterval()).Before(now) {
		syncStatusTask := &replicator.ReplicationTask{
			TaskType: replicator.ReplicationTaskType.Ptr(replicator.ReplicationTaskTypeSyncShardStatus),
			SyncShardStatusTaskAttributes: &replicator.SyncShardStatusTaskAttributes{
				SourceCluster: common.StringPtr(p.currentClusterNamer),
				ShardId:       common.Int64Ptr(int64(p.shard.GetShardID())),
				Timestamp:     common.Int64Ptr(now.UnixNano()),
			},
		}
		// ignore the error
		if syncErr := p.replicator.Publish(syncStatusTask); syncErr == nil {
			p.lastShardSyncTimestamp = now
		}
	}
	return err
}

// GetAllHistory return history
func GetAllHistory(historyMgr persistence.HistoryManager, historyV2Mgr persistence.HistoryV2Manager,
	metricsClient metrics.Client, logger log.Logger, byBatch bool,
	domainID string, workflowID string, runID string, firstEventID int64,
	nextEventID int64, eventStoreVersion int32, branchToken []byte, shardID *int) (*shared.History, []*shared.History, error) {

	// overall result
	historyEvents := []*shared.HistoryEvent{}
	historyBatches := []*shared.History{}
	historySize := 0
	var err error

	// variable used for each page
	pageHistoryEvents := []*shared.HistoryEvent{}
	pageHistoryBatches := []*shared.History{}
	var pageToken []byte
	var pageHistorySize int

	for hasMore := true; hasMore; hasMore = len(pageToken) > 0 {
		pageHistoryEvents, pageHistoryBatches, pageToken, pageHistorySize, err = PaginateHistory(
			historyMgr, historyV2Mgr, metricsClient, logger, byBatch,
			domainID, workflowID, runID, firstEventID, nextEventID, pageToken,
			eventStoreVersion, branchToken, defaultHistoryPageSize, shardID,
		)
		if err != nil {
			return nil, nil, err
		}

		historyEvents = append(historyEvents, pageHistoryEvents...)
		historyBatches = append(historyBatches, pageHistoryBatches...)
		historySize += pageHistorySize
	}

	// Emit metric and log for history size
	if metricsClient != nil {
		metricsClient.RecordTimer(metrics.ReplicatorQueueProcessorScope, metrics.HistorySize, time.Duration(historySize))
	}
	if historySize > common.GetHistoryWarnSizeLimit {
		logger.Warn("GetHistory size threshold breached",
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
			tag.WorkflowDomainID(domainID),
			tag.WorkflowSize(int64(historySize)))
	}

	history := &shared.History{
		Events: historyEvents,
	}
	return history, historyBatches, nil
}

// PaginateHistory return paged history
func PaginateHistory(historyMgr persistence.HistoryManager, historyV2Mgr persistence.HistoryV2Manager,
	metricsClient metrics.Client, logger log.Logger, byBatch bool,
	domainID, workflowID, runID string, firstEventID,
	nextEventID int64, tokenIn []byte, eventStoreVersion int32, branchToken []byte, pageSize int, shardID *int) ([]*shared.HistoryEvent, []*shared.History, []byte, int, error) {

	historyEvents := []*shared.HistoryEvent{}
	historyBatches := []*shared.History{}
	var tokenOut []byte
	var historySize int

	if eventStoreVersion == persistence.EventStoreVersionV2 {
		req := &persistence.ReadHistoryBranchRequest{
			BranchToken:   branchToken,
			MinEventID:    firstEventID,
			MaxEventID:    nextEventID,
			PageSize:      pageSize,
			NextPageToken: tokenIn,
			ShardID:       shardID,
		}
		if byBatch {
			response, err := historyV2Mgr.ReadHistoryBranchByBatch(req)
			if err != nil {
				return nil, nil, nil, 0, err
			}

			// Keep track of total history size
			historySize += response.Size
			historyBatches = append(historyBatches, response.History...)
			tokenOut = response.NextPageToken

		} else {
			response, err := historyV2Mgr.ReadHistoryBranch(req)
			if err != nil {
				return nil, nil, nil, 0, err
			}

			// Keep track of total history size
			historySize += response.Size
			historyEvents = append(historyEvents, response.HistoryEvents...)
			tokenOut = response.NextPageToken
		}
	} else {
		req := &persistence.GetWorkflowExecutionHistoryRequest{
			DomainID: domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			FirstEventID:  firstEventID,
			NextEventID:   nextEventID,
			PageSize:      pageSize,
			NextPageToken: tokenIn,
		}

		if byBatch {
			response, err := historyMgr.GetWorkflowExecutionHistoryByBatch(req)
			if err != nil {
				return nil, nil, nil, 0, err
			}

			// Keep track of total history size
			historySize += response.Size
			historyBatches = append(historyBatches, response.History...)
			tokenOut = response.NextPageToken

		} else {
			response, err := historyMgr.GetWorkflowExecutionHistory(req)
			if err != nil {
				return nil, nil, nil, 0, err
			}

			// Keep track of total history size
			historySize += response.Size
			historyEvents = append(historyEvents, response.History.Events...)
			tokenOut = response.NextPageToken
		}
	}

	return historyEvents, historyBatches, tokenOut, historySize, nil
}

func convertLastReplicationInfo(info map[string]*persistence.ReplicationInfo) map[string]*shared.ReplicationInfo {
	replicationInfoMap := make(map[string]*shared.ReplicationInfo)
	for k, v := range info {
		replicationInfoMap[k] = &shared.ReplicationInfo{
			Version:     common.Int64Ptr(v.Version),
			LastEventId: common.Int64Ptr(v.LastEventID),
		}
	}

	return replicationInfoMap
}
