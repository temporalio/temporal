// Copyright (c) 2019 Uber Technologies, Inc.
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
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/yarpc/yarpcerrors"

	h "github.com/uber/cadence/.gen/go/history"
	r "github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

const (
	dropSyncShardTaskTimeThreshold   = 10 * time.Minute
	replicationTimeout               = 30 * time.Second
	taskErrorRetryBackoffCoefficient = 1.2
	dlqErrorRetryWait                = time.Second
)

var (
	// ErrUnknownReplicationTask is the error to indicate unknown replication task type
	ErrUnknownReplicationTask = &shared.BadRequestError{Message: "unknown replication task"}
)

type (
	// ReplicationTaskProcessor is responsible for processing replication tasks for a shard.
	ReplicationTaskProcessor struct {
		currentCluster    string
		sourceCluster     string
		status            int32
		shard             ShardContext
		historyEngine     Engine
		historySerializer persistence.PayloadSerializer
		config            *Config
		domainCache       cache.DomainCache
		metricsClient     metrics.Client
		logger            log.Logger

		taskRetryPolicy backoff.RetryPolicy
		dlqRetryPolicy  backoff.RetryPolicy
		noTaskRetrier   backoff.Retrier

		lastProcessedMessageID int64
		lastRetrievedMessageID int64

		requestChan chan<- *request
		done        chan struct{}
	}

	request struct {
		token    *r.ReplicationToken
		respChan chan<- *r.ReplicationMessages
	}
)

// NewReplicationTaskProcessor creates a new replication task processor.
func NewReplicationTaskProcessor(
	shard ShardContext,
	historyEngine Engine,
	config *Config,
	metricsClient metrics.Client,
	replicationTaskFetcher *ReplicationTaskFetcher,
) *ReplicationTaskProcessor {
	taskRetryPolicy := backoff.NewExponentialRetryPolicy(config.ReplicationTaskProcessorErrorRetryWait())
	taskRetryPolicy.SetBackoffCoefficient(taskErrorRetryBackoffCoefficient)
	taskRetryPolicy.SetMaximumAttempts(config.ReplicationTaskProcessorErrorRetryMaxAttempts())

	dlqRetryPolicy := backoff.NewExponentialRetryPolicy(dlqErrorRetryWait)
	dlqRetryPolicy.SetExpirationInterval(backoff.NoInterval)

	noTaskBackoffPolicy := backoff.NewExponentialRetryPolicy(config.ReplicationTaskProcessorNoTaskRetryWait())
	noTaskBackoffPolicy.SetBackoffCoefficient(1)
	noTaskBackoffPolicy.SetExpirationInterval(backoff.NoInterval)
	noTaskRetrier := backoff.NewRetrier(noTaskBackoffPolicy, backoff.SystemClock)

	return &ReplicationTaskProcessor{
		currentCluster:    shard.GetClusterMetadata().GetCurrentClusterName(),
		sourceCluster:     replicationTaskFetcher.GetSourceCluster(),
		status:            common.DaemonStatusInitialized,
		shard:             shard,
		historyEngine:     historyEngine,
		historySerializer: persistence.NewPayloadSerializer(),
		domainCache:       shard.GetDomainCache(),
		metricsClient:     metricsClient,
		logger:            shard.GetLogger(),
		taskRetryPolicy:   taskRetryPolicy,
		noTaskRetrier:     noTaskRetrier,
		requestChan:       replicationTaskFetcher.GetRequestChan(),
		done:              make(chan struct{}),
	}
}

// Start starts the processor
func (p *ReplicationTaskProcessor) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	go p.processorLoop()
	p.logger.Info("ReplicationTaskProcessor started.")
}

// Stop stops the processor
func (p *ReplicationTaskProcessor) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(p.done)
}

func (p *ReplicationTaskProcessor) processorLoop() {
	p.lastProcessedMessageID = p.shard.GetClusterReplicationLevel(p.sourceCluster)

	defer func() {
		p.logger.Info("Closing replication task processor.", tag.ReadLevel(p.lastRetrievedMessageID))
	}()

Loop:
	for {
		// for each iteration, do close check first
		select {
		case <-p.done:
			p.logger.Info("ReplicationTaskProcessor shutting down.")
			return
		default:
		}

		respChan := p.sendFetchMessageRequest()

		select {
		case response, ok := <-respChan:
			if !ok {
				p.logger.Debug("Fetch replication messages chan closed.")
				continue Loop
			}

			p.logger.Debug("Got fetch replication messages response.",
				tag.ReadLevel(response.GetLastRetrivedMessageId()),
				tag.Bool(response.GetHasMore()),
				tag.Counter(len(response.GetReplicationTasks())),
			)

			p.processResponse(response)
		case <-p.done:
			p.logger.Info("ReplicationTaskProcessor shutting down.")
			return
		}
	}
}

func (p *ReplicationTaskProcessor) sendFetchMessageRequest() <-chan *r.ReplicationMessages {
	respChan := make(chan *r.ReplicationMessages, 1)
	// TODO: when we support prefetching, LastRetrivedMessageId can be different than LastProcessedMessageId
	p.requestChan <- &request{
		token: &r.ReplicationToken{
			ShardID:                common.Int32Ptr(int32(p.shard.GetShardID())),
			LastRetrivedMessageId:  common.Int64Ptr(p.lastRetrievedMessageID),
			LastProcessedMessageId: common.Int64Ptr(p.lastProcessedMessageID),
		},
		respChan: respChan,
	}
	return respChan
}

func (p *ReplicationTaskProcessor) processResponse(response *r.ReplicationMessages) {
	// Note here we check replication tasks instead of hasMore. The expectation is that in a steady state
	// we will receive replication tasks but hasMore is false (meaning that we are always catching up).
	// So hasMore might not be a good indicator for additional wait.
	if len(response.ReplicationTasks) == 0 {
		backoffDuration := p.noTaskRetrier.NextBackOff()
		time.Sleep(backoffDuration)
		return
	}

	for _, replicationTask := range response.ReplicationTasks {
		err := p.processSingleTask(replicationTask)
		if err != nil {
			// Processor is shutdown. Exit without updating the checkpoint.
			return
		}
	}

	p.lastProcessedMessageID = response.GetLastRetrivedMessageId()
	p.lastRetrievedMessageID = response.GetLastRetrivedMessageId()
	err := p.shard.UpdateClusterReplicationLevel(p.sourceCluster, p.lastRetrievedMessageID)
	if err != nil {
		p.logger.Error("Error updating replication level for shard", tag.Error(err), tag.OperationFailed)
	}

	scope := p.metricsClient.Scope(metrics.ReplicationTaskFetcherScope, metrics.TargetClusterTag(p.sourceCluster))
	scope.UpdateGauge(metrics.LastRetrievedMessageID, float64(p.lastRetrievedMessageID))
	p.noTaskRetrier.Reset()
}

func (p *ReplicationTaskProcessor) processSingleTask(replicationTask *r.ReplicationTask) error {
	err := backoff.Retry(func() error {
		return p.processTaskOnce(replicationTask)
	}, p.taskRetryPolicy, isTransientRetryableError)

	if err != nil {
		p.logger.Error(
			"Failed to apply replication task after retry. Putting task into DLQ.",
			tag.TaskID(replicationTask.GetSourceTaskId()),
			tag.Error(err),
		)

		return p.putReplicationTaskToDLQ(replicationTask)
	}

	return nil
}

func (p *ReplicationTaskProcessor) processTaskOnce(replicationTask *r.ReplicationTask) error {
	var err error
	var scope int
	switch replicationTask.GetTaskType() {
	case r.ReplicationTaskTypeDomain:
		// Domain replication task should be handled in worker (domainReplicationMessageProcessor)
		panic("task type not supported")
	case r.ReplicationTaskTypeSyncShardStatus:
		scope = metrics.SyncShardTaskScope
		err = p.handleSyncShardTask(replicationTask)
	case r.ReplicationTaskTypeSyncActivity:
		scope = metrics.SyncActivityTaskScope
		err = p.handleActivityTask(replicationTask)
	case r.ReplicationTaskTypeHistory:
		scope = metrics.HistoryReplicationTaskScope
		err = p.handleHistoryReplicationTask(replicationTask)
	case r.ReplicationTaskTypeHistoryMetadata:
		// Without kafka we should not have size limits so we don't necessary need this in the new replication scheme.
	case r.ReplicationTaskTypeHistoryV2:
		scope = metrics.HistoryReplicationV2TaskScope
		err = p.handleHistoryReplicationTaskV2(replicationTask)
	default:
		p.logger.Error("Unknown task type.")
		scope = metrics.ReplicatorScope
		err = ErrUnknownReplicationTask
	}

	if err != nil {
		p.updateFailureMetric(scope, err)
	} else {
		p.logger.Debug("Successfully applied replication task.", tag.TaskID(replicationTask.GetSourceTaskId()))
		p.metricsClient.Scope(
			metrics.ReplicationTaskFetcherScope,
			metrics.TargetClusterTag(p.sourceCluster),
		).IncCounter(metrics.ReplicationTasksApplied)
	}

	return err
}

func (p *ReplicationTaskProcessor) putReplicationTaskToDLQ(replicationTask *r.ReplicationTask) error {
	request, err := p.generateDLQRequest(replicationTask)
	if err != nil {
		p.logger.Error("Failed to generate DLQ replication task.", tag.Error(err))
		// We cannot deserialize the task. Dropping it.
		return nil
	}

	// The following is guaranteed to success or retry forever until processor is shutdown.
	return backoff.Retry(func() error {
		err := p.shard.GetExecutionManager().PutReplicationTaskToDLQ(request)
		if err != nil {
			p.logger.Error("Failed to put replication task to DLQ.", tag.Error(err))
		}
		return err
	}, p.dlqRetryPolicy, p.shouldRetryDLQ)
}

func (p *ReplicationTaskProcessor) generateDLQRequest(
	replicationTask *r.ReplicationTask,
) (*persistence.PutReplicationTaskToDLQRequest, error) {
	switch *replicationTask.TaskType {
	case r.ReplicationTaskTypeSyncActivity:
		taskAttributes := replicationTask.GetSyncActicvityTaskAttributes()
		return &persistence.PutReplicationTaskToDLQRequest{
			SourceClusterName: p.sourceCluster,
			TaskInfo: &persistence.ReplicationTaskInfo{
				DomainID:    taskAttributes.GetDomainId(),
				WorkflowID:  taskAttributes.GetWorkflowId(),
				RunID:       taskAttributes.GetRunId(),
				TaskID:      replicationTask.GetSourceTaskId(),
				TaskType:    persistence.ReplicationTaskTypeSyncActivity,
				ScheduledID: taskAttributes.GetScheduledId(),
			},
		}, nil

	case r.ReplicationTaskTypeHistory:
		taskAttributes := replicationTask.GetHistoryTaskAttributes()
		return &persistence.PutReplicationTaskToDLQRequest{
			SourceClusterName: p.sourceCluster,
			TaskInfo: &persistence.ReplicationTaskInfo{
				DomainID:            taskAttributes.GetDomainId(),
				WorkflowID:          taskAttributes.GetWorkflowId(),
				RunID:               taskAttributes.GetRunId(),
				TaskID:              replicationTask.GetSourceTaskId(),
				TaskType:            persistence.ReplicationTaskTypeHistory,
				FirstEventID:        taskAttributes.GetFirstEventId(),
				NextEventID:         taskAttributes.GetNextEventId(),
				Version:             taskAttributes.GetVersion(),
				LastReplicationInfo: toPersistenceReplicationInfo(taskAttributes.GetReplicationInfo()),
				ResetWorkflow:       taskAttributes.GetResetWorkflow(),
			},
		}, nil
	case r.ReplicationTaskTypeHistoryV2:
		taskAttributes := replicationTask.GetHistoryTaskV2Attributes()

		eventsDataBlob := persistence.NewDataBlobFromThrift(taskAttributes.GetEvents())
		events, err := p.historySerializer.DeserializeBatchEvents(eventsDataBlob)
		if err != nil {
			return nil, err
		}

		if len(events) == 0 {
			p.logger.Error("Empty events in a batch")
			return nil, fmt.Errorf("corrupted history event batch, empty events")
		}

		return &persistence.PutReplicationTaskToDLQRequest{
			SourceClusterName: p.sourceCluster,
			TaskInfo: &persistence.ReplicationTaskInfo{
				DomainID:     taskAttributes.GetDomainId(),
				WorkflowID:   taskAttributes.GetWorkflowId(),
				RunID:        taskAttributes.GetRunId(),
				TaskID:       replicationTask.GetSourceTaskId(),
				TaskType:     persistence.ReplicationTaskTypeHistory,
				FirstEventID: events[0].GetEventId(),
				NextEventID:  events[len(events)-1].GetEventId(),
				Version:      events[0].GetVersion(),
			},
		}, nil
	default:
		return nil, fmt.Errorf("unknown replication task type")
	}
}

func isTransientRetryableError(err error) bool {
	switch err.(type) {
	case *shared.BadRequestError:
		return false
	default:
		return true
	}
}

func (p *ReplicationTaskProcessor) shouldRetryDLQ(err error) bool {
	if err == nil {
		return false
	}

	select {
	case <-p.done:
		p.logger.Info("ReplicationTaskProcessor shutting down.")
		return false
	default:
		return true
	}
}

func toPersistenceReplicationInfo(
	info map[string]*shared.ReplicationInfo,
) map[string]*persistence.ReplicationInfo {
	replicationInfoMap := make(map[string]*persistence.ReplicationInfo)
	for k, v := range info {
		replicationInfoMap[k] = &persistence.ReplicationInfo{
			Version:     v.GetVersion(),
			LastEventID: v.GetLastEventId(),
		}
	}

	return replicationInfoMap
}

func (p *ReplicationTaskProcessor) updateFailureMetric(scope int, err error) {
	// Always update failure counter for all replicator errors
	p.metricsClient.IncCounter(scope, metrics.ReplicatorFailures)

	// Also update counter to distinguish between type of failures
	switch err := err.(type) {
	case *h.ShardOwnershipLostError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrShardOwnershipLostCounter)
	case *shared.BadRequestError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrBadRequestCounter)
	case *shared.DomainNotActiveError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrDomainNotActiveCounter)
	case *shared.WorkflowExecutionAlreadyStartedError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrExecutionAlreadyStartedCounter)
	case *shared.EntityNotExistsError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrEntityNotExistsCounter)
	case *shared.LimitExceededError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrLimitExceededCounter)
	case *shared.RetryTaskError:
		p.metricsClient.IncCounter(scope, metrics.CadenceErrRetryTaskCounter)
	case *yarpcerrors.Status:
		if err.Code() == yarpcerrors.CodeDeadlineExceeded {
			p.metricsClient.IncCounter(scope, metrics.CadenceErrContextTimeoutCounter)
		}
	}
}

func (p *ReplicationTaskProcessor) handleActivityTask(
	task *r.ReplicationTask,
) error {

	attr := task.SyncActicvityTaskAttributes
	doContinue, err := p.filterTask(attr.GetDomainId())
	if err != nil || !doContinue {
		return err
	}

	request := &h.SyncActivityRequest{
		DomainId:           attr.DomainId,
		WorkflowId:         attr.WorkflowId,
		RunId:              attr.RunId,
		Version:            attr.Version,
		ScheduledId:        attr.ScheduledId,
		ScheduledTime:      attr.ScheduledTime,
		StartedId:          attr.StartedId,
		StartedTime:        attr.StartedTime,
		LastHeartbeatTime:  attr.LastHeartbeatTime,
		Details:            attr.Details,
		Attempt:            attr.Attempt,
		LastFailureReason:  attr.LastFailureReason,
		LastWorkerIdentity: attr.LastWorkerIdentity,
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return p.historyEngine.SyncActivity(ctx, request)
}

func (p *ReplicationTaskProcessor) handleHistoryReplicationTask(
	task *r.ReplicationTask,
) error {

	attr := task.HistoryTaskAttributes
	doContinue, err := p.filterTask(attr.GetDomainId())
	if err != nil || !doContinue {
		return err
	}

	request := &h.ReplicateEventsRequest{
		SourceCluster: common.StringPtr(p.sourceCluster),
		DomainUUID:    attr.DomainId,
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: attr.WorkflowId,
			RunId:      attr.RunId,
		},
		FirstEventId:      attr.FirstEventId,
		NextEventId:       attr.NextEventId,
		Version:           attr.Version,
		ReplicationInfo:   attr.ReplicationInfo,
		History:           attr.History,
		NewRunHistory:     attr.NewRunHistory,
		ForceBufferEvents: common.BoolPtr(false),
		ResetWorkflow:     attr.ResetWorkflow,
		NewRunNDC:         attr.NewRunNDC,
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return p.historyEngine.ReplicateEvents(ctx, request)
}

func (p *ReplicationTaskProcessor) handleHistoryReplicationTaskV2(
	task *r.ReplicationTask,
) error {

	attr := task.HistoryTaskV2Attributes
	doContinue, err := p.filterTask(attr.GetDomainId())
	if err != nil || !doContinue {
		return err
	}

	request := &h.ReplicateEventsV2Request{
		DomainUUID: attr.DomainId,
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: attr.WorkflowId,
			RunId:      attr.RunId,
		},
		VersionHistoryItems: attr.VersionHistoryItems,
		Events:              attr.Events,
		// new run events does not need version history since there is no prior events
		NewRunEvents: attr.NewRunEvents,
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return p.historyEngine.ReplicateEventsV2(ctx, request)
}

func (p *ReplicationTaskProcessor) handleSyncShardTask(
	task *r.ReplicationTask,
) error {

	attr := task.SyncShardStatusTaskAttributes
	if time.Now().Sub(time.Unix(0, attr.GetTimestamp())) > dropSyncShardTaskTimeThreshold {
		return nil
	}

	req := &h.SyncShardStatusRequest{
		SourceCluster: attr.SourceCluster,
		ShardId:       attr.ShardId,
		Timestamp:     attr.Timestamp,
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return p.historyEngine.SyncShardStatus(ctx, req)
}

func (p *ReplicationTaskProcessor) filterTask(
	domainID string,
) (bool, error) {

	domainEntry, err := p.domainCache.GetDomainByID(domainID)
	if err != nil {
		return false, err
	}

	shouldProcessTask := false
FilterLoop:
	for _, targetCluster := range domainEntry.GetReplicationConfig().Clusters {
		if p.currentCluster == targetCluster.ClusterName {
			shouldProcessTask = true
			break FilterLoop
		}
	}
	return shouldProcessTask, nil
}
