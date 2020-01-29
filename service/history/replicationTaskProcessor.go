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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination replicationTaskProcessor_mock.go -self_package github.com/uber/cadence/service/history

package history

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.uber.org/yarpc/yarpcerrors"

	h "github.com/temporalio/temporal/.gen/go/history"
	"github.com/temporalio/temporal/.gen/go/shared"
	hc "github.com/temporalio/temporal/client/history"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/xdc"
	"github.com/temporalio/temporal/service/frontend/adapter"
)

const (
	dropSyncShardTaskTimeThreshold   = 10 * time.Minute
	replicationTimeout               = 30 * time.Second
	taskErrorRetryBackoffCoefficient = 1.2
	dlqErrorRetryWait                = time.Second
	emptyMessageID                   = -1
)

var (
	// ErrUnknownReplicationTask is the error to indicate unknown replication task type
	ErrUnknownReplicationTask = &shared.BadRequestError{Message: "unknown replication task"}
)

type (
	// ReplicationTaskProcessorImpl is responsible for processing replication tasks for a shard.
	ReplicationTaskProcessorImpl struct {
		currentCluster      string
		sourceCluster       string
		status              int32
		shard               ShardContext
		historyEngine       Engine
		historySerializer   persistence.PayloadSerializer
		config              *Config
		domainCache         cache.DomainCache
		metricsClient       metrics.Client
		logger              log.Logger
		nDCHistoryResender  xdc.NDCHistoryResender
		historyRereplicator xdc.HistoryRereplicator

		taskRetryPolicy backoff.RetryPolicy
		dlqRetryPolicy  backoff.RetryPolicy
		noTaskRetrier   backoff.Retrier

		lastProcessedMessageID int64
		lastRetrievedMessageID int64

		requestChan   chan<- *request
		syncShardChan chan *commonproto.SyncShardStatus
		done          chan struct{}
	}

	// ReplicationTaskProcessor is responsible for processing replication tasks for a shard.
	ReplicationTaskProcessor interface {
		common.Daemon
	}

	request struct {
		token    *commonproto.ReplicationToken
		respChan chan<- *commonproto.ReplicationMessages
	}
)

// NewReplicationTaskProcessor creates a new replication task processor.
func NewReplicationTaskProcessor(
	shard ShardContext,
	historyEngine Engine,
	config *Config,
	historyClient hc.Client,
	metricsClient metrics.Client,
	replicationTaskFetcher ReplicationTaskFetcher,
) *ReplicationTaskProcessorImpl {
	taskRetryPolicy := backoff.NewExponentialRetryPolicy(config.ReplicationTaskProcessorErrorRetryWait())
	taskRetryPolicy.SetBackoffCoefficient(taskErrorRetryBackoffCoefficient)
	taskRetryPolicy.SetMaximumAttempts(config.ReplicationTaskProcessorErrorRetryMaxAttempts())

	dlqRetryPolicy := backoff.NewExponentialRetryPolicy(dlqErrorRetryWait)
	dlqRetryPolicy.SetExpirationInterval(backoff.NoInterval)

	noTaskBackoffPolicy := backoff.NewExponentialRetryPolicy(config.ReplicationTaskProcessorNoTaskRetryWait())
	noTaskBackoffPolicy.SetBackoffCoefficient(1)
	noTaskBackoffPolicy.SetExpirationInterval(backoff.NoInterval)
	noTaskRetrier := backoff.NewRetrier(noTaskBackoffPolicy, backoff.SystemClock)

	nDCHistoryResender := xdc.NewNDCHistoryResender(
		shard.GetDomainCache(),
		shard.GetService().GetClientBean().GetRemoteAdminClient(replicationTaskFetcher.GetSourceCluster()),
		func(ctx context.Context, request *h.ReplicateEventsV2Request) error {
			return historyClient.ReplicateEventsV2(ctx, request)
		},
		shard.GetService().GetPayloadSerializer(),
		shard.GetLogger(),
	)
	historyRereplicator := xdc.NewHistoryRereplicator(
		replicationTaskFetcher.GetSourceCluster(),
		shard.GetDomainCache(),
		shard.GetService().GetClientBean().GetRemoteAdminClient(replicationTaskFetcher.GetSourceCluster()),
		func(ctx context.Context, request *h.ReplicateRawEventsRequest) error {
			return historyClient.ReplicateRawEvents(ctx, request)
		},
		shard.GetService().GetPayloadSerializer(),
		replicationTimeout,
		shard.GetLogger(),
	)
	return &ReplicationTaskProcessorImpl{
		currentCluster:         shard.GetClusterMetadata().GetCurrentClusterName(),
		sourceCluster:          replicationTaskFetcher.GetSourceCluster(),
		status:                 common.DaemonStatusInitialized,
		shard:                  shard,
		historyEngine:          historyEngine,
		historySerializer:      persistence.NewPayloadSerializer(),
		config:                 config,
		domainCache:            shard.GetDomainCache(),
		metricsClient:          metricsClient,
		logger:                 shard.GetLogger(),
		nDCHistoryResender:     nDCHistoryResender,
		historyRereplicator:    historyRereplicator,
		taskRetryPolicy:        taskRetryPolicy,
		noTaskRetrier:          noTaskRetrier,
		requestChan:            replicationTaskFetcher.GetRequestChan(),
		syncShardChan:          make(chan *commonproto.SyncShardStatus),
		done:                   make(chan struct{}),
		lastProcessedMessageID: emptyMessageID,
		lastRetrievedMessageID: emptyMessageID,
	}
}

// Start starts the processor
func (p *ReplicationTaskProcessorImpl) Start() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	go p.processorLoop()
	go p.syncShardStatusLoop()
	go p.cleanupReplicationTaskLoop()
	p.logger.Info("ReplicationTaskProcessor started.")
}

// Stop stops the processor
func (p *ReplicationTaskProcessorImpl) Stop() {
	if !atomic.CompareAndSwapInt32(&p.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	p.logger.Info("ReplicationTaskProcessor shutting down.")
	close(p.done)
}

func (p *ReplicationTaskProcessorImpl) processorLoop() {
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
				tag.ReadLevel(response.GetLastRetrievedMessageId()),
				tag.Bool(response.GetHasMore()),
				tag.Counter(len(response.GetReplicationTasks())),
			)

			p.processResponse(response)
		case <-p.done:
			return
		}
	}
}

func (p *ReplicationTaskProcessorImpl) cleanupReplicationTaskLoop() {

	timer := time.NewTimer(backoff.JitDuration(
		p.config.ShardSyncMinInterval(),
		p.config.ShardSyncTimerJitterCoefficient(),
	))
	for {
		select {
		case <-p.done:
			timer.Stop()
			return
		case <-timer.C:
			err := p.cleanupAckedReplicationTasks()
			if err != nil {
				p.logger.Error("Failed to clean up replication messages.", tag.Error(err))
				p.metricsClient.Scope(metrics.ReplicationTaskCleanupScope).IncCounter(metrics.ReplicationTaskCleanupFailure)
			}
			timer.Reset(backoff.JitDuration(
				p.config.ShardSyncMinInterval(),
				p.config.ShardSyncTimerJitterCoefficient(),
			))
		}
	}
}

func (p *ReplicationTaskProcessorImpl) cleanupAckedReplicationTasks() error {

	clusterMetadata := p.shard.GetClusterMetadata()
	currentCluster := clusterMetadata.GetCurrentClusterName()
	minAckLevel := int64(math.MaxInt64)
	for clusterName, clusterInfo := range clusterMetadata.GetAllClusterInfo() {
		if !clusterInfo.Enabled {
			continue
		}

		if clusterName != currentCluster {
			ackLevel := p.shard.GetClusterReplicationLevel(clusterName)
			if ackLevel < minAckLevel {
				minAckLevel = ackLevel
			}
		}
	}

	p.logger.Info("Cleaning up replication task queue.", tag.ReadLevel(minAckLevel))
	p.metricsClient.Scope(metrics.ReplicationTaskCleanupScope).IncCounter(metrics.ReplicationTaskCleanupCount)
	return p.shard.GetExecutionManager().RangeCompleteReplicationTask(
		&persistence.RangeCompleteReplicationTaskRequest{
			InclusiveEndTaskID: minAckLevel,
		},
	)
}

func (p *ReplicationTaskProcessorImpl) sendFetchMessageRequest() <-chan *commonproto.ReplicationMessages {
	respChan := make(chan *commonproto.ReplicationMessages, 1)
	// TODO: when we support prefetching, LastRetrievedMessageId can be different than LastProcessedMessageId
	p.requestChan <- &request{
		token: &commonproto.ReplicationToken{
			ShardID:                int32(p.shard.GetShardID()),
			LastRetrievedMessageId: p.lastRetrievedMessageID,
			LastProcessedMessageId: p.lastProcessedMessageID,
		},
		respChan: respChan,
	}
	return respChan
}

func (p *ReplicationTaskProcessorImpl) processResponse(response *commonproto.ReplicationMessages) {

	p.syncShardChan <- response.GetSyncShardStatus()
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

	p.lastProcessedMessageID = response.GetLastRetrievedMessageId()
	p.lastRetrievedMessageID = response.GetLastRetrievedMessageId()
	scope := p.metricsClient.Scope(metrics.ReplicationTaskFetcherScope, metrics.TargetClusterTag(p.sourceCluster))
	scope.UpdateGauge(metrics.LastRetrievedMessageID, float64(p.lastRetrievedMessageID))
	p.noTaskRetrier.Reset()
}

func (p *ReplicationTaskProcessorImpl) syncShardStatusLoop() {

	timer := time.NewTimer(backoff.JitDuration(
		p.config.ShardSyncMinInterval(),
		p.config.ShardSyncTimerJitterCoefficient(),
	))
	var syncShardTask *commonproto.SyncShardStatus
	for {
		select {
		case syncShardRequest := <-p.syncShardChan:
			syncShardTask = syncShardRequest
		case <-timer.C:
			if err := p.handleSyncShardStatus(
				syncShardTask,
			); err != nil {
				p.logger.Error("failed to sync shard status", tag.Error(err))
				p.metricsClient.Scope(metrics.HistorySyncShardStatusScope).IncCounter(metrics.SyncShardFromRemoteFailure)
			}
			timer.Reset(backoff.JitDuration(
				p.config.ShardSyncMinInterval(),
				p.config.ShardSyncTimerJitterCoefficient(),
			))
		case <-p.done:
			timer.Stop()
			return
		}
	}
}

func (p *ReplicationTaskProcessorImpl) handleSyncShardStatus(
	status *commonproto.SyncShardStatus,
) error {

	if status == nil ||
		p.shard.GetTimeSource().Now().Sub(
			time.Unix(0, status.GetTimestamp())) > dropSyncShardTaskTimeThreshold {
		return nil
	}
	p.metricsClient.Scope(metrics.HistorySyncShardStatusScope).IncCounter(metrics.SyncShardFromRemoteCounter)
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return p.historyEngine.SyncShardStatus(ctx, &h.SyncShardStatusRequest{
		SourceCluster: common.StringPtr(p.sourceCluster),
		ShardId:       common.Int64Ptr(int64(p.shard.GetShardID())),
		Timestamp:     &status.Timestamp,
	})
}

func (p *ReplicationTaskProcessorImpl) processSingleTask(replicationTask *commonproto.ReplicationTask) error {
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

func (p *ReplicationTaskProcessorImpl) processTaskOnce(replicationTask *commonproto.ReplicationTask) error {
	var err error
	var scope int
	switch replicationTask.GetTaskType() {
	case enums.ReplicationTaskTypeDomain:
		// Domain replication task should be handled in worker (domainReplicationMessageProcessor)
		panic("task type not supported")
	case enums.ReplicationTaskTypeSyncShardStatus:
		// Shard status will be sent as part of the Replication message without kafka
	case enums.ReplicationTaskTypeSyncActivity:
		scope = metrics.SyncActivityTaskScope
		err = p.handleActivityTask(replicationTask)
	case enums.ReplicationTaskTypeHistory:
		scope = metrics.HistoryReplicationTaskScope
		err = p.handleHistoryReplicationTask(replicationTask)
	case enums.ReplicationTaskTypeHistoryMetadata:
		// Without kafka we should not have size limits so we don't necessary need this in the new replication scheme.
	case enums.ReplicationTaskTypeHistoryV2:
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

func (p *ReplicationTaskProcessorImpl) putReplicationTaskToDLQ(replicationTask *commonproto.ReplicationTask) error {
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
			p.metricsClient.IncCounter(metrics.ReplicationTaskFetcherScope, metrics.ReplicationDLQFailed)
		}
		return err
	}, p.dlqRetryPolicy, p.shouldRetryDLQ)
}

func (p *ReplicationTaskProcessorImpl) generateDLQRequest(
	replicationTask *commonproto.ReplicationTask,
) (*persistence.PutReplicationTaskToDLQRequest, error) {
	switch replicationTask.TaskType {
	case enums.ReplicationTaskTypeSyncActivity:
		taskAttributes := replicationTask.GetSyncActivityTaskAttributes()
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

	case enums.ReplicationTaskTypeHistory:
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
	case enums.ReplicationTaskTypeHistoryV2:
		taskAttributes := replicationTask.GetHistoryTaskV2Attributes()

		eventsDataBlob := persistence.NewDataBlobFromProto(taskAttributes.GetEvents())
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

func (p *ReplicationTaskProcessorImpl) shouldRetryDLQ(err error) bool {
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
	info map[string]*commonproto.ReplicationInfo,
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

func (p *ReplicationTaskProcessorImpl) updateFailureMetric(scope int, err error) {
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

func (p *ReplicationTaskProcessorImpl) handleActivityTask(
	task *commonproto.ReplicationTask,
) error {

	attr := task.GetSyncActivityTaskAttributes()
	doContinue, err := p.filterTask(attr.GetDomainId())
	if err != nil || !doContinue {
		return err
	}

	request := &h.SyncActivityRequest{
		DomainId:           &attr.DomainId,
		WorkflowId:         &attr.WorkflowId,
		RunId:              &attr.RunId,
		Version:            &attr.Version,
		ScheduledId:        &attr.ScheduledId,
		ScheduledTime:      &attr.ScheduledTime,
		StartedId:          &attr.StartedId,
		StartedTime:        &attr.StartedTime,
		LastHeartbeatTime:  &attr.LastHeartbeatTime,
		Details:            attr.Details,
		Attempt:            &attr.Attempt,
		LastFailureReason:  &attr.LastFailureReason,
		LastWorkerIdentity: &attr.LastWorkerIdentity,
		VersionHistory:     adapter.ToThriftVersionHistory(attr.GetVersionHistory()),
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	err = p.historyEngine.SyncActivity(ctx, request)
	// Handle resend error
	retryV2Err, okV2 := p.convertRetryTaskV2Error(err)
	//TODO: remove handling retry error v1 after 2DC deprecation
	retryV1Err, okV1 := p.convertRetryTaskError(err)

	if !okV1 && !okV2 {
		return err
	} else if okV1 {
		if retryV1Err.GetRunId() == "" {
			return err
		}
		p.metricsClient.IncCounter(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientRequests)
		stopwatch := p.metricsClient.StartTimer(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientLatency)
		defer stopwatch.Stop()

		// this is the retry error
		if resendErr := p.historyRereplicator.SendMultiWorkflowHistory(
			attr.GetDomainId(),
			attr.GetWorkflowId(),
			retryV1Err.GetRunId(),
			retryV1Err.GetNextEventId(),
			attr.GetRunId(),
			attr.GetScheduledId()+1, // the next event ID should be at activity schedule ID + 1
		); resendErr != nil {
			p.logger.Error("error resend history for sync activity", tag.Error(resendErr))
			// should return the replication error, not the resending error
			return err
		}
	} else if okV2 {
		p.metricsClient.IncCounter(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientRequests)
		stopwatch := p.metricsClient.StartTimer(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientLatency)
		defer stopwatch.Stop()

		if resendErr := p.nDCHistoryResender.SendSingleWorkflowHistory(
			retryV2Err.GetDomainId(),
			retryV2Err.GetWorkflowId(),
			retryV2Err.GetRunId(),
			retryV2Err.GetStartEventId(),
			retryV2Err.GetStartEventVersion(),
			retryV2Err.GetEndEventId(),
			retryV2Err.GetEndEventVersion(),
		); resendErr != nil {
			p.logger.Error("error resend history for sync activity", tag.Error(resendErr))
			// should return the replication error, not the resending error
			return err
		}
	}
	// should try again after back fill the history
	return p.historyEngine.SyncActivity(ctx, request)
}

//TODO: remove this part after 2DC deprecation
func (p *ReplicationTaskProcessorImpl) handleHistoryReplicationTask(
	task *commonproto.ReplicationTask,
) error {

	attr := task.GetHistoryTaskAttributes()
	doContinue, err := p.filterTask(attr.GetDomainId())
	if err != nil || !doContinue {
		return err
	}

	request := &h.ReplicateEventsRequest{
		SourceCluster: common.StringPtr(p.sourceCluster),
		DomainUUID:    &attr.DomainId,
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: &attr.WorkflowId,
			RunId:      &attr.RunId,
		},
		FirstEventId:      &attr.FirstEventId,
		NextEventId:       &attr.NextEventId,
		Version:           &attr.Version,
		ReplicationInfo:   adapter.ToThriftReplicationInfos(attr.ReplicationInfo),
		History:           adapter.ToThriftHistory(attr.History),
		NewRunHistory:     adapter.ToThriftHistory(attr.NewRunHistory),
		ForceBufferEvents: common.BoolPtr(false),
		ResetWorkflow:     &attr.ResetWorkflow,
		NewRunNDC:         &attr.NewRunNDC,
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()

	err = p.historyEngine.ReplicateEvents(ctx, request)
	retryErr, ok := p.convertRetryTaskError(err)
	if !ok || retryErr.GetRunId() == "" {
		return err
	}

	p.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.CadenceClientRequests)
	stopwatch := p.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.CadenceClientLatency)
	defer stopwatch.Stop()

	resendErr := p.historyRereplicator.SendMultiWorkflowHistory(
		attr.GetDomainId(),
		attr.GetWorkflowId(),
		retryErr.GetRunId(),
		retryErr.GetNextEventId(),
		attr.GetRunId(),
		attr.GetFirstEventId(),
	)
	if resendErr != nil {
		p.logger.Error("error resend history for history event", tag.Error(resendErr))
		// should return the replication error, not the resending error
		return err
	}

	return p.historyEngine.ReplicateEvents(ctx, request)
}

func (p *ReplicationTaskProcessorImpl) handleHistoryReplicationTaskV2(
	task *commonproto.ReplicationTask,
) error {

	attr := task.GetHistoryTaskV2Attributes()
	doContinue, err := p.filterTask(attr.GetDomainId())
	if err != nil || !doContinue {
		return err
	}

	request := &h.ReplicateEventsV2Request{
		DomainUUID: &attr.DomainId,
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: &attr.WorkflowId,
			RunId:      &attr.RunId,
		},
		VersionHistoryItems: adapter.ToThriftVersionHistoryItems(attr.VersionHistoryItems),
		Events:              adapter.ToThriftDataBlob(attr.Events),
		// new run events does not need version history since there is no prior events
		NewRunEvents: adapter.ToThriftDataBlob(attr.NewRunEvents),
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()

	err = p.historyEngine.ReplicateEventsV2(ctx, request)
	retryErr, ok := p.convertRetryTaskV2Error(err)
	if !ok {
		return err
	}
	p.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.CadenceClientRequests)
	stopwatch := p.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.CadenceClientLatency)
	defer stopwatch.Stop()

	if resendErr := p.nDCHistoryResender.SendSingleWorkflowHistory(
		retryErr.GetDomainId(),
		retryErr.GetWorkflowId(),
		retryErr.GetRunId(),
		retryErr.GetStartEventId(),
		retryErr.GetStartEventVersion(),
		retryErr.GetEndEventId(),
		retryErr.GetEndEventVersion(),
	); resendErr != nil {
		p.logger.Error("error resend history for history event v2", tag.Error(resendErr))
		// should return the replication error, not the resending error
		return err
	}

	return p.historyEngine.ReplicateEventsV2(ctx, request)
}

func (p *ReplicationTaskProcessorImpl) filterTask(
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

//TODO: remove this code after 2DC deprecation
func (p *ReplicationTaskProcessorImpl) convertRetryTaskError(
	err error,
) (*shared.RetryTaskError, bool) {

	retError, ok := err.(*shared.RetryTaskError)
	return retError, ok
}

func (p *ReplicationTaskProcessorImpl) convertRetryTaskV2Error(
	err error,
) (*shared.RetryTaskV2Error, bool) {

	retError, ok := err.(*shared.RetryTaskV2Error)
	return retError, ok
}
