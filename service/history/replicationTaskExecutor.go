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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination replicationTaskExecutor_mock.go -self_package github.com/uber/cadence/service/history

package history

import (
	"context"

	"github.com/uber/cadence/.gen/go/history"
	r "github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/xdc"
)

type (
	replicationTaskExecutor interface {
		execute(sourceCluster string, replicationTask *r.ReplicationTask, forceApply bool) (int, error)
	}

	replicationTaskExecutorImpl struct {
		currentCluster      string
		domainCache         cache.DomainCache
		nDCHistoryResender  xdc.NDCHistoryResender
		historyRereplicator xdc.HistoryRereplicator
		historyEngine       Engine

		metricsClient metrics.Client
		logger        log.Logger
	}
)

// newReplicationTaskExecutor creates an replication task executor
// The executor uses by 1) DLQ replication task handler 2) history replication task processor
func newReplicationTaskExecutor(
	currentCluster string,
	domainCache cache.DomainCache,
	nDCHistoryResender xdc.NDCHistoryResender,
	historyRereplicator xdc.HistoryRereplicator,
	historyEngine Engine,
	metricsClient metrics.Client,
	logger log.Logger,
) replicationTaskExecutor {
	return &replicationTaskExecutorImpl{
		currentCluster:      currentCluster,
		domainCache:         domainCache,
		nDCHistoryResender:  nDCHistoryResender,
		historyRereplicator: historyRereplicator,
		historyEngine:       historyEngine,
		metricsClient:       metricsClient,
		logger:              logger,
	}
}

func (e *replicationTaskExecutorImpl) execute(
	sourceCluster string,
	replicationTask *r.ReplicationTask,
	forceApply bool,
) (int, error) {

	var err error
	var scope int
	switch replicationTask.GetTaskType() {
	case r.ReplicationTaskTypeSyncShardStatus:
		// Shard status will be sent as part of the Replication message without kafka
		scope = metrics.SyncShardTaskScope
	case r.ReplicationTaskTypeSyncActivity:
		scope = metrics.SyncActivityTaskScope
		err = e.handleActivityTask(replicationTask, forceApply)
	case r.ReplicationTaskTypeHistory:
		scope = metrics.HistoryReplicationTaskScope
		err = e.handleHistoryReplicationTask(sourceCluster, replicationTask, forceApply)
	case r.ReplicationTaskTypeHistoryMetadata:
		// Without kafka we should not have size limits so we don't necessary need this in the new replication scheme.
		scope = metrics.HistoryMetadataReplicationTaskScope
	case r.ReplicationTaskTypeHistoryV2:
		scope = metrics.HistoryReplicationV2TaskScope
		err = e.handleHistoryReplicationTaskV2(replicationTask, forceApply)
	default:
		e.logger.Error("Unknown task type.")
		scope = metrics.ReplicatorScope
		err = ErrUnknownReplicationTask
	}

	return scope, err
}

func (e *replicationTaskExecutorImpl) handleActivityTask(
	task *r.ReplicationTask,
	forceApply bool,
) error {

	attr := task.SyncActivityTaskAttributes
	doContinue, err := e.filterTask(attr.GetDomainId(), forceApply)
	if err != nil || !doContinue {
		return err
	}

	request := &history.SyncActivityRequest{
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
		VersionHistory:     attr.GetVersionHistory(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	err = e.historyEngine.SyncActivity(ctx, request)
	// Handle resend error
	retryV2Err, okV2 := e.convertRetryTaskV2Error(err)
	//TODO: remove handling retry error v1 after 2DC deprecation
	retryV1Err, okV1 := e.convertRetryTaskError(err)

	if !okV1 && !okV2 {
		return err
	} else if okV1 {
		if retryV1Err.GetRunId() == "" {
			return err
		}
		e.metricsClient.IncCounter(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientRequests)
		stopwatch := e.metricsClient.StartTimer(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientLatency)
		defer stopwatch.Stop()

		// this is the retry error
		if resendErr := e.historyRereplicator.SendMultiWorkflowHistory(
			attr.GetDomainId(),
			attr.GetWorkflowId(),
			retryV1Err.GetRunId(),
			retryV1Err.GetNextEventId(),
			attr.GetRunId(),
			attr.GetScheduledId()+1, // the next event ID should be at activity schedule ID + 1
		); resendErr != nil {
			e.logger.Error("error resend history for sync activity", tag.Error(resendErr))
			// should return the replication error, not the resending error
			return err
		}
	} else if okV2 {
		e.metricsClient.IncCounter(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientRequests)
		stopwatch := e.metricsClient.StartTimer(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientLatency)
		defer stopwatch.Stop()

		if resendErr := e.nDCHistoryResender.SendSingleWorkflowHistory(
			retryV2Err.GetDomainId(),
			retryV2Err.GetWorkflowId(),
			retryV2Err.GetRunId(),
			retryV2Err.StartEventId,
			retryV2Err.StartEventVersion,
			retryV2Err.EndEventId,
			retryV2Err.EndEventVersion,
		); resendErr != nil {
			e.logger.Error("error resend history for sync activity", tag.Error(resendErr))
			// should return the replication error, not the resending error
			return err
		}
	}
	// should try again after back fill the history
	return e.historyEngine.SyncActivity(ctx, request)
}

//TODO: remove this part after 2DC deprecation
func (e *replicationTaskExecutorImpl) handleHistoryReplicationTask(
	sourceCluster string,
	task *r.ReplicationTask,
	forceApply bool,
) error {

	attr := task.HistoryTaskAttributes
	doContinue, err := e.filterTask(attr.GetDomainId(), forceApply)
	if err != nil || !doContinue {
		return err
	}

	request := &history.ReplicateEventsRequest{
		SourceCluster: common.StringPtr(sourceCluster),
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

	err = e.historyEngine.ReplicateEvents(ctx, request)
	retryErr, ok := e.convertRetryTaskError(err)
	if !ok || retryErr.GetRunId() == "" {
		return err
	}

	e.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.CadenceClientRequests)
	stopwatch := e.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.CadenceClientLatency)
	defer stopwatch.Stop()

	resendErr := e.historyRereplicator.SendMultiWorkflowHistory(
		attr.GetDomainId(),
		attr.GetWorkflowId(),
		retryErr.GetRunId(),
		retryErr.GetNextEventId(),
		attr.GetRunId(),
		attr.GetFirstEventId(),
	)
	if resendErr != nil {
		e.logger.Error("error resend history for history event", tag.Error(resendErr))
		// should return the replication error, not the resending error
		return err
	}

	return e.historyEngine.ReplicateEvents(ctx, request)
}

func (e *replicationTaskExecutorImpl) handleHistoryReplicationTaskV2(
	task *r.ReplicationTask,
	forceApply bool,
) error {

	attr := task.HistoryTaskV2Attributes
	doContinue, err := e.filterTask(attr.GetDomainId(), forceApply)
	if err != nil || !doContinue {
		return err
	}

	request := &history.ReplicateEventsV2Request{
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

	err = e.historyEngine.ReplicateEventsV2(ctx, request)
	retryErr, ok := e.convertRetryTaskV2Error(err)
	if !ok {
		return err
	}
	e.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.CadenceClientRequests)
	stopwatch := e.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.CadenceClientLatency)
	defer stopwatch.Stop()

	if resendErr := e.nDCHistoryResender.SendSingleWorkflowHistory(
		retryErr.GetDomainId(),
		retryErr.GetWorkflowId(),
		retryErr.GetRunId(),
		retryErr.StartEventId,
		retryErr.StartEventVersion,
		retryErr.EndEventId,
		retryErr.EndEventVersion,
	); resendErr != nil {
		e.logger.Error("error resend history for history event v2", tag.Error(resendErr))
		// should return the replication error, not the resending error
		return err
	}

	return e.historyEngine.ReplicateEventsV2(ctx, request)
}

func (e *replicationTaskExecutorImpl) filterTask(
	domainID string,
	forceApply bool,
) (bool, error) {

	if forceApply {
		return true, nil
	}

	domainEntry, err := e.domainCache.GetDomainByID(domainID)
	if err != nil {
		return false, err
	}

	shouldProcessTask := false
FilterLoop:
	for _, targetCluster := range domainEntry.GetReplicationConfig().Clusters {
		if e.currentCluster == targetCluster.ClusterName {
			shouldProcessTask = true
			break FilterLoop
		}
	}
	return shouldProcessTask, nil
}

//TODO: remove this code after 2DC deprecation
func (e *replicationTaskExecutorImpl) convertRetryTaskError(
	err error,
) (*shared.RetryTaskError, bool) {

	retError, ok := err.(*shared.RetryTaskError)
	return retError, ok
}

func (e *replicationTaskExecutorImpl) convertRetryTaskV2Error(
	err error,
) (*shared.RetryTaskV2Error, bool) {

	retError, ok := err.(*shared.RetryTaskV2Error)
	return retError, ok
}
