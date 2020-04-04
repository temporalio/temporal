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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination replicationTaskExecutor_mock.go -self_package github.com/temporalio/temporal/service/history

package history

import (
	"context"

	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/xdc"
)

type (
	replicationTaskExecutor interface {
		execute(sourceCluster string, replicationTask *replicationgenpb.ReplicationTask, forceApply bool) (int, error)
	}

	replicationTaskExecutorImpl struct {
		currentCluster      string
		namespaceCache      cache.NamespaceCache
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
	namespaceCache cache.NamespaceCache,
	nDCHistoryResender xdc.NDCHistoryResender,
	historyRereplicator xdc.HistoryRereplicator,
	historyEngine Engine,
	metricsClient metrics.Client,
	logger log.Logger,
) replicationTaskExecutor {
	return &replicationTaskExecutorImpl{
		currentCluster:      currentCluster,
		namespaceCache:      namespaceCache,
		nDCHistoryResender:  nDCHistoryResender,
		historyRereplicator: historyRereplicator,
		historyEngine:       historyEngine,
		metricsClient:       metricsClient,
		logger:              logger,
	}
}

func (e *replicationTaskExecutorImpl) execute(
	sourceCluster string,
	replicationTask *replicationgenpb.ReplicationTask,
	forceApply bool,
) (int, error) {

	var err error
	var scope int
	switch replicationTask.GetTaskType() {
	case replicationgenpb.ReplicationTaskType_SyncShardStatusTask:
		// Shard status will be sent as part of the Replication message without kafka
		scope = metrics.SyncShardTaskScope
	case replicationgenpb.ReplicationTaskType_SyncActivityTask:
		scope = metrics.SyncActivityTaskScope
		err = e.handleActivityTask(replicationTask, forceApply)
	case replicationgenpb.ReplicationTaskType_HistoryTask:
		scope = metrics.HistoryReplicationTaskScope
		err = e.handleHistoryReplicationTask(sourceCluster, replicationTask, forceApply)
	case replicationgenpb.ReplicationTaskType_HistoryMetadataTask:
		// Without kafka we should not have size limits so we don't necessary need this in the new replication scheme.
		scope = metrics.HistoryMetadataReplicationTaskScope
	case replicationgenpb.ReplicationTaskType_HistoryV2Task:
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
	task *replicationgenpb.ReplicationTask,
	forceApply bool,
) error {

	attr := task.GetSyncActivityTaskAttributes()
	doContinue, err := e.filterTask(attr.GetNamespaceId(), forceApply)
	if err != nil || !doContinue {
		return err
	}

	request := &historyservice.SyncActivityRequest{
		NamespaceId:        attr.NamespaceId,
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
		if retryV1Err.RunId == "" {
			return err
		}
		e.metricsClient.IncCounter(metrics.HistoryRereplicationByActivityReplicationScope, metrics.ClientRequests)
		stopwatch := e.metricsClient.StartTimer(metrics.HistoryRereplicationByActivityReplicationScope, metrics.ClientLatency)
		defer stopwatch.Stop()

		// this is the retry error
		if resendErr := e.historyRereplicator.SendMultiWorkflowHistory(
			attr.GetNamespaceId(),
			attr.GetWorkflowId(),
			retryV1Err.RunId,
			retryV1Err.NextEventId,
			attr.GetRunId(),
			attr.GetScheduledId()+1, // the next event ID should be at activity schedule ID + 1
		); resendErr != nil {
			e.logger.Error("error resend history for sync activity", tag.Error(resendErr))
			// should return the replication error, not the resending error
			return err
		}
	} else if okV2 {
		e.metricsClient.IncCounter(metrics.HistoryRereplicationByActivityReplicationScope, metrics.ClientRequests)
		stopwatch := e.metricsClient.StartTimer(metrics.HistoryRereplicationByActivityReplicationScope, metrics.ClientLatency)
		defer stopwatch.Stop()

		if resendErr := e.nDCHistoryResender.SendSingleWorkflowHistory(
			retryV2Err.NamespaceId,
			retryV2Err.WorkflowId,
			retryV2Err.RunId,
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
	task *replicationgenpb.ReplicationTask,
	forceApply bool,
) error {

	attr := task.GetHistoryTaskAttributes()
	doContinue, err := e.filterTask(attr.GetNamespaceId(), forceApply)
	if err != nil || !doContinue {
		return err
	}

	request := &historyservice.ReplicateEventsRequest{
		SourceCluster: sourceCluster,
		NamespaceId:   attr.NamespaceId,
		WorkflowExecution: &executionpb.WorkflowExecution{
			WorkflowId: attr.WorkflowId,
			RunId:      attr.RunId,
		},
		FirstEventId:      attr.FirstEventId,
		NextEventId:       attr.NextEventId,
		Version:           attr.Version,
		ReplicationInfo:   attr.ReplicationInfo,
		History:           attr.History,
		NewRunHistory:     attr.NewRunHistory,
		ForceBufferEvents: false,
		ResetWorkflow:     attr.ResetWorkflow,
		NewRunNDC:         attr.NewRunNDC,
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()

	err = e.historyEngine.ReplicateEvents(ctx, request)
	retryErr, ok := e.convertRetryTaskError(err)
	if !ok || retryErr.RunId == "" {
		return err
	}

	e.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.ClientRequests)
	stopwatch := e.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.ClientLatency)
	defer stopwatch.Stop()

	resendErr := e.historyRereplicator.SendMultiWorkflowHistory(
		attr.GetNamespaceId(),
		attr.GetWorkflowId(),
		retryErr.RunId,
		retryErr.NextEventId,
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
	task *replicationgenpb.ReplicationTask,
	forceApply bool,
) error {

	attr := task.GetHistoryTaskV2Attributes()
	doContinue, err := e.filterTask(attr.GetNamespaceId(), forceApply)
	if err != nil || !doContinue {
		return err
	}

	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: attr.NamespaceId,
		WorkflowExecution: &executionpb.WorkflowExecution{
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
	e.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.ClientRequests)
	stopwatch := e.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.ClientLatency)
	defer stopwatch.Stop()

	if resendErr := e.nDCHistoryResender.SendSingleWorkflowHistory(
		retryErr.NamespaceId,
		retryErr.WorkflowId,
		retryErr.RunId,
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
	namespaceID string,
	forceApply bool,
) (bool, error) {

	if forceApply {
		return true, nil
	}

	namespaceEntry, err := e.namespaceCache.GetNamespaceByID(namespaceID)
	if err != nil {
		return false, err
	}

	shouldProcessTask := false
FilterLoop:
	for _, targetCluster := range namespaceEntry.GetReplicationConfig().Clusters {
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
) (*serviceerror.RetryTask, bool) {

	retError, ok := err.(*serviceerror.RetryTask)
	return retError, ok
}

func (e *replicationTaskExecutorImpl) convertRetryTaskV2Error(
	err error,
) (*serviceerror.RetryTaskV2, bool) {

	retError, ok := err.(*serviceerror.RetryTaskV2)
	return retError, ok
}
