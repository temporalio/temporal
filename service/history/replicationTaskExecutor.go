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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination replicationTaskExecutor_mock.go

package history

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/shard"
)

type (
	replicationTaskExecutor interface {
		execute(replicationTask *replicationspb.ReplicationTask, forceApply bool) (int, error)
	}

	replicationTaskExecutorImpl struct {
		currentCluster     string
		shard              shard.Context
		namespaceRegistry  namespace.Registry
		nDCHistoryResender xdc.NDCHistoryResender
		historyEngine      shard.Engine

		metricsClient metrics.Client
		logger        log.Logger
	}
)

// newReplicationTaskExecutor creates an replication task executor
// The executor uses by 1) DLQ replication task handler 2) history replication task processor
func newReplicationTaskExecutor(
	shard shard.Context,
	namespaceRegistry namespace.Registry,
	nDCHistoryResender xdc.NDCHistoryResender,
	historyEngine shard.Engine,
	metricsClient metrics.Client,
	logger log.Logger,
) replicationTaskExecutor {
	return &replicationTaskExecutorImpl{
		currentCluster:     shard.GetClusterMetadata().GetCurrentClusterName(),
		shard:              shard,
		namespaceRegistry:  namespaceRegistry,
		nDCHistoryResender: nDCHistoryResender,
		historyEngine:      historyEngine,
		metricsClient:      metricsClient,
		logger:             logger,
	}
}

func (e *replicationTaskExecutorImpl) execute(
	replicationTask *replicationspb.ReplicationTask,
	forceApply bool,
) (int, error) {

	var err error
	var scope int
	switch replicationTask.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK:
		// Shard status will be sent as part of the Replication message without kafka
		scope = metrics.SyncShardTaskScope
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:
		scope = metrics.SyncActivityTaskScope
		err = e.handleActivityTask(replicationTask, forceApply)
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK:
		// Without kafka we should not have size limits so we don't necessary need this in the new replication scheme.
		scope = metrics.HistoryMetadataReplicationTaskScope
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:
		scope = metrics.HistoryReplicationTaskScope
		err = e.handleHistoryReplicationTask(replicationTask, forceApply)
	default:
		e.logger.Error("Unknown task type.")
		scope = metrics.ReplicatorScope
		err = ErrUnknownReplicationTask
	}

	return scope, err
}

func (e *replicationTaskExecutorImpl) handleActivityTask(
	task *replicationspb.ReplicationTask,
	forceApply bool,
) error {

	attr := task.GetSyncActivityTaskAttributes()
	doContinue, err := e.filterTask(namespace.ID(attr.GetNamespaceId()), forceApply)
	if err != nil || !doContinue {
		return err
	}

	replicationStopWatch := e.metricsClient.StartTimer(metrics.SyncActivityTaskScope, metrics.ServiceLatency)
	defer replicationStopWatch.Stop()

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
		LastFailure:        attr.LastFailure,
		LastWorkerIdentity: attr.LastWorkerIdentity,
		VersionHistory:     attr.GetVersionHistory(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()

	err = e.historyEngine.SyncActivity(ctx, request)
	switch retryErr := err.(type) {
	case nil:
		return nil

	case *serviceerrors.RetryReplication:
		e.metricsClient.IncCounter(metrics.HistoryRereplicationByActivityReplicationScope, metrics.ClientRequests)
		stopwatch := e.metricsClient.StartTimer(metrics.HistoryRereplicationByActivityReplicationScope, metrics.ClientLatency)
		defer stopwatch.Stop()

		if resendErr := e.nDCHistoryResender.SendSingleWorkflowHistory(
			namespace.ID(retryErr.NamespaceId),
			retryErr.WorkflowId,
			retryErr.RunId,
			retryErr.StartEventId,
			retryErr.StartEventVersion,
			retryErr.EndEventId,
			retryErr.EndEventVersion,
		); resendErr != nil {
			e.logger.Error("error resend history for sync activity", tag.Error(resendErr))
			// should return the replication error, not the resending error
			return err
		}
		return e.historyEngine.SyncActivity(ctx, request)

	default:
		return err
	}
}

func (e *replicationTaskExecutorImpl) handleHistoryReplicationTask(
	task *replicationspb.ReplicationTask,
	forceApply bool,
) error {

	attr := task.GetHistoryTaskV2Attributes()
	doContinue, err := e.filterTask(namespace.ID(attr.GetNamespaceId()), forceApply)
	if err != nil || !doContinue {
		return err
	}

	replicationStopWatch := e.metricsClient.StartTimer(metrics.HistoryReplicationTaskScope, metrics.ServiceLatency)
	defer replicationStopWatch.Stop()

	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: attr.NamespaceId,
		WorkflowExecution: &commonpb.WorkflowExecution{
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
	switch retryErr := err.(type) {
	case nil:
		return nil

	case *serviceerrors.RetryReplication:
		e.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.ClientRequests)
		resendStopWatch := e.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.ClientLatency)
		defer resendStopWatch.Stop()

		if resendErr := e.nDCHistoryResender.SendSingleWorkflowHistory(
			namespace.ID(retryErr.NamespaceId),
			retryErr.WorkflowId,
			retryErr.RunId,
			retryErr.StartEventId,
			retryErr.StartEventVersion,
			retryErr.EndEventId,
			retryErr.EndEventVersion,
		); resendErr != nil {
			e.logger.Error("error resend history for history event", tag.Error(resendErr))
			// should return the replication error, not the resending error
			return err
		}

		return e.historyEngine.ReplicateEventsV2(ctx, request)

	default:
		return err
	}
}

func (e *replicationTaskExecutorImpl) filterTask(
	namespaceID namespace.ID,
	forceApply bool,
) (bool, error) {

	if forceApply {
		return true, nil
	}

	namespaceEntry, err := e.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return false, err
	}

	shouldProcessTask := false
FilterLoop:
	for _, targetCluster := range namespaceEntry.ClusterNames() {
		if e.currentCluster == targetCluster {
			shouldProcessTask = true
			break FilterLoop
		}
	}
	return shouldProcessTask, nil
}
