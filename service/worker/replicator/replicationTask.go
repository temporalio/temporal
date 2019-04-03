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

package replicator

import (
	"context"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/uber-common/bark"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/locks"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/xdc"
)

type (
	workflowReplicationTask struct {
		partitionID definition.WorkflowIdentifier
		taskID      int64
		attempt     int
		kafkaMsg    messaging.Message
		logger      bark.Logger

		config              *Config
		historyClient       history.Client
		metricsClient       metrics.Client
		historyRereplicator xdc.HistoryRereplicator
		resendLock          locks.IDMutex
	}

	activityReplicationTask struct {
		workflowReplicationTask
		req *h.SyncActivityRequest
	}

	historyReplicationTask struct {
		workflowReplicationTask
		req *h.ReplicateEventsRequest
	}
)

const (
	replicationTaskRetryDelay = 500 * time.Microsecond
)

func newActivityReplicationTask(task *replicator.ReplicationTask, msg messaging.Message, logger bark.Logger,
	config *Config, historyClient history.Client, metricsClient metrics.Client,
	historyRereplicator xdc.HistoryRereplicator) *activityReplicationTask {

	attr := task.SyncActicvityTaskAttributes
	return &activityReplicationTask{
		workflowReplicationTask: workflowReplicationTask{
			partitionID: definition.NewWorkflowIdentifier(
				attr.GetDomainId(), attr.GetWorkflowId(), attr.GetRunId(),
			),
			taskID:   attr.GetScheduledId(),
			attempt:  0,
			kafkaMsg: msg,
			logger: logger.WithFields(bark.Fields{
				logging.TagDomainID:            attr.GetDomainId(),
				logging.TagWorkflowExecutionID: attr.GetWorkflowId(),
				logging.TagWorkflowRunID:       attr.GetRunId(),
				logging.TagEventID:             attr.GetScheduledId(),
				logging.TagVersion:             attr.GetVersion(),
			}),
			config:              config,
			historyClient:       historyClient,
			metricsClient:       metricsClient,
			historyRereplicator: historyRereplicator,
		},
		req: &h.SyncActivityRequest{
			DomainId:          attr.DomainId,
			WorkflowId:        attr.WorkflowId,
			RunId:             attr.RunId,
			Version:           attr.Version,
			ScheduledId:       attr.ScheduledId,
			ScheduledTime:     attr.ScheduledTime,
			StartedId:         attr.StartedId,
			StartedTime:       attr.StartedTime,
			LastHeartbeatTime: attr.LastHeartbeatTime,
			Details:           attr.Details,
			Attempt:           attr.Attempt,
		},
	}
}

func newHistoryReplicationTask(task *replicator.ReplicationTask, msg messaging.Message, sourceCluster string, logger bark.Logger,
	config *Config, historyClient history.Client, metricsClient metrics.Client,
	historyRereplicator xdc.HistoryRereplicator) *historyReplicationTask {

	attr := task.HistoryTaskAttributes
	return &historyReplicationTask{
		workflowReplicationTask: workflowReplicationTask{
			partitionID: definition.NewWorkflowIdentifier(
				attr.GetDomainId(), attr.GetWorkflowId(), attr.GetRunId(),
			),
			taskID:   attr.GetFirstEventId(),
			attempt:  0,
			kafkaMsg: msg,
			logger: logger.WithFields(bark.Fields{
				logging.TagDomainID:            attr.GetDomainId(),
				logging.TagWorkflowExecutionID: attr.GetWorkflowId(),
				logging.TagWorkflowRunID:       attr.GetRunId(),
				logging.TagFirstEventID:        attr.GetFirstEventId(),
				logging.TagNextEventID:         attr.GetNextEventId(),
				logging.TagVersion:             attr.GetVersion(),
			}),
			config:              config,
			historyClient:       historyClient,
			metricsClient:       metricsClient,
			historyRereplicator: historyRereplicator,
		},
		req: &h.ReplicateEventsRequest{
			SourceCluster: common.StringPtr(sourceCluster),
			DomainUUID:    attr.DomainId,
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: attr.WorkflowId,
				RunId:      attr.RunId,
			},
			FirstEventId:            attr.FirstEventId,
			NextEventId:             attr.NextEventId,
			Version:                 attr.Version,
			ReplicationInfo:         attr.ReplicationInfo,
			History:                 attr.History,
			NewRunHistory:           attr.NewRunHistory,
			ForceBufferEvents:       common.BoolPtr(false),
			EventStoreVersion:       attr.EventStoreVersion,
			NewRunEventStoreVersion: attr.NewRunEventStoreVersion,
			ResetWorkflow:           attr.ResetWorkflow,
		},
	}
}

func (t *activityReplicationTask) Execute() error {
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return t.historyClient.SyncActivity(ctx, t.req)
}

func (t *activityReplicationTask) HandleErr(err error) error {
	retryErr, ok := t.convertRetryTaskError(err)
	if !ok || retryErr.GetRunId() == "" {
		return err
	}

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientLatency)
	defer stopwatch.Stop()

	// this is the retry error
	beginRunID := retryErr.GetRunId()
	beginEventID := retryErr.GetNextEventId()
	endRunID := t.partitionID.RunID
	endEventID := t.taskID + 1 // the next event ID should be at activity schedule ID + 1
	resendErr := t.historyRereplicator.SendMultiWorkflowHistory(
		t.partitionID.DomainID, t.partitionID.WorkflowID,
		beginRunID, beginEventID, endRunID, endEventID,
	)

	if resendErr != nil {
		t.logger.WithField(logging.TagErr, resendErr).Error("error resend history")
		// should return the replication error, not the resending error
		return err
	}
	// should try again
	return t.Execute()
}

func (t *activityReplicationTask) RetryErr(err error) bool {
	t.attempt++

	if t.attempt <= t.config.ReplicationTaskMaxRetry() && isTransientRetryableError(err) {
		time.Sleep(replicationTaskRetryDelay)
		return true
	}
	return false
}

func (t *historyReplicationTask) Execute() error {
	ctx, cancel := context.WithTimeout(context.Background(), replicationTimeout)
	defer cancel()
	return t.historyClient.ReplicateEvents(ctx, t.req)
}

func (t *historyReplicationTask) HandleErr(err error) error {
	retryErr, ok := t.convertRetryTaskError(err)
	if !ok || retryErr.GetRunId() == "" {
		return err
	}

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.CadenceClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.CadenceClientLatency)
	defer stopwatch.Stop()

	// this is the retry error
	beginRunID := retryErr.GetRunId()
	beginEventID := retryErr.GetNextEventId()
	endRunID := t.partitionID.RunID
	endEventID := t.taskID
	resendErr := t.historyRereplicator.SendMultiWorkflowHistory(
		t.partitionID.DomainID, t.partitionID.WorkflowID,
		beginRunID, beginEventID, endRunID, endEventID,
	)
	if resendErr != nil {
		t.logger.WithField(logging.TagErr, resendErr).Error("error resend history")
		// should return the replication error, not the resending error
		return err
	}
	// should try again
	return t.Execute()
}

func (t *historyReplicationTask) RetryErr(err error) bool {
	t.attempt++
	if t.attempt >= t.config.ReplicatorHistoryBufferRetryCount() {
		t.req.ForceBufferEvents = common.BoolPtr(true)
	}

	if t.attempt <= t.config.ReplicationTaskMaxRetry() && isTransientRetryableError(err) {
		time.Sleep(replicationTaskRetryDelay)
		return true
	}
	return false
}

func (t *workflowReplicationTask) PartitionID() interface{} {
	return t.partitionID
}

func (t *workflowReplicationTask) TaskID() int64 {
	return t.taskID
}

func (t *workflowReplicationTask) HashCode() uint32 {
	return farm.Fingerprint32([]byte(t.partitionID.WorkflowID))
}

func (t *workflowReplicationTask) Ack() {
	// the underlying implementation will not return anything other than nil
	// do logging just in case
	err := t.kafkaMsg.Ack()
	if err != nil {
		t.logger.Error("Unable to ack.")
	}
}

func (t *workflowReplicationTask) Nack() {
	// the underlying implementation will not return anything other than nil
	// do logging just in case
	err := t.kafkaMsg.Nack()
	if err != nil {
		t.logger.Error("Unable to nack.")
	}
}

func (t *workflowReplicationTask) convertRetryTaskError(err error) (*shared.RetryTaskError, bool) {
	retError, ok := err.(*shared.RetryTaskError)
	return retError, ok
}
