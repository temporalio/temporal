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

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/task"
	"github.com/uber/cadence/common/xdc"
)

type (
	workflowReplicationTask struct {
		metricsScope int
		startTime    time.Time
		queueID      definition.WorkflowIdentifier
		taskID       int64
		attempt      int
		kafkaMsg     messaging.Message
		logger       log.Logger
		state        task.State

		config        *Config
		timeSource    clock.TimeSource
		historyClient history.Client
		metricsClient metrics.Client
	}

	activityReplicationTask struct {
		workflowReplicationTask
		req                 *h.SyncActivityRequest
		historyRereplicator xdc.HistoryRereplicator
		nDCHistoryResender  xdc.NDCHistoryResender
	}

	historyReplicationTask struct {
		workflowReplicationTask
		req                 *h.ReplicateEventsRequest
		historyRereplicator xdc.HistoryRereplicator
	}

	historyMetadataReplicationTask struct {
		workflowReplicationTask
		sourceCluster       string
		firstEventID        int64
		nextEventID         int64
		historyRereplicator xdc.HistoryRereplicator
	}

	historyReplicationV2Task struct {
		workflowReplicationTask
		req                *h.ReplicateEventsV2Request
		nDCHistoryResender xdc.NDCHistoryResender
	}
)

var _ task.Task = (*activityReplicationTask)(nil)
var _ task.Task = (*historyReplicationTask)(nil)
var _ task.Task = (*historyMetadataReplicationTask)(nil)
var _ task.Task = (*historyReplicationV2Task)(nil)

const (
	replicationTaskRetryDelay = 500 * time.Microsecond
)

func newActivityReplicationTask(
	replicationTask *replicator.ReplicationTask,
	msg messaging.Message,
	logger log.Logger,
	config *Config,
	timeSource clock.TimeSource,
	historyClient history.Client,
	metricsClient metrics.Client,
	historyRereplicator xdc.HistoryRereplicator,
	nDCHistoryResender xdc.NDCHistoryResender,
) *activityReplicationTask {

	attr := replicationTask.SyncActivityTaskAttributes

	logger = logger.WithTags(tag.WorkflowDomainID(attr.GetDomainId()),
		tag.WorkflowID(attr.GetWorkflowId()),
		tag.WorkflowRunID(attr.GetRunId()),
		tag.WorkflowEventID(attr.GetScheduledId()),
		tag.FailoverVersion(attr.GetVersion()))
	return &activityReplicationTask{
		workflowReplicationTask: workflowReplicationTask{
			metricsScope: metrics.SyncActivityTaskScope,
			startTime:    timeSource.Now(),
			queueID: definition.NewWorkflowIdentifier(
				attr.GetDomainId(), attr.GetWorkflowId(), attr.GetRunId(),
			),
			taskID:        attr.GetScheduledId(),
			attempt:       0,
			kafkaMsg:      msg,
			logger:        logger,
			state:         task.TaskStatePending,
			config:        config,
			timeSource:    timeSource,
			historyClient: historyClient,
			metricsClient: metricsClient,
		},
		req: &h.SyncActivityRequest{
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
			LastFailureDetails: attr.LastFailureDetails,
			VersionHistory:     attr.VersionHistory,
		},
		historyRereplicator: historyRereplicator,
		nDCHistoryResender:  nDCHistoryResender,
	}
}

func newHistoryReplicationTask(
	replicationTask *replicator.ReplicationTask,
	msg messaging.Message,
	sourceCluster string,
	logger log.Logger,
	config *Config,
	timeSource clock.TimeSource,
	historyClient history.Client,
	metricsClient metrics.Client,
	historyRereplicator xdc.HistoryRereplicator,
) *historyReplicationTask {

	attr := replicationTask.HistoryTaskAttributes
	logger = logger.WithTags(tag.WorkflowDomainID(attr.GetDomainId()),
		tag.WorkflowID(attr.GetWorkflowId()),
		tag.WorkflowRunID(attr.GetRunId()),
		tag.WorkflowFirstEventID(attr.GetFirstEventId()),
		tag.WorkflowNextEventID(attr.GetNextEventId()),
		tag.FailoverVersion(attr.GetVersion()))
	return &historyReplicationTask{
		workflowReplicationTask: workflowReplicationTask{
			metricsScope: metrics.HistoryReplicationTaskScope,
			startTime:    timeSource.Now(),
			queueID: definition.NewWorkflowIdentifier(
				attr.GetDomainId(), attr.GetWorkflowId(), attr.GetRunId(),
			),
			taskID:        attr.GetFirstEventId(),
			attempt:       0,
			kafkaMsg:      msg,
			logger:        logger,
			state:         task.TaskStatePending,
			config:        config,
			timeSource:    timeSource,
			historyClient: historyClient,
			metricsClient: metricsClient,
		},
		req: &h.ReplicateEventsRequest{
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
		},
		historyRereplicator: historyRereplicator,
	}
}

func newHistoryMetadataReplicationTask(
	replicationTask *replicator.ReplicationTask,
	msg messaging.Message,
	sourceCluster string,
	logger log.Logger,
	config *Config,
	timeSource clock.TimeSource,
	historyClient history.Client,
	metricsClient metrics.Client,
	historyRereplicator xdc.HistoryRereplicator,
) *historyMetadataReplicationTask {

	attr := replicationTask.HistoryMetadataTaskAttributes
	logger = logger.WithTags(tag.WorkflowDomainID(attr.GetDomainId()),
		tag.WorkflowID(attr.GetWorkflowId()),
		tag.WorkflowRunID(attr.GetRunId()),
		tag.WorkflowFirstEventID(attr.GetFirstEventId()),
		tag.WorkflowNextEventID(attr.GetNextEventId()))
	return &historyMetadataReplicationTask{
		workflowReplicationTask: workflowReplicationTask{
			metricsScope: metrics.HistoryMetadataReplicationTaskScope,
			startTime:    timeSource.Now(),
			queueID: definition.NewWorkflowIdentifier(
				attr.GetDomainId(), attr.GetWorkflowId(), attr.GetRunId(),
			),
			taskID:        attr.GetFirstEventId(),
			attempt:       0,
			kafkaMsg:      msg,
			logger:        logger,
			state:         task.TaskStatePending,
			config:        config,
			timeSource:    timeSource,
			historyClient: historyClient,
			metricsClient: metricsClient,
		},
		sourceCluster:       sourceCluster,
		firstEventID:        attr.GetFirstEventId(),
		nextEventID:         attr.GetNextEventId(),
		historyRereplicator: historyRereplicator,
	}
}

func newHistoryReplicationV2Task(
	replicationTask *replicator.ReplicationTask,
	msg messaging.Message,
	logger log.Logger,
	config *Config,
	timeSource clock.TimeSource,
	historyClient history.Client,
	metricsClient metrics.Client,
	nDCHistoryResender xdc.NDCHistoryResender,
) *historyReplicationV2Task {

	attr := replicationTask.HistoryTaskV2Attributes
	logger = logger.WithTags(tag.WorkflowDomainID(attr.GetDomainId()),
		tag.WorkflowID(attr.GetWorkflowId()),
		tag.WorkflowRunID(attr.GetRunId()),
	)
	return &historyReplicationV2Task{
		workflowReplicationTask: workflowReplicationTask{
			metricsScope: metrics.HistoryReplicationTaskScope,
			startTime:    timeSource.Now(),
			queueID: definition.NewWorkflowIdentifier(
				attr.GetDomainId(), attr.GetWorkflowId(), attr.GetRunId(),
			),
			taskID:        attr.GetTaskId(),
			attempt:       0,
			kafkaMsg:      msg,
			logger:        logger,
			state:         task.TaskStatePending,
			config:        config,
			timeSource:    timeSource,
			historyClient: historyClient,
			metricsClient: metricsClient,
		},
		req: &h.ReplicateEventsV2Request{
			DomainUUID: attr.DomainId,
			WorkflowExecution: &shared.WorkflowExecution{
				WorkflowId: attr.WorkflowId,
				RunId:      attr.RunId,
			},
			VersionHistoryItems: attr.VersionHistoryItems,
			Events:              attr.Events,
			NewRunEvents:        attr.NewRunEvents,
		},
		nDCHistoryResender: nDCHistoryResender,
	}
}

func (t *activityReplicationTask) Execute() error {
	ctx, cancel := context.WithTimeout(context.Background(), t.config.ReplicationTaskContextTimeout())
	defer cancel()
	return t.historyClient.SyncActivity(ctx, t.req)
}

func (t *activityReplicationTask) HandleErr(
	err error,
) error {
	if t.attempt < t.config.ReplicatorActivityBufferRetryCount() {
		return err
	}

	retryV1Err, okV1 := t.convertRetryTaskError(err)
	retryV2Err, okV2 := t.convertRetryTaskV2Error(err)

	if !okV1 && !okV2 {
		return err
	} else if okV1 {
		if retryV1Err.GetRunId() == "" {
			return err
		}

		t.metricsClient.IncCounter(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientRequests)
		stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientLatency)
		defer stopwatch.Stop()

		// this is the retry error
		beginRunID := retryV1Err.GetRunId()
		beginEventID := retryV1Err.GetNextEventId()
		endRunID := t.queueID.RunID
		endEventID := t.taskID + 1 // the next event ID should be at activity schedule ID + 1
		resendErr := t.historyRereplicator.SendMultiWorkflowHistory(
			t.queueID.DomainID, t.queueID.WorkflowID,
			beginRunID, beginEventID, endRunID, endEventID,
		)

		if resendErr != nil {
			t.logger.Error("error resend history", tag.Error(resendErr))
			// should return the replication error, not the resending error
			return err
		}
	} else if okV2 {
		t.metricsClient.IncCounter(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientRequests)
		stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByActivityReplicationScope, metrics.CadenceClientLatency)
		defer stopwatch.Stop()

		if resendErr := t.nDCHistoryResender.SendSingleWorkflowHistory(
			retryV2Err.GetDomainId(),
			retryV2Err.GetWorkflowId(),
			retryV2Err.GetRunId(),
			retryV2Err.StartEventId,
			retryV2Err.StartEventVersion,
			retryV2Err.EndEventId,
			retryV2Err.EndEventVersion,
		); resendErr != nil {
			t.logger.Error("error resend history", tag.Error(resendErr))
			// should return the replication error, not the resending error
			return err
		}
	} else {
		return &shared.InternalServiceError{Message: "activityReplicationTask encounter error which cannot be handled"}
	}

	// should try again
	return t.Execute()
}

func (t *historyReplicationTask) Execute() error {
	ctx, cancel := context.WithTimeout(context.Background(), t.config.ReplicationTaskContextTimeout())
	defer cancel()
	return t.historyClient.ReplicateEvents(ctx, t.req)
}

func (t *historyReplicationTask) HandleErr(
	err error,
) error {
	if t.attempt < t.config.ReplicatorHistoryBufferRetryCount() {
		return err
	}

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
	endRunID := t.queueID.RunID
	endEventID := t.taskID
	resendErr := t.historyRereplicator.SendMultiWorkflowHistory(
		t.queueID.DomainID, t.queueID.WorkflowID,
		beginRunID, beginEventID, endRunID, endEventID,
	)
	if resendErr != nil {
		t.logger.Error("error resend history", tag.Error(resendErr))
		// should return the replication error, not the resending error
		return err
	}
	// should try again
	return t.Execute()
}

func (t *historyMetadataReplicationTask) Execute() error {
	t.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryMetadataReplicationScope, metrics.CadenceClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryMetadataReplicationScope, metrics.CadenceClientLatency)
	defer stopwatch.Stop()

	return t.historyRereplicator.SendMultiWorkflowHistory(
		t.queueID.DomainID, t.queueID.WorkflowID,
		t.queueID.RunID, t.firstEventID,
		t.queueID.RunID, t.nextEventID,
	)
}

func (t *historyMetadataReplicationTask) HandleErr(
	err error,
) error {
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
	endRunID := t.queueID.RunID
	endEventID := t.taskID
	resendErr := t.historyRereplicator.SendMultiWorkflowHistory(
		t.queueID.DomainID, t.queueID.WorkflowID,
		beginRunID, beginEventID, endRunID, endEventID,
	)
	if resendErr != nil {
		t.logger.Error("error resend history", tag.Error(resendErr))
		// should return the replication error, not the resending error
		return err
	}
	// should try again
	return t.Execute()
}

func (t *historyReplicationV2Task) Execute() error {
	ctx, cancel := context.WithTimeout(context.Background(), t.config.ReplicationTaskContextTimeout())
	defer cancel()
	return t.historyClient.ReplicateEventsV2(ctx, t.req)
}

func (t *historyReplicationV2Task) HandleErr(err error) error {
	if t.attempt < t.config.ReplicatorHistoryBufferRetryCount() {
		return err
	}

	retryErr, ok := t.convertRetryTaskV2Error(err)
	if !ok {
		return err
	}

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.CadenceClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.CadenceClientLatency)
	defer stopwatch.Stop()

	if resendErr := t.nDCHistoryResender.SendSingleWorkflowHistory(
		retryErr.GetDomainId(),
		retryErr.GetWorkflowId(),
		retryErr.GetRunId(),
		retryErr.StartEventId,
		retryErr.StartEventVersion,
		retryErr.EndEventId,
		retryErr.EndEventVersion,
	); resendErr != nil {
		t.logger.Error("error resend history", tag.Error(resendErr))
		// should return the replication error, not the resending error
		return err
	}
	// should try again
	return t.Execute()
}

func (t *workflowReplicationTask) RetryErr(err error) bool {
	t.attempt++

	if t.attempt <= t.config.ReplicationTaskMaxRetryCount() &&
		t.timeSource.Now().Sub(t.startTime) <= t.config.ReplicationTaskMaxRetryDuration() &&
		isTransientRetryableError(err) {

		time.Sleep(replicationTaskRetryDelay)
		return true
	}
	return false
}

func (t *workflowReplicationTask) State() task.State {
	return t.state
}

func (t *workflowReplicationTask) Ack() {
	t.metricsClient.IncCounter(t.metricsScope, metrics.ReplicatorMessages)
	t.metricsClient.RecordTimer(t.metricsScope, metrics.ReplicatorLatency, t.timeSource.Now().Sub(t.startTime))

	t.state = task.TaskStateAcked
	// the underlying implementation will not return anything other than nil
	// do logging just in case
	err := t.kafkaMsg.Ack()
	if err != nil {
		t.logger.Error("Unable to ack.")
	}
}

func (t *workflowReplicationTask) Nack() {
	t.metricsClient.IncCounter(t.metricsScope, metrics.ReplicatorMessages)
	t.metricsClient.RecordTimer(t.metricsScope, metrics.ReplicatorLatency, t.timeSource.Now().Sub(t.startTime))

	t.logger.Info("Replication task moved to DLQ",
		tag.WorkflowDomainID(t.queueID.DomainID),
		tag.WorkflowID(t.queueID.WorkflowID),
		tag.WorkflowRunID(t.queueID.RunID),
		tag.TaskID(t.taskID),
	)

	t.state = task.TaskStateNacked
	// the underlying implementation will not return anything other than nil
	// do logging just in case
	err := t.kafkaMsg.Nack()
	if err != nil {
		t.logger.Error("Unable to nack.")
	}
}

func (t *workflowReplicationTask) convertRetryTaskError(
	err error,
) (*shared.RetryTaskError, bool) {

	retError, ok := err.(*shared.RetryTaskError)
	return retError, ok
}

func (t *workflowReplicationTask) convertRetryTaskV2Error(
	err error,
) (*shared.RetryTaskV2Error, bool) {

	retError, ok := err.(*shared.RetryTaskV2Error)
	return retError, ok
}
