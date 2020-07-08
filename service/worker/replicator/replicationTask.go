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

package replicator

import (
	"context"
	"time"

	commonpb "go.temporal.io/temporal-proto/common/v1"
	"go.temporal.io/temporal-proto/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/messaging"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/task"
	"go.temporal.io/server/common/xdc"
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
		req                 *historyservice.SyncActivityRequest
		historyRereplicator xdc.HistoryRereplicator
		nDCHistoryResender  xdc.NDCHistoryResender
	}

	historyReplicationTask struct {
		workflowReplicationTask
		req                 *historyservice.ReplicateEventsRequest
		historyRereplicator xdc.HistoryRereplicator
	}

	historyMetadataReplicationTask struct {
		workflowReplicationTask
		sourceCluster       string
		firstEventID        int64
		nextEventID         int64
		version             int64
		historyRereplicator xdc.HistoryRereplicator
		nDCHistoryResender  xdc.NDCHistoryResender
	}

	historyReplicationV2Task struct {
		workflowReplicationTask
		req                *historyservice.ReplicateEventsV2Request
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
	replicationTask *replicationspb.ReplicationTask,
	msg messaging.Message,
	logger log.Logger,
	config *Config,
	timeSource clock.TimeSource,
	historyClient history.Client,
	metricsClient metrics.Client,
	historyRereplicator xdc.HistoryRereplicator,
	nDCHistoryResender xdc.NDCHistoryResender,
) *activityReplicationTask {

	attr := replicationTask.GetSyncActivityTaskAttributes()

	logger = logger.WithTags(tag.WorkflowNamespaceID(attr.GetNamespaceId()),
		tag.WorkflowID(attr.GetWorkflowId()),
		tag.WorkflowRunID(attr.GetRunId()),
		tag.WorkflowEventID(attr.GetScheduledId()),
		tag.FailoverVersion(attr.GetVersion()))
	return &activityReplicationTask{
		workflowReplicationTask: workflowReplicationTask{
			metricsScope: metrics.SyncActivityTaskScope,
			startTime:    timeSource.Now(),
			queueID: definition.NewWorkflowIdentifier(
				attr.GetNamespaceId(), attr.GetWorkflowId(), attr.GetRunId(),
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
		req: &historyservice.SyncActivityRequest{
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
			VersionHistory:     attr.VersionHistory,
		},
		historyRereplicator: historyRereplicator,
		nDCHistoryResender:  nDCHistoryResender,
	}
}

func newHistoryReplicationTask(
	replicationTask *replicationspb.ReplicationTask,
	msg messaging.Message,
	sourceCluster string,
	logger log.Logger,
	config *Config,
	timeSource clock.TimeSource,
	historyClient history.Client,
	metricsClient metrics.Client,
	historyRereplicator xdc.HistoryRereplicator,
) *historyReplicationTask {

	attr := replicationTask.GetHistoryTaskAttributes()
	logger = logger.WithTags(tag.WorkflowNamespaceID(attr.GetNamespaceId()),
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
				attr.GetNamespaceId(), attr.GetWorkflowId(), attr.GetRunId(),
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
		req: &historyservice.ReplicateEventsRequest{
			SourceCluster: sourceCluster,
			NamespaceId:   attr.NamespaceId,
			WorkflowExecution: &commonpb.WorkflowExecution{
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
			NewRunNdc:         attr.NewRunNdc,
		},
		historyRereplicator: historyRereplicator,
	}
}

func newHistoryMetadataReplicationTask(
	replicationTask *replicationspb.ReplicationTask,
	msg messaging.Message,
	sourceCluster string,
	logger log.Logger,
	config *Config,
	timeSource clock.TimeSource,
	historyClient history.Client,
	metricsClient metrics.Client,
	historyRereplicator xdc.HistoryRereplicator,
	nDCHistoryResender xdc.NDCHistoryResender,
) *historyMetadataReplicationTask {

	attr := replicationTask.GetHistoryMetadataTaskAttributes()
	logger = logger.WithTags(tag.WorkflowNamespaceID(attr.GetNamespaceId()),
		tag.WorkflowID(attr.GetWorkflowId()),
		tag.WorkflowRunID(attr.GetRunId()),
		tag.WorkflowFirstEventID(attr.GetFirstEventId()),
		tag.WorkflowNextEventID(attr.GetNextEventId()))

	return &historyMetadataReplicationTask{
		workflowReplicationTask: workflowReplicationTask{
			metricsScope: metrics.HistoryMetadataReplicationTaskScope,
			startTime:    timeSource.Now(),
			queueID: definition.NewWorkflowIdentifier(
				attr.GetNamespaceId(), attr.GetWorkflowId(), attr.GetRunId(),
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
		version:             attr.GetVersion(),
		historyRereplicator: historyRereplicator,
		nDCHistoryResender:  nDCHistoryResender,
	}
}

func newHistoryReplicationV2Task(
	replicationTask *replicationspb.ReplicationTask,
	msg messaging.Message,
	logger log.Logger,
	config *Config,
	timeSource clock.TimeSource,
	historyClient history.Client,
	metricsClient metrics.Client,
	nDCHistoryResender xdc.NDCHistoryResender,
) *historyReplicationV2Task {

	attr := replicationTask.GetHistoryTaskV2Attributes()
	logger = logger.WithTags(tag.WorkflowNamespaceID(attr.GetNamespaceId()),
		tag.WorkflowID(attr.GetWorkflowId()),
		tag.WorkflowRunID(attr.GetRunId()),
	)
	return &historyReplicationV2Task{
		workflowReplicationTask: workflowReplicationTask{
			metricsScope: metrics.HistoryReplicationTaskScope,
			startTime:    timeSource.Now(),
			queueID: definition.NewWorkflowIdentifier(
				attr.GetNamespaceId(), attr.GetWorkflowId(), attr.GetRunId(),
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
		req: &historyservice.ReplicateEventsV2Request{
			NamespaceId: attr.NamespaceId,
			WorkflowExecution: &commonpb.WorkflowExecution{
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
	_, err := t.historyClient.SyncActivity(ctx, t.req)
	return err
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
		if retryV1Err.RunId == "" {
			return err
		}

		t.metricsClient.IncCounter(metrics.HistoryRereplicationByActivityReplicationScope, metrics.ClientRequests)
		stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByActivityReplicationScope, metrics.ClientLatency)
		defer stopwatch.Stop()

		// this is the retry error
		beginRunID := retryV1Err.RunId
		beginEventID := retryV1Err.NextEventId
		endRunID := t.queueID.RunID
		endEventID := t.taskID + 1 // the next event ID should be at activity schedule ID + 1
		resendErr := t.historyRereplicator.SendMultiWorkflowHistory(
			t.queueID.NamespaceID, t.queueID.WorkflowID,
			beginRunID, beginEventID, endRunID, endEventID,
		)

		if resendErr != nil {
			t.logger.Error("error resend history", tag.Error(resendErr))
			// should return the replication error, not the resending error
			return err
		}
	} else if okV2 {
		t.metricsClient.IncCounter(metrics.HistoryRereplicationByActivityReplicationScope, metrics.ClientRequests)
		stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByActivityReplicationScope, metrics.ClientLatency)
		defer stopwatch.Stop()

		if resendErr := t.nDCHistoryResender.SendSingleWorkflowHistory(
			retryV2Err.NamespaceId,
			retryV2Err.WorkflowId,
			retryV2Err.RunId,
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
		return serviceerror.NewInternal("activityReplicationTask encounter error which cannot be handled")
	}

	// should try again
	return t.Execute()
}

func (t *historyReplicationTask) Execute() error {
	ctx, cancel := context.WithTimeout(context.Background(), t.config.ReplicationTaskContextTimeout())
	defer cancel()
	_, err := t.historyClient.ReplicateEvents(ctx, t.req)
	return err
}

func (t *historyReplicationTask) HandleErr(
	err error,
) error {
	if t.attempt < t.config.ReplicatorHistoryBufferRetryCount() {
		return err
	}

	retryErr, ok := t.convertRetryTaskError(err)
	if !ok || retryErr.RunId == "" {
		return err
	}

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.ClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.ClientLatency)
	defer stopwatch.Stop()

	// this is the retry error
	beginRunID := retryErr.RunId
	beginEventID := retryErr.NextEventId
	endRunID := t.queueID.RunID
	endEventID := t.taskID
	resendErr := t.historyRereplicator.SendMultiWorkflowHistory(
		t.queueID.NamespaceID, t.queueID.WorkflowID,
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
	t.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryMetadataReplicationScope, metrics.ClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryMetadataReplicationScope, metrics.ClientLatency)
	defer stopwatch.Stop()

	if t.version != common.EmptyVersion {
		return t.nDCHistoryResender.SendSingleWorkflowHistory(
			t.queueID.NamespaceID,
			t.queueID.WorkflowID,
			t.queueID.RunID,
			t.firstEventID-1, //NDC resend API is exclusive-exclusive.
			t.version,
			t.nextEventID,
			t.version)
	} else {
		return t.historyRereplicator.SendMultiWorkflowHistory(
			t.queueID.NamespaceID, t.queueID.WorkflowID,
			t.queueID.RunID, t.firstEventID,
			t.queueID.RunID, t.nextEventID,
		)
	}
}

func (t *historyMetadataReplicationTask) HandleErr(
	err error,
) error {
	retryErr, ok := t.convertRetryTaskError(err)
	if !ok || retryErr.RunId == "" {
		return err
	}

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.ClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.ClientLatency)
	defer stopwatch.Stop()

	// this is the retry error
	beginRunID := retryErr.RunId
	beginEventID := retryErr.NextEventId
	endRunID := t.queueID.RunID
	endEventID := t.taskID
	resendErr := t.historyRereplicator.SendMultiWorkflowHistory(
		t.queueID.NamespaceID, t.queueID.WorkflowID,
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
	_, err := t.historyClient.ReplicateEventsV2(ctx, t.req)
	return err
}

func (t *historyReplicationV2Task) HandleErr(err error) error {
	if t.attempt < t.config.ReplicatorHistoryBufferRetryCount() {
		return err
	}

	retryErr, ok := t.convertRetryTaskV2Error(err)
	if !ok {
		return err
	}

	t.metricsClient.IncCounter(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.ClientRequests)
	stopwatch := t.metricsClient.StartTimer(metrics.HistoryRereplicationByHistoryReplicationScope, metrics.ClientLatency)
	defer stopwatch.Stop()

	if resendErr := t.nDCHistoryResender.SendSingleWorkflowHistory(
		retryErr.NamespaceId,
		retryErr.WorkflowId,
		retryErr.RunId,
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
		tag.WorkflowNamespaceID(t.queueID.NamespaceID),
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
) (*serviceerror.RetryTask, bool) {

	retError, ok := err.(*serviceerror.RetryTask)
	return retError, ok
}

func (t *workflowReplicationTask) convertRetryTaskV2Error(
	err error,
) (*serviceerror.RetryTaskV2, bool) {

	retError, ok := err.(*serviceerror.RetryTaskV2)
	return retError, ok
}
