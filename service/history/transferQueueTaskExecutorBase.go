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

package history

import (
	"context"
	"time"

	"github.com/gogo/protobuf/proto"
	commonpb "go.temporal.io/temporal-proto/common"
	eventpb "go.temporal.io/temporal-proto/event"
	executionpb "go.temporal.io/temporal-proto/execution"
	namespacepb "go.temporal.io/temporal-proto/namespace"
	"go.temporal.io/temporal-proto/serviceerror"
	tasklistpb "go.temporal.io/temporal-proto/tasklist"

	m "github.com/temporalio/temporal/.gen/proto/matchingservice"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/service/worker/archiver"
)

const (
	transferActiveTaskDefaultTimeout = 30 * time.Second
)

type (
	transferQueueTaskExecutorBase struct {
		shard          ShardContext
		historyService *historyEngineImpl
		cache          *historyCache
		logger         log.Logger
		metricsClient  metrics.Client
		matchingClient matching.Client
		visibilityMgr  persistence.VisibilityManager
		config         *Config
	}
)

func newTransferQueueTaskExecutorBase(
	shard ShardContext,
	historyService *historyEngineImpl,
	logger log.Logger,
	metricsClient metrics.Client,
	config *Config,
) *transferQueueTaskExecutorBase {
	return &transferQueueTaskExecutorBase{
		shard:          shard,
		historyService: historyService,
		cache:          historyService.historyCache,
		logger:         logger,
		metricsClient:  metricsClient,
		matchingClient: shard.GetService().GetMatchingClient(),
		visibilityMgr:  shard.GetService().GetVisibilityManager(),
		config:         config,
	}
}

func (t *transferQueueTaskExecutorBase) getNamespaceIDAndWorkflowExecution(
	task *persistenceblobs.TransferTaskInfo,
) (string, executionpb.WorkflowExecution) {

	return primitives.UUIDString(task.GetNamespaceId()), executionpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      primitives.UUIDString(task.GetRunId()),
	}
}

func (t *transferQueueTaskExecutorBase) pushActivity(
	task *persistenceblobs.TransferTaskInfo,
	activityScheduleToStartTimeout int32,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	if task.TaskType != persistence.TransferTaskTypeActivityTask {
		t.logger.Fatal("Cannot process non activity task", tag.TaskType(task.GetTaskType()))
	}

	_, err := t.matchingClient.AddActivityTask(ctx, &m.AddActivityTaskRequest{
		NamespaceId:       primitives.UUIDString(task.GetTargetNamespaceId()),
		SourceNamespaceId: primitives.UUIDString(task.GetNamespaceId()),
		Execution: &executionpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      primitives.UUIDString(task.GetRunId()),
		},
		TaskList:                      &tasklistpb.TaskList{Name: task.TaskList},
		ScheduleId:                    task.GetScheduleId(),
		ScheduleToStartTimeoutSeconds: activityScheduleToStartTimeout,
	})

	return err
}

func (t *transferQueueTaskExecutorBase) pushDecision(
	task *persistenceblobs.TransferTaskInfo,
	tasklist *tasklistpb.TaskList,
	decisionScheduleToStartTimeout int32,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	if task.TaskType != persistence.TransferTaskTypeDecisionTask {
		t.logger.Fatal("Cannot process non decision task", tag.TaskType(task.GetTaskType()))
	}

	_, err := t.matchingClient.AddDecisionTask(ctx, &m.AddDecisionTaskRequest{
		NamespaceId: primitives.UUIDString(task.GetNamespaceId()),
		Execution: &executionpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowId(),
			RunId:      primitives.UUIDString(task.GetRunId()),
		},
		TaskList:                      tasklist,
		ScheduleId:                    task.GetScheduleId(),
		ScheduleToStartTimeoutSeconds: decisionScheduleToStartTimeout,
	})
	return err
}

func (t *transferQueueTaskExecutorBase) recordWorkflowStarted(
	namespaceID string,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTimeUnixNano int64,
	executionTimeUnixNano int64,
	runTimeout int32,
	taskID int64,
	taskList string,
	visibilityMemo *commonpb.Memo,
	searchAttributes map[string]*commonpb.Payload,
) error {

	namespace := defaultNamespace

	if namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(namespaceID); err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
	} else {
		namespace = namespaceEntry.GetInfo().Name
		// if sampled for longer retention is enabled, only record those sampled events
		if namespaceEntry.IsSampledForLongerRetentionEnabled(workflowID) &&
			!namespaceEntry.IsSampledForLongerRetention(workflowID) {
			return nil
		}
	}

	request := &persistence.RecordWorkflowExecutionStartedRequest{
		NamespaceID: namespaceID,
		Namespace:   namespace,
		Execution: executionpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		WorkflowTypeName:   workflowTypeName,
		StartTimestamp:     startTimeUnixNano,
		ExecutionTimestamp: executionTimeUnixNano,
		RunTimeout:         int64(runTimeout),
		TaskID:             taskID,
		Memo:               visibilityMemo,
		TaskList:           taskList,
		SearchAttributes:   searchAttributes,
	}

	return t.visibilityMgr.RecordWorkflowExecutionStarted(request)
}

func (t *transferQueueTaskExecutorBase) upsertWorkflowExecution(
	namespaceID string,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTimeUnixNano int64,
	executionTimeUnixNano int64,
	workflowTimeout int32,
	taskID int64,
	taskList string,
	visibilityMemo *commonpb.Memo,
	searchAttributes map[string]*commonpb.Payload,
) error {

	namespace := defaultNamespace
	namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return err
		}
	} else {
		namespace = namespaceEntry.GetInfo().Name
	}

	request := &persistence.UpsertWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Namespace:   namespace,
		Execution: executionpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		WorkflowTypeName:   workflowTypeName,
		StartTimestamp:     startTimeUnixNano,
		ExecutionTimestamp: executionTimeUnixNano,
		WorkflowTimeout:    int64(workflowTimeout),
		TaskID:             taskID,
		Memo:               visibilityMemo,
		TaskList:           taskList,
		SearchAttributes:   searchAttributes,
	}

	return t.visibilityMgr.UpsertWorkflowExecution(request)
}

func (t *transferQueueTaskExecutorBase) recordWorkflowClosed(
	namespaceID string,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTimeUnixNano int64,
	executionTimeUnixNano int64,
	endTimeUnixNano int64,
	status executionpb.WorkflowExecutionStatus,
	historyLength int64,
	taskID int64,
	visibilityMemo *commonpb.Memo,
	taskList string,
	searchAttributes map[string]*commonpb.Payload,
) error {

	// Record closing in visibility store
	retentionSeconds := int64(0)
	namespace := defaultNamespace
	recordWorkflowClose := true
	archiveVisibility := false

	namespaceEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if err != nil && !isWorkflowNotExistError(err) {
		return err
	}

	if err == nil {
		// retention in namespace config is in days, convert to seconds
		retentionSeconds = int64(namespaceEntry.GetRetentionDays(workflowID)) * int64(secondsInDay)
		namespace = namespaceEntry.GetInfo().Name
		// if sampled for longer retention is enabled, only record those sampled events
		if namespaceEntry.IsSampledForLongerRetentionEnabled(workflowID) &&
			!namespaceEntry.IsSampledForLongerRetention(workflowID) {
			recordWorkflowClose = false
		}

		clusterConfiguredForVisibilityArchival := t.shard.GetService().GetArchivalMetadata().GetVisibilityConfig().ClusterConfiguredForArchival()
		namespaceConfiguredForVisibilityArchival := namespaceEntry.GetConfig().VisibilityArchivalStatus == namespacepb.ArchivalStatus_Enabled
		archiveVisibility = clusterConfiguredForVisibilityArchival && namespaceConfiguredForVisibilityArchival
	}

	if recordWorkflowClose {
		if err := t.visibilityMgr.RecordWorkflowExecutionClosed(&persistence.RecordWorkflowExecutionClosedRequest{
			NamespaceID: namespaceID,
			Namespace:   namespace,
			Execution: executionpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			WorkflowTypeName:   workflowTypeName,
			StartTimestamp:     startTimeUnixNano,
			ExecutionTimestamp: executionTimeUnixNano,
			CloseTimestamp:     endTimeUnixNano,
			Status:             status,
			HistoryLength:      historyLength,
			RetentionSeconds:   retentionSeconds,
			TaskID:             taskID,
			Memo:               visibilityMemo,
			TaskList:           taskList,
			SearchAttributes:   searchAttributes,
		}); err != nil {
			return err
		}
	}

	if archiveVisibility {
		ctx, cancel := context.WithTimeout(context.Background(), t.config.TransferProcessorVisibilityArchivalTimeLimit())
		defer cancel()
		_, err := t.historyService.archivalClient.Archive(ctx, &archiver.ClientRequest{
			ArchiveRequest: &archiver.ArchiveRequest{
				NamespaceID:        namespaceID,
				Namespace:          namespace,
				WorkflowID:         workflowID,
				RunID:              runID,
				WorkflowTypeName:   workflowTypeName,
				StartTimestamp:     startTimeUnixNano,
				ExecutionTimestamp: executionTimeUnixNano,
				CloseTimestamp:     endTimeUnixNano,
				Status:             status,
				HistoryLength:      historyLength,
				Memo:               visibilityMemo,
				SearchAttributes:   searchAttributes,
				VisibilityURI:      namespaceEntry.GetConfig().VisibilityArchivalURI,
				URI:                namespaceEntry.GetConfig().HistoryArchivalURI,
				Targets:            []archiver.ArchivalTarget{archiver.ArchiveTargetVisibility},
			},
			CallerService:        common.HistoryServiceName,
			AttemptArchiveInline: true, // archive visibility inline by default
		})
		return err
	}
	return nil
}

// Argument startEvent is to save additional call of msBuilder.GetStartEvent
func getWorkflowExecutionTimestamp(
	msBuilder mutableState,
	startEvent *eventpb.HistoryEvent,
) time.Time {
	// Use value 0 to represent workflows that don't need backoff. Since ES doesn't support
	// comparison between two field, we need a value to differentiate them from cron workflows
	// or later runs of a workflow that needs retry.
	executionTimestamp := time.Unix(0, 0)
	if startEvent == nil {
		return executionTimestamp
	}

	if backoffSeconds := startEvent.GetWorkflowExecutionStartedEventAttributes().GetFirstDecisionTaskBackoffSeconds(); backoffSeconds != 0 {
		startTimestamp := time.Unix(0, startEvent.GetTimestamp())
		executionTimestamp = startTimestamp.Add(time.Duration(backoffSeconds) * time.Second)
	}
	return executionTimestamp
}

func getWorkflowMemo(
	memo map[string]*commonpb.Payload,
) *commonpb.Memo {

	if memo == nil {
		return nil
	}
	return &commonpb.Memo{Fields: memo}
}

func copySearchAttributes(
	input map[string]*commonpb.Payload,
) map[string]*commonpb.Payload {

	if input == nil {
		return nil
	}

	result := make(map[string]*commonpb.Payload)
	for k, v := range input {
		result[k] = proto.Clone(v).(*commonpb.Payload)
	}
	return result
}

func isWorkflowNotExistError(err error) bool {
	_, ok := err.(*serviceerror.NotFound)
	return ok
}
