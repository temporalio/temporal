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

	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client/matching"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/worker/archiver"
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

func (t *transferQueueTaskExecutorBase) getDomainIDAndWorkflowExecution(
	task *persistence.TransferTaskInfo,
) (string, workflow.WorkflowExecution) {

	return task.DomainID, workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(task.RunID),
	}
}

func (t *transferQueueTaskExecutorBase) pushActivity(
	task *persistence.TransferTaskInfo,
	activityScheduleToStartTimeout int32,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	if task.TaskType != persistence.TransferTaskTypeActivityTask {
		t.logger.Fatal("Cannot process non activity task", tag.TaskType(task.GetTaskType()))
	}

	err := t.matchingClient.AddActivityTask(ctx, &m.AddActivityTaskRequest{
		DomainUUID:       common.StringPtr(task.TargetDomainID),
		SourceDomainUUID: common.StringPtr(task.DomainID),
		Execution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(task.WorkflowID),
			RunId:      common.StringPtr(task.RunID),
		},
		TaskList:                      &workflow.TaskList{Name: &task.TaskList},
		ScheduleId:                    &task.ScheduleID,
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(activityScheduleToStartTimeout),
	})

	return err
}

func (t *transferQueueTaskExecutorBase) pushDecision(
	task *persistence.TransferTaskInfo,
	tasklist *workflow.TaskList,
	decisionScheduleToStartTimeout int32,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	if task.TaskType != persistence.TransferTaskTypeDecisionTask {
		t.logger.Fatal("Cannot process non decision task", tag.TaskType(task.GetTaskType()))
	}

	err := t.matchingClient.AddDecisionTask(ctx, &m.AddDecisionTaskRequest{
		DomainUUID: common.StringPtr(task.DomainID),
		Execution: &workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(task.WorkflowID),
			RunId:      common.StringPtr(task.RunID),
		},
		TaskList:                      tasklist,
		ScheduleId:                    common.Int64Ptr(task.ScheduleID),
		ScheduleToStartTimeoutSeconds: common.Int32Ptr(decisionScheduleToStartTimeout),
	})
	return err
}

func (t *transferQueueTaskExecutorBase) recordWorkflowStarted(
	domainID string,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTimeUnixNano int64,
	executionTimeUnixNano int64,
	workflowTimeout int32,
	taskID int64,
	visibilityMemo *workflow.Memo,
	searchAttributes map[string][]byte,
) error {

	domain := defaultDomainName

	if domainEntry, err := t.shard.GetDomainCache().GetDomainByID(domainID); err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return err
		}
	} else {
		domain = domainEntry.GetInfo().Name
		// if sampled for longer retention is enabled, only record those sampled events
		if domainEntry.IsSampledForLongerRetentionEnabled(workflowID) &&
			!domainEntry.IsSampledForLongerRetention(workflowID) {
			return nil
		}
	}

	request := &persistence.RecordWorkflowExecutionStartedRequest{
		DomainUUID: domainID,
		Domain:     domain,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		WorkflowTypeName:   workflowTypeName,
		StartTimestamp:     startTimeUnixNano,
		ExecutionTimestamp: executionTimeUnixNano,
		WorkflowTimeout:    int64(workflowTimeout),
		TaskID:             taskID,
		Memo:               visibilityMemo,
		SearchAttributes:   searchAttributes,
	}

	return t.visibilityMgr.RecordWorkflowExecutionStarted(request)
}

func (t *transferQueueTaskExecutorBase) upsertWorkflowExecution(
	domainID string,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTimeUnixNano int64,
	executionTimeUnixNano int64,
	workflowTimeout int32,
	taskID int64,
	visibilityMemo *workflow.Memo,
	searchAttributes map[string][]byte,
) error {

	domain := defaultDomainName
	domainEntry, err := t.shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return err
		}
	} else {
		domain = domainEntry.GetInfo().Name
	}

	request := &persistence.UpsertWorkflowExecutionRequest{
		DomainUUID: domainID,
		Domain:     domain,
		Execution: workflow.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		WorkflowTypeName:   workflowTypeName,
		StartTimestamp:     startTimeUnixNano,
		ExecutionTimestamp: executionTimeUnixNano,
		WorkflowTimeout:    int64(workflowTimeout),
		TaskID:             taskID,
		Memo:               visibilityMemo,
		SearchAttributes:   searchAttributes,
	}

	return t.visibilityMgr.UpsertWorkflowExecution(request)
}

func (t *transferQueueTaskExecutorBase) recordWorkflowClosed(
	domainID string,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTimeUnixNano int64,
	executionTimeUnixNano int64,
	endTimeUnixNano int64,
	closeStatus workflow.WorkflowExecutionCloseStatus,
	historyLength int64,
	taskID int64,
	visibilityMemo *workflow.Memo,
	searchAttributes map[string][]byte,
) error {

	// Record closing in visibility store
	retentionSeconds := int64(0)
	domain := defaultDomainName
	recordWorkflowClose := true
	archiveVisibility := false

	domainEntry, err := t.shard.GetDomainCache().GetDomainByID(domainID)
	if err != nil && !isWorkflowNotExistError(err) {
		return err
	}

	if err == nil {
		// retention in domain config is in days, convert to seconds
		retentionSeconds = int64(domainEntry.GetRetentionDays(workflowID)) * int64(secondsInDay)
		domain = domainEntry.GetInfo().Name
		// if sampled for longer retention is enabled, only record those sampled events
		if domainEntry.IsSampledForLongerRetentionEnabled(workflowID) &&
			!domainEntry.IsSampledForLongerRetention(workflowID) {
			recordWorkflowClose = false
		}

		clusterConfiguredForVisibilityArchival := t.shard.GetService().GetArchivalMetadata().GetVisibilityConfig().ClusterConfiguredForArchival()
		domainConfiguredForVisibilityArchival := domainEntry.GetConfig().VisibilityArchivalStatus == workflow.ArchivalStatusEnabled
		archiveVisibility = clusterConfiguredForVisibilityArchival && domainConfiguredForVisibilityArchival
	}

	if recordWorkflowClose {
		if err := t.visibilityMgr.RecordWorkflowExecutionClosed(&persistence.RecordWorkflowExecutionClosedRequest{
			DomainUUID: domainID,
			Domain:     domain,
			Execution: workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
			WorkflowTypeName:   workflowTypeName,
			StartTimestamp:     startTimeUnixNano,
			ExecutionTimestamp: executionTimeUnixNano,
			CloseTimestamp:     endTimeUnixNano,
			Status:             closeStatus,
			HistoryLength:      historyLength,
			RetentionSeconds:   retentionSeconds,
			TaskID:             taskID,
			Memo:               visibilityMemo,
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
				DomainID:           domainID,
				DomainName:         domain,
				WorkflowID:         workflowID,
				RunID:              runID,
				WorkflowTypeName:   workflowTypeName,
				StartTimestamp:     startTimeUnixNano,
				ExecutionTimestamp: executionTimeUnixNano,
				CloseTimestamp:     endTimeUnixNano,
				CloseStatus:        closeStatus,
				HistoryLength:      historyLength,
				Memo:               visibilityMemo,
				SearchAttributes:   searchAttributes,
				VisibilityURI:      domainEntry.GetConfig().VisibilityArchivalURI,
				URI:                domainEntry.GetConfig().HistoryArchivalURI,
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
	startEvent *workflow.HistoryEvent,
) time.Time {
	// Use value 0 to represent workflows that don't need backoff. Since ES doesn't support
	// comparison between two field, we need a value to differentiate them from cron workflows
	// or later runs of a workflow that needs retry.
	executionTimestamp := time.Unix(0, 0)
	if startEvent == nil {
		return executionTimestamp
	}

	if backoffSeconds := startEvent.WorkflowExecutionStartedEventAttributes.GetFirstDecisionTaskBackoffSeconds(); backoffSeconds != 0 {
		startTimestamp := time.Unix(0, startEvent.GetTimestamp())
		executionTimestamp = startTimestamp.Add(time.Duration(backoffSeconds) * time.Second)
	}
	return executionTimestamp
}

func getWorkflowMemo(
	memo map[string][]byte,
) *workflow.Memo {

	if memo == nil {
		return nil
	}
	return &workflow.Memo{Fields: memo}
}

func copySearchAttributes(
	input map[string][]byte,
) map[string][]byte {

	if input == nil {
		return nil
	}

	result := make(map[string][]byte)
	for k, v := range input {
		val := make([]byte, len(v))
		copy(val, v)
		result[k] = val
	}
	return result
}

func isWorkflowNotExistError(err error) bool {
	_, ok := err.(*workflow.EntityNotExistsError)
	return ok
}
