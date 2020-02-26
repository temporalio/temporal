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

package history

import (
	ctx "context"
	"time"

	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/serviceerror"

	workflow "github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/.gen/proto/matchingservice"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/adapter"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/service/worker/archiver"
)

type (
	maxReadAckLevel func() int64

	updateTransferAckLevel func(ackLevel int64) error
	transferQueueShutdown  func() error

	transferQueueProcessorBase struct {
		shard                  ShardContext
		config                 *Config
		historyService         *historyEngineImpl
		options                *QueueProcessorOptions
		executionManager       persistence.ExecutionManager
		visibilityMgr          persistence.VisibilityManager
		matchingClient         matching.Client
		maxReadAckLevel        maxReadAckLevel
		updateTransferAckLevel updateTransferAckLevel
		transferQueueShutdown  transferQueueShutdown
		logger                 log.Logger
	}
)

const (
	secondsInDay = int32(24 * time.Hour / time.Second)

	defaultDomainName = "defaultDomainName"
)

func newTransferQueueProcessorBase(
	shard ShardContext,
	config *Config,
	historyService *historyEngineImpl,
	options *QueueProcessorOptions,
	visibilityMgr persistence.VisibilityManager,
	matchingClient matching.Client,
	maxReadAckLevel maxReadAckLevel,
	updateTransferAckLevel updateTransferAckLevel,
	transferQueueShutdown transferQueueShutdown,
	logger log.Logger,
) *transferQueueProcessorBase {

	return &transferQueueProcessorBase{
		shard:                  shard,
		config:                 config,
		historyService:         historyService,
		options:                options,
		executionManager:       shard.GetExecutionManager(),
		visibilityMgr:          visibilityMgr,
		matchingClient:         matchingClient,
		maxReadAckLevel:        maxReadAckLevel,
		updateTransferAckLevel: updateTransferAckLevel,
		transferQueueShutdown:  transferQueueShutdown,
		logger:                 logger,
	}
}

func (t *transferQueueProcessorBase) readTasks(
	readLevel int64,
) ([]queueTaskInfo, bool, error) {

	response, err := t.executionManager.GetTransferTasks(&persistence.GetTransferTasksRequest{
		ReadLevel:    readLevel,
		MaxReadLevel: t.maxReadAckLevel(),
		BatchSize:    t.options.BatchSize(),
	})

	if err != nil {
		return nil, false, err
	}

	tasks := make([]queueTaskInfo, len(response.Tasks))
	for i := range response.Tasks {
		tasks[i] = response.Tasks[i]
	}

	return tasks, len(response.NextPageToken) != 0, nil
}

func (t *transferQueueProcessorBase) updateAckLevel(
	ackLevel int64,
) error {

	return t.updateTransferAckLevel(ackLevel)
}

func (t *transferQueueProcessorBase) queueShutdown() error {
	return t.transferQueueShutdown()
}

func (t *transferQueueProcessorBase) getDomainIDAndWorkflowExecution(
	task *persistenceblobs.TransferTaskInfo,
) (string, workflow.WorkflowExecution) {

	return primitives.UUID(task.DomainID).String(), workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(task.WorkflowID),
		RunId:      common.StringPtr(primitives.UUID(task.RunID).String()),
	}
}

func (t *transferQueueProcessorBase) pushActivity(
	task *persistenceblobs.TransferTaskInfo,
	activityScheduleToStartTimeout int32,
) error {

	ctx, cancel := ctx.WithTimeout(ctx.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	if task.TaskType != persistence.TransferTaskTypeActivityTask {
		t.logger.Fatal("Cannot process non activity task", tag.TaskType(int(task.GetTaskType())))
	}

	_, err := t.matchingClient.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{
		DomainUUID:       primitives.UUID(task.TargetDomainID).String(),
		SourceDomainUUID: primitives.UUID(task.DomainID).String(),
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      primitives.UUID(task.RunID).String(),
		},
		TaskList:                      &commonproto.TaskList{Name: task.TaskList},
		ScheduleId:                    task.ScheduleID,
		ScheduleToStartTimeoutSeconds: activityScheduleToStartTimeout,
	})

	return err
}

func (t *transferQueueProcessorBase) pushDecision(
	task *persistenceblobs.TransferTaskInfo,
	tasklist *workflow.TaskList,
	decisionScheduleToStartTimeout int32,
) error {

	ctx, cancel := ctx.WithTimeout(ctx.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	if task.TaskType != persistence.TransferTaskTypeDecisionTask {
		t.logger.Fatal("Cannot process non decision task", tag.TaskType(int(task.GetTaskType())))
	}

	_, err := t.matchingClient.AddDecisionTask(ctx, &matchingservice.AddDecisionTaskRequest{
		DomainUUID: primitives.UUID(task.DomainID).String(),
		Execution: &commonproto.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      primitives.UUID(task.RunID).String(),
		},
		TaskList:                      adapter.ToProtoTaskList(tasklist),
		ScheduleId:                    task.ScheduleID,
		ScheduleToStartTimeoutSeconds: decisionScheduleToStartTimeout,
	})
	return err
}

func (t *transferQueueProcessorBase) recordWorkflowStarted(
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
		if _, ok := err.(*serviceerror.NotFound); !ok {
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

func (t *transferQueueProcessorBase) upsertWorkflowExecution(
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
		if _, ok := err.(*serviceerror.NotFound); !ok {
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

func (t *transferQueueProcessorBase) recordWorkflowClosed(
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
		ctx, cancel := ctx.WithTimeout(ctx.Background(), t.config.TransferProcessorVisibilityArchivalTimeLimit())
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
	_, ok := err.(*serviceerror.NotFound)
	return ok
}
