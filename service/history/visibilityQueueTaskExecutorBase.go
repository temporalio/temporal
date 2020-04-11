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
	"time"

	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/client/matching"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
)

const (
	visibilityActiveTaskDefaultTimeout = 30 * time.Second
)

type (
	visibilityQueueTaskExecutorBase struct {
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

func newVisibilityQueueTaskExecutorBase(
	shard ShardContext,
	historyService *historyEngineImpl,
	logger log.Logger,
	metricsClient metrics.Client,
	config *Config,
) *visibilityQueueTaskExecutorBase {
	return &visibilityQueueTaskExecutorBase{
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

func (t *visibilityQueueTaskExecutorBase) getDomainIDAndWorkflowExecution(
	task *persistenceblobs.VisibilityTaskInfo,
) (string, commonproto.WorkflowExecution) {

	return primitives.UUIDString(task.DomainID), commonproto.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      primitives.UUIDString(task.RunID),
	}
}

func (t *visibilityQueueTaskExecutorBase) recordWorkflowStarted(
	domainID string,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTimeUnixNano int64,
	executionTimeUnixNano int64,
	workflowTimeout int32,
	taskID int64,
	visibilityMemo *commonproto.Memo,
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
		Execution: commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
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

func (t *visibilityQueueTaskExecutorBase) upsertWorkflowExecution(
	domainID string,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTimeUnixNano int64,
	executionTimeUnixNano int64,
	workflowTimeout int32,
	taskID int64,
	visibilityMemo *commonproto.Memo,
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
		Execution: commonproto.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
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

func (t *visibilityQueueTaskExecutorBase) recordWorkflowClosed(
	domainID string,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTimeUnixNano int64,
	executionTimeUnixNano int64,
	endTimeUnixNano int64,
	closeStatus enums.WorkflowExecutionCloseStatus,
	historyLength int64,
	taskID int64,
	visibilityMemo *commonproto.Memo,
	searchAttributes map[string][]byte,
) error {

	// Record closing in visibility store
	retentionSeconds := int64(0)
	domain := defaultDomainName
	recordWorkflowClose := true

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
	}

	if recordWorkflowClose {
		if err := t.visibilityMgr.RecordWorkflowExecutionClosed(&persistence.RecordWorkflowExecutionClosedRequest{
			DomainUUID: domainID,
			Domain:     domain,
			Execution: commonproto.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
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

	return nil
}
