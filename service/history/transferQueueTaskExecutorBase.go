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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/api/matchingservice/v1"
	m "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/archiver"
)

const (
	transferActiveTaskDefaultTimeout = 20 * time.Second
)

type (
	transferQueueTaskExecutorBase struct {
		shard                    shard.Context
		historyService           *historyEngineImpl
		cache                    workflow.Cache
		logger                   log.Logger
		metricsClient            metrics.Client
		matchingClient           matchingservice.MatchingServiceClient
		config                   *configs.Config
		searchAttributesProvider searchattribute.Provider
	}
)

func newTransferQueueTaskExecutorBase(
	shard shard.Context,
	historyEngine *historyEngineImpl,
	logger log.Logger,
	metricsClient metrics.Client,
	config *configs.Config,
	matchingClient matchingservice.MatchingServiceClient,
) *transferQueueTaskExecutorBase {
	return &transferQueueTaskExecutorBase{
		shard:                    shard,
		historyService:           historyEngine,
		cache:                    historyEngine.historyCache,
		logger:                   logger,
		metricsClient:            metricsClient,
		matchingClient:           matchingClient,
		config:                   config,
		searchAttributesProvider: shard.GetSearchAttributesProvider(),
	}
}

func (t *transferQueueTaskExecutorBase) getNamespaceIDAndWorkflowExecution(
	task tasks.Task,
) (namespace.ID, commonpb.WorkflowExecution) {

	return namespace.ID(task.GetNamespaceID()), commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}
}

func (t *transferQueueTaskExecutorBase) pushActivity(
	task *tasks.ActivityTask,
	activityScheduleToStartTimeout *time.Duration,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	_, err := t.matchingClient.AddActivityTask(ctx, &m.AddActivityTaskRequest{
		NamespaceId:       task.TargetNamespaceID,
		SourceNamespaceId: task.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: task.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		ScheduleId:             task.ScheduleID,
		ScheduleToStartTimeout: activityScheduleToStartTimeout,
	})

	return err
}

func (t *transferQueueTaskExecutorBase) pushWorkflowTask(
	task *tasks.WorkflowTask,
	taskqueue *taskqueuepb.TaskQueue,
	workflowTaskScheduleToStartTimeout *time.Duration,
) error {

	ctx, cancel := context.WithTimeout(context.Background(), transferActiveTaskDefaultTimeout)
	defer cancel()

	_, err := t.matchingClient.AddWorkflowTask(ctx, &m.AddWorkflowTaskRequest{
		NamespaceId: task.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		TaskQueue:              taskqueue,
		ScheduleId:             task.ScheduleID,
		ScheduleToStartTimeout: workflowTaskScheduleToStartTimeout,
	})
	return err
}

func (t *transferQueueTaskExecutorBase) recordWorkflowClosed(
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	workflowTypeName string,
	startTime time.Time,
	executionTime time.Time,
	endTime time.Time,
	status enumspb.WorkflowExecutionStatus,
	historyLength int64,
	visibilityMemo *commonpb.Memo,
	searchAttributes *commonpb.SearchAttributes,
) error {

	namespaceEntry, err := t.shard.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	clusterConfiguredForVisibilityArchival := t.shard.GetArchivalMetadata().GetVisibilityConfig().ClusterConfiguredForArchival()
	namespaceConfiguredForVisibilityArchival := namespaceEntry.VisibilityArchivalState().State == enumspb.ARCHIVAL_STATE_ENABLED
	archiveVisibility := clusterConfiguredForVisibilityArchival && namespaceConfiguredForVisibilityArchival

	if !archiveVisibility {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.config.TransferProcessorVisibilityArchivalTimeLimit())
	defer cancel()

	saTypeMap, err := t.searchAttributesProvider.GetSearchAttributes(t.config.DefaultVisibilityIndexName, false)
	if err != nil {
		return err
	}

	// Setting search attributes types here because archival client needs to stringify them
	// and it might not have access to type map (i.e. type needs to be embedded).
	searchattribute.ApplyTypeMap(searchAttributes, saTypeMap)

	_, err = t.historyService.archivalClient.Archive(ctx, &archiver.ClientRequest{
		ArchiveRequest: &archiver.ArchiveRequest{
			NamespaceID:      namespaceID.String(),
			Namespace:        namespaceEntry.Name().String(),
			WorkflowID:       workflowID,
			RunID:            runID,
			WorkflowTypeName: workflowTypeName,
			StartTime:        startTime,
			ExecutionTime:    executionTime,
			CloseTime:        endTime,
			Status:           status,
			HistoryLength:    historyLength,
			Memo:             visibilityMemo,
			SearchAttributes: searchAttributes,
			VisibilityURI:    namespaceEntry.VisibilityArchivalState().URI,
			HistoryURI:       namespaceEntry.HistoryArchivalState().URI,
			Targets:          []archiver.ArchivalTarget{archiver.ArchiveTargetVisibility},
		},
		CallerService:        common.HistoryServiceName,
		AttemptArchiveInline: true, // archive visibility inline by default
	})

	return err
}
