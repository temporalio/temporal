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
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/archiver"
)

const (
	taskTimeout             = time.Second * 3
	taskGetExecutionTimeout = time.Second
	taskHistoryOpTimeout    = 20 * time.Second
)

type (
	transferQueueTaskExecutorBase struct {
		currentClusterName       string
		shard                    shard.Context
		registry                 namespace.Registry
		cache                    workflow.Cache
		archivalClient           archiver.Client
		logger                   log.Logger
		metricProvider           metrics.MetricsHandler
		metricsClient            metrics.Client
		historyClient            historyservice.HistoryServiceClient
		matchingClient           matchingservice.MatchingServiceClient
		config                   *configs.Config
		searchAttributesProvider searchattribute.Provider
		workflowDeleteManager    workflow.DeleteManager
	}
)

func newTransferQueueTaskExecutorBase(
	shard shard.Context,
	workflowCache workflow.Cache,
	archivalClient archiver.Client,
	logger log.Logger,
	metricProvider metrics.MetricsHandler,
	matchingClient matchingservice.MatchingServiceClient,
) *transferQueueTaskExecutorBase {
	return &transferQueueTaskExecutorBase{
		currentClusterName:       shard.GetClusterMetadata().GetCurrentClusterName(),
		shard:                    shard,
		registry:                 shard.GetNamespaceRegistry(),
		cache:                    workflowCache,
		archivalClient:           archivalClient,
		logger:                   logger,
		metricProvider:           metricProvider,
		metricsClient:            shard.GetMetricsClient(),
		historyClient:            shard.GetHistoryClient(),
		matchingClient:           matchingClient,
		config:                   shard.GetConfig(),
		searchAttributesProvider: shard.GetSearchAttributesProvider(),
		workflowDeleteManager: workflow.NewDeleteManager(
			shard,
			workflowCache,
			shard.GetConfig(),
			archivalClient,
			shard.GetTimeSource(),
		),
	}
}

func (t *transferQueueTaskExecutorBase) pushActivity(
	ctx context.Context,
	task *tasks.ActivityTask,
	activityScheduleToStartTimeout *time.Duration,
) error {
	_, err := t.matchingClient.AddActivityTask(ctx, &matchingservice.AddActivityTaskRequest{
		NamespaceId:       task.NamespaceID,
		SourceNamespaceId: task.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: task.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		ScheduledEventId:       task.ScheduledEventID,
		ScheduleToStartTimeout: activityScheduleToStartTimeout,
		Clock:                  vclock.NewVectorClock(t.shard.GetClusterMetadata().GetClusterID(), t.shard.GetShardID(), task.TaskID),
	})
	if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
		// NotFound error is not expected for AddTasks calls
		// but will be ignored by task error handling logic, so log it here
		tasks.InitializeLogger(task, t.logger).Error("Matching returned not found error for AddActivityTask", tag.Error(err))
	}

	return err
}

func (t *transferQueueTaskExecutorBase) pushWorkflowTask(
	ctx context.Context,
	task *tasks.WorkflowTask,
	taskqueue *taskqueuepb.TaskQueue,
	workflowTaskScheduleToStartTimeout *time.Duration,
) error {
	_, err := t.matchingClient.AddWorkflowTask(ctx, &matchingservice.AddWorkflowTaskRequest{
		NamespaceId: task.NamespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: task.WorkflowID,
			RunId:      task.RunID,
		},
		TaskQueue:              taskqueue,
		ScheduledEventId:       task.ScheduledEventID,
		ScheduleToStartTimeout: workflowTaskScheduleToStartTimeout,
		Clock:                  vclock.NewVectorClock(t.shard.GetClusterMetadata().GetClusterID(), t.shard.GetShardID(), task.TaskID),
	})
	if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
		// NotFound error is not expected for AddTasks calls
		// but will be ignored by task error handling logic, so log it here
		tasks.InitializeLogger(task, t.logger).Error("Matching returned not found error for AddWorkflowTask", tag.Error(err))
	}

	return err
}

func (t *transferQueueTaskExecutorBase) archiveVisibility(
	ctx context.Context,
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
	namespaceEntry, err := t.registry.GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	clusterConfiguredForVisibilityArchival := t.shard.GetArchivalMetadata().GetVisibilityConfig().ClusterConfiguredForArchival()
	namespaceConfiguredForVisibilityArchival := namespaceEntry.VisibilityArchivalState().State == enumspb.ARCHIVAL_STATE_ENABLED
	archiveVisibility := clusterConfiguredForVisibilityArchival && namespaceConfiguredForVisibilityArchival

	if !archiveVisibility {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, t.config.TransferProcessorVisibilityArchivalTimeLimit())
	defer cancel()

	saTypeMap, err := t.searchAttributesProvider.GetSearchAttributes(t.config.DefaultVisibilityIndexName, false)
	if err != nil {
		return err
	}

	// Setting search attributes types here because archival client needs to stringify them
	// and it might not have access to type map (i.e. type needs to be embedded).
	searchattribute.ApplyTypeMap(searchAttributes, saTypeMap)

	_, err = t.archivalClient.Archive(ctx, &archiver.ClientRequest{
		ArchiveRequest: &archiver.ArchiveRequest{
			ShardID:          t.shard.GetShardID(),
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

func (t *transferQueueTaskExecutorBase) processDeleteExecutionTask(
	ctx context.Context,
	task *tasks.DeleteExecutionTask,
) error {
	return t.deleteExecution(ctx, task, false)
}

func (t *transferQueueTaskExecutorBase) deleteExecution(
	ctx context.Context,
	task tasks.Task,
	forceDeleteFromOpenVisibility bool,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}

	weCtx, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespace.ID(task.GetNamespaceID()),
		workflowExecution,
		workflow.CallerTypeTask,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTransferTask(ctx, weCtx, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}

	// Do not validate version here because DeleteExecutionTask transfer task is created only for
	// explicit API call, and in this case execution needs to be deleted regardless of the version.

	return t.workflowDeleteManager.DeleteWorkflowExecution(
		ctx,
		namespace.ID(task.GetNamespaceID()),
		workflowExecution,
		weCtx,
		mutableState,
		forceDeleteFromOpenVisibility,
	)
}
