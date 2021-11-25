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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
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

type (
	timerQueueTaskExecutorBase struct {
		shard                    shard.Context
		historyService           *historyEngineImpl
		cache                    workflow.Cache
		logger                   log.Logger
		metricsClient            metrics.Client
		config                   *configs.Config
		searchAttributesProvider searchattribute.Provider
	}
)

func newTimerQueueTaskExecutorBase(
	shard shard.Context,
	historyEngine *historyEngineImpl,
	logger log.Logger,
	metricsClient metrics.Client,
	config *configs.Config,
) *timerQueueTaskExecutorBase {
	return &timerQueueTaskExecutorBase{
		shard:                    shard,
		historyService:           historyEngine,
		cache:                    historyEngine.historyCache,
		logger:                   logger,
		metricsClient:            metricsClient,
		config:                   config,
		searchAttributesProvider: shard.GetSearchAttributesProvider(),
	}
}

func (t *timerQueueTaskExecutorBase) executeDeleteHistoryEventTask(
	ctx context.Context,
	task *tasks.DeleteHistoryEventTask,
) (retError error) {
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, taskTimeout)

	defer cancel()

	namespaceID, execution := t.getNamespaceIDAndWorkflowExecution(task)
	weContext, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(weContext, task, t.metricsClient, t.logger)
	if err != nil {
		return err
	}
	if mutableState == nil || mutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	lastWriteVersion, err := mutableState.GetLastWriteVersion()
	if err != nil {
		return err
	}
	ok, err := verifyTaskVersion(t.shard, t.logger, namespace.ID(task.NamespaceID), lastWriteVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	namespaceRegistryEntry, err := t.shard.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(task.NamespaceID))
	if err != nil {
		return err
	}
	clusterConfiguredForHistoryArchival := t.shard.GetArchivalMetadata().GetHistoryConfig().ClusterConfiguredForArchival()
	namespaceConfiguredForHistoryArchival := namespaceRegistryEntry.HistoryArchivalState().State == enumspb.ARCHIVAL_STATE_ENABLED
	archiveHistory := clusterConfiguredForHistoryArchival && namespaceConfiguredForHistoryArchival

	// TODO: @ycyang once archival backfill is in place cluster:paused && namespace:enabled should be a nop rather than a delete
	if archiveHistory {
		t.metricsClient.IncCounter(metrics.HistoryProcessDeleteHistoryEventScope, metrics.WorkflowCleanupArchiveCount)
		return t.archiveWorkflow(task, weContext, mutableState, namespaceRegistryEntry)
	}

	t.metricsClient.IncCounter(metrics.HistoryProcessDeleteHistoryEventScope, metrics.WorkflowCleanupDeleteCount)
	return t.deleteWorkflow(task, weContext, mutableState)
}

func (t *timerQueueTaskExecutorBase) deleteWorkflow(
	task *tasks.DeleteHistoryEventTask,
	workflowContext workflow.Context,
	msBuilder workflow.MutableState,
) error {
	branchToken, err := msBuilder.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	if err := t.shard.DeleteWorkflowExecution(
		definition.WorkflowKey{
			NamespaceID: task.NamespaceID,
			WorkflowID:  task.WorkflowID,
			RunID:       task.RunID,
		},
		branchToken,
		task.Version,
	); err != nil {
		return err
	}

	// calling clear here to force accesses of mutable state to read database
	// if this is not called then callers will get mutable state even though its been removed from database
	workflowContext.Clear()
	return nil
}

func (t *timerQueueTaskExecutorBase) archiveWorkflow(
	task *tasks.DeleteHistoryEventTask,
	workflowContext workflow.Context,
	msBuilder workflow.MutableState,
	namespaceRegistryEntry *namespace.Namespace,
) error {
	branchToken, err := msBuilder.GetCurrentBranchToken()
	if err != nil {
		return err
	}
	closeFailoverVersion, err := msBuilder.GetLastWriteVersion()
	if err != nil {
		return err
	}

	req := &archiver.ClientRequest{
		ArchiveRequest: &archiver.ArchiveRequest{
			NamespaceID:          task.NamespaceID,
			WorkflowID:           task.WorkflowID,
			RunID:                task.RunID,
			Namespace:            namespaceRegistryEntry.Name().String(),
			ShardID:              t.shard.GetShardID(),
			Targets:              []archiver.ArchivalTarget{archiver.ArchiveTargetHistory},
			HistoryURI:           namespaceRegistryEntry.HistoryArchivalState().URI,
			NextEventID:          msBuilder.GetNextEventID(),
			BranchToken:          branchToken,
			CloseFailoverVersion: closeFailoverVersion,
		},
		CallerService:        common.HistoryServiceName,
		AttemptArchiveInline: false, // archive in workflow by default
	}
	executionStats, err := workflowContext.LoadExecutionStats()
	if err == nil && executionStats.HistorySize < int64(t.config.TimerProcessorHistoryArchivalSizeLimit()) {
		req.AttemptArchiveInline = true
	}

	saTypeMap, err := t.searchAttributesProvider.GetSearchAttributes(t.config.DefaultVisibilityIndexName, false)
	if err != nil {
		return err
	}
	// Setting search attributes types here because archival client needs to stringify them
	// and it might not have access to typeMap (i.e. type needs to be embedded).
	searchattribute.ApplyTypeMap(req.ArchiveRequest.SearchAttributes, saTypeMap)

	ctx, cancel := context.WithTimeout(context.Background(), t.config.TimerProcessorArchivalTimeLimit())
	defer cancel()
	resp, err := t.historyService.archivalClient.Archive(ctx, req)
	if err != nil {
		return err
	}

	// delete visibility record here regardless if it's been archived inline or not
	// since the entire record is included as part of the archive request.

	// delete workflow history if history archival is not needed or history has been archived inline
	if resp.HistoryArchivedInline {
		// branchToken was retrieved above
		t.metricsClient.IncCounter(metrics.HistoryProcessDeleteHistoryEventScope, metrics.WorkflowCleanupDeleteHistoryInlineCount)
	} else {
		// branchToken == nil means don't delete history
		branchToken = nil
	}

	if err := t.shard.DeleteWorkflowExecution(
		definition.WorkflowKey{
			NamespaceID: task.NamespaceID,
			WorkflowID:  task.WorkflowID,
			RunID:       task.RunID,
		},
		branchToken,
		task.Version,
	); err != nil {
		return err
	}

	// calling clear here to force accesses of mutable state to read database
	// if this is not called then callers will get mutable state even though its been removed from database
	workflowContext.Clear()
	return nil
}

func (t *timerQueueTaskExecutorBase) getNamespaceIDAndWorkflowExecution(
	task tasks.Task,
) (namespace.ID, commonpb.WorkflowExecution) {

	return namespace.ID(task.GetNamespaceID()), commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}
}
