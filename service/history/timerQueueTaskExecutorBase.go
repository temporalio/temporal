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

	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/service/worker/archiver"
)

type (
	timerQueueTaskExecutorBase struct {
		shard          ShardContext
		historyService *historyEngineImpl
		cache          *historyCache
		logger         log.Logger
		metricsClient  metrics.Client
		config         *Config
	}
)

func newTimerQueueTaskExecutorBase(
	shard ShardContext,
	historyService *historyEngineImpl,
	logger log.Logger,
	metricsClient metrics.Client,
	config *Config,
) *timerQueueTaskExecutorBase {
	return &timerQueueTaskExecutorBase{
		shard:          shard,
		historyService: historyService,
		cache:          historyService.historyCache,
		logger:         logger,
		metricsClient:  metricsClient,
		config:         config,
	}
}

func (t *timerQueueTaskExecutorBase) executeDeleteHistoryEventTask(
	task *persistenceblobs.TimerTaskInfo,
) (retError error) {

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(t.getDomainIDAndWorkflowExecution(task))
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
	ok, err := verifyTaskVersion(t.shard, t.logger, task.DomainID, lastWriteVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	domainCacheEntry, err := t.shard.GetDomainCache().GetDomainByID(primitives.UUIDString(task.DomainID))
	if err != nil {
		return err
	}
	clusterConfiguredForHistoryArchival := t.shard.GetService().GetArchivalMetadata().GetHistoryConfig().ClusterConfiguredForArchival()
	domainConfiguredForHistoryArchival := domainCacheEntry.GetConfig().HistoryArchivalStatus == enums.ArchivalStatusEnabled
	archiveHistory := clusterConfiguredForHistoryArchival && domainConfiguredForHistoryArchival

	// TODO: @ycyang once archival backfill is in place cluster:paused && domain:enabled should be a nop rather than a delete
	if archiveHistory {
		t.metricsClient.IncCounter(metrics.HistoryProcessDeleteHistoryEventScope, metrics.WorkflowCleanupArchiveCount)
		return t.archiveWorkflow(task, weContext, mutableState, domainCacheEntry)
	}

	t.metricsClient.IncCounter(metrics.HistoryProcessDeleteHistoryEventScope, metrics.WorkflowCleanupDeleteCount)
	return t.deleteWorkflow(task, weContext, mutableState)
}

func (t *timerQueueTaskExecutorBase) deleteWorkflow(
	task *persistenceblobs.TimerTaskInfo,
	context workflowExecutionContext,
	msBuilder mutableState,
) error {

	if err := t.deleteCurrentWorkflowExecution(task); err != nil {
		return err
	}

	if err := t.deleteWorkflowExecution(task); err != nil {
		return err
	}

	if err := t.deleteWorkflowHistory(task, msBuilder); err != nil {
		return err
	}

	if err := t.deleteWorkflowVisibility(task); err != nil {
		return err
	}
	// calling clear here to force accesses of mutable state to read database
	// if this is not called then callers will get mutable state even though its been removed from database
	context.clear()
	return nil
}

func (t *timerQueueTaskExecutorBase) archiveWorkflow(
	task *persistenceblobs.TimerTaskInfo,
	workflowContext workflowExecutionContext,
	msBuilder mutableState,
	domainCacheEntry *cache.DomainCacheEntry,
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
			DomainID:             primitives.UUIDString(task.DomainID),
			WorkflowID:           task.WorkflowID,
			RunID:                primitives.UUIDString(task.RunID),
			DomainName:           domainCacheEntry.GetInfo().Name,
			ShardID:              t.shard.GetShardID(),
			Targets:              []archiver.ArchivalTarget{archiver.ArchiveTargetHistory},
			URI:                  domainCacheEntry.GetConfig().HistoryArchivalURI,
			NextEventID:          msBuilder.GetNextEventID(),
			BranchToken:          branchToken,
			CloseFailoverVersion: closeFailoverVersion,
		},
		CallerService:        common.HistoryServiceName,
		AttemptArchiveInline: false, // archive in workflow by default
	}
	executionStats, err := workflowContext.loadExecutionStats()
	if err == nil && executionStats.HistorySize < int64(t.config.TimerProcessorHistoryArchivalSizeLimit()) {
		req.AttemptArchiveInline = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.config.TimerProcessorArchivalTimeLimit())
	defer cancel()
	resp, err := t.historyService.archivalClient.Archive(ctx, req)
	if err != nil {
		return err
	}

	if err := t.deleteCurrentWorkflowExecution(task); err != nil {
		return err
	}
	if err := t.deleteWorkflowExecution(task); err != nil {
		return err
	}
	// delete workflow history if history archival is not needed or history as been archived inline
	if resp.HistoryArchivedInline {
		t.metricsClient.IncCounter(metrics.HistoryProcessDeleteHistoryEventScope, metrics.WorkflowCleanupDeleteHistoryInlineCount)
		if err := t.deleteWorkflowHistory(task, msBuilder); err != nil {
			return err
		}
	}
	// delete visibility record here regardless if it's been archived inline or not
	// since the entire record is included as part of the archive request.
	if err := t.deleteWorkflowVisibility(task); err != nil {
		return err
	}
	// calling clear here to force accesses of mutable state to read database
	// if this is not called then callers will get mutable state even though its been removed from database
	workflowContext.clear()
	return nil
}

func (t *timerQueueTaskExecutorBase) deleteWorkflowExecution(
	task *persistenceblobs.TimerTaskInfo,
) error {

	op := func() error {
		return t.shard.GetExecutionManager().DeleteWorkflowExecution(&persistence.DeleteWorkflowExecutionRequest{
			DomainID:   primitives.UUIDString(task.DomainID),
			WorkflowID: task.WorkflowID,
			RunID:      primitives.UUIDString(task.RunID),
		})
	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerQueueTaskExecutorBase) deleteCurrentWorkflowExecution(
	task *persistenceblobs.TimerTaskInfo,
) error {

	op := func() error {
		return t.shard.GetExecutionManager().DeleteCurrentWorkflowExecution(&persistence.DeleteCurrentWorkflowExecutionRequest{
			DomainID:   primitives.UUIDString(task.DomainID),
			WorkflowID: task.WorkflowID,
			RunID:      primitives.UUIDString(task.RunID),
		})
	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerQueueTaskExecutorBase) deleteWorkflowHistory(
	task *persistenceblobs.TimerTaskInfo,
	msBuilder mutableState,
) error {

	op := func() error {
		branchToken, err := msBuilder.GetCurrentBranchToken()
		if err != nil {
			return err
		}
		return t.shard.GetHistoryManager().DeleteHistoryBranch(&persistence.DeleteHistoryBranchRequest{
			BranchToken: branchToken,
			ShardID:     common.IntPtr(t.shard.GetShardID()),
		})

	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerQueueTaskExecutorBase) deleteWorkflowVisibility(
	task *persistenceblobs.TimerTaskInfo,
) error {

	op := func() error {
		request := &persistence.VisibilityDeleteWorkflowExecutionRequest{
			DomainID:   primitives.UUIDString(task.DomainID),
			WorkflowID: task.WorkflowID,
			RunID:      primitives.UUIDString(task.RunID),
			TaskID:     task.TaskID,
		}
		// TODO: expose GetVisibilityManager method on shardContext interface
		return t.shard.GetService().GetVisibilityManager().DeleteWorkflowExecution(request) // delete from db
	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerQueueTaskExecutorBase) getDomainIDAndWorkflowExecution(
	task *persistenceblobs.TimerTaskInfo,
) (string, commonproto.WorkflowExecution) {

	return primitives.UUIDString(task.DomainID), commonproto.WorkflowExecution{
		WorkflowId: task.WorkflowID,
		RunId:      primitives.UUIDString(task.RunID),
	}
}
