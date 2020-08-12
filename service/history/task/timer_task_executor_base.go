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

package task

import (
	"context"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/worker/archiver"
)

var (
	persistenceOperationRetryPolicy = common.CreatePersistenceRetryPolicy()
)

type (
	timerTaskExecutorBase struct {
		shard          shard.Context
		archiverClient archiver.Client
		executionCache *execution.Cache
		logger         log.Logger
		metricsClient  metrics.Client
		config         *config.Config
	}
)

func newTimerTaskExecutorBase(
	shard shard.Context,
	archiverClient archiver.Client,
	executionCache *execution.Cache,
	logger log.Logger,
	metricsClient metrics.Client,
	config *config.Config,
) *timerTaskExecutorBase {
	return &timerTaskExecutorBase{
		shard:          shard,
		archiverClient: archiverClient,
		executionCache: executionCache,
		logger:         logger,
		metricsClient:  metricsClient,
		config:         config,
	}
}

func (t *timerTaskExecutorBase) executeDeleteHistoryEventTask(
	task *persistence.TimerTaskInfo,
) (retError error) {

	context, release, err := t.executionCache.GetOrCreateWorkflowExecutionWithTimeout(
		task.DomainID,
		getWorkflowExecution(task),
		taskDefaultTimeout,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(context, task, t.metricsClient, t.logger)
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

	domainCacheEntry, err := t.shard.GetDomainCache().GetDomainByID(task.DomainID)
	if err != nil {
		return err
	}
	clusterConfiguredForHistoryArchival := t.shard.GetService().GetArchivalMetadata().GetHistoryConfig().ClusterConfiguredForArchival()
	domainConfiguredForHistoryArchival := domainCacheEntry.GetConfig().HistoryArchivalStatus == workflow.ArchivalStatusEnabled
	archiveHistory := clusterConfiguredForHistoryArchival && domainConfiguredForHistoryArchival

	// TODO: @ycyang once archival backfill is in place cluster:paused && domain:enabled should be a nop rather than a delete
	if archiveHistory {
		t.metricsClient.IncCounter(metrics.HistoryProcessDeleteHistoryEventScope, metrics.WorkflowCleanupArchiveCount)
		return t.archiveWorkflow(task, context, mutableState, domainCacheEntry)
	}

	t.metricsClient.IncCounter(metrics.HistoryProcessDeleteHistoryEventScope, metrics.WorkflowCleanupDeleteCount)
	return t.deleteWorkflow(task, context, mutableState)
}

func (t *timerTaskExecutorBase) deleteWorkflow(
	task *persistence.TimerTaskInfo,
	context execution.Context,
	msBuilder execution.MutableState,
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
	context.Clear()
	return nil
}

func (t *timerTaskExecutorBase) archiveWorkflow(
	task *persistence.TimerTaskInfo,
	workflowContext execution.Context,
	msBuilder execution.MutableState,
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
			DomainID:             task.DomainID,
			WorkflowID:           task.WorkflowID,
			RunID:                task.RunID,
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
	executionStats, err := workflowContext.LoadExecutionStats()
	if err == nil && executionStats.HistorySize < int64(t.config.TimerProcessorHistoryArchivalSizeLimit()) {
		req.AttemptArchiveInline = true
	}

	ctx, cancel := context.WithTimeout(context.Background(), t.config.TimerProcessorArchivalTimeLimit())
	defer cancel()
	resp, err := t.archiverClient.Archive(ctx, req)
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
	workflowContext.Clear()
	return nil
}

func (t *timerTaskExecutorBase) deleteWorkflowExecution(
	task *persistence.TimerTaskInfo,
) error {

	op := func() error {
		return t.shard.GetExecutionManager().DeleteWorkflowExecution(&persistence.DeleteWorkflowExecutionRequest{
			DomainID:   task.DomainID,
			WorkflowID: task.WorkflowID,
			RunID:      task.RunID,
		})
	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerTaskExecutorBase) deleteCurrentWorkflowExecution(
	task *persistence.TimerTaskInfo,
) error {

	op := func() error {
		return t.shard.GetExecutionManager().DeleteCurrentWorkflowExecution(&persistence.DeleteCurrentWorkflowExecutionRequest{
			DomainID:   task.DomainID,
			WorkflowID: task.WorkflowID,
			RunID:      task.RunID,
		})
	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerTaskExecutorBase) deleteWorkflowHistory(
	task *persistence.TimerTaskInfo,
	msBuilder execution.MutableState,
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

func (t *timerTaskExecutorBase) deleteWorkflowVisibility(
	task *persistence.TimerTaskInfo,
) error {

	op := func() error {
		request := &persistence.VisibilityDeleteWorkflowExecutionRequest{
			DomainID:   task.DomainID,
			WorkflowID: task.WorkflowID,
			RunID:      task.RunID,
			TaskID:     task.TaskID,
		}
		// TODO: expose GetVisibilityManager method on shardContext interface
		return t.shard.GetService().GetVisibilityManager().DeleteWorkflowExecution(request) // delete from db
	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}
