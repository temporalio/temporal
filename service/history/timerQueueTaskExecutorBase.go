package history

import (
	"context"

	executionpb "go.temporal.io/temporal-proto/execution"
	namespacepb "go.temporal.io/temporal-proto/namespace"

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

	weContext, release, err := t.cache.getOrCreateWorkflowExecutionForBackground(t.getNamespaceIDAndWorkflowExecution(task))
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
	ok, err := verifyTaskVersion(t.shard, t.logger, task.GetNamespaceId(), lastWriteVersion, task.Version, task)
	if err != nil || !ok {
		return err
	}

	namespaceCacheEntry, err := t.shard.GetNamespaceCache().GetNamespaceByID(primitives.UUIDString(task.GetNamespaceId()))
	if err != nil {
		return err
	}
	clusterConfiguredForHistoryArchival := t.shard.GetService().GetArchivalMetadata().GetHistoryConfig().ClusterConfiguredForArchival()
	namespaceConfiguredForHistoryArchival := namespaceCacheEntry.GetConfig().HistoryArchivalStatus == namespacepb.ArchivalStatus_Enabled
	archiveHistory := clusterConfiguredForHistoryArchival && namespaceConfiguredForHistoryArchival

	// TODO: @ycyang once archival backfill is in place cluster:paused && namespace:enabled should be a nop rather than a delete
	if archiveHistory {
		t.metricsClient.IncCounter(metrics.HistoryProcessDeleteHistoryEventScope, metrics.WorkflowCleanupArchiveCount)
		return t.archiveWorkflow(task, weContext, mutableState, namespaceCacheEntry)
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
	namespaceCacheEntry *cache.NamespaceCacheEntry,
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
			NamespaceID:          primitives.UUIDString(task.GetNamespaceId()),
			WorkflowID:           task.GetWorkflowId(),
			RunID:                primitives.UUIDString(task.GetRunId()),
			Namespace:            namespaceCacheEntry.GetInfo().Name,
			ShardID:              t.shard.GetShardID(),
			Targets:              []archiver.ArchivalTarget{archiver.ArchiveTargetHistory},
			URI:                  namespaceCacheEntry.GetConfig().HistoryArchivalURI,
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
			NamespaceID: primitives.UUIDString(task.GetNamespaceId()),
			WorkflowID:  task.GetWorkflowId(),
			RunID:       primitives.UUIDString(task.GetRunId()),
		})
	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerQueueTaskExecutorBase) deleteCurrentWorkflowExecution(
	task *persistenceblobs.TimerTaskInfo,
) error {

	op := func() error {
		return t.shard.GetExecutionManager().DeleteCurrentWorkflowExecution(&persistence.DeleteCurrentWorkflowExecutionRequest{
			NamespaceID: primitives.UUIDString(task.GetNamespaceId()),
			WorkflowID:  task.GetWorkflowId(),
			RunID:       primitives.UUIDString(task.GetRunId()),
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
			NamespaceID: primitives.UUIDString(task.GetNamespaceId()),
			WorkflowID:  task.GetWorkflowId(),
			RunID:       primitives.UUIDString(task.GetRunId()),
			TaskID:      task.GetTaskId(),
		}
		// TODO: expose GetVisibilityManager method on shardContext interface
		return t.shard.GetService().GetVisibilityManager().DeleteWorkflowExecution(request) // delete from db
	}
	return backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
}

func (t *timerQueueTaskExecutorBase) getNamespaceIDAndWorkflowExecution(
	task *persistenceblobs.TimerTaskInfo,
) (string, executionpb.WorkflowExecution) {

	return primitives.UUIDString(task.GetNamespaceId()), executionpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowId(),
		RunId:      primitives.UUIDString(task.GetRunId()),
	}
}
