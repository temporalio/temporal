//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination delete_manager_mock.go

package workflow

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/worker/archiver"
)

type (
	DeleteManager interface {
		DeleteWorkflowExecution(ctx context.Context, namespaceID namespace.ID, we commonpb.WorkflowExecution, archiveIfEnabled bool) (retError error)
		DeleteWorkflowExecutionFromTimerTask(namespaceID namespace.ID, we commonpb.WorkflowExecution, weCtx Context, ms MutableState, timerTaskVersion int64) (retError error)
	}

	DeleteManagerImpl struct {
		shard          shard.Context
		historyCache   Cache
		config         *configs.Config
		metricsClient  metrics.Client
		archivalClient archiver.Client
	}
)

var _ DeleteManager = (*DeleteManagerImpl)(nil)

func NewDeleteManager(
	shard shard.Context,
	cache Cache,
	config *configs.Config,
	archiverClient archiver.Client,
) *DeleteManagerImpl {
	deleteManager := &DeleteManagerImpl{
		shard:          shard,
		historyCache:   cache,
		metricsClient:  shard.GetMetricsClient(),
		config:         config,
		archivalClient: archiverClient,
	}

	return deleteManager
}

func (e *DeleteManagerImpl) DeleteWorkflowExecution(
	ctx context.Context,
	namespaceID namespace.ID,
	we commonpb.WorkflowExecution,
	archiveIfEnabled bool,
) (retError error) {

	weCtx, release, err := e.historyCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		we,
		CallerTypeAPI,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := weCtx.LoadWorkflowExecution()
	if err != nil {
		return err
	}

	return e.deleteWorkflowExecutionInternal(
		namespaceID,
		we,
		weCtx,
		mutableState,
		mutableState.GetCurrentVersion(),
		archiveIfEnabled,
		e.metricsClient.Scope(metrics.HistoryDeleteWorkflowExecutionScope),
	)
}

func (e *DeleteManagerImpl) DeleteWorkflowExecutionFromTimerTask(
	namespaceID namespace.ID,
	we commonpb.WorkflowExecution,
	weCtx Context,
	ms MutableState,
	timerTaskVersion int64,
) (retError error) {

	err := e.deleteWorkflowExecutionInternal(
		namespaceID,
		we,
		weCtx,
		ms,
		timerTaskVersion,
		true,
		e.metricsClient.Scope(metrics.HistoryProcessDeleteHistoryEventScope),
	)

	if err != nil && errors.Is(err, consts.ErrWorkflowIsRunning) {
		// If workflow is running then just ignore DeleteHistoryEventTask timer task.
		// This should almost never happen because DeleteHistoryEventTask is created only for closed workflows.
		// But cross DC replication can resurrect workflow and therefore DeleteHistoryEventTask should be ignored.
		return nil
	}

	return err
}

func (e *DeleteManagerImpl) deleteWorkflowExecutionInternal(
	namespaceID namespace.ID,
	we commonpb.WorkflowExecution,
	weCtx Context,
	ms MutableState,
	deleteVisibilityTaskVersion int64,
	archiveIfEnabled bool,
	scope metrics.Scope,
) error {

	if ms.IsWorkflowExecutionRunning() {
		return consts.ErrWorkflowIsRunning
	}

	currentBranchToken, err := ms.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	shouldDeleteHistory := true
	if archiveIfEnabled {
		shouldDeleteHistory, err = e.archiveWorkflowIfEnabled(namespaceID, we, currentBranchToken, weCtx, ms, scope)
		if err != nil {
			return err
		}
	}

	if !shouldDeleteHistory {
		// currentBranchToken == nil means don't delete history.
		currentBranchToken = nil
	}
	if err := e.shard.DeleteWorkflowExecution(
		definition.WorkflowKey{
			NamespaceID: namespaceID.String(),
			WorkflowID:  we.GetWorkflowId(),
			RunID:       we.GetRunId(),
		},
		currentBranchToken,
		deleteVisibilityTaskVersion,
	); err != nil {
		return err
	}

	// Clear workflow execution context here to prevent further readers to get stale copy of non-exiting workflow execution.
	weCtx.Clear()

	scope.IncCounter(metrics.WorkflowCleanupDeleteCount)
	return nil
}

func (e *DeleteManagerImpl) archiveWorkflowIfEnabled(
	namespaceID namespace.ID,
	workflowExecution commonpb.WorkflowExecution,
	currentBranchToken []byte,
	weCtx Context,
	mutableState MutableState,
	scope metrics.Scope,
) (bool, error) {

	namespaceRegistryEntry, err := e.shard.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return false, err
	}
	clusterConfiguredForHistoryArchival := e.shard.GetArchivalMetadata().GetHistoryConfig().ClusterConfiguredForArchival()
	namespaceConfiguredForHistoryArchival := namespaceRegistryEntry.HistoryArchivalState().State == enumspb.ARCHIVAL_STATE_ENABLED
	archiveHistory := clusterConfiguredForHistoryArchival && namespaceConfiguredForHistoryArchival

	// TODO: @ycyang once archival backfill is in place cluster:paused && namespace:enabled should be a nop rather than a delete
	if !archiveHistory {
		return true, nil
	}

	closeFailoverVersion, err := mutableState.GetLastWriteVersion()
	if err != nil {
		return false, err
	}

	req := &archiver.ClientRequest{
		ArchiveRequest: &archiver.ArchiveRequest{
			NamespaceID:          namespaceID.String(),
			WorkflowID:           workflowExecution.GetWorkflowId(),
			RunID:                workflowExecution.GetRunId(),
			Namespace:            namespaceRegistryEntry.Name().String(),
			ShardID:              e.shard.GetShardID(),
			Targets:              []archiver.ArchivalTarget{archiver.ArchiveTargetHistory},
			HistoryURI:           namespaceRegistryEntry.HistoryArchivalState().URI,
			NextEventID:          mutableState.GetNextEventID(),
			BranchToken:          currentBranchToken,
			CloseFailoverVersion: closeFailoverVersion,
		},
		CallerService:        common.HistoryServiceName,
		AttemptArchiveInline: false, // archive in workflow by default
	}
	executionStats, err := weCtx.LoadExecutionStats()
	if err == nil && executionStats.HistorySize < int64(e.config.TimerProcessorHistoryArchivalSizeLimit()) {
		req.AttemptArchiveInline = true
	}

	saTypeMap, err := e.shard.GetSearchAttributesProvider().GetSearchAttributes(e.config.DefaultVisibilityIndexName, false)
	if err != nil {
		return false, err
	}
	// Setting search attributes types here because archival client needs to stringify them,
	// and it might not have access to typeMap (i.e. type needs to be embedded).
	searchattribute.ApplyTypeMap(req.ArchiveRequest.SearchAttributes, saTypeMap)

	ctx, cancel := context.WithTimeout(context.Background(), e.config.TimerProcessorArchivalTimeLimit())
	defer cancel()
	resp, err := e.archivalClient.Archive(ctx, req)
	if err != nil {
		return false, err
	}

	var deleteHistory bool
	if resp.HistoryArchivedInline {
		scope.IncCounter(metrics.WorkflowCleanupDeleteHistoryInlineCount)
		deleteHistory = true
	} else {
		scope.IncCounter(metrics.WorkflowCleanupArchiveCount)
		// Don't delete workflow history if it wasn't achieve inline because archival workflow will need it.
		deleteHistory = false
	}

	return deleteHistory, nil
}
