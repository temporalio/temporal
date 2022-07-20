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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination delete_manager_mock.go

package workflow

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/worker/archiver"
)

type (
	DeleteManager interface {
		AddDeleteWorkflowExecutionTask(ctx context.Context, nsID namespace.ID, we commonpb.WorkflowExecution, ms MutableState, transferQueueAckLevel int64, visibilityQueueAckLevel int64) error
		DeleteWorkflowExecution(ctx context.Context, nsID namespace.ID, we commonpb.WorkflowExecution, weCtx Context, ms MutableState, forceDeleteFromOpenVisibility bool) error
		DeleteWorkflowExecutionByRetention(ctx context.Context, nsID namespace.ID, we commonpb.WorkflowExecution, weCtx Context, ms MutableState) error
	}

	DeleteManagerImpl struct {
		shard          shard.Context
		historyCache   Cache
		config         *configs.Config
		metricsClient  metrics.Client
		archivalClient archiver.Client
		timeSource     clock.TimeSource
	}
)

var _ DeleteManager = (*DeleteManagerImpl)(nil)

func NewDeleteManager(
	shard shard.Context,
	cache Cache,
	config *configs.Config,
	archiverClient archiver.Client,
	timeSource clock.TimeSource,
) *DeleteManagerImpl {
	deleteManager := &DeleteManagerImpl{
		shard:          shard,
		historyCache:   cache,
		metricsClient:  shard.GetMetricsClient(),
		config:         config,
		archivalClient: archiverClient,
		timeSource:     timeSource,
	}

	return deleteManager
}

func (m *DeleteManagerImpl) AddDeleteWorkflowExecutionTask(
	ctx context.Context,
	nsID namespace.ID,
	we commonpb.WorkflowExecution,
	ms MutableState,
	transferQueueAckLevel int64,
	visibilityQueueAckLevel int64,
) error {

	// In active cluster, create `DeleteWorkflowExecutionTask` only if workflow is closed successfully
	// and all pending transfer and visibility tasks are completed.
	// This check is required to avoid race condition between close and delete tasks.
	// Otherwise, mutable state might be deleted before close task is executed, and therefore close task will be dropped.
	//
	// In passive cluster, transfer task queue check can be ignored but not visibility task queue.
	// If visibility close task is executed after visibility record is deleted then it will resurrect record in closed state.
	//
	// Unfortunately, queue ack levels are updated with delay (default 30s),
	// therefore this API will return error if workflow is deleted within 30 seconds after close.
	// The check is on API call side, not on task processor side, because delete visibility task doesn't have access to mutable state.
	if (ms.GetExecutionInfo().CloseTransferTaskId != 0 && // Workflow execution still might be running in passive cluster or closed before this field was added (v1.17).
		ms.GetExecutionInfo().CloseTransferTaskId > transferQueueAckLevel && // Transfer close task wasn't executed.
		ms.GetNamespaceEntry().ActiveInCluster(m.shard.GetClusterMetadata().GetCurrentClusterName())) ||
		(ms.GetExecutionInfo().CloseVisibilityTaskId != 0 && // Workflow execution still might be running in passive cluster or closed before this field was added (v1.17).
			ms.GetExecutionInfo().CloseVisibilityTaskId > visibilityQueueAckLevel) { // Visibility close task wasn't executed.

		return consts.ErrWorkflowNotReady
	}

	taskGenerator := taskGeneratorProvider.NewTaskGenerator(m.shard, ms)

	deleteTask, err := taskGenerator.GenerateDeleteExecutionTask(m.timeSource.Now())
	if err != nil {
		return err
	}

	return m.shard.AddTasks(ctx, &persistence.AddHistoryTasksRequest{
		ShardID: m.shard.GetShardID(),
		// RangeID is set by shard
		NamespaceID: nsID.String(),
		WorkflowID:  we.GetWorkflowId(),
		RunID:       we.GetRunId(),
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryTransfer: {deleteTask},
		},
	})
}

func (m *DeleteManagerImpl) DeleteWorkflowExecution(
	ctx context.Context,
	nsID namespace.ID,
	we commonpb.WorkflowExecution,
	weCtx Context,
	ms MutableState,
	forceDeleteFromOpenVisibility bool,
) error {

	return m.deleteWorkflowExecutionInternal(
		ctx,
		nsID,
		we,
		weCtx,
		ms,
		false,
		forceDeleteFromOpenVisibility,
		m.metricsClient.Scope(metrics.HistoryDeleteWorkflowExecutionScope),
	)
}

func (m *DeleteManagerImpl) DeleteWorkflowExecutionByRetention(
	ctx context.Context,
	nsID namespace.ID,
	we commonpb.WorkflowExecution,
	weCtx Context,
	ms MutableState,
) error {

	return m.deleteWorkflowExecutionInternal(
		ctx,
		nsID,
		we,
		weCtx,
		ms,
		true,  // When retention is fired, archive workflow execution.
		false, // When retention is fired, workflow execution is always closed.
		m.metricsClient.Scope(metrics.HistoryProcessDeleteHistoryEventScope),
	)
}

func (m *DeleteManagerImpl) deleteWorkflowExecutionInternal(
	ctx context.Context,
	namespaceID namespace.ID,
	we commonpb.WorkflowExecution,
	weCtx Context,
	ms MutableState,
	archiveIfEnabled bool,
	forceDeleteFromOpenVisibility bool,
	scope metrics.Scope,
) error {

	currentBranchToken, err := ms.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	shouldDeleteHistory := true
	if archiveIfEnabled {
		shouldDeleteHistory, err = m.archiveWorkflowIfEnabled(ctx, namespaceID, we, currentBranchToken, weCtx, ms, scope)
		if err != nil {
			return err
		}
	}

	if !shouldDeleteHistory {
		// currentBranchToken == nil means don't delete history.
		currentBranchToken = nil
	}

	// These two fields are needed for cassandra standard visibility.
	// TODO (alex): Remove them when cassandra standard visibility is removed.
	var startTime *time.Time
	var closeTime *time.Time
	// There are cases when workflow execution is closed but visibility is not updated and still open.
	// This happens, for example, when workflow execution is deleted right from CloseExecutionTask.
	// Therefore, force to delete from open visibility regardless of execution state.
	if forceDeleteFromOpenVisibility || ms.GetExecutionState().State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		startTime = ms.GetExecutionInfo().GetStartTime()
	} else {
		closeTime, err = ms.GetWorkflowCloseTime(ctx)
		if err != nil {
			return err
		}
	}

	op := func(ctx context.Context) error {
		return m.shard.DeleteWorkflowExecution(
			ctx,
			definition.WorkflowKey{
				NamespaceID: namespaceID.String(),
				WorkflowID:  we.GetWorkflowId(),
				RunID:       we.GetRunId(),
			},
			currentBranchToken,
			startTime,
			closeTime,
		)
	}
	if err = backoff.ThrottleRetryContext(
		ctx,
		op,
		PersistenceOperationRetryPolicy,
		common.IsPersistenceTransientError,
	); err != nil {
		return err
	}

	// Clear workflow execution context here to prevent further readers to get stale copy of non-exiting workflow execution.
	weCtx.Clear()

	scope.IncCounter(metrics.WorkflowCleanupDeleteCount)
	return nil
}

func (m *DeleteManagerImpl) archiveWorkflowIfEnabled(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowExecution commonpb.WorkflowExecution,
	currentBranchToken []byte,
	weCtx Context,
	ms MutableState,
	scope metrics.Scope,
) (bool, error) {

	namespaceRegistryEntry := ms.GetNamespaceEntry()

	clusterConfiguredForHistoryArchival := m.shard.GetArchivalMetadata().GetHistoryConfig().ClusterConfiguredForArchival()
	namespaceConfiguredForHistoryArchival := namespaceRegistryEntry.HistoryArchivalState().State == enumspb.ARCHIVAL_STATE_ENABLED
	archiveHistory := clusterConfiguredForHistoryArchival && namespaceConfiguredForHistoryArchival

	// TODO: @ycyang once archival backfill is in place cluster:paused && namespace:enabled should be a nop rather than a delete
	if !archiveHistory {
		return true, nil
	}

	closeFailoverVersion, err := ms.GetLastWriteVersion()
	if err != nil {
		return false, err
	}

	req := &archiver.ClientRequest{
		ArchiveRequest: &archiver.ArchiveRequest{
			ShardID:              m.shard.GetShardID(),
			NamespaceID:          namespaceID.String(),
			WorkflowID:           workflowExecution.GetWorkflowId(),
			RunID:                workflowExecution.GetRunId(),
			Namespace:            namespaceRegistryEntry.Name().String(),
			Targets:              []archiver.ArchivalTarget{archiver.ArchiveTargetHistory},
			HistoryURI:           namespaceRegistryEntry.HistoryArchivalState().URI,
			NextEventID:          ms.GetNextEventID(),
			BranchToken:          currentBranchToken,
			CloseFailoverVersion: closeFailoverVersion,
		},
		CallerService:        common.HistoryServiceName,
		AttemptArchiveInline: false, // archive in workflow by default
	}
	executionStats, err := weCtx.LoadExecutionStats(ctx)
	if err == nil && executionStats.HistorySize < int64(m.config.TimerProcessorHistoryArchivalSizeLimit()) {
		req.AttemptArchiveInline = true
	}

	saTypeMap, err := m.shard.GetSearchAttributesProvider().GetSearchAttributes(m.config.DefaultVisibilityIndexName, false)
	if err != nil {
		return false, err
	}
	// Setting search attributes types here because archival client needs to stringify them,
	// and it might not have access to typeMap (i.e. type needs to be embedded).
	searchattribute.ApplyTypeMap(req.ArchiveRequest.SearchAttributes, saTypeMap)

	ctx, cancel := context.WithTimeout(context.Background(), m.config.TimerProcessorArchivalTimeLimit())
	defer cancel()
	resp, err := m.archivalClient.Archive(ctx, req)
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
