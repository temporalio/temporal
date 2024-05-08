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
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/service/history/archival"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
)

// NewArchivalQueueTaskExecutor creates a new queue task executor for the archival queue.
// If you use this executor, you must monitor for any metrics.ArchivalTaskInvalidURI errors.
// If this metric is emitted, it means that an archival URI is invalid and the task will never succeed, which is a
// serious problem because the archival queue retries tasks forever.
func NewArchivalQueueTaskExecutor(
	archiver archival.Archiver,
	shardContext shard.Context,
	workflowCache cache.Cache,
	relocatableAttributesFetcher workflow.RelocatableAttributesFetcher,
	metricsHandler metrics.Handler,
	logger log.Logger,
) queues.Executor {
	return &archivalQueueTaskExecutor{
		archiver:                     archiver,
		shardContext:                 shardContext,
		workflowCache:                workflowCache,
		relocatableAttributesFetcher: relocatableAttributesFetcher,
		metricsHandler:               metricsHandler,
		logger:                       logger,
	}
}

// archivalQueueTaskExecutor is an implementation of queues.Executor for the archival queue.
type archivalQueueTaskExecutor struct {
	archiver                     archival.Archiver
	shardContext                 shard.Context
	workflowCache                cache.Cache
	metricsHandler               metrics.Handler
	logger                       log.Logger
	relocatableAttributesFetcher workflow.RelocatableAttributesFetcher
}

// Execute executes a task from the archival queue.
func (e *archivalQueueTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	taskType := queues.GetArchivalTaskTypeTagValue(task)
	tags := []metrics.Tag{
		getNamespaceTagByID(e.shardContext.GetNamespaceRegistry(), task.GetNamespaceID()),
		metrics.TaskTypeTag(taskType),
		// OperationTag is for consistency on tags with other executors,
		// since those tags will be used to emit a common set of metrics.
		metrics.OperationTag(taskType),
	}

	var err error
	switch task := task.(type) {
	case *tasks.ArchiveExecutionTask:
		err = e.processArchiveExecutionTask(ctx, task)
		if err == ErrMutableStateIsNil || err == ErrWorkflowExecutionIsStillRunning {
			// If either of these errors are returned, it means that we can just drop the task.
			err = nil
		}
	default:
		err = fmt.Errorf("task with invalid type sent to archival queue: %+v", task)
	}
	return queues.ExecuteResponse{
		ExecutionMetricTags: tags,
		ExecutedAsActive:    true,
		ExecutionErr:        err,
	}
}

// processArchiveExecutionTask processes a tasks.ArchiveExecutionTask
// First, we load the mutable state to populate an archival.Request.
// Second, we unlock the mutable state and send the archival request to the archival.Archiver.
// Finally, we lock the mutable state again, and send a deletion follow-up task to the history engine.
func (e *archivalQueueTaskExecutor) processArchiveExecutionTask(ctx context.Context, task *tasks.ArchiveExecutionTask) (err error) {
	logger := log.With(e.logger, tag.Task(task))
	request, err := e.getArchiveTaskRequest(ctx, logger, task)
	if err != nil {
		return err
	}
	if len(request.Targets) > 0 {
		_, err = e.archiver.Archive(ctx, request)
		if err != nil {
			return err
		}
	}
	return e.addDeletionTask(ctx, logger, task, request.CloseTime.AsTime())
}

// getArchiveTaskRequest returns an archival request for the given archive execution task.
func (e *archivalQueueTaskExecutor) getArchiveTaskRequest(
	ctx context.Context,
	logger log.Logger,
	task *tasks.ArchiveExecutionTask,
) (request *archival.Request, err error) {
	mutableState, err := e.loadAndVersionCheckMutableState(ctx, logger, task)
	if err != nil {
		return nil, err
	}
	defer func() {
		mutableState.Release(err)
	}()

	namespaceEntry := mutableState.GetNamespaceEntry()
	namespaceName := namespaceEntry.Name()
	nextEventID := mutableState.GetNextEventID()
	executionInfo := mutableState.GetExecutionInfo()
	executionState := mutableState.GetExecutionState()

	logger = log.With(logger,
		tag.WorkflowNamespaceID(executionInfo.NamespaceId),
		tag.WorkflowID(executionInfo.WorkflowId),
		tag.WorkflowRunID(executionState.RunId),
	)

	closeTime, err := mutableState.GetWorkflowCloseTime(ctx)
	if err != nil {
		return nil, err
	}
	executionDuration, err := mutableState.GetWorkflowExecutionDuration(ctx)
	if err != nil {
		return nil, err
	}
	branchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}

	var historyURI, visibilityURI carchiver.URI
	var targets []archival.Target
	if e.shardContext.GetArchivalMetadata().GetVisibilityConfig().ClusterConfiguredForArchival() &&
		namespaceEntry.VisibilityArchivalState().State == enumspb.ARCHIVAL_STATE_ENABLED {
		targets = append(targets, archival.TargetVisibility)
		visibilityURIString := namespaceEntry.VisibilityArchivalState().URI
		visibilityURI, err = carchiver.NewURI(visibilityURIString)
		if err != nil {
			metrics.ArchivalTaskInvalidURI.With(e.metricsHandler).Record(
				1,
				metrics.NamespaceTag(namespaceName.String()),
				metrics.FailureTag(metrics.InvalidVisibilityURITagValue),
			)
			logger.Error(
				"Failed to parse visibility URI.",
				tag.ArchivalURI(visibilityURIString),
				tag.Error(err),
			)
			return nil, fmt.Errorf("failed to parse visibility URI for archival task: %w", err)
		}
	}
	if e.shardContext.GetArchivalMetadata().GetHistoryConfig().ClusterConfiguredForArchival() &&
		namespaceEntry.HistoryArchivalState().State == enumspb.ARCHIVAL_STATE_ENABLED {
		historyURIString := namespaceEntry.HistoryArchivalState().URI
		historyURI, err = carchiver.NewURI(historyURIString)
		if err != nil {
			metrics.ArchivalTaskInvalidURI.With(e.metricsHandler).Record(
				1,
				metrics.NamespaceTag(namespaceName.String()),
				metrics.FailureTag(metrics.InvalidHistoryURITagValue),
			)
			logger.Error(
				"Failed to parse history URI.",
				tag.ArchivalURI(historyURIString),
				tag.Error(err),
			)
			return nil, fmt.Errorf("failed to parse history URI for archival task: %w", err)
		}
		targets = append(targets, archival.TargetHistory)
	}

	workflowAttributes, err := e.relocatableAttributesFetcher.Fetch(ctx, mutableState)
	if err != nil {
		return nil, err
	}

	request = &archival.Request{
		ShardID:              e.shardContext.GetShardID(),
		NamespaceID:          task.NamespaceID,
		Namespace:            namespaceName.String(),
		WorkflowID:           task.WorkflowID,
		RunID:                task.RunID,
		BranchToken:          branchToken,
		NextEventID:          nextEventID,
		CloseFailoverVersion: mutableState.LastWriteVersion,
		HistoryURI:           historyURI,
		VisibilityURI:        visibilityURI,
		WorkflowTypeName:     executionInfo.GetWorkflowTypeName(),
		StartTime:            executionInfo.GetStartTime(),
		ExecutionTime:        executionInfo.GetExecutionTime(),
		CloseTime:            timestamppb.New(closeTime),
		ExecutionDuration:    durationpb.New(executionDuration),
		Status:               executionState.Status,
		HistoryLength:        nextEventID - 1,
		Memo:                 workflowAttributes.Memo,
		SearchAttributes:     workflowAttributes.SearchAttributes,
		Targets:              targets,
		CallerService:        string(primitives.HistoryService),
	}
	return request, nil
}

// addDeletionTask adds a task to delete workflow history events from primary storage.
func (e *archivalQueueTaskExecutor) addDeletionTask(
	ctx context.Context,
	logger log.Logger,
	task *tasks.ArchiveExecutionTask,
	closeTime time.Time,
) error {
	mutableState, err := e.loadAndVersionCheckMutableState(ctx, logger, task)
	if err != nil {
		return err
	}
	defer func() {
		mutableState.Release(err)
	}()

	taskGenerator := workflow.NewTaskGenerator(
		e.shardContext.GetNamespaceRegistry(),
		mutableState,
		e.shardContext.GetConfig(),
		e.shardContext.GetArchivalMetadata(),
	)
	err = taskGenerator.GenerateDeleteHistoryEventTask(closeTime)
	if err != nil {
		return err
	}
	err = e.shardContext.AddTasks(ctx, &persistence.AddHistoryTasksRequest{
		ShardID:     e.shardContext.GetShardID(),
		NamespaceID: task.GetNamespaceID(),
		WorkflowID:  task.WorkflowID,
		Tasks:       mutableState.PopTasks(),
	})
	return err
}

// lockedMutableState is a wrapper around mutable state that includes the last write version of the mutable state and
// a function to release this mutable state's context when we're done with it.
type lockedMutableState struct {
	// MutableState is the mutable state that is being wrapped. You may call any method on this object safely since
	// the state is locked.
	workflow.MutableState
	// LastWriteVersion is the last write version of the mutable state. We store this here so that we don't have to
	// call GetLastWriteVersion() on the mutable state object again.
	LastWriteVersion int64
	// Release is a function that releases the context of the mutable state. This function should be called when
	// you are done with the mutable state.
	Release cache.ReleaseCacheFunc
}

// newLockedMutableState returns a new lockedMutableState with the given mutable state,
// last write version and release function
func newLockedMutableState(
	mutableState workflow.MutableState,
	version int64,
	releaseFunc cache.ReleaseCacheFunc,
) *lockedMutableState {
	return &lockedMutableState{
		MutableState:     mutableState,
		LastWriteVersion: version,
		Release:          releaseFunc,
	}
}

var (
	// ErrMutableStateIsNil is returned when the mutable state is nil
	ErrMutableStateIsNil = errors.New("mutable state is nil")
	// ErrWorkflowExecutionIsStillRunning is returned when the workflow execution is still running
	ErrWorkflowExecutionIsStillRunning = errors.New("workflow execution is still running")
)

// loadAndVersionCheckMutableState loads the mutable state for the given task and performs a version check to verify
// that the mutable state's last write version is equal to the task's version. If the version check fails then
// the mutable state is released and an error is returned. This can happen if this task was already processed.
func (e *archivalQueueTaskExecutor) loadAndVersionCheckMutableState(
	ctx context.Context,
	logger log.Logger,
	task *tasks.ArchiveExecutionTask,
) (lockedMutableState *lockedMutableState, err error) {
	weContext, release, err := getWorkflowExecutionContextForTask(ctx, e.shardContext, e.workflowCache, task)
	if err != nil {
		return nil, err
	}
	defer func() {
		// If we return an error, the caller will not release the mutable state, so we need to do it here.
		if err != nil {
			release(err)
		}
		// If we don't return an error, the caller will release the mutable state, so we don't need to do it here.
	}()

	mutableState, err := weContext.LoadMutableState(ctx, e.shardContext)
	if err != nil {
		return nil, err
	}
	if mutableState == nil {
		logger.Warn("Dropping archival task because mutable state is nil.")
		return nil, ErrMutableStateIsNil
	}
	if mutableState.IsWorkflowExecutionRunning() {
		logger.Warn("Dropping archival task because workflow is still running.")
		return nil, ErrWorkflowExecutionIsStillRunning
	}
	lastWriteVersion, err := mutableState.GetLastWriteVersion()
	if err != nil {
		return nil, err
	}
	namespaceEntry := mutableState.GetNamespaceEntry()
	err = CheckTaskVersion(
		e.shardContext,
		logger,
		namespaceEntry,
		lastWriteVersion,
		task.GetVersion(),
		task,
	)
	if err != nil {
		return nil, err
	}
	return newLockedMutableState(mutableState, lastWriteVersion, release), nil
}
