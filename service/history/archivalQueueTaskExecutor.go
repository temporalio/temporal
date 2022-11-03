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

	common2 "go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/archival"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type archivalQueueTaskExecutor struct {
	archiver      archival.Archiver
	shardContext  shard.Context
	workflowCache workflow.Cache
	logger        log.Logger
	metricsClient metrics.MetricsHandler
}

func newArchivalQueueTaskExecutor(archiver archival.Archiver, shardContext shard.Context, workflowCache workflow.Cache,
	metricsHandler metrics.MetricsHandler, logger log.Logger) *archivalQueueTaskExecutor {
	return &archivalQueueTaskExecutor{
		archiver:      archiver,
		shardContext:  shardContext,
		workflowCache: workflowCache,
		logger:        logger,
		metricsClient: metricsHandler,
	}
}

func (e *archivalQueueTaskExecutor) Execute(ctx context.Context, executable queues.Executable) (tags []metrics.Tag,
	isActive bool, err error) {
	task := executable.GetTask()
	taskType := queues.GetArchivalTaskTypeTagValue(task)
	tags = []metrics.Tag{
		getNamespaceTagByID(e.shardContext.GetNamespaceRegistry(), task.GetNamespaceID()),
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType), // for backward compatibility
	}
	switch task := task.(type) {
	case *tasks.ArchiveExecutionTask:
		err = e.processArchiveExecutionTask(ctx, task)
	default:
		err = fmt.Errorf("task with invalid type sent to archival queue: %+v", task)
	}
	return tags, true, err
}

func (e *archivalQueueTaskExecutor) processArchiveExecutionTask(ctx context.Context,
	task *tasks.ArchiveExecutionTask) (err error) {
	weContext, release, err := getWorkflowExecutionContextForTask(ctx, e.workflowCache, task)
	if err != nil {
		return err
	}
	defer func() { release(err) }()

	mutableState, err := weContext.LoadMutableState(ctx)
	if err != nil {
		return err
	}
	if mutableState == nil || mutableState.IsWorkflowExecutionRunning() {
		return errors.New("cannot archive workflow which is running")
	}
	branchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	namespaceEntry := mutableState.GetNamespaceEntry()
	namespaceName := namespaceEntry.Name()
	nextEventID := mutableState.GetNextEventID()
	closeFailoverVersion, err := mutableState.GetLastWriteVersion()
	if err != nil {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()
	workflowTypeName := executionInfo.GetWorkflowTypeName()
	startTime := executionInfo.GetStartTime()
	if startTime == nil {
		return errors.New("can't archive workflow with nil start time")
	}
	executionTime := executionInfo.GetExecutionTime()
	if executionTime == nil {
		return errors.New("can't archive workflow with nil execution time")
	}
	closeTime := executionInfo.GetCloseTime()
	if closeTime == nil {
		return errors.New("can't archive workflow with nil close time")
	}
	executionState := mutableState.GetExecutionState()
	memo := getWorkflowMemo(copyMemo(executionInfo.Memo))
	_, err = e.archiver.Archive(ctx, &archival.Request{
		ShardID:              e.shardContext.GetShardID(),
		NamespaceID:          task.NamespaceID,
		Namespace:            namespaceName.String(),
		WorkflowID:           task.WorkflowID,
		RunID:                task.RunID,
		BranchToken:          branchToken,
		NextEventID:          nextEventID,
		CloseFailoverVersion: closeFailoverVersion,
		HistoryURI:           namespaceEntry.HistoryArchivalState().URI,
		WorkflowTypeName:     workflowTypeName,
		StartTime:            *startTime,
		ExecutionTime:        *executionTime,
		CloseTime:            *closeTime,
		Status:               executionState.Status,
		HistoryLength:        nextEventID - 1,
		Memo:                 memo,
		SearchAttributes:     getSearchAttributes(copySearchAttributes(executionInfo.SearchAttributes)),
		VisibilityURI:        namespaceEntry.VisibilityArchivalState().URI,
		Targets:              []archival.Target{archival.TargetHistory, archival.TargetVisibility},
		CallerService:        common2.HistoryServiceName,
	})
	if err != nil {
		return err
	}
	retention := namespaceEntry.Retention()
	if retention == 0 {
		retention = 7 * 24 * time.Hour
	}
	deleteTime := closeTime.Add(retention)
	mutableState.AddTasks(&tasks.DeleteHistoryEventTask{
		WorkflowKey:                 task.WorkflowKey,
		VisibilityTimestamp:         deleteTime,
		Version:                     task.Version,
		BranchToken:                 branchToken,
		WorkflowDataAlreadyArchived: true,
	})
	return err
}
