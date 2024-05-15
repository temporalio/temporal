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

package matching

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
)

type (
	// genericTaskInfo contains the info for an activity or workflow task
	genericTaskInfo struct {
		*persistencespb.AllocatedTaskInfo
		completionFunc func(*persistencespb.AllocatedTaskInfo, error)
	}
	// queryTaskInfo contains the info for a query task
	queryTaskInfo struct {
		taskID  string
		request *matchingservice.QueryWorkflowRequest
	}
	// nexusTaskInfo contains the info for a nexus task
	nexusTaskInfo struct {
		taskID   string
		deadline time.Time
		request  *matchingservice.DispatchNexusTaskRequest
	}
	// startedTaskInfo contains info for any task received from
	// another matching host. This type of task is already marked as started
	startedTaskInfo struct {
		workflowTaskInfo *matchingservice.PollWorkflowTaskQueueResponse
		activityTaskInfo *matchingservice.PollActivityTaskQueueResponse
		nexusTaskInfo    *matchingservice.PollNexusTaskQueueResponse
	}
	// internalTask represents an activity, workflow, query or started (received from another host).
	// this struct is more like a union and only one of [ query, event, forwarded ] is
	// non-nil for any given task
	internalTask struct {
		event            *genericTaskInfo // non-nil for activity or workflow task that's locally generated
		query            *queryTaskInfo   // non-nil for a query task that's locally sync matched
		nexus            *nexusTaskInfo   // non-nil for a nexus task that's locally sync matched
		started          *startedTaskInfo // non-nil for a task received from a parent partition which is already started
		namespace        namespace.Name
		source           enumsspb.TaskSource
		responseC        chan error // non-nil only where there is a caller waiting for response (sync-match)
		backlogCountHint func() int64
		// forwardInfo contains information about forward source partition and versioning decisions made by it
		// a parent partition receiving forwarded tasks makes no versioning decisions and only follows what the source
		// partition instructed.
		forwardInfo *taskqueuespb.TaskForwardInfo
		// redirectInfo is only set when redirect rule is applied on the task. for forwarded tasks, this is populated
		// based on forwardInfo.
		redirectInfo *taskqueuespb.BuildIdRedirectInfo
	}
)

func newInternalTaskForSyncMatch(
	info *persistencespb.TaskInfo,
	forwardInfo *taskqueuespb.TaskForwardInfo,
) *internalTask {
	var redirectInfo *taskqueuespb.BuildIdRedirectInfo
	// if this task is not forwarded, source can only be history
	source := enumsspb.TASK_SOURCE_HISTORY
	if forwardInfo != nil {
		// if task is forwarded, it may be history or backlog. setting based on forward info
		source = forwardInfo.TaskSource
		redirectInfo = forwardInfo.GetRedirectInfo()
	}
	task := &internalTask{
		event: &genericTaskInfo{
			AllocatedTaskInfo: &persistencespb.AllocatedTaskInfo{
				Data:   info,
				TaskId: syncMatchTaskId,
			},
		},
		forwardInfo:  forwardInfo,
		source:       source,
		redirectInfo: redirectInfo,
		responseC:    make(chan error, 1),
	}
	return task
}

func newInternalTaskFromBacklog(
	info *persistencespb.AllocatedTaskInfo,
	completionFunc func(*persistencespb.AllocatedTaskInfo, error),
) *internalTask {
	task := &internalTask{
		event: &genericTaskInfo{
			AllocatedTaskInfo: info,
			completionFunc:    completionFunc,
		},
		source: enumsspb.TASK_SOURCE_DB_BACKLOG,
	}
	return task
}

func newInternalQueryTask(
	taskID string,
	request *matchingservice.QueryWorkflowRequest,
) *internalTask {
	return &internalTask{
		query: &queryTaskInfo{
			taskID:  taskID,
			request: request,
		},
		forwardInfo: request.GetForwardInfo(),
		responseC:   make(chan error, 1),
		source:      enumsspb.TASK_SOURCE_HISTORY,
	}
}

func newInternalNexusTask(
	taskID string,
	deadline time.Time,
	request *matchingservice.DispatchNexusTaskRequest,
) *internalTask {
	return &internalTask{
		nexus: &nexusTaskInfo{
			taskID:   taskID,
			deadline: deadline,
			request:  request,
		},
		forwardInfo: request.GetForwardInfo(),
		responseC:     make(chan error, 1),
		source:      enumsspb.TASK_SOURCE_HISTORY,
	}
}

func newInternalStartedTask(info *startedTaskInfo) *internalTask {
	return &internalTask{started: info}
}

// isQuery returns true if the underlying task is a query task
func (task *internalTask) isQuery() bool {
	return task.query != nil
}

// isStarted is true when this task is already marked as started
func (task *internalTask) isStarted() bool {
	return task.started != nil
}

// isForwarded returns true if the underlying task is forwarded by a remote matching host
// forwarded tasks are already marked as started in history
func (task *internalTask) isForwarded() bool {
	return task.forwardInfo != nil
}

func (task *internalTask) isSyncMatchTask() bool {
	return task.responseC != nil
}

func (task *internalTask) workflowExecution() *commonpb.WorkflowExecution {
	switch {
	case task.event != nil:
		return &commonpb.WorkflowExecution{WorkflowId: task.event.Data.GetWorkflowId(), RunId: task.event.Data.GetRunId()}
	case task.query != nil:
		return task.query.request.GetQueryRequest().GetExecution()
	case task.started != nil && task.started.workflowTaskInfo != nil:
		return task.started.workflowTaskInfo.WorkflowExecution
	case task.started != nil && task.started.activityTaskInfo != nil:
		return task.started.activityTaskInfo.WorkflowExecution
	}
	return &commonpb.WorkflowExecution{}
}

// pollWorkflowTaskQueueResponse returns the poll response for a workflow task that is
// already marked as started. This method should only be called when isStarted() is true
func (task *internalTask) pollWorkflowTaskQueueResponse() *matchingservice.PollWorkflowTaskQueueResponse {
	if task.isStarted() {
		return task.started.workflowTaskInfo
	}
	return nil
}

// pollActivityTaskQueueResponse returns the poll response for an activity task that is
// already marked as started. This method should only be called when isStarted() is true
func (task *internalTask) pollActivityTaskQueueResponse() *matchingservice.PollActivityTaskQueueResponse {
	if task.isStarted() {
		return task.started.activityTaskInfo
	}
	return nil
}

// pollNexusTaskQueueResponse returns the poll response for a nexus task that is ready for dispatching. This method
// should only be called when isStarted() is true
func (task *internalTask) pollNexusTaskQueueResponse() *matchingservice.PollNexusTaskQueueResponse {
	if task.isStarted() {
		return task.started.nexusTaskInfo
	}
	return nil
}

// finish marks a task as finished. Should be called after a poller picks up a task
// and marks it as started. If the task is unable to marked as started, then this
// method should be called with a non-nil error argument.
func (task *internalTask) finish(err error) {
	switch {
	case task.responseC != nil:
		task.responseC <- err
	case task.event.completionFunc != nil:
		// TODO: this probably should not be done synchronously in PollWorkflow/ActivityTaskQueue
		task.event.completionFunc(task.event.AllocatedTaskInfo, err)
	}
}
