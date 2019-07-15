// Copyright (c) 2019 Uber Technologies, Inc.
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
	m "github.com/uber/cadence/.gen/go/matching"
	s "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
)

type (
	// genericTaskInfo contains the info for an activity or decision task
	genericTaskInfo struct {
		*persistence.TaskInfo
		completionFunc func(*persistence.TaskInfo, error)
	}
	// queryTaskInfo contains the info for a query task
	queryTaskInfo struct {
		taskID       string
		queryRequest *m.QueryWorkflowRequest
	}
	// forwardedTaskInfo contains the forwarded info for any task forwarded from
	// another matching host. Forwarded tasks are already started
	forwardedTaskInfo struct {
		decisionTaskInfo *m.PollForDecisionTaskResponse
		activityTaskInfo *s.PollForActivityTaskResponse
	}
	// internalTask represents an activity, decision, query or remote forwarded task.
	// this struct is more like a union and only one of [ query, generic, forwarded ] is
	// non-nil for any given task
	internalTask struct {
		query            *queryTaskInfo     // non-nil for locally matched matched query task
		generic          *genericTaskInfo   // non-nil for locally matched activity or decision task
		forwarded        *forwardedTaskInfo // non-nil for a remote forwarded task
		domainName       string
		responseC        chan error // non-nil only where there is a caller waiting for response (sync-match)
		backlogCountHint int64
	}
)

func newInternalTask(
	info *persistence.TaskInfo,
	completionFunc func(*persistence.TaskInfo, error),
	forSyncMatch bool,
) *internalTask {
	task := &internalTask{
		generic: &genericTaskInfo{
			TaskInfo:       info,
			completionFunc: completionFunc,
		},
	}
	if forSyncMatch {
		task.responseC = make(chan error, 1)
	}
	return task
}

func newInternalQueryTask(
	taskID string,
	request *m.QueryWorkflowRequest,
) *internalTask {
	return &internalTask{
		query: &queryTaskInfo{
			taskID:       taskID,
			queryRequest: request,
		},
		responseC: make(chan error, 1),
	}
}

func newInternalForwardedTask(info *forwardedTaskInfo) *internalTask {
	return &internalTask{forwarded: info}
}

// isQuery returns true if the underlying task is a query task
func (task *internalTask) isQuery() bool {
	return task.query != nil
}

// isForwarded returns true if the underlying task is forwarded by a remote matching host
// forwarded tasks are already marked as started in history
func (task *internalTask) isForwarded() bool {
	return task.forwarded != nil
}

func (task *internalTask) workflowExecution() *s.WorkflowExecution {
	switch {
	case task.generic != nil:
		return &s.WorkflowExecution{WorkflowId: &task.generic.WorkflowID, RunId: &task.generic.RunID}
	case task.query != nil:
		return task.query.queryRequest.GetQueryRequest().GetExecution()
	case task.forwarded != nil && task.forwarded.decisionTaskInfo != nil:
		return task.forwarded.decisionTaskInfo.WorkflowExecution
	case task.forwarded != nil && task.forwarded.activityTaskInfo != nil:
		return task.forwarded.activityTaskInfo.WorkflowExecution
	}
	return &s.WorkflowExecution{}
}

// finish marks a task as finished. Should be called after a poller picks up a task
// and marks it as started. If the task is unable to marked as started, then this
// method should be called with a non-nil error argument.
func (task *internalTask) finish(err error) {
	switch {
	case task.responseC != nil:
		task.responseC <- err
	case task.generic.completionFunc != nil:
		task.generic.completionFunc(task.generic.TaskInfo, err)
	}
}
