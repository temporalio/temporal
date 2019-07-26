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
		taskID  string
		request *m.QueryWorkflowRequest
	}
	// startedTaskInfo contains info for any task received from
	// another matching host. This type of task is already marked as started
	startedTaskInfo struct {
		decisionTaskInfo *m.PollForDecisionTaskResponse
		activityTaskInfo *s.PollForActivityTaskResponse
	}
	// internalTask represents an activity, decision, query or started (received from another host).
	// this struct is more like a union and only one of [ query, event, forwarded ] is
	// non-nil for any given task
	internalTask struct {
		event            *genericTaskInfo // non-nil for activity or decision task that's locally generated
		query            *queryTaskInfo   // non-nil for a query task that's locally sync matched
		started          *startedTaskInfo // non-nil for a task received from a parent partition which is already started
		domainName       string
		forwardedFrom    string     // name of the child partition this task is forwarded from (empty if not forwarded)
		responseC        chan error // non-nil only where there is a caller waiting for response (sync-match)
		backlogCountHint int64
	}
)

func newInternalTask(
	info *persistence.TaskInfo,
	completionFunc func(*persistence.TaskInfo, error),
	forwardedFrom string,
	forSyncMatch bool,
) *internalTask {
	task := &internalTask{
		event: &genericTaskInfo{
			TaskInfo:       info,
			completionFunc: completionFunc,
		},
		forwardedFrom: forwardedFrom,
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
			taskID:  taskID,
			request: request,
		},
		responseC: make(chan error, 1),
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
	return task.forwardedFrom != ""
}

func (task *internalTask) workflowExecution() *s.WorkflowExecution {
	switch {
	case task.event != nil:
		return &s.WorkflowExecution{WorkflowId: &task.event.WorkflowID, RunId: &task.event.RunID}
	case task.query != nil:
		return task.query.request.GetQueryRequest().GetExecution()
	case task.started != nil && task.started.decisionTaskInfo != nil:
		return task.started.decisionTaskInfo.WorkflowExecution
	case task.started != nil && task.started.activityTaskInfo != nil:
		return task.started.activityTaskInfo.WorkflowExecution
	}
	return &s.WorkflowExecution{}
}

// pollForDecisionResponse returns the poll response for a decision task that is
// already marked as started. This method should only be called when isStarted() is true
func (task *internalTask) pollForDecisionResponse() *m.PollForDecisionTaskResponse {
	if task.isStarted() {
		return task.started.decisionTaskInfo
	}
	return nil
}

// pollForActivityResponse returns the poll response for an activity task that is
// already marked as started. This method should only be called when isStarted() is true
func (task *internalTask) pollForActivityResponse() *s.PollForActivityTaskResponse {
	if task.isStarted() {
		return task.started.activityTaskInfo
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
		task.event.completionFunc(task.event.TaskInfo, err)
	}
}
