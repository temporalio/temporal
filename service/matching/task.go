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
	queryTaskInfo struct {
		taskID       string
		queryRequest *m.QueryWorkflowRequest
	}
	// internalTask represents an activity, decision or query task
	// holds task specific info and additional metadata
	internalTask struct {
		info              *persistence.TaskInfo
		syncResponseCh    chan error
		workflowExecution s.WorkflowExecution
		queryInfo         *queryTaskInfo
		backlogCountHint  int64
		domainName        string
		completionFunc    func(*internalTask, error)
	}
)

func newInternalTask(
	info *persistence.TaskInfo,
	completionFunc func(*internalTask, error),
	forSyncMatch bool,
) *internalTask {
	task := &internalTask{
		info:           info,
		completionFunc: completionFunc,
		workflowExecution: s.WorkflowExecution{
			WorkflowId: &info.WorkflowID,
			RunId:      &info.RunID,
		},
	}
	if forSyncMatch {
		task.syncResponseCh = make(chan error, 1)
	}
	return task
}

func newInternalQueryTask(
	queryInfo *queryTaskInfo,
	completionFunc func(*internalTask, error),
) *internalTask {
	return &internalTask{
		info: &persistence.TaskInfo{
			DomainID:   queryInfo.queryRequest.GetDomainUUID(),
			WorkflowID: queryInfo.queryRequest.QueryRequest.Execution.GetWorkflowId(),
			RunID:      queryInfo.queryRequest.QueryRequest.Execution.GetRunId(),
		},
		completionFunc:    completionFunc,
		queryInfo:         queryInfo,
		workflowExecution: *queryInfo.queryRequest.QueryRequest.GetExecution(),
		syncResponseCh:    make(chan error, 1),
	}
}

// isQuery returns true if the underlying task is a query task
func (task *internalTask) isQuery() bool {
	return task.queryInfo != nil
}

// finish marks a task as finished. Should be called after a poller picks up a task
// and marks it as started. If the task is unable to marked as started, then this
// method should be called with a non-nil error argument.
func (task *internalTask) finish(err error) {
	task.completionFunc(task, err)
}
