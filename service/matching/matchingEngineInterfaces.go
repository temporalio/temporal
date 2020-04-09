// Copyright (c) 2017 Uber Technologies, Inc.
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
	workflow "github.com/uber/cadence/.gen/go/shared"
)

type (
	// Engine exposes interfaces for clients to poll for activity and decision tasks.
	Engine interface {
		Stop()
		AddDecisionTask(hCtx *handlerContext, request *m.AddDecisionTaskRequest) (syncMatch bool, err error)
		AddActivityTask(hCtx *handlerContext, request *m.AddActivityTaskRequest) (syncMatch bool, err error)
		PollForDecisionTask(hCtx *handlerContext, request *m.PollForDecisionTaskRequest) (*m.PollForDecisionTaskResponse, error)
		PollForActivityTask(hCtx *handlerContext, request *m.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error)
		QueryWorkflow(hCtx *handlerContext, request *m.QueryWorkflowRequest) (*workflow.QueryWorkflowResponse, error)
		RespondQueryTaskCompleted(hCtx *handlerContext, request *m.RespondQueryTaskCompletedRequest) error
		CancelOutstandingPoll(hCtx *handlerContext, request *m.CancelOutstandingPollRequest) error
		DescribeTaskList(hCtx *handlerContext, request *m.DescribeTaskListRequest) (*workflow.DescribeTaskListResponse, error)
		ListTaskListPartitions(hCtx *handlerContext, request *m.ListTaskListPartitionsRequest) (*workflow.ListTaskListPartitionsResponse, error)
	}
)
