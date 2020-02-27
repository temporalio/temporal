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
	"context"

	"github.com/temporalio/temporal/.gen/proto/matchingservice"
)

type (
	// Engine exposes interfaces for clients to poll for activity and decision tasks.
	Engine interface {
		Stop()
		AddDecisionTask(ctx context.Context, addRequest *matchingservice.AddDecisionTaskRequest) (syncMatch bool, err error)
		AddActivityTask(ctx context.Context, addRequest *matchingservice.AddActivityTaskRequest) (syncMatch bool, err error)
		PollForDecisionTask(ctx context.Context, request *matchingservice.PollForDecisionTaskRequest) (*matchingservice.PollForDecisionTaskResponse, error)
		PollForActivityTask(ctx context.Context, request *matchingservice.PollForActivityTaskRequest) (*matchingservice.PollForActivityTaskResponse, error)
		QueryWorkflow(ctx context.Context, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		RespondQueryTaskCompleted(ctx context.Context, request *matchingservice.RespondQueryTaskCompletedRequest) error
		CancelOutstandingPoll(ctx context.Context, request *matchingservice.CancelOutstandingPollRequest) error
		DescribeTaskList(ctx context.Context, request *matchingservice.DescribeTaskListRequest) (*matchingservice.DescribeTaskListResponse, error)
		ListTaskListPartitions(ctx context.Context, request *matchingservice.ListTaskListPartitionsRequest) (*matchingservice.ListTaskListPartitionsResponse, error)
	}
)
