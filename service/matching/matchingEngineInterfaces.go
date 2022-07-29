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
	"go.temporal.io/server/api/matchingservice/v1"
)

type (
	// Engine exposes interfaces for clients to poll for activity and workflow tasks.
	Engine interface {
		Stop()
		AddWorkflowTask(hCtx *handlerContext, addRequest *matchingservice.AddWorkflowTaskRequest) (syncMatch bool, err error)
		AddActivityTask(hCtx *handlerContext, addRequest *matchingservice.AddActivityTaskRequest) (syncMatch bool, err error)
		PollWorkflowTaskQueue(hCtx *handlerContext, request *matchingservice.PollWorkflowTaskQueueRequest) (*matchingservice.PollWorkflowTaskQueueResponse, error)
		PollActivityTaskQueue(hCtx *handlerContext, request *matchingservice.PollActivityTaskQueueRequest) (*matchingservice.PollActivityTaskQueueResponse, error)
		QueryWorkflow(hCtx *handlerContext, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		RespondQueryTaskCompleted(hCtx *handlerContext, request *matchingservice.RespondQueryTaskCompletedRequest) error
		CancelOutstandingPoll(hCtx *handlerContext, request *matchingservice.CancelOutstandingPollRequest) error
		DescribeTaskQueue(hCtx *handlerContext, request *matchingservice.DescribeTaskQueueRequest) (*matchingservice.DescribeTaskQueueResponse, error)
		ListTaskQueuePartitions(hCtx *handlerContext, request *matchingservice.ListTaskQueuePartitionsRequest) (*matchingservice.ListTaskQueuePartitionsResponse, error)
		UpdateWorkerBuildIdOrdering(ctx *handlerContext, request *matchingservice.UpdateWorkerBuildIdOrderingRequest) (*matchingservice.UpdateWorkerBuildIdOrderingResponse, error)
		GetWorkerBuildIdOrdering(ctx *handlerContext, request *matchingservice.GetWorkerBuildIdOrderingRequest) (*matchingservice.GetWorkerBuildIdOrderingResponse, error)
	}
)
