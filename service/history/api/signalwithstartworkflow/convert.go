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

package signalwithstartworkflow

import (
	"time"

	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
)

func ConvertToStartRequest(
	namespaceID namespace.ID,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
	now time.Time,
) *historyservice.StartWorkflowExecutionRequest {
	req := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                request.GetNamespace(),
		WorkflowId:               request.GetWorkflowId(),
		WorkflowType:             request.GetWorkflowType(),
		TaskQueue:                request.GetTaskQueue(),
		Input:                    request.GetInput(),
		WorkflowExecutionTimeout: request.GetWorkflowExecutionTimeout(),
		WorkflowRunTimeout:       request.GetWorkflowRunTimeout(),
		WorkflowTaskTimeout:      request.GetWorkflowTaskTimeout(),
		Identity:                 request.GetIdentity(),
		RequestId:                request.GetRequestId(),
		WorkflowIdReusePolicy:    request.GetWorkflowIdReusePolicy(),
		RetryPolicy:              request.GetRetryPolicy(),
		CronSchedule:             request.GetCronSchedule(),
		Memo:                     request.GetMemo(),
		SearchAttributes:         request.GetSearchAttributes(),
		Header:                   request.GetHeader(),
	}

	return common.CreateHistoryStartWorkflowRequest(namespaceID.String(), req, nil, now)
}
