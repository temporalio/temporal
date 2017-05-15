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

package frontend

import (
	"github.com/uber/cadence/.gen/go/shared"
)

// Client is the interface exposed by frontend service client
type Client interface {
	RegisterDomain(registerRequest *shared.RegisterDomainRequest) error
	DescribeDomain(describeRequest *shared.DescribeDomainRequest) (*shared.DescribeDomainResponse, error)
	UpdateDomain(updateRequest *shared.UpdateDomainRequest) (*shared.UpdateDomainResponse, error)
	DeprecateDomain(deprecateRequest *shared.DeprecateDomainRequest) error
	GetWorkflowExecutionHistory(getRequest *shared.GetWorkflowExecutionHistoryRequest) (*shared.GetWorkflowExecutionHistoryResponse, error)
	PollForActivityTask(pollRequest *shared.PollForActivityTaskRequest) (*shared.PollForActivityTaskResponse, error)
	PollForDecisionTask(pollRequest *shared.PollForDecisionTaskRequest) (*shared.PollForDecisionTaskResponse, error)
	RecordActivityTaskHeartbeat(heartbeatRequest *shared.RecordActivityTaskHeartbeatRequest) (*shared.RecordActivityTaskHeartbeatResponse, error)
	RespondActivityTaskCompleted(completeRequest *shared.RespondActivityTaskCompletedRequest) error
	RespondActivityTaskFailed(failRequest *shared.RespondActivityTaskFailedRequest) error
	RespondActivityTaskCanceled(cancelRequest *shared.RespondActivityTaskCanceledRequest) error
	RespondDecisionTaskCompleted(completeRequest *shared.RespondDecisionTaskCompletedRequest) error
	StartWorkflowExecution(startRequest *shared.StartWorkflowExecutionRequest) (*shared.StartWorkflowExecutionResponse, error)
	RequestCancelWorkflowExecution(cancelRequest *shared.RequestCancelWorkflowExecutionRequest) error
	SignalWorkflowExecution(request *shared.SignalWorkflowExecutionRequest) error
	TerminateWorkflowExecution(terminateRequest *shared.TerminateWorkflowExecutionRequest) error
	ListOpenWorkflowExecutions(listRequest *shared.ListOpenWorkflowExecutionsRequest) (*shared.ListOpenWorkflowExecutionsResponse, error)
	ListClosedWorkflowExecutions(listRequest *shared.ListClosedWorkflowExecutionsRequest) (*shared.ListClosedWorkflowExecutionsResponse, error)
}
