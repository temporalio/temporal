// Copyright (c) 2019 Temporal Technologies, Inc.
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
	"context"

	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/service/frontend/adapter"
	"github.com/temporalio/temporal/tpb"
)

var _ tpb.WorkflowServiceYARPCServer = (*WorkflowHandlerGRPC)(nil)

type (
	// WorkflowHandlerGRPC - gRPC handler interface for workflow service
	WorkflowHandlerGRPC struct {
		workflowHandlerThrift *WorkflowHandler
	}
)

// NewWorkflowHandlerGRPC creates a thrift handler for the cadence service
func NewWorkflowHandlerGRPC(
	workflowHandlerThrift *WorkflowHandler,
) *WorkflowHandlerGRPC {
	handler := &WorkflowHandlerGRPC{
		workflowHandlerThrift: workflowHandlerThrift,
	}

	return handler
}

// RegisterHandler register this handler, must be called before Start()
// if DCRedirectionHandler is also used, use RegisterHandler in DCRedirectionHandler instead
func (wh *WorkflowHandlerGRPC) RegisterHandler() {
	wh.workflowHandlerThrift.Service.GetDispatcher().Register(tpb.BuildWorkflowServiceYARPCProcedures(wh))
}

// RegisterDomain creates a new domain which can be used as a container for all resources.  Domain is a top level
// entity within Cadence, used as a container for all resources like workflow executions, tasklists, etc.  Domain
// acts as a sandbox and provides isolation for all resources within the domain.  All resources belongs to exactly one
// domain.
func (wh *WorkflowHandlerGRPC) RegisterDomain(ctx context.Context, registerRequest *tpb.RegisterDomainRequest) (_ *tpb.RegisterDomainResponse, retError error) {
	defer log.CapturePanicGRPC(wh.workflowHandlerThrift.GetLogger(), &retError)

	err := wh.workflowHandlerThrift.RegisterDomain(ctx, adapter.ToThriftRegisterDomainRequest(registerRequest))
	if err != nil {
		return nil, adapter.ToProtoError(err)
	}
	return &tpb.RegisterDomainResponse{}, nil
}
