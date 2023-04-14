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

package pollupdate

import (
	"context"
	"sync"

	"go.temporal.io/api/serviceerror"
	updatepb "go.temporal.io/api/update/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/workflow"
)

// updateObserver is an implementation of workflow.Observer that observes the
// outcome of workflow execution update.
type updateObserver struct {
	mut sync.Mutex
	id  string
	fut *future.Proxy[*updatepb.Outcome]
}

type WorkflowCtxLookup func(
	context.Context,
	*clockspb.VectorClock,
	api.MutableStateConsistencyPredicate,
	definition.WorkflowKey,
	workflow.LockPriority,
) (api.WorkflowContext, error)

// Invoke waits for the outcome of a workflow execution update. It may block for
// as long as is specified by the context.Context argument.
func Invoke(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionUpdateRequest,
	findWorkflow WorkflowCtxLookup,
	wfobservers *workflow.ObserverSet,
) (*historyservice.PollWorkflowExecutionUpdateResponse, error) {
	return nil, serviceerror.NewUnimplemented("not implemented")
}

func (uo *updateObserver) Connect(ctx context.Context, wfctx workflow.Context) error {
	return serviceerror.NewUnimplemented("not implemented")
}

func (uo *updateObserver) AwaitOutcome(ctx context.Context) (*updatepb.Outcome, error) {
	return nil, serviceerror.NewUnimplemented("not implemented")
}
