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

	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/workflow"
)

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
) (_ *historyservice.PollWorkflowExecutionUpdateResponse, retErr error) {
	updateRef := req.GetRequest().GetUpdateRef()
	wfkey := definition.WorkflowKey{
		NamespaceID: req.GetNamespaceId(),
		WorkflowID:  updateRef.GetWorkflowExecution().GetWorkflowId(),
		RunID:       updateRef.GetWorkflowExecution().GetRunId(),
	}
	apictx, err := findWorkflow(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		wfkey,
		workflow.LockPriorityLow,
	)
	if err != nil {
		return nil, err
	}
	defer func() { apictx.GetReleaseFn()(retErr) }()
	updateReg := apictx.GetContext().UpdateRegistry()
	if futureOutcome, ok := updateReg.Outcome(updateRef.GetUpdateId()); ok {
		apictx.GetReleaseFn()(nil)
		outcome, err := futureOutcome.Get(ctx)
		if err != nil {
			return nil, err
		}
		return &historyservice.PollWorkflowExecutionUpdateResponse{
			Response: &workflowservice.PollWorkflowExecutionUpdateResponse{
				Outcome: outcome,
			},
		}, nil
	}

	ms := apictx.GetMutableState()
	outcome, err := ms.GetUpdateOutcome(ctx, updateRef.GetUpdateId())
	if err != nil {
		return nil, err
	}
	return &historyservice.PollWorkflowExecutionUpdateResponse{
		Response: &workflowservice.PollWorkflowExecutionUpdateResponse{
			Outcome: outcome,
		},
	}, nil
}
