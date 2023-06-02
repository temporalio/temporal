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
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
)

func Invoke(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionUpdateRequest,
	ctxLookup api.WorkflowConsistencyChecker,
) (*historyservice.PollWorkflowExecutionUpdateResponse, error) {
	updateRef := req.GetRequest().GetUpdateRef()
	wfexec := updateRef.GetWorkflowExecution()
	upd, ok, err := func() (*update.Update, bool, error) {
		wfctx, err := ctxLookup.GetWorkflowContext(
			ctx,
			nil,
			api.BypassMutableStateConsistencyPredicate,
			definition.NewWorkflowKey(
				req.GetNamespaceId(),
				wfexec.GetWorkflowId(),
				wfexec.GetRunId(),
			),
			workflow.LockPriorityHigh,
		)
		if err != nil {
			return nil, false, err
		}
		release := wfctx.GetReleaseFn()
		defer release(nil)
		upd, found := wfctx.GetUpdateRegistry(ctx).Find(ctx, updateRef.UpdateId)
		return upd, found, nil
	}()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("update %q not found", updateRef.GetUpdateId()))
	}
	outcome, err := upd.WaitOutcome(ctx)
	if err != nil {
		return nil, err
	}
	return &historyservice.PollWorkflowExecutionUpdateResponse{
		Response: &workflowservice.PollWorkflowExecutionUpdateResponse{
			Outcome: outcome,
		},
	}, nil
}
