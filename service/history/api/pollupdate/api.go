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

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	updatepb "go.temporal.io/api/update/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
)

func Invoke(
	ctx context.Context,
	req *historyservice.PollWorkflowExecutionUpdateRequest,
	shardContext shard.Context,
	ctxLookup api.WorkflowConsistencyChecker,
) (*historyservice.PollWorkflowExecutionUpdateResponse, error) {
	waitStage := req.GetRequest().GetWaitPolicy().GetLifecycleStage()
	updateRef := req.GetRequest().GetUpdateRef()
	wfexec := updateRef.GetWorkflowExecution()
	wfKey, upd, err := func() (*definition.WorkflowKey, *update.Update, error) {
		workflowLease, err := ctxLookup.GetWorkflowLease(
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
			return nil, nil, err
		}
		release := workflowLease.GetReleaseFn()
		defer release(nil)
		wfCtx := workflowLease.GetContext()
		upd := wfCtx.UpdateRegistry(ctx, nil).Find(ctx, updateRef.UpdateId)
		wfKey := wfCtx.GetWorkflowKey()
		return &wfKey, upd, nil
	}()
	if err != nil {
		return nil, err
	}
	if upd == nil {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("update %q not found", updateRef.GetUpdateId()))
	}

	namespaceID := namespace.ID(req.GetNamespaceId())
	ns, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}
	softTimeout := shardContext.GetConfig().LongPollExpirationInterval(ns.Name().String())
	// If the long-poll times out due to softTimeout
	// then return a non-error empty response with actual reached stage.
	status, err := upd.WaitLifecycleStage(ctx, waitStage, softTimeout)
	if err != nil {
		return nil, err
	}

	return &historyservice.PollWorkflowExecutionUpdateResponse{
		Response: &workflowservice.PollWorkflowExecutionUpdateResponse{
			Outcome: status.Outcome,
			Stage:   status.Stage,
			UpdateRef: &updatepb.UpdateRef{
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: wfKey.WorkflowID,
					RunId:      wfKey.RunID,
				},
				UpdateId: updateRef.UpdateId,
			},
		},
	}, nil
}
