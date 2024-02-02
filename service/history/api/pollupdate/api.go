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
	enumspb "go.temporal.io/api/enums/v1"
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
	wfKey, upd, ok, err := func() (*definition.WorkflowKey, *update.Update, bool, error) {
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
			return nil, nil, false, err
		}
		release := workflowLease.GetReleaseFn()
		defer release(nil)
		upd, found := workflowLease.GetUpdateRegistry(ctx).Find(ctx, updateRef.UpdateId)
		wfKey := workflowLease.GetWorkflowKey()
		return &wfKey, upd, found, nil
	}()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, serviceerror.NewNotFound(fmt.Sprintf("update %q not found", updateRef.GetUpdateId()))
	}

	var status update.UpdateStatus

	switch waitStage {
	case enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED:
		status, err = upd.Status()
		if err != nil {
			return nil, err
		}
	case enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED, enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED:
		namespaceID := namespace.ID(req.GetNamespaceId())
		ns, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
		if err != nil {
			return nil, err
		}
		serverTimeout := shardContext.GetConfig().LongPollExpirationInterval(ns.Name().String())
		// If the long-poll times out due to serverTimeout then return a non-error empty response.
		status, err = upd.WaitLifecycleStage(ctx, waitStage, serverTimeout)
		if err != nil {
			return nil, err
		}
	default:
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("support for LifecycleStage=%v is not implemented", waitStage))
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
