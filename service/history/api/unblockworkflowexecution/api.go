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

package unblockworkflowexecution

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (retError error) {
	err := api.ValidateNamespaceUUID(namespace.ID(workflowKey.NamespaceID))
	if err != nil {
		return err
	}

	updateWorkflowFn := func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
		mutableState := workflowLease.GetMutableState()
		pendingBackoff := mutableState.IsWorkflowPendingOnWorkflowTaskBackoff()
		if !pendingBackoff {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("workflow `%s` isn't blocked on a backoff task", workflowKey))
		}

		return &api.UpdateWorkflowAction{
			Noop:               false,
			CreateWorkflowTask: true,
			AbortUpdates:       false,
		}, nil
	}
	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		workflowKey,
		updateWorkflowFn,
		nil,
		shard,
		workflowConsistencyChecker,
	)
	return err
}
