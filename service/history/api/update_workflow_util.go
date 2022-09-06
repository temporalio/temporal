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

package api

import (
	"context"

	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func GetAndUpdateWorkflowWithNew(
	ctx context.Context,
	reqClock *clockspb.VectorClock,
	consistencyCheckFn MutableStateConsistencyPredicate,
	workflowKey definition.WorkflowKey,
	action UpdateWorkflowActionFunc,
	newWorkflowFn func() (workflow.Context, workflow.MutableState, error),
	shard shard.Context,
	workflowConsistencyChecker WorkflowConsistencyChecker,
) (retError error) {
	workflowContext, err := workflowConsistencyChecker.GetWorkflowContext(
		ctx,
		reqClock,
		consistencyCheckFn,
		workflowKey,
	)
	if err != nil {
		return err
	}
	defer func() { workflowContext.GetReleaseFn()(retError) }()

	return UpdateWorkflowWithNew(shard, ctx, workflowContext, action, newWorkflowFn)
}

func UpdateWorkflowWithNew(
	shard shard.Context,
	ctx context.Context,
	workflowContext WorkflowContext,
	action UpdateWorkflowActionFunc,
	newWorkflowFn func() (workflow.Context, workflow.MutableState, error),
) (retError error) {

	// conduct caller action
	postActions, err := action(workflowContext)
	if err != nil {
		return err
	}
	if postActions.Noop {
		return nil
	}

	mutableState := workflowContext.GetMutableState()
	if postActions.CreateWorkflowTask {
		// Create a transfer task to schedule a workflow task
		if !mutableState.HasPendingWorkflowTask() {
			if _, err := mutableState.AddWorkflowTaskScheduledEvent(
				false,
			); err != nil {
				return err
			}
		}
	}

	var updateErr error
	if newWorkflowFn != nil {
		newContext, newMutableState, err := newWorkflowFn()
		if err != nil {
			return err
		}
		lastWriteVersion, err := mutableState.GetLastWriteVersion()
		if err != nil {
			return err
		}
		if err = NewWorkflowVersionCheck(shard, lastWriteVersion, newMutableState); err != nil {
			return err
		}

		updateErr = workflowContext.GetContext().UpdateWorkflowExecutionWithNewAsActive(
			ctx,
			shard.GetTimeSource().Now(),
			newContext,
			newMutableState,
		)
	} else {
		updateErr = workflowContext.GetContext().UpdateWorkflowExecutionAsActive(
			ctx,
			shard.GetTimeSource().Now(),
		)
	}

	return updateErr
}
